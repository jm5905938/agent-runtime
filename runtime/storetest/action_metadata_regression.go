package storetest

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"agent-runtime/core"
	"agent-runtime/domain"
)

func testActionMetadataValidation(t *testing.T, backend core.RecoveryStore) {
	ctx := context.Background()
	s := readySession(t, backend)
	for i, test := range []struct {
		name string
		edit func(*domain.ActionRecord)
	}{
		{"blank_action_id", func(a *domain.ActionRecord) { a.Request.ID = " \t\u3000\n" }},
		{"empty_action_type", func(a *domain.ActionRecord) { a.Request.Type = "" }},
		{"ascii_blank_action_type", func(a *domain.ActionRecord) { a.Request.Type = " \t\n" }},
		{"unicode_blank_action_type", func(a *domain.ActionRecord) { a.Request.Type = "\u3000\u00a0" }},
		{"empty_result_event_id", func(a *domain.ActionRecord) { a.ResultEventID = "" }},
		{"ascii_blank_result_event_id", func(a *domain.ActionRecord) { a.ResultEventID = " \t\n" }},
		{"unicode_blank_result_event_id", func(a *domain.ActionRecord) { a.ResultEventID = "\u3000\u00a0" }},
		{"blank_agent_id", func(a *domain.ActionRecord) { a.AgentID = " \t\u3000\n" }},
		{"blank_execution_id", func(a *domain.ActionRecord) { a.Request.BindExecution(" \t\u3000\n") }},
		{"blank_handler_version", func(a *domain.ActionRecord) { a.HandlerVersion = " \t\u3000\n" }},
		{"blank_idempotency_key", func(a *domain.ActionRecord) { a.IdempotencyKey = " \t\u3000\n" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			agent := createAgent(t, s, domain.ID(fmt.Sprintf("agent-%d", i)))
			claim := newExecution(t, s, agent.ID)
			first, invalid := newAction(claim), newAction(claim)
			test.edit(&invalid)
			executionBefore, err := s.LoadExecution(ctx, claim.Token.ExecutionID)
			must(t, err)
			deliveryBefore, err := s.LoadDelivery(ctx, claim.Token.Delivery)
			must(t, err)
			actionsBefore, err := s.ListActions(ctx)
			must(t, err)
			deliveriesBefore, err := s.ListDeliveries(ctx)
			must(t, err)
			if _, err := s.CommitExecution(ctx, core.ExecutionCommit{
				Token: claim.Token, StateUpdate: map[string]any{"changed": true}, Actions: []domain.ActionRecord{first, invalid},
			}); !errors.Is(err, core.ErrStoreConflict) {
				t.Errorf("纯空白action元数据未拒绝: %v", err)
			}
			agentAfter, err := s.LoadAgent(ctx, agent.ID)
			must(t, err)
			sameJSON(t, agentAfter, agent)
			executionAfter, err := s.LoadExecution(ctx, claim.Token.ExecutionID)
			must(t, err)
			sameJSON(t, executionAfter, executionBefore)
			deliveryAfter, err := s.LoadDelivery(ctx, claim.Token.Delivery)
			must(t, err)
			sameJSON(t, deliveryAfter, deliveryBefore)
			actionsAfter, err := s.ListActions(ctx)
			must(t, err)
			sameJSON(t, actionsAfter, actionsBefore)
			deliveriesAfter, err := s.ListDeliveries(ctx)
			must(t, err)
			sameJSON(t, deliveriesAfter, deliveriesBefore)
			for _, action := range []domain.ActionRecord{first, invalid} {
				if _, err := s.LoadAction(ctx, action.Request.ID); !errors.Is(err, core.ErrStoreNotFound) {
					t.Fatalf("非法提交留下action: %v", err)
				}
				if _, err := s.LoadEvent(ctx, action.ResultEventID); !errors.Is(err, core.ErrStoreNotFound) {
					t.Fatalf("非法提交留下结果event: %v", err)
				}
			}
			_, err = s.CommitExecution(ctx, core.ExecutionCommit{Token: claim.Token, Actions: []domain.ActionRecord{first}})
			must(t, err)
			_, err = s.CompleteAction(ctx, completion(claimAction(t, s, first.Request.ID), "有效token完成"))
			must(t, err)
		})
	}
	beforeReopen := snapshotFailureContract(t, s)
	must(t, s.Close(ctx))
	reopened := openSession(t, backend)
	report, err := reopened.Recover(ctx)
	must(t, err)
	if len(report.UnknownActions) != 0 || len(report.RetryableActions) != 0 || len(report.RequeuedDeliveries) != 0 {
		t.Fatalf("拒绝非法提交后重试完成的记录产生多余恢复报告: %+v", report)
	}
	sameJSON(t, snapshotFailureContract(t, reopened), beforeReopen)
}

func testActionMetadataRoundTrip(t *testing.T, backend core.RecoveryStore) {
	ctx := context.Background()
	s := readySession(t, backend)
	agent := createAgent(t, s, " agent with spaces ")
	execution := newExecution(t, s, agent.ID)
	action := newAction(execution)
	action.Request.ID = " action with spaces "
	action.Request.Type = " echo with spaces "
	action.HandlerVersion = " version with spaces "
	action.IdempotencyKey = " key with spaces "
	action.ResultEventID = " result event with spaces "
	_, err := s.CommitExecution(ctx, core.ExecutionCommit{Token: execution.Token, Actions: []domain.ActionRecord{action}})
	must(t, err)
	must(t, s.Close(ctx))

	reopened := openSession(t, backend)
	report, err := reopened.Recover(ctx)
	must(t, err)
	if len(report.UnknownActions) != 0 || len(report.RetryableActions) != 0 || len(report.RequeuedDeliveries) != 0 {
		t.Fatalf("pending action重开产生多余恢复报告: %+v", report)
	}
	saved, err := reopened.LoadAction(ctx, action.Request.ID)
	must(t, err)
	sameJSON(t, saved.Action, action)
	claim := claimAction(t, reopened, action.Request.ID)
	input := completion(claim, "保留有效字段原文")
	_, err = reopened.CompleteAction(ctx, input)
	must(t, err)
	completed, err := reopened.LoadAction(ctx, action.Request.ID)
	must(t, err)
	sameJSON(t, completed.Action.Request, action.Request)
	if completed.Action.AgentID != action.AgentID || completed.Action.ResultEventID != action.ResultEventID ||
		completed.Action.HandlerVersion != action.HandlerVersion || completed.Action.IdempotencyKey != action.IdempotencyKey {
		t.Fatalf("完成修改了有效action元数据: %+v", completed.Action)
	}
	beforeReopen := snapshotFailureContract(t, reopened)
	must(t, reopened.Close(ctx))

	finished := openSession(t, backend)
	report, err = finished.Recover(ctx)
	must(t, err)
	if len(report.UnknownActions) != 0 || len(report.RetryableActions) != 0 || len(report.RequeuedDeliveries) != 0 {
		t.Fatalf("已完成action重开产生多余恢复报告: %+v", report)
	}
	saved, err = finished.LoadAction(ctx, action.Request.ID)
	must(t, err)
	sameJSON(t, saved, completed)
	event, err := finished.LoadEvent(ctx, action.ResultEventID)
	must(t, err)
	sameJSON(t, event, input.Event)
	delivery, err := finished.LoadDelivery(ctx, domain.DeliveryKey{AgentID: agent.ID, EventID: action.ResultEventID})
	must(t, err)
	if delivery.Status != domain.DeliveryStatusPending {
		t.Fatalf("结果delivery状态变化: %+v", delivery)
	}
	sameJSON(t, snapshotFailureContract(t, finished), beforeReopen)
}
