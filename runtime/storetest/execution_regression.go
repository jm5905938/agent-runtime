package storetest

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"

	"agent-runtime/core"
	"agent-runtime/domain"
)

func testExecutionActionValidation(t *testing.T, backend core.RecoveryStore) {
	ctx := context.Background()
	s := readySession(t, backend)
	other := createAgent(t, s, "other")
	existingClaim := newExecution(t, s, other.ID)
	existing := newAction(existingClaim)
	_, err := s.CommitExecution(ctx, core.ExecutionCommit{Token: existingClaim.Token, Actions: []domain.ActionRecord{existing}})
	must(t, err)
	existingEvent := domain.NewEvent("existing", nil)
	receive(t, s, other.ID, existingEvent)

	for i, test := range []struct {
		name   string
		change func(*domain.ActionRecord, *domain.ActionRecord)
	}{
		{"other_agent", func(a, _ *domain.ActionRecord) { a.AgentID = other.ID }},
		{"other_execution", func(a, _ *domain.ActionRecord) { a.Request.BindExecution(existingClaim.Token.ExecutionID) }},
		{"nil_execution", func(a, _ *domain.ActionRecord) { a.Request.ExecutionID = nil }},
		{"empty_execution", func(a, _ *domain.ActionRecord) { a.Request.BindExecution("") }},
		{"empty_action_id", func(a, _ *domain.ActionRecord) { a.Request.ID = "" }},
		{"empty_action_type", func(a, _ *domain.ActionRecord) { a.Request.Type = "" }},
		{"initial_status", func(a, _ *domain.ActionRecord) { a.Status = domain.ActionStatusRunning }},
		{"initial_attempt_count", func(a, _ *domain.ActionRecord) { a.AttemptCount = 1 }},
		{"initial_result", func(a, _ *domain.ActionRecord) { a.Result = &domain.ActionResult{} }},
		{"initial_error", func(a, _ *domain.ActionRecord) { a.LastError = &domain.Failure{} }},
		{"empty_handler_version", func(a, _ *domain.ActionRecord) { a.HandlerVersion = " \t" }},
		{"invalid_recovery_policy", func(a, _ *domain.ActionRecord) { a.RecoveryPolicy = "invalid" }},
		{"empty_idempotency_key", func(a, _ *domain.ActionRecord) { a.IdempotencyKey = " \t" }},
		{"zero_attempt_limit", func(a, _ *domain.ActionRecord) { a.MaxAttempts = 0 }},
		{"empty_result_event_id", func(a, _ *domain.ActionRecord) { a.ResultEventID = "" }},
		{"invalid_handler_text", func(a, _ *domain.ActionRecord) { a.HandlerVersion = string([]byte{0xff}) }},
		{"existing_action_id", func(a, _ *domain.ActionRecord) { a.Request.ID = existing.Request.ID }},
		{"existing_result_event", func(a, _ *domain.ActionRecord) { a.ResultEventID = existingEvent.ID }},
		{"reserved_result_event", func(a, _ *domain.ActionRecord) { a.ResultEventID = existing.ResultEventID }},
		{"duplicate_action_id", func(a, first *domain.ActionRecord) { a.Request.ID = first.Request.ID }},
		{"duplicate_result_event_id", func(a, first *domain.ActionRecord) { a.ResultEventID = first.ResultEventID }},
	} {
		t.Run(test.name, func(t *testing.T) {
			agent := createAgent(t, s, domain.ID(fmt.Sprintf("agent-%d", i)))
			claim := newExecution(t, s, agent.ID)
			first, invalid := newAction(claim), newAction(claim)
			test.change(&invalid, &first)
			beforeExecution, err := s.LoadExecution(ctx, claim.Token.ExecutionID)
			must(t, err)
			beforeDelivery, err := s.LoadDelivery(ctx, claim.Token.Delivery)
			must(t, err)
			beforeActions, err := s.ListActions(ctx)
			must(t, err)
			func() {
				defer func() {
					if value := recover(); value != nil {
						t.Errorf("非法action触发panic: %v", value)
					}
				}()
				if _, err := s.CommitExecution(ctx, core.ExecutionCommit{
					Token: claim.Token, StateUpdate: map[string]any{"changed": true}, Actions: []domain.ActionRecord{first, invalid},
				}); err == nil {
					t.Error("非法action仍提交成功")
				}
			}()
			savedAgent, err := s.LoadAgent(ctx, agent.ID)
			must(t, err)
			sameJSON(t, savedAgent, agent)
			savedExecution, err := s.LoadExecution(ctx, claim.Token.ExecutionID)
			must(t, err)
			sameJSON(t, savedExecution, beforeExecution)
			savedDelivery, err := s.LoadDelivery(ctx, claim.Token.Delivery)
			must(t, err)
			sameJSON(t, savedDelivery, beforeDelivery)
			savedActions, err := s.ListActions(ctx)
			must(t, err)
			sameJSON(t, savedActions, beforeActions)
			if _, err := s.LoadAction(ctx, first.Request.ID); !errors.Is(err, core.ErrStoreNotFound) {
				t.Fatalf("非法提交留下首个action: %v", err)
			}
			//同一token仍能提交，拒绝未消耗执行进度
			_, err = s.CommitExecution(ctx, core.ExecutionCommit{Token: claim.Token, Actions: []domain.ActionRecord{first}})
			must(t, err)
		})
	}
}

func testExecutionStateVersionExhaustion(t *testing.T, backend core.RecoveryStore) {
	ctx := context.Background()
	s := readySession(t, backend)
	agent := domain.AgentInstance{
		ID: "exhausted", Definition: domain.DefinitionRef{ID: "storetest", Version: "1"},
		Status: domain.AgentStatusActive, StateVersion: math.MaxUint64, State: map[string]any{"keep": "保留"},
	}
	must(t, s.CreateAgent(ctx, agent))
	claim := newExecution(t, s, agent.ID)
	beforeExecution, err := s.LoadExecution(ctx, claim.Token.ExecutionID)
	must(t, err)
	beforeDelivery, err := s.LoadDelivery(ctx, claim.Token.Delivery)
	must(t, err)
	action := newAction(claim)
	if _, err := s.CommitExecution(ctx, core.ExecutionCommit{
		Token: claim.Token, StateUpdate: map[string]any{"keep": "已修改"}, Actions: []domain.ActionRecord{action},
	}); err == nil {
		t.Error("状态版本耗尽仍提交成功")
	}
	savedAgent, err := s.LoadAgent(ctx, agent.ID)
	must(t, err)
	sameJSON(t, savedAgent, agent)
	savedExecution, err := s.LoadExecution(ctx, claim.Token.ExecutionID)
	must(t, err)
	sameJSON(t, savedExecution, beforeExecution)
	savedDelivery, err := s.LoadDelivery(ctx, claim.Token.Delivery)
	must(t, err)
	sameJSON(t, savedDelivery, beforeDelivery)
	if _, err := s.LoadAction(ctx, action.Request.ID); !errors.Is(err, core.ErrStoreNotFound) {
		t.Fatalf("版本耗尽提交留下action: %v", err)
	}
}

func testExecutionAttemptOrder(t *testing.T, backend core.RecoveryStore) {
	ctx := context.Background()
	s := readySession(t, backend)
	agent := createAgent(t, s, "agent")
	claim := newExecution(t, s, agent.ID)
	for number := uint64(1); number <= 12; number++ {
		if claim.Attempt.Number != number {
			t.Fatalf("claim attempt序号不正确: 实际%d预期%d", claim.Attempt.Number, number)
		}
		must(t, s.FailExecution(ctx, core.ExecutionFailure{Token: claim.Token, Failure: domain.Failure{
			Kind: domain.ErrorKindBusiness, Message: "重试测试",
		}}))
		if number != 12 {
			must(t, s.RequeueDelivery(ctx, claim.Token.Delivery))
			claim = claimExecution(t, s, claim.Token.Delivery)
		}
	}
	stored, err := s.LoadExecution(ctx, claim.Token.ExecutionID)
	must(t, err)
	if len(stored.Attempts) != 12 {
		t.Fatalf("attempt历史数量不正确: %d", len(stored.Attempts))
	}
	for i, attempt := range stored.Attempts {
		if attempt.Number != uint64(i+1) || attempt.Status != domain.AttemptStatusFailed {
			t.Fatalf("attempt历史未按数值排序: %+v", stored.Attempts)
		}
	}
}

func testExecutionFailureTokens(t *testing.T, backend core.RecoveryStore) {
	ctx := context.Background()
	s := readySession(t, backend)
	agent := createAgent(t, s, "agent")
	claim := newExecution(t, s, agent.ID)
	beforeExecution, err := s.LoadExecution(ctx, claim.Token.ExecutionID)
	must(t, err)
	beforeDelivery, err := s.LoadDelivery(ctx, claim.Token.Delivery)
	must(t, err)
	for _, change := range []func(*core.ExecutionToken){
		func(token *core.ExecutionToken) { token.Delivery.AgentID = "missing-agent" },
		func(token *core.ExecutionToken) { token.Delivery.EventID = "missing-event" },
		func(token *core.ExecutionToken) { token.ExecutionID = "wrong-execution" },
		func(token *core.ExecutionToken) { token.AttemptID = "wrong-attempt" },
		func(token *core.ExecutionToken) { token.ExpectedStateVersion++ },
	} {
		token := claim.Token
		change(&token)
		if err := s.FailExecution(ctx, core.ExecutionFailure{Token: token, Failure: domain.Failure{
			Kind: domain.ErrorKindBusiness, Message: "过期token",
		}}); !errors.Is(err, core.ErrStoreStaleClaim) {
			t.Fatalf("未完整校验failure token: %+v, %v", token, err)
		}
		savedAgent, err := s.LoadAgent(ctx, agent.ID)
		must(t, err)
		sameJSON(t, savedAgent, agent)
		savedExecution, err := s.LoadExecution(ctx, claim.Token.ExecutionID)
		must(t, err)
		sameJSON(t, savedExecution, beforeExecution)
		savedDelivery, err := s.LoadDelivery(ctx, claim.Token.Delivery)
		must(t, err)
		sameJSON(t, savedDelivery, beforeDelivery)
	}
	must(t, s.FailExecution(ctx, core.ExecutionFailure{Token: claim.Token, Failure: domain.Failure{
		Kind: domain.ErrorKindBusiness, Message: "有效token",
	}}))
}
