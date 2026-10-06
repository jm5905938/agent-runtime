package storetest

import (
	"context"
	"errors"
	"testing"
	"time"

	"agent-runtime/core"
	"agent-runtime/domain"
)

func testActionCompletion(t *testing.T, backend core.RecoveryStore) {
	ctx := context.Background()
	s := readySession(t, backend)
	agent := createAgent(t, s, "agent")
	execution := newExecution(t, s, agent.ID)
	action := newAction(execution)
	_, err := s.CommitExecution(ctx, core.ExecutionCommit{Token: execution.Token, Actions: []domain.ActionRecord{action}})
	must(t, err)
	claim := claimAction(t, s, action.Request.ID)
	input := completion(claim, "hello")
	if _, err := s.ReceiveEvent(ctx, agent.ID, input.Event); !errors.Is(err, core.ErrStoreConflict) {
		t.Fatalf("普通入口占用了action结果event: %v", err)
	}
	bad := completion(claim, "hello")
	bad.Event.Payload["status"] = "failed"
	if _, err := s.CompleteAction(ctx, bad); !errors.Is(err, core.ErrStoreConflict) {
		t.Fatalf("不匹配结果event被接受: %v", err)
	}
	before, err := s.LoadAction(ctx, action.Request.ID)
	must(t, err)
	if before.Action.Status != domain.ActionStatusRunning || before.Action.Result != nil || len(before.Attempts) != 1 || before.Attempts[0].FinishedAt != nil {
		t.Fatalf("无效完成留下部分记录: %+v", before)
	}
	if _, err := s.LoadEvent(ctx, action.ResultEventID); !errors.Is(err, core.ErrStoreNotFound) {
		t.Fatalf("无效完成留下event: %v", err)
	}
	result, err := s.CompleteAction(ctx, input)
	must(t, err)
	sameJSON(t, result, input.Result)
	key := domain.DeliveryKey{AgentID: agent.ID, EventID: action.ResultEventID}
	firstDelivery, err := s.LoadDelivery(ctx, key)
	must(t, err)
	duplicate := input
	duplicate.Event.CreatedAt = input.Event.CreatedAt.Add(time.Hour)
	result, err = s.CompleteAction(ctx, duplicate)
	must(t, err)
	sameJSON(t, result, input.Result)
	if _, err := s.CompleteAction(ctx, completion(claim, "different")); !errors.Is(err, core.ErrStoreConflict) {
		t.Fatalf("不同最终结果未冲突: %v", err)
	}
	saved, err := s.LoadAction(ctx, action.Request.ID)
	must(t, err)
	event, err := s.LoadEvent(ctx, action.ResultEventID)
	must(t, err)
	delivery, err := s.LoadDelivery(ctx, key)
	must(t, err)
	deliveries, err := s.ListDeliveries(ctx)
	must(t, err)
	if saved.Action.Status != domain.ActionStatusSucceeded || saved.Action.Result == nil || len(saved.Attempts) != 1 ||
		saved.Attempts[0].Status != domain.ActionStatusSucceeded || saved.Attempts[0].FinishedAt == nil ||
		!event.CreatedAt.Equal(input.Event.CreatedAt) || *delivery != *firstDelivery || delivery.Status != domain.DeliveryStatusPending || len(deliveries) != 2 {
		t.Fatalf("重复完成改变记录: action=%+v event=%+v delivery=%+v", saved, event, delivery)
	}
	saved.Action.Result.Output["text"] = "快照已修改"
	saved.Action.Request.Payload["text"] = "请求已修改"
	*saved.Action.Request.ExecutionID = "来源已修改"
	*saved.Attempts[0].FinishedAt = time.Time{}
	result.Output["text"] = "返回值已修改"
	event.Payload["result"].(map[string]any)["text"] = "event已修改"
	again, err := s.LoadAction(ctx, action.Request.ID)
	must(t, err)
	if again.Action.Result.Output["text"] != "hello" || again.Action.Request.Payload["text"] != "hello" ||
		*again.Action.Request.ExecutionID != execution.Token.ExecutionID || again.Attempts[0].FinishedAt.IsZero() {
		t.Fatalf("action快照影响保存记录: %+v", again)
	}
	event, err = s.LoadEvent(ctx, action.ResultEventID)
	must(t, err)
	sameJSON(t, event, input.Event)
	if _, err := s.ClaimAction(ctx, action.Request.ID); !errors.Is(err, core.ErrStoreConflict) {
		t.Fatalf("已完成action被重新领取: %v", err)
	}
}

func testActionUnknown(t *testing.T, backend core.RecoveryStore, policy domain.RecoveryPolicy) {
	ctx := context.Background()
	s := readySession(t, backend)
	agent := createAgent(t, s, "agent")
	execution := newExecution(t, s, agent.ID)
	action := newAction(execution)
	action.RecoveryPolicy = policy
	_, err := s.CommitExecution(ctx, core.ExecutionCommit{Token: execution.Token, Actions: []domain.ActionRecord{action}})
	must(t, err)
	first := claimAction(t, s, action.Request.ID)
	failure := domain.Failure{Kind: domain.ErrorKindUnknown, Message: "未收到最终结果"}
	must(t, s.RecordActionUnknown(ctx, first.Token, failure))
	saved, err := s.LoadAction(ctx, action.Request.ID)
	must(t, err)
	if saved.Action.Status != domain.ActionStatusUnknown || saved.Action.Result != nil || saved.Action.LastError == nil ||
		*saved.Action.LastError != failure || len(saved.Attempts) != 1 || saved.Attempts[0].Status != domain.ActionStatusUnknown || saved.Attempts[0].Error == nil || *saved.Attempts[0].Error != failure {
		t.Fatalf("unknown丢失原始原因或伪造最终结果: %+v", saved)
	}
	if _, err := s.LoadEvent(ctx, action.ResultEventID); !errors.Is(err, core.ErrStoreNotFound) {
		t.Fatalf("unknown生成了结果event: %v", err)
	}
	second, err := s.ClaimAction(ctx, action.Request.ID)
	if policy == domain.RecoveryPolicyManual {
		if !errors.Is(err, core.ErrStoreConflict) {
			t.Fatalf("manual unknown被重试: %v", err)
		}
		return
	}
	must(t, err)
	if second.Token.AttemptNumber != 2 || second.Record.Request.ID != action.Request.ID ||
		second.Record.ResultEventID != action.ResultEventID ||
		second.Record.HandlerVersion != action.HandlerVersion || second.Record.RecoveryPolicy != policy || second.Record.MaxAttempts != action.MaxAttempts {
		t.Fatalf("safe_retry改变固定元数据: %+v", second)
	}
	if _, err := s.CompleteAction(ctx, completion(first, "hello")); !errors.Is(err, core.ErrStoreStaleClaim) {
		t.Fatalf("旧action token完成成功: %v", err)
	}
	if err := s.RecordActionUnknown(ctx, first.Token, failure); !errors.Is(err, core.ErrStoreStaleClaim) {
		t.Fatalf("旧action token覆盖unknown: %v", err)
	}
	must(t, s.RecordActionUnknown(ctx, second.Token, failure))
	if _, err := s.ClaimAction(ctx, action.Request.ID); !errors.Is(err, core.ErrStoreConflict) {
		t.Fatalf("action超出保存的尝试上限: %v", err)
	}
	saved, err = s.LoadAction(ctx, action.Request.ID)
	must(t, err)
	if saved.Action.Status != domain.ActionStatusUnknown || saved.Action.AttemptCount != 2 || len(saved.Attempts) != 2 ||
		saved.Attempts[0].Number != 1 || saved.Attempts[1].Number != 2 || saved.Attempts[0].ID == saved.Attempts[1].ID {
		t.Fatalf("unknown重试历史不完整: %+v", saved)
	}
}
