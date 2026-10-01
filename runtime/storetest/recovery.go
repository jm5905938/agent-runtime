package storetest

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"

	"agent-runtime/core"
	"agent-runtime/domain"
)

func testSessionGates(t *testing.T, backend core.RecoveryStore) {
	ctx := context.Background()
	s := openSession(t, backend)
	if other, err := backend.OpenSession(ctx); !errors.Is(err, core.ErrStoreOwned) {
		if other != nil {
			must(t, other.Close(ctx))
		}
		t.Fatalf("已有session时仍可取得所有权: %v", err)
	}
	writes, reads := sessionOperations(ctx, s)
	for _, op := range writes {
		if err := op.call(); !errors.Is(err, core.ErrRecoveryRequired) {
			t.Errorf("恢复前%s未被门禁拒绝: %v", op.name, err)
		}
	}
	for _, op := range reads {
		if err := op.call(); err != nil && !errors.Is(err, core.ErrStoreNotFound) {
			t.Errorf("恢复前%s不能读取: %v", op.name, err)
		}
	}
	_, err := s.Recover(ctx)
	must(t, err)
	createAgent(t, s, "saved")
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := s.Close(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("取消关闭未返回context错误: %v", err)
	}
	if other, err := backend.OpenSession(ctx); !errors.Is(err, core.ErrStoreOwned) {
		if other != nil {
			must(t, other.Close(ctx))
		}
		t.Fatalf("取消关闭释放了所有权: %v", err)
	}
	must(t, s.Close(ctx))
	must(t, s.Close(canceled))
	for _, op := range append(writes, reads...) {
		if err := op.call(); !errors.Is(err, core.ErrStoreClosed) {
			t.Errorf("关闭后%s未被拒绝: %v", op.name, err)
		}
	}
	if _, err := s.Recover(ctx); !errors.Is(err, core.ErrStoreClosed) {
		t.Fatalf("关闭后仍能恢复: %v", err)
	}
	reopened := readySession(t, backend)
	if _, err := reopened.LoadAgent(ctx, "saved"); err != nil {
		t.Fatalf("重开后丢失agent: %v", err)
	}
	if _, err := s.ListAgents(canceled); !errors.Is(err, core.ErrStoreClosed) {
		t.Fatalf("旧session未优先拒绝访问: %v", err)
	}
}

type operation struct {
	name string
	call func() error
}

func sessionOperations(ctx context.Context, s core.RecoverySession) ([]operation, []operation) {
	writes := []operation{
		{"create", func() error { return s.CreateAgent(ctx, domain.AgentInstance{}) }},
		{"receive", func() error { _, err := s.ReceiveEvent(ctx, "missing", domain.Event{}); return err }},
		{"claim_execution", func() error { _, err := s.ClaimExecution(ctx, domain.DeliveryKey{}); return err }},
		{"commit", func() error { _, err := s.CommitExecution(ctx, core.ExecutionCommit{}); return err }},
		{"fail", func() error { return s.FailExecution(ctx, core.ExecutionFailure{}) }},
		{"requeue", func() error { return s.RequeueDelivery(ctx, domain.DeliveryKey{}) }},
		{"claim_action", func() error { _, err := s.ClaimAction(ctx, "missing"); return err }},
		{"complete", func() error { _, err := s.CompleteAction(ctx, core.ActionCompletion{}); return err }},
		{"unknown", func() error { return s.RecordActionUnknown(ctx, core.ActionToken{}, domain.Failure{}) }},
	}
	reads := []operation{
		{"load_agent", func() error { _, err := s.LoadAgent(ctx, "missing"); return err }},
		{"list_agents", func() error { _, err := s.ListAgents(ctx); return err }},
		{"event", func() error { _, err := s.LoadEvent(ctx, "missing"); return err }},
		{"delivery", func() error {
			_, err := s.LoadDelivery(ctx, domain.DeliveryKey{AgentID: "missing", EventID: "missing"})
			return err
		}},
		{"deliveries", func() error { _, err := s.ListDeliveries(ctx); return err }},
		{"execution", func() error { _, err := s.LoadExecution(ctx, "missing"); return err }},
		{"action", func() error { _, err := s.LoadAction(ctx, "missing"); return err }},
		{"actions", func() error { _, err := s.ListActions(ctx); return err }},
	}
	return writes, reads
}

func testSessionRecovery(t *testing.T, backend core.RecoveryStore) {
	ctx := context.Background()
	s := readySession(t, backend)
	agent := createAgent(t, s, "agent")
	source := newExecution(t, s, agent.ID)
	safe, manual, exhausted, pending, completed := newAction(source), newAction(source), newAction(source), newAction(source), newAction(source)
	manual.RecoveryPolicy = domain.RecoveryPolicyManual
	exhausted.MaxAttempts = 1
	_, err := s.CommitExecution(ctx, core.ExecutionCommit{Token: source.Token, Actions: []domain.ActionRecord{safe, manual, exhausted, pending, completed}})
	must(t, err)
	safeClaim := claimAction(t, s, safe.Request.ID)
	claimAction(t, s, manual.Request.ID)
	claimAction(t, s, exhausted.Request.ID)
	completeClaim := claimAction(t, s, completed.Request.ID)
	_, err = s.CompleteAction(ctx, completion(completeClaim, "hello"))
	must(t, err)
	stillPending := receive(t, s, agent.ID, domain.NewEvent("pending", nil))
	running := newExecution(t, s, agent.ID)
	beforeAgent, err := s.LoadAgent(ctx, agent.ID)
	must(t, err)
	beforeCompleted, err := s.LoadAction(ctx, completed.Request.ID)
	must(t, err)
	must(t, s.Close(ctx))
	reopened := openSession(t, backend)
	report, err := reopened.Recover(ctx)
	must(t, err)
	want := core.RecoveryReport{
		RequeuedDeliveries: []domain.DeliveryKey{running.Token.Delivery},
		UnknownActions:     []domain.ID{safe.Request.ID, manual.Request.ID, exhausted.Request.ID},
		RetryableActions:   []domain.ID{safe.Request.ID},
	}
	sameJSON(t, report, want)
	afterAgent, err := reopened.LoadAgent(ctx, agent.ID)
	must(t, err)
	sameJSON(t, afterAgent, beforeAgent)
	afterCompleted, err := reopened.LoadAction(ctx, completed.Request.ID)
	must(t, err)
	sameJSON(t, afterCompleted, beforeCompleted)
	delivery, err := reopened.LoadDelivery(ctx, stillPending.Delivery.Key)
	must(t, err)
	if *delivery != stillPending.Delivery {
		t.Fatalf("恢复改变原有pending: %+v", delivery)
	}
	stored, err := reopened.LoadExecution(ctx, running.Token.ExecutionID)
	must(t, err)
	if stored.Execution.Status != domain.ExecutionStatusPending || stored.Execution.AttemptCount != 1 ||
		stored.Execution.StartedAt != nil || stored.Execution.FinishedAt != nil || stored.Execution.Result != nil || stored.Execution.Error != "" ||
		len(stored.Attempts) != 1 || stored.Attempts[0].Status != domain.AttemptStatusInterrupted ||
		stored.Attempts[0].FinishedAt == nil || stored.Attempts[0].Error == nil || stored.Attempts[0].Error.Kind != domain.ErrorKindInterrupted {
		t.Fatalf("execution恢复记录不完整: %+v", stored)
	}
	for _, id := range want.UnknownActions {
		action, err := reopened.LoadAction(ctx, id)
		must(t, err)
		if action.Action.Status != domain.ActionStatusUnknown || action.Action.Result != nil || action.Action.LastError == nil ||
			action.Action.LastError.Kind != domain.ErrorKindInterrupted || len(action.Attempts) != 1 ||
			action.Attempts[0].Status != domain.ActionStatusUnknown || action.Attempts[0].FinishedAt == nil {
			t.Fatalf("action恢复记录不完整: %+v", action)
		}
		if _, err := reopened.LoadEvent(ctx, action.Action.ResultEventID); !errors.Is(err, core.ErrStoreNotFound) {
			t.Fatalf("恢复unknown伪造结果event: %v", err)
		}
	}
	pendingAction, err := reopened.LoadAction(ctx, pending.Request.ID)
	must(t, err)
	sameJSON(t, pendingAction.Action, pending)
	if len(pendingAction.Attempts) != 0 {
		t.Fatal("恢复为pending action新增attempt")
	}
	for _, id := range []domain.ID{manual.Request.ID, exhausted.Request.ID} {
		if _, err := reopened.ClaimAction(ctx, id); !errors.Is(err, core.ErrStoreConflict) {
			t.Fatalf("不可重试unknown被领取: %v", err)
		}
	}
	for _, reclaim := range []bool{false, true} {
		if reclaim {
			next := claimExecution(t, reopened, running.Token.Delivery)
			if next.Token.ExecutionID != running.Token.ExecutionID || next.Token.AttemptID == running.Token.AttemptID || next.Attempt.Number != 2 {
				t.Fatalf("恢复后execution身份不稳定: %+v", next)
			}
			nextAction := claimAction(t, reopened, safe.Request.ID)
			if nextAction.Token.AttemptNumber != 2 || nextAction.Record.IdempotencyKey != safe.IdempotencyKey ||
				nextAction.Record.ResultEventID != safe.ResultEventID || nextAction.Record.HandlerVersion != safe.HandlerVersion ||
				nextAction.Record.RecoveryPolicy != safe.RecoveryPolicy || nextAction.Record.MaxAttempts != safe.MaxAttempts {
				t.Fatalf("恢复后action固定元数据改变: %+v", nextAction)
			}
		}
		if _, err := reopened.CommitExecution(ctx, core.ExecutionCommit{Token: running.Token}); !errors.Is(err, core.ErrStoreStaleClaim) {
			t.Fatalf("恢复后旧execution token提交成功: %v", err)
		}
		if err := reopened.FailExecution(ctx, core.ExecutionFailure{Token: running.Token}); !errors.Is(err, core.ErrStoreStaleClaim) {
			t.Fatalf("恢复后旧execution token失败记账成功: %v", err)
		}
		if _, err := reopened.CompleteAction(ctx, completion(safeClaim, "hello")); !errors.Is(err, core.ErrStoreStaleClaim) {
			t.Fatalf("恢复后旧action token完成成功: %v", err)
		}
		if err := reopened.RecordActionUnknown(ctx, safeClaim.Token, domain.Failure{}); !errors.Is(err, core.ErrStoreStaleClaim) {
			t.Fatalf("恢复后旧action token记账成功: %v", err)
		}
	}
	report.RequeuedDeliveries[0].EventID = "changed"
	report.UnknownActions[0] = "changed"
	report.RetryableActions[0] = "changed"
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cached, err := reopened.Recover(ctx)
			if err != nil || !reflect.DeepEqual(cached, want) {
				t.Errorf("重复恢复报告不稳定: %+v, %v", cached, err)
				return
			}
			cached.UnknownActions[0] = "changed again"
		}()
	}
	wg.Wait()
	stored, err = reopened.LoadExecution(ctx, running.Token.ExecutionID)
	must(t, err)
	currentAction, err := reopened.LoadAction(ctx, safe.Request.ID)
	must(t, err)
	if stored.Execution.Status != domain.ExecutionStatusRunning || len(stored.Attempts) != 2 ||
		currentAction.Action.Status != domain.ActionStatusRunning || len(currentAction.Attempts) != 2 {
		t.Fatal("重复恢复回收了本会话正在运行的记录")
	}
}
