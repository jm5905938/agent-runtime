package storetest

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"sync"
	"testing"
	"time"

	"agent-runtime/core"
	"agent-runtime/domain"
)

func testAgentSnapshots(t *testing.T, backend core.RecoveryStore) {
	ctx := context.Background()
	s := readySession(t, backend)
	agent := domain.AgentInstance{
		ID: "z-agent", Name: "数字往返", Definition: domain.DefinitionRef{ID: "example", Version: "7"},
		Status: domain.AgentStatusPaused, StateVersion: math.MaxUint64,
		State: map[string]any{
			"integer": json.Number("9007199254740993"), "decimal": json.Number("0.12345678901234567890123456789"),
			"nested": map[string]any{"items": []any{"原值"}},
		},
	}
	must(t, s.CreateAgent(ctx, agent))
	saved, err := s.LoadAgent(ctx, agent.ID)
	must(t, err)
	sameJSON(t, saved, agent)
	agent.State["nested"].(map[string]any)["items"].([]any)[0] = "输入已修改"
	saved.State["nested"].(map[string]any)["items"].([]any)[0] = "快照已修改"
	again, err := s.LoadAgent(ctx, agent.ID)
	must(t, err)
	if again.State["nested"].(map[string]any)["items"].([]any)[0] != "原值" {
		t.Fatal("agent持有外部可修改引用")
	}
	if err := s.CreateAgent(ctx, agent); !errors.Is(err, core.ErrStoreConflict) {
		t.Fatalf("重复agent未冲突: %v", err)
	}
	createAgent(t, s, "a-agent")
	listed, err := s.ListAgents(ctx)
	must(t, err)
	if len(listed) != 2 || listed[0].ID != "a-agent" || listed[1].ID != "z-agent" {
		t.Fatalf("agent顺序不正确: %+v", listed)
	}
	listed[1].State["nested"].(map[string]any)["items"].([]any)[0] = "列表已修改"
	must(t, s.Close(ctx))
	reopened := readySession(t, backend)
	got, err := reopened.LoadAgent(ctx, agent.ID)
	must(t, err)
	sameJSON(t, got, again)
}

func testEventReceipt(t *testing.T, backend core.RecoveryStore) {
	ctx := context.Background()
	s := readySession(t, backend)
	agent := createAgent(t, s, "first")
	other := createAgent(t, s, "second")
	event := domain.NewEvent("test", map[string]any{"count": json.Number("12"), "null": nil})
	event.ID = "z-event"
	first := receive(t, s, agent.ID, event)
	if first.Duplicate || first.Delivery.Status != domain.DeliveryStatusPending || first.Delivery.ExecutionID == "" {
		t.Fatalf("首次接收不正确: %+v", first)
	}
	if _, err := s.LoadExecution(ctx, first.Delivery.ExecutionID); !errors.Is(err, core.ErrStoreNotFound) {
		t.Fatalf("接收时已创建execution: %v", err)
	}
	duplicate := event
	duplicate.CreatedAt = event.CreatedAt.Add(time.Hour)
	duplicate.Payload = map[string]any{"null": []any(nil), "count": json.Number("1.2e1")}
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			got, err := s.ReceiveEvent(ctx, agent.ID, duplicate)
			if err != nil || !got.Duplicate || got.Delivery != first.Delivery {
				t.Errorf("并发重投改变记录: %+v, %v", got, err)
			}
		}()
	}
	wg.Wait()
	saved, err := s.LoadEvent(ctx, event.ID)
	must(t, err)
	sameJSON(t, saved, event)
	conflict := duplicate
	conflict.Payload = map[string]any{"count": json.Number("13"), "null": nil}
	if _, err := s.ReceiveEvent(ctx, other.ID, conflict); !errors.Is(err, core.ErrStoreConflict) {
		t.Fatalf("不同内容未冲突: %v", err)
	}
	if _, err := s.LoadDelivery(ctx, domain.DeliveryKey{AgentID: other.ID, EventID: event.ID}); !errors.Is(err, core.ErrStoreNotFound) {
		t.Fatalf("冲突后留下delivery: %v", err)
	}
	next := domain.NewEvent("test", nil)
	next.ID = "a-event"
	second := receive(t, s, agent.ID, next)
	third := receive(t, s, other.ID, duplicate)
	if third.Duplicate || third.Delivery.ExecutionID == first.Delivery.ExecutionID {
		t.Fatalf("跨agent投递未分配独立execution: %+v", third)
	}
	receive(t, s, agent.ID, duplicate)
	deliveries, err := s.ListDeliveries(ctx)
	must(t, err)
	sameJSON(t, deliveries, []domain.Delivery{first.Delivery, second.Delivery, third.Delivery})
	claimExecution(t, s, first.Delivery.Key)
	filtered, err := s.ListDeliveries(ctx, domain.DeliveryStatusRunning, domain.DeliveryStatusPending)
	must(t, err)
	if len(filtered) != 3 || filtered[0].Key != first.Delivery.Key || filtered[1].Key != second.Delivery.Key || filtered[2].Key != third.Delivery.Key {
		t.Fatalf("状态筛选改变接收顺序: %+v", filtered)
	}
	filtered, err = s.ListDeliveries(ctx, domain.DeliveryStatusPending)
	must(t, err)
	sameJSON(t, filtered, []domain.Delivery{second.Delivery, third.Delivery})
	orphan := domain.NewEvent("missing", nil)
	if _, err := s.ReceiveEvent(ctx, "missing", orphan); !errors.Is(err, core.ErrStoreNotFound) {
		t.Fatalf("缺失agent仍接收成功: %v", err)
	}
	if _, err := s.LoadEvent(ctx, orphan.ID); !errors.Is(err, core.ErrStoreNotFound) {
		t.Fatalf("缺失agent仍保存event: %v", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := s.ReceiveEvent(canceled, agent.ID, orphan); !errors.Is(err, context.Canceled) {
		t.Fatalf("已取消接收未返回context错误: %v", err)
	}
	if _, err := s.LoadEvent(ctx, orphan.ID); !errors.Is(err, core.ErrStoreNotFound) {
		t.Fatalf("已取消接收留下event: %v", err)
	}
	must(t, s.Close(ctx))
	reopened := readySession(t, backend)
	deliveries, err = reopened.ListDeliveries(ctx)
	must(t, err)
	sameJSON(t, deliveries, []domain.Delivery{first.Delivery, second.Delivery, third.Delivery})
}

func testExecutionClaims(t *testing.T, backend core.RecoveryStore) {
	ctx := context.Background()
	s := readySession(t, backend)
	agent := createAgent(t, s, "first")
	keys := make([]domain.DeliveryKey, 8)
	for i := range keys {
		keys[i] = receive(t, s, agent.ID, domain.NewEvent("test", nil)).Delivery.Key
	}
	winners := make(chan *core.ExecutionClaim, len(keys))
	start := make(chan struct{})
	var wg sync.WaitGroup
	for _, key := range keys {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			claim, err := s.ClaimExecution(ctx, key)
			if err == nil {
				winners <- claim
			} else if !errors.Is(err, core.ErrExecutionInProgress) {
				t.Errorf("并发claim错误: %v", err)
			}
		}()
	}
	close(start)
	wg.Wait()
	if len(winners) != 1 {
		t.Fatalf("同agent有%d个claim成功", len(winners))
	}
	claim := <-winners
	other := createAgent(t, s, "second")
	newExecution(t, s, other.ID)
	for _, token := range []core.ExecutionToken{
		{Delivery: claim.Token.Delivery, ExecutionID: "wrong", AttemptID: claim.Token.AttemptID, ExpectedStateVersion: claim.Token.ExpectedStateVersion},
		{Delivery: claim.Token.Delivery, ExecutionID: claim.Token.ExecutionID, AttemptID: "wrong", ExpectedStateVersion: claim.Token.ExpectedStateVersion},
		{Delivery: claim.Token.Delivery, ExecutionID: claim.Token.ExecutionID, AttemptID: claim.Token.AttemptID, ExpectedStateVersion: claim.Token.ExpectedStateVersion + 1},
		{Delivery: domain.DeliveryKey{AgentID: other.ID, EventID: claim.Token.Delivery.EventID}, ExecutionID: claim.Token.ExecutionID, AttemptID: claim.Token.AttemptID, ExpectedStateVersion: claim.Token.ExpectedStateVersion},
	} {
		if _, err := s.CommitExecution(ctx, core.ExecutionCommit{Token: token}); !errors.Is(err, core.ErrStoreStaleClaim) {
			t.Fatalf("错误token提交成功: %+v, %v", token, err)
		}
	}
	_, err := s.CommitExecution(ctx, core.ExecutionCommit{Token: claim.Token})
	must(t, err)
	if _, err := s.CommitExecution(ctx, core.ExecutionCommit{Token: claim.Token}); !errors.Is(err, core.ErrStoreStaleClaim) {
		t.Fatalf("已完成claim被重复提交: %v", err)
	}
	saved, err := s.LoadAgent(ctx, agent.ID)
	must(t, err)
	if saved.StateVersion != agent.StateVersion+1 {
		t.Fatalf("空更新未递增版本: %+v", saved)
	}
}

func testExecutionCommit(t *testing.T, backend core.RecoveryStore) {
	ctx := context.Background()
	s := readySession(t, backend)
	agent := createAgent(t, s, "agent")
	claim := newExecution(t, s, agent.ID)
	first, second := newAction(claim), newAction(claim)
	first.Request.ID, second.Request.ID = "z-action", "a-action"
	second.Request.Payload["invalid"] = math.NaN()
	update := map[string]any{"nested": map[string]any{"value": "更新"}, "null": nil}
	if _, err := s.CommitExecution(ctx, core.ExecutionCommit{Token: claim.Token, StateUpdate: update, Actions: []domain.ActionRecord{first, second}}); err == nil {
		t.Fatal("无效第二个action被提交")
	}
	saved, err := s.LoadAgent(ctx, agent.ID)
	must(t, err)
	sameJSON(t, saved, agent)
	execution, err := s.LoadExecution(ctx, claim.Token.ExecutionID)
	must(t, err)
	delivery, err := s.LoadDelivery(ctx, claim.Token.Delivery)
	must(t, err)
	actions, err := s.ListActions(ctx)
	must(t, err)
	if execution.Execution.Status != domain.ExecutionStatusRunning || execution.Execution.Result != nil || len(execution.Attempts) != 1 ||
		execution.Attempts[0].Status != domain.AttemptStatusRunning || execution.Attempts[0].FinishedAt != nil ||
		delivery.Status != domain.DeliveryStatusRunning || len(actions) != 0 {
		t.Fatalf("提交失败留下部分结果: execution=%+v delivery=%+v actions=%+v", execution, delivery, actions)
	}
	for _, id := range []domain.ID{first.Request.ID, second.Request.ID} {
		if _, err := s.LoadAction(ctx, id); !errors.Is(err, core.ErrStoreNotFound) {
			t.Fatalf("提交失败留下可查询action: %v", err)
		}
	}
	delete(second.Request.Payload, "invalid")
	result, err := s.CommitExecution(ctx, core.ExecutionCommit{Token: claim.Token, StateUpdate: update, Actions: []domain.ActionRecord{first, second}})
	must(t, err)
	update["nested"].(map[string]any)["value"] = "输入已修改"
	result.Actions[0].Payload["text"] = "结果已修改"
	first.Request.Payload["text"] = "action已修改"
	saved, err = s.LoadAgent(ctx, agent.ID)
	must(t, err)
	sameJSON(t, saved.State, map[string]any{"keep": "保留", "nested": map[string]any{"value": "更新"}, "null": nil})
	if saved.StateVersion != agent.StateVersion+1 {
		t.Fatalf("提交未更新版本: %+v", saved)
	}
	execution, err = s.LoadExecution(ctx, claim.Token.ExecutionID)
	must(t, err)
	delivery, err = s.LoadDelivery(ctx, claim.Token.Delivery)
	must(t, err)
	actions, err = s.ListActions(ctx, domain.ActionStatusPending)
	must(t, err)
	if delivery.Status != domain.DeliveryStatusCompleted || execution.Execution.Status != domain.ExecutionStatusCompleted ||
		execution.Execution.Result == nil || execution.Execution.Result.Actions[0].Payload["text"] != "hello" ||
		execution.Attempts[0].Status != domain.AttemptStatusSucceeded || execution.Attempts[0].FinishedAt == nil ||
		len(actions) != 2 || actions[0].Request.ID != "z-action" || actions[1].Request.ID != "a-action" || actions[0].Request.Payload["text"] != "hello" {
		t.Fatalf("提交记录或顺序不完整: execution=%+v delivery=%+v actions=%+v", execution, delivery, actions)
	}
	claimAction(t, s, first.Request.ID)
	actions, err = s.ListActions(ctx, domain.ActionStatusRunning, domain.ActionStatusPending)
	must(t, err)
	if len(actions) != 2 || actions[0].Request.ID != first.Request.ID || actions[1].Request.ID != second.Request.ID {
		t.Fatalf("action状态筛选改变提交顺序: %+v", actions)
	}
	actions, err = s.ListActions(ctx, domain.ActionStatusPending)
	must(t, err)
	if len(actions) != 1 || actions[0].Request.ID != second.Request.ID {
		t.Fatalf("action状态筛选不正确: %+v", actions)
	}
}

func testExecutionRetry(t *testing.T, backend core.RecoveryStore, interrupted bool) {
	ctx := context.Background()
	s := readySession(t, backend)
	agent := createAgent(t, s, "agent")
	first := newExecution(t, s, agent.ID)
	failure := domain.Failure{Kind: domain.ErrorKindBusiness, Message: "本次失败"}
	if interrupted {
		failure.Kind = domain.ErrorKindInterrupted
	}
	must(t, s.FailExecution(ctx, core.ExecutionFailure{Token: first.Token, Failure: failure, Interrupted: interrupted}))
	if _, err := s.ClaimExecution(ctx, first.Token.Delivery); !errors.Is(err, core.ErrDeliveryFailed) {
		t.Fatalf("失败投递未经retry又被领取: %v", err)
	}
	must(t, s.RequeueDelivery(ctx, first.Token.Delivery))
	requeued, err := s.LoadExecution(ctx, first.Token.ExecutionID)
	must(t, err)
	if requeued.Execution.AttemptCount != 1 || len(requeued.Attempts) != 1 || requeued.Execution.Status != domain.ExecutionStatusPending {
		t.Fatalf("retry改变历史或新增attempt: %+v", requeued)
	}
	second := claimExecution(t, s, first.Token.Delivery)
	if second.Token.ExecutionID != first.Token.ExecutionID || second.Token.AttemptID == first.Token.AttemptID || second.Attempt.Number != 2 {
		t.Fatalf("retry丢失execution身份或attempt序号: %+v", second)
	}
	if _, err := s.CommitExecution(ctx, core.ExecutionCommit{Token: first.Token}); !errors.Is(err, core.ErrStoreStaleClaim) {
		t.Fatalf("旧token提交未拒绝: %v", err)
	}
	if err := s.FailExecution(ctx, core.ExecutionFailure{Token: first.Token, Failure: failure}); !errors.Is(err, core.ErrStoreStaleClaim) {
		t.Fatalf("旧token失败记账未拒绝: %v", err)
	}
	_, err = s.CommitExecution(ctx, core.ExecutionCommit{Token: second.Token})
	must(t, err)
	saved, err := s.LoadExecution(ctx, first.Token.ExecutionID)
	must(t, err)
	want := domain.AttemptStatusFailed
	if interrupted {
		want = domain.AttemptStatusInterrupted
	}
	if len(saved.Attempts) != 2 || saved.Attempts[0].Number != 1 || saved.Attempts[0].Status != want ||
		saved.Attempts[0].Error == nil || *saved.Attempts[0].Error != failure || saved.Attempts[1].Number != 2 || saved.Attempts[1].Status != domain.AttemptStatusSucceeded {
		t.Fatalf("重试历史不完整: %+v", saved)
	}
	if err := s.RequeueDelivery(ctx, first.Token.Delivery); !errors.Is(err, core.ErrStoreConflict) {
		t.Fatalf("已完成投递被retry: %v", err)
	}
}
