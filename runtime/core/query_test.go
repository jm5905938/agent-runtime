package core

import (
	"agent-runtime/domain"
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func queryHasBlock(reasons []BlockReason, code QueryBlockCode) bool {
	for _, reason := range reasons {
		if reason.Code == code && reason.Message != "" {
			return true
		}
	}
	return false
}

func queryAgent(t *testing.T, runtime *Runtime, id domain.ID) AgentQuery {
	t.Helper()
	query, err := runtime.QueryAgent(id)
	if err != nil {
		t.Fatal(err)
	}
	return query
}

func TestQueryPendingDeliveriesAndAgentList(t *testing.T) {
	runtime := NewRuntime()
	ref := domain.DefinitionRef{ID: "query", Version: "1"}
	calls := 0
	if err := runtime.RegisterDefinition(ref, functionRunner(func(ExecutionContext) (ExecutionResult, error) {
		calls++
		return ExecutionResult{}, nil
	})); err != nil {
		t.Fatal(err)
	}
	for _, id := range []domain.ID{"z-agent", "a-agent"} {
		if err := runtime.RestoreAgent(domain.AgentInstance{
			ID: id, Name: string(id), Definition: ref, Status: domain.AgentStatusActive,
			State: map[string]any{"nested": []any{"original"}},
		}); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range []domain.ID{"z-event", "a-event"} {
		event := domain.NewEvent("query", map[string]any{"nested": []any{"original"}})
		event.ID = id
		if _, err := runtime.SubmitContext(context.Background(), "z-agent", event); err != nil {
			t.Fatal(err)
		}
		if _, err := runtime.SubmitContext(context.Background(), "z-agent", event); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := runtime.SubmitContext(context.Background(), "a-agent", domain.NewEvent("other", nil)); err != nil {
		t.Fatal(err)
	}
	before := queryAgent(t, runtime, "z-agent")
	if len(before.Deliveries) != 2 || len(before.Actions) != 0 || before.Agent.StateVersion != 0 {
		t.Fatalf("查询改变了记录或混入其他agent: %+v", before)
	}
	for i, id := range []domain.ID{"z-event", "a-event"} {
		delivery := before.Deliveries[i]
		if delivery.Event.ID != id || delivery.Delivery.Key.EventID != id || delivery.Delivery.ExecutionID == "" ||
			delivery.Execution != nil || len(delivery.Attempts) != 0 || !delivery.Ready || len(delivery.BlockedBy) != 0 {
			t.Fatalf("未领取的delivery或保存顺序丢失: %+v", delivery)
		}
	}
	agents, err := runtime.Agents()
	if err != nil || len(agents) != 2 || agents[0].ID != "a-agent" || agents[1].ID != "z-agent" {
		t.Fatalf("agent列表错误: %+v, %v", agents, err)
	}
	agents[1].State["nested"].([]any)[0] = "changed"
	changed := queryAgent(t, runtime, "z-agent")
	changed.Agent.State["nested"].([]any)[0] = "changed"
	changed.Deliveries[0].Event.Payload["nested"].([]any)[0] = "changed"
	changed.Deliveries[0].Delivery.ExecutionID = "changed"
	if after := queryAgent(t, runtime, "z-agent"); !reflect.DeepEqual(before, after) || calls != 0 {
		t.Fatalf("查询没有隔离或调用了runner: calls=%d, after=%+v", calls, after)
	}
}

func TestQueryExecutionHistoryAndSnapshotIsolation(t *testing.T) {
	runtime := NewRuntime()
	ref := domain.DefinitionRef{ID: "query", Version: "1"}
	fail := true
	if err := runtime.RegisterDefinition(ref, functionRunner(func(ctx ExecutionContext) (ExecutionResult, error) {
		if ctx.Event.Type == "action.result" {
			return ExecutionResult{}, nil
		}
		if fail {
			return ExecutionResult{}, errors.New("原始runner错误")
		}
		return ExecutionResult{
			StateUpdate: map[string]any{"nested": []any{"saved"}},
			Actions: []domain.Action{
				{ID: "z-action", Type: "ok", Payload: map[string]any{"nested": []any{"saved"}}},
				{ID: "a-action", Type: "fail", Payload: map[string]any{"nested": []any{"saved"}}},
			},
		}, nil
	})); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Executor().Register("ok", handlerFunc(func(domain.Action) (map[string]any, error) {
		return map[string]any{"nested": []any{"saved"}}, nil
	})); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Executor().Register("fail", handlerFunc(func(domain.Action) (map[string]any, error) {
		return nil, errors.New("原始handler错误")
	})); err != nil {
		t.Fatal(err)
	}
	agent, err := runtime.CreateAgent("query", ref, nil)
	if err != nil {
		t.Fatal(err)
	}
	event := domain.NewEvent("request", map[string]any{"nested": []any{"saved"}})
	if _, err := runtime.Process(agent.ID, event); !errors.Is(err, ErrExecutionFailed) {
		t.Fatalf("缺少执行失败: %v", err)
	}
	failed := queryAgent(t, runtime, agent.ID).Deliveries[0]
	if failed.Ready || !queryHasBlock(failed.BlockedBy, BlockRetryRequired) || len(failed.Attempts) != 1 ||
		failed.Attempts[0].Error == nil || !strings.Contains(failed.Attempts[0].Error.Message, "原始runner错误") {
		t.Fatalf("丢失失败原因: %+v", failed)
	}
	if err := runtime.Retry(context.Background(), failed.Delivery.Key); err != nil {
		t.Fatal(err)
	}
	retried := queryAgent(t, runtime, agent.ID).Deliveries[0]
	if !retried.Ready || retried.Delivery.ExecutionID != failed.Delivery.ExecutionID || len(retried.Attempts) != 1 ||
		retried.Attempts[0].Status != domain.AttemptStatusFailed {
		t.Fatalf("重试丢失原记录: %+v", retried)
	}
	fail = false
	if _, err := runtime.Process(agent.ID, event); err != nil {
		t.Fatal(err)
	}
	pending := queryAgent(t, runtime, agent.ID)
	if len(pending.Actions) != 2 || !pending.Actions[0].Ready || !pending.Actions[1].Ready ||
		pending.Actions[0].Action.Request.ID != "z-action" || pending.Actions[1].Action.Request.ID != "a-action" {
		t.Fatalf("action未保持提交顺序: %+v", pending.Actions)
	}
	if err := runtime.RunUntilIdle(); err != nil {
		t.Fatal(err)
	}
	before := queryAgent(t, runtime, agent.ID)
	if len(before.Deliveries) != 3 || before.Deliveries[0].Delivery.Key != failed.Delivery.Key ||
		before.Deliveries[1].Event.ID != before.Actions[0].Action.ResultEventID ||
		before.Deliveries[2].Event.ID != before.Actions[1].Action.ResultEventID {
		t.Fatalf("结果event或投递顺序丢失: %+v", before)
	}
	for i, status := range []domain.ActionStatus{domain.ActionStatusSucceeded, domain.ActionStatusFailed} {
		action := before.Actions[i]
		if action.Ready || len(action.BlockedBy) != 0 || action.Action.Status != status || action.Action.Result == nil ||
			action.Action.Result.Status != status || len(action.Attempts) != 1 || action.Attempts[0].Status != status {
			t.Fatalf("最终action状态错误: %+v", action)
		}
	}
	if !strings.Contains(before.Actions[1].Action.Result.Error.Message, "原始handler错误") {
		t.Fatal("丢失handler原始错误")
	}
	changed := queryAgent(t, runtime, agent.ID)
	changed.Agent.State["nested"].([]any)[0] = "changed"
	first := &changed.Deliveries[0]
	if first.Execution.Status != domain.ExecutionStatusCompleted || len(first.Attempts) != 2 ||
		first.Attempts[0].Number != 1 || first.Attempts[1].Number != 2 || first.Ready || len(first.BlockedBy) != 0 {
		t.Fatalf("execution历史错误: %+v", first)
	}
	first.Event.Payload["nested"].([]any)[0] = "changed"
	first.Execution.Result.StateUpdate["nested"].([]any)[0] = "changed"
	first.Execution.Result.Actions[0].Payload["nested"].([]any)[0] = "changed"
	*first.Execution.Result.Actions[0].ExecutionID = "changed"
	*first.Execution.StartedAt = first.Execution.CreatedAt
	*first.Execution.FinishedAt = first.Execution.CreatedAt
	first.Attempts[0].Error.Message = "changed"
	*first.Attempts[0].FinishedAt = first.Execution.CreatedAt
	for i := range changed.Actions {
		action := &changed.Actions[i]
		action.Action.Request.Payload["nested"].([]any)[0] = "changed"
		*action.Action.Request.ExecutionID = "changed"
		*action.Attempts[0].FinishedAt = first.Execution.CreatedAt
		if i == 0 {
			action.Action.Result.Output["nested"].([]any)[0] = "changed"
		} else {
			action.Action.LastError.Message = "changed"
			action.Action.Result.Error.Message = "changed"
			action.Attempts[0].Error.Message = "changed"
		}
	}
	if after := queryAgent(t, runtime, agent.ID); !reflect.DeepEqual(before, after) {
		t.Fatalf("修改查询结果影响存储: %+v", after)
	}
}

func TestQueryBindingAndLifecycleBlockers(t *testing.T) {
	store := NewMemoryStore()
	_, agent, original := actionBridgeRuntime(t, store, EchoHandler{})
	runtime, err := NewRuntimeWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.RegisterDefinition(domain.DefinitionRef{ID: agent.Definition.ID, Version: "2"}, functionRunner(actionBridgeRunner)); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.SubmitContext(context.Background(), agent.ID, domain.NewEvent("pending", nil)); err != nil {
		t.Fatal(err)
	}
	query := queryAgent(t, runtime, agent.ID)
	if query.Agent.BindingError == "" || query.Deliveries[0].Ready || len(query.Deliveries[0].BlockedBy) != 0 ||
		query.Deliveries[1].Ready || !queryHasBlock(query.Deliveries[1].BlockedBy, BlockDefinitionUnavailable) ||
		!queryHasBlock(query.Actions[0].BlockedBy, BlockHandlerUnavailable) {
		t.Fatalf("缺失绑定判定错误: %+v", query)
	}
	agents, err := runtime.Agents()
	if err != nil || len(agents) != 1 || agents[0].BindingError != query.Agent.BindingError {
		t.Fatalf("agent列表丢失绑定原因: %+v, %v", agents, err)
	}
	if err := runtime.Executor().Register("bridge-test", EchoHandler{}); err != nil {
		t.Fatal(err)
	}
	query = queryAgent(t, runtime, agent.ID)
	if !query.Actions[0].Ready || len(query.Actions[0].BlockedBy) != 0 || query.Deliveries[1].Ready {
		t.Fatalf("已提交action错误依赖definition: %+v", query)
	}
	wrong, err := NewRuntimeWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	if err := wrong.Executor().RegisterWithOptions("bridge-test", EchoHandler{}, HandlerOptions{
		Version: "wrong", RecoveryPolicy: domain.RecoveryPolicyManual, MaxAttempts: 1,
	}); err != nil {
		t.Fatal(err)
	}
	action := queryAgent(t, wrong, agent.ID).Actions[0]
	if action.Ready || !queryHasBlock(action.BlockedBy, BlockHandlerUnavailable) || action.Action.HandlerVersion != original.HandlerVersion {
		t.Fatalf("未检查保存的handler版本: %+v", action)
	}
	paused := domain.NewAgentInstance("paused")
	paused.Status, paused.Definition = domain.AgentStatusPaused, agent.Definition
	if err := runtime.RestoreAgent(paused); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.SubmitContext(context.Background(), paused.ID, domain.NewEvent("pending", nil)); err != nil {
		t.Fatal(err)
	}
	delivery := queryAgent(t, runtime, paused.ID).Deliveries[0]
	if delivery.Ready || !queryHasBlock(delivery.BlockedBy, BlockAgentInactive) {
		t.Fatalf("非active agent未阻塞: %+v", delivery)
	}
}

func TestQueryUnknownActionReadiness(t *testing.T) {
	for _, scenario := range []struct {
		name     string
		policy   domain.RecoveryPolicy
		attempts int
		code     QueryBlockCode
	}{
		{"manual", domain.RecoveryPolicyManual, 1, BlockManualUnknown},
		{"safe_retry", domain.RecoveryPolicySafeRetry, 1, ""},
		{"exhausted", domain.RecoveryPolicySafeRetry, 3, BlockAttemptsExhausted},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			runtime := p4Open(t, NewMemoryRecoveryStore())
			agent := p4Agent(t, runtime, functionRunner(func(ExecutionContext) (ExecutionResult, error) {
				return ExecutionResult{Actions: []domain.Action{domain.NewAction("echo", nil)}}, nil
			}), EchoHandler{}, scenario.policy)
			result, err := runtime.Process(agent.ID, domain.NewEvent("request", nil))
			if err != nil {
				t.Fatal(err)
			}
			for range scenario.attempts {
				claim, err := runtime.store.ClaimAction(context.Background(), result.Actions[0].ID)
				if err != nil {
					t.Fatal(err)
				}
				if err := runtime.store.RecordActionUnknown(context.Background(), claim.Token, domain.Failure{
					Kind: domain.ErrorKindInterrupted, Message: "原始中断原因",
				}); err != nil {
					t.Fatal(err)
				}
			}
			query := queryAgent(t, runtime, agent.ID)
			action := query.Actions[0]
			if action.Ready != (scenario.code == "") || (scenario.code != "" && !queryHasBlock(action.BlockedBy, scenario.code)) ||
				action.Action.Status != domain.ActionStatusUnknown || action.Action.Result != nil ||
				action.Action.LastError.Message != "原始中断原因" || len(action.Attempts) != scenario.attempts || len(query.Deliveries) != 1 {
				t.Fatalf("unknown策略或原始原因丢失: %+v", query)
			}
			for i, attempt := range action.Attempts {
				if attempt.Number != uint64(i+1) || attempt.Error.Message != "原始中断原因" {
					t.Fatalf("action尝试历史错误: %+v", action.Attempts)
				}
			}
			action.Action.LastError.Message = "changed"
			action.Attempts[0].Error.Message = "changed"
			if fresh := queryAgent(t, runtime, agent.ID).Actions[0]; fresh.Action.LastError.Message != "原始中断原因" ||
				fresh.Attempts[0].Error.Message != "原始中断原因" || fresh.Action.AttemptCount != uint64(scenario.attempts) {
				t.Fatalf("unknown查询修改了存储: %+v", fresh)
			}
		})
	}
}

func TestQueryUnknownRequiresRecoveryRuntime(t *testing.T) {
	runtime := NewRuntime()
	agent := p4Agent(t, runtime, functionRunner(func(ExecutionContext) (ExecutionResult, error) {
		return ExecutionResult{Actions: []domain.Action{domain.NewAction("echo", nil)}}, nil
	}), handlerFunc(func(domain.Action) (map[string]any, error) { panic("结果未知") }), domain.RecoveryPolicySafeRetry)
	if _, err := runtime.Process(agent.ID, domain.NewEvent("request", nil)); err != nil {
		t.Fatal(err)
	}
	if err := runtime.RunUntilIdle(); err != nil {
		t.Fatal(err)
	}
	action := queryAgent(t, runtime, agent.ID).Actions[0]
	if action.Ready || !queryHasBlock(action.BlockedBy, BlockRecoveryRequired) || action.Action.AttemptCount != 1 {
		t.Fatalf("非恢复runtime错误承诺重试: %+v", action)
	}
}

func TestQueryRunningExecutionOnlyBlocksSameAgentDeliveries(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	runtime, agent, _ := actionBridgeRuntime(t, store, EchoHandler{})
	running, err := runtime.SubmitContext(ctx, agent.ID, domain.NewEvent("running", nil))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.ClaimExecution(ctx, running.Delivery.Key); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.SubmitContext(ctx, agent.ID, domain.NewEvent("pending", nil)); err != nil {
		t.Fatal(err)
	}
	other, err := runtime.CreateAgent("other", agent.Definition, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.SubmitContext(ctx, other.ID, domain.NewEvent("pending", nil)); err != nil {
		t.Fatal(err)
	}
	query := queryAgent(t, runtime, agent.ID)
	for _, delivery := range query.Deliveries[1:] {
		if delivery.Ready || !queryHasBlock(delivery.BlockedBy, BlockExecutionRunning) {
			t.Fatalf("同agent执行未阻塞delivery: %+v", delivery)
		}
	}
	if !query.Actions[0].Ready || !queryAgent(t, runtime, other.ID).Deliveries[0].Ready {
		t.Fatal("running execution错误阻塞了已提交action或其他agent")
	}
}
