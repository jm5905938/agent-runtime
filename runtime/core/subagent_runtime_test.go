package core

import (
	"agent-runtime/codec"
	"agent-runtime/domain"
	"context"
	"errors"
	"strings"
	"testing"
)

type subagentFlowRunner struct{}

func (subagentFlowRunner) Run(input ExecutionContext) (ExecutionResult, error) {
	if input.Event.Type == "start" {
		return ExecutionResult{Actions: []domain.Action{domain.NewAction(SubagentSpawnActionType, map[string]any{"message": "独立任务"})}}, nil
	}
	if input.Event.Payload["action_type"] == SubagentSpawnActionType {
		result := input.Event.Payload["result"].(map[string]any)
		return ExecutionResult{Actions: []domain.Action{domain.NewAction(SubagentWaitActionType, map[string]any{"task_id": result["task_id"]})}}, nil
	}
	return ExecutionResult{StateUpdate: map[string]any{"finished": input.Event.Payload["result"]}}, nil
}

func bindSubagentFlow(t *testing.T, runtime *Runtime, child AgentRunner) {
	t.Helper()
	for ref, runner := range map[domain.DefinitionRef]AgentRunner{
		{ID: "main", Version: "1"}: subagentFlowRunner{}, {ID: "subagent", Version: "1"}: child,
	} {
		if err := runtime.RegisterDefinition(ref, runner); err != nil {
			t.Fatal(err)
		}
	}
	if err := runtime.RegisterSubagentTools(); err != nil {
		t.Fatal(err)
	}
}

func subagentFlowChild() AgentRunner {
	return functionRunner(func(input ExecutionContext) (ExecutionResult, error) {
		return ExecutionResult{TaskResult: &domain.SubagentResult{Status: domain.SubagentStatusSucceeded, Output: map[string]any{"message": "完成"}}}, nil
	})
}

func TestSubagentRuntimeWaitAndTreeScope(t *testing.T) {
	ctx := context.Background()
	runtime := p4Open(t, NewMemoryRecoveryStore())
	bindSubagentFlow(t, runtime, subagentFlowChild())
	parent, err := runtime.CreateAgent("parent", domain.DefinitionRef{ID: "main", Version: "1"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	unrelated, err := runtime.CreateAgent("unrelated", parent.Definition, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []domain.ID{parent.ID, unrelated.ID} {
		if err := runtime.Submit(id, domain.NewEvent("start", nil)); err != nil {
			t.Fatal(err)
		}
	}
	if err := runtime.RunAgentUntilIdleContext(ctx, parent.ID); err != nil {
		t.Fatal(err)
	}
	query, err := runtime.QueryAgent(parent.ID)
	if err != nil || len(query.Tasks) != 1 {
		t.Fatalf("委派未保存: %+v %v", query, err)
	}
	for _, action := range query.Actions {
		if action.Action.Request.Type == SubagentWaitActionType && (action.Ready || action.Action.AttemptCount != 0 || len(action.BlockedBy) != 1 || action.BlockedBy[0].Code != BlockSubagentWaiting) {
			t.Fatalf("wait门控消耗attempt或查询不一致: %+v", action)
		}
	}
	if err := runtime.RunAgentTreeUntilIdleContext(ctx, parent.ID); err != nil {
		t.Fatal(err)
	}
	query, err = runtime.QueryAgent(parent.ID)
	if err != nil || query.Tasks[0].Result == nil || query.Tasks[0].Result.Status != domain.SubagentStatusSucceeded || query.Agent.State["finished"] == nil {
		t.Fatalf("任务树未完成: %+v %v", query, err)
	}
	other, err := runtime.Agent(unrelated.ID)
	if err != nil || other.StateVersion != 0 {
		t.Fatalf("推进了无关agent: %+v %v", other, err)
	}
}

func TestSubagentRuntimeRecoversSpawnBeforeAcknowledgement(t *testing.T) {
	ctx := context.Background()
	backend := NewMemoryRecoveryStore()
	runtime := p4Open(t, backend)
	bindSubagentFlow(t, runtime, subagentFlowChild())
	parent, err := runtime.CreateAgent("parent", domain.DefinitionRef{ID: "main", Version: "1"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	result, err := runtime.Process(parent.ID, domain.NewEvent("start", nil))
	if err != nil {
		t.Fatal(err)
	}
	claim, err := runtime.store.ClaimAction(ctx, result.Actions[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	handler := subagentHandler{runtime: runtime, kind: SubagentSpawnActionType}
	accepted, err := handler.ExecuteClaim(ctx, *claim)
	if err != nil {
		t.Fatal(err)
	}
	p4Close(t, runtime)
	reopened := p4Open(t, backend)
	bindSubagentFlow(t, reopened, subagentFlowChild())
	if err := reopened.RunAgentTreeUntilIdleContext(ctx, parent.ID); err != nil {
		t.Fatal(err)
	}
	query, err := reopened.QueryAgent(parent.ID)
	if err != nil || len(query.Tasks) != 1 || string(query.Tasks[0].ChildAgentID) != accepted["child_id"] || query.Tasks[0].Result == nil {
		t.Fatalf("恢复重复创建或丢失child: %+v %v", query, err)
	}
	if query.Actions[0].Action.AttemptCount != 2 || query.Actions[0].Action.Result.Output["child_id"] != accepted["child_id"] {
		t.Fatalf("spawn恢复没有复用原handle: %+v", query.Actions[0])
	}
}

func TestSubagentRuntimeBoundsToolResultAndRetainsFullResult(t *testing.T) {
	for _, message := range []string{strings.Repeat("🙂<&", 10000), strings.Repeat("完整", 10000)} {
		task := domain.SubagentTask{ID: "task", ChildAgentID: "child", Result: &domain.SubagentResult{Status: domain.SubagentStatusSucceeded, Output: map[string]any{"message": message}}}
		output, err := subagentToolResult(task)
		if err != nil {
			t.Fatal(err)
		}
		data, err := codec.Encode(output)
		if err != nil || len(data) > 8*1024 || output["result_reference"] != "subagent-task:task" || task.Result.Output["message"] != message {
			t.Fatalf("工具结果超限或原结果丢失: bytes=%d output=%+v err=%v", len(data), output, err)
		}
	}
}

func TestSubagentCancelledToolResolutionRemainsIdempotent(t *testing.T) {
	ctx := context.Background()
	backend := NewMemoryRecoveryStore()
	runtime := p4Open(t, backend)
	child := functionRunner(func(input ExecutionContext) (ExecutionResult, error) {
		action := domain.NewAction("tool.clock", nil)
		return ExecutionResult{StateUpdate: map[string]any{
			"request_status": "waiting", "waiting_action_id": string(action.ID),
			"waiting_action_type": action.Type, "waiting_execution_id": string(input.ExecutionID),
		}, Actions: []domain.Action{action}}, nil
	})
	bindSubagentFlow(t, runtime, child)
	if err := runtime.Executor().RegisterWithOptions("tool.clock", handlerFunc(func(domain.Action) (map[string]any, error) {
		t.Fatal("耗尽的unknown工具不应再次执行")
		return nil, nil
	}), HandlerOptions{Version: "1", RecoveryPolicy: domain.RecoveryPolicySafeRetry, MaxAttempts: 1}); err != nil {
		t.Fatal(err)
	}
	parent, err := runtime.CreateAgent("parent", domain.DefinitionRef{ID: "main", Version: "1"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.Submit(parent.ID, domain.NewEvent("start", nil)); err != nil {
		t.Fatal(err)
	}
	if err := runtime.RunAgentUntilIdleContext(ctx, parent.ID); err != nil {
		t.Fatal(err)
	}
	query, err := runtime.QueryAgent(parent.ID)
	if err != nil || len(query.Tasks) != 1 {
		t.Fatalf("任务创建失败: %+v %v", query, err)
	}
	task := query.Tasks[0]
	result, err := runtime.processDelivery(ctx, domain.DeliveryKey{AgentID: task.ChildAgentID, EventID: task.InitialEventID})
	if err != nil || len(result.Actions) != 1 {
		t.Fatalf("child未创建工具action: %+v %v", result, err)
	}
	actionID := result.Actions[0].ID
	claim, err := runtime.store.ClaimAction(ctx, actionID)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.store.RecordActionUnknown(ctx, claim.Token, domain.Failure{Kind: domain.ErrorKindUnknown, Message: "工具结果未知"}); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.CancelSubagentTaskContext(ctx, parent.ID, task.ID); err != nil {
		t.Fatal(err)
	}
	first, err := runtime.ResolveAction(ctx, actionID, ResolutionAbandon, "放弃未知工具结果")
	if err != nil || first.Duplicate {
		t.Fatalf("取消中的unknown工具无法放弃: %+v %v", first, err)
	}
	if err := runtime.RunAgentTreeUntilIdleContext(ctx, parent.ID); err != nil {
		t.Fatal(err)
	}
	query, err = runtime.QueryAgent(parent.ID)
	if err != nil || query.Tasks[0].Result == nil || query.Tasks[0].Result.Status != domain.SubagentStatusCancelled {
		t.Fatalf("放弃后任务未结束: %+v %v", query, err)
	}
	p4Close(t, runtime)
	reopened := p4Open(t, backend)
	duplicate, err := reopened.ResolveAction(ctx, actionID, ResolutionAbandon, "放弃未知工具结果")
	if err != nil || !duplicate.Duplicate || duplicate.Resolution != first.Resolution || duplicate.Delivery != first.Delivery {
		t.Fatalf("取消完成后无法查询原receipt: %+v %v", duplicate, err)
	}
	for _, decision := range []ResolutionDecision{ResolutionAbandon, ResolutionRetry} {
		if _, err := reopened.ResolveAction(ctx, actionID, decision, "另一个决定"); !errors.Is(err, ErrStoreConflict) {
			t.Fatalf("已结束任务接受不同决定%s: %v", decision, err)
		}
	}
}
