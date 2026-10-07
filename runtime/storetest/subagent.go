package storetest

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"agent-runtime/core"
	"agent-runtime/domain"
)

func subagentStore(t *testing.T, session core.RecoverySession) core.SubagentStore {
	t.Helper()
	store, ok := session.(core.SubagentStore)
	if !ok {
		t.Skip("session不支持subagent契约")
	}
	return store
}

func subagentSpawn(t *testing.T, session core.RecoverySession, parentID, childID domain.ID) core.SubagentSpawn {
	t.Helper()
	parent := createAgent(t, session, parentID)
	execution := newExecution(t, session, parent.ID)
	action := newAction(execution)
	action.Request.Type, action.Request.Payload = core.SubagentSpawnActionType, map[string]any{"message": "独立任务"}
	_, err := session.CommitExecution(context.Background(), core.ExecutionCommit{Token: execution.Token, Actions: []domain.ActionRecord{action}})
	must(t, err)
	claim := claimAction(t, session, action.Request.ID)
	return core.SubagentSpawn{
		Token: claim.Token,
		Child: domain.AgentInstance{ID: childID, Name: "child", Definition: domain.DefinitionRef{ID: "subagent", Version: "1"},
			Status: domain.AgentStatusActive, State: map[string]any{"nested": map[string]any{"value": "独立"}}},
		Event: domain.NewEvent("subagent.request", map[string]any{"message": "独立任务"}),
	}
}

func subagentInitial(t *testing.T, session core.RecoverySession, task *domain.SubagentTask) *core.ExecutionClaim {
	t.Helper()
	return claimExecution(t, session, domain.DeliveryKey{AgentID: task.ChildAgentID, EventID: task.InitialEventID})
}

func subagentCommit(claim *core.ExecutionClaim) core.ExecutionCommit {
	return core.ExecutionCommit{Token: claim.Token, StateUpdate: map[string]any{"request_status": "succeeded"},
		TaskResult: &domain.SubagentResult{Status: domain.SubagentStatusSucceeded, Output: map[string]any{"message": "完整结果"}}}
}

func testSubagentSpawnValidation(t *testing.T, backend core.RecoveryStore) {
	ctx := context.Background()
	session := readySession(t, backend)
	store := subagentStore(t, session)
	spawn := subagentSpawn(t, session, "parent", "child")
	source, err := session.LoadAction(ctx, spawn.Token.ActionID)
	must(t, err)
	agents, err := session.ListAgents(ctx)
	must(t, err)
	deliveries, err := session.ListDeliveries(ctx)
	must(t, err)
	for _, change := range []func(*core.SubagentSpawn){
		func(s *core.SubagentSpawn) { s.Child.ID = "parent" },
		func(s *core.SubagentSpawn) { s.Child.Status = domain.AgentStatusTerminated },
		func(s *core.SubagentSpawn) { s.Child.StateVersion = 1 },
		func(s *core.SubagentSpawn) { s.Child.Definition.Version = "" },
		func(s *core.SubagentSpawn) { s.Child.State = map[string]any{"invalid": math.NaN()} },
		func(s *core.SubagentSpawn) { s.Event.Type = "" },
		func(s *core.SubagentSpawn) { s.Event.Payload = map[string]any{"invalid": math.NaN()} },
		func(s *core.SubagentSpawn) { s.Event.ID = source.Action.ResultEventID },
	} {
		bad := spawn
		change(&bad)
		if _, err := store.SpawnSubagent(ctx, bad); err == nil {
			t.Fatalf("无效创建被接受: %+v", bad)
		}
		savedAgents, err := session.ListAgents(ctx)
		must(t, err)
		sameJSON(t, savedAgents, agents)
		savedDeliveries, err := session.ListDeliveries(ctx)
		must(t, err)
		sameJSON(t, savedDeliveries, deliveries)
		tasks, err := store.ListSubagentTasks(ctx)
		must(t, err)
		if len(tasks) != 0 {
			t.Fatalf("无效创建留下task: %+v", tasks)
		}
		if _, err := session.LoadEvent(ctx, spawn.Event.ID); !errors.Is(err, core.ErrStoreNotFound) {
			t.Fatalf("无效创建留下event: %v", err)
		}
	}
	task, err := store.SpawnSubagent(ctx, spawn)
	must(t, err)
	want := *task
	duplicate := spawn
	duplicate.Event.CreatedAt = duplicate.Event.CreatedAt.Add(time.Hour)
	repeated, err := store.SpawnSubagent(ctx, duplicate)
	must(t, err)
	sameJSON(t, repeated, task)
	duplicate.Event.Payload = map[string]any{"message": "改变任务"}
	if _, err := store.SpawnSubagent(ctx, duplicate); !errors.Is(err, core.ErrStoreConflict) {
		t.Fatalf("重复创建改变了任务: %v", err)
	}
	spawn.Child.State["nested"].(map[string]any)["value"] = "输入已改"
	spawn.Event.Payload["message"] = "输入已改"
	task.ChildAgentID = "返回值已改"
	saved, err := store.LoadSubagentTask(ctx, want.ID)
	must(t, err)
	sameJSON(t, saved, want)
	child, err := session.LoadAgent(ctx, want.ChildAgentID)
	must(t, err)
	sameJSON(t, child.State, map[string]any{"nested": map[string]any{"value": "独立"}})
	event, err := session.LoadEvent(ctx, want.InitialEventID)
	must(t, err)
	sameJSON(t, event.Payload, map[string]any{"message": "独立任务"})
	old := spawn
	old.Token.AttemptNumber++
	if _, err := store.SpawnSubagent(ctx, old); !errors.Is(err, core.ErrStoreStaleClaim) {
		t.Fatalf("旧token重用已创建任务: %v", err)
	}
}

func testSubagentSpawnRecovery(t *testing.T, backend core.RecoveryStore) {
	ctx := context.Background()
	session := readySession(t, backend)
	store := subagentStore(t, session)
	spawn := subagentSpawn(t, session, "parent", "child")
	task, err := store.SpawnSubagent(ctx, spawn)
	must(t, err)
	old := subagentInitial(t, session, task)
	must(t, session.Close(ctx))
	reopened := readySession(t, backend)
	store = subagentStore(t, reopened)
	if _, err := store.SpawnSubagent(ctx, spawn); !errors.Is(err, core.ErrStoreStaleClaim) {
		t.Fatalf("恢复后旧spawn token仍有效: %v", err)
	}
	if _, err := reopened.CommitExecution(ctx, subagentCommit(old)); !errors.Is(err, core.ErrStoreStaleClaim) {
		t.Fatalf("恢复后旧child token仍有效: %v", err)
	}
	retry := claimAction(t, reopened, spawn.Token.ActionID)
	spawn.Token = retry.Token
	saved, err := store.SpawnSubagent(ctx, spawn)
	must(t, err)
	sameJSON(t, saved, task)
	current := subagentInitial(t, reopened, task)
	if current.Token.ExecutionID != old.Token.ExecutionID || current.Token.AttemptID == old.Token.AttemptID || current.Attempt.Number != 2 {
		t.Fatalf("恢复改变child execution身份或attempt顺序: %+v", current)
	}
	_, err = reopened.CommitExecution(ctx, subagentCommit(current))
	must(t, err)
	tasks, err := store.ListSubagentTasks(ctx)
	must(t, err)
	if len(tasks) != 1 || tasks[0].Result == nil || tasks[0].CompletionExecutionID != old.Token.ExecutionID {
		t.Fatalf("恢复重试创建了额外任务或丢失完成关联: %+v", tasks)
	}
}

func testSubagentCompletion(t *testing.T, backend core.RecoveryStore) {
	ctx := context.Background()
	session := readySession(t, backend)
	store := subagentStore(t, session)
	spawn := subagentSpawn(t, session, "parent", "child")
	task, err := store.SpawnSubagent(ctx, spawn)
	must(t, err)
	claim := subagentInitial(t, session, task)
	commit := subagentCommit(claim)
	commit.TaskResult.Status = domain.SubagentStatusFailed
	commit.TaskResult.Output = map[string]any{"nested": map[string]any{"values": []any{"完整部分结果"}}}
	commit.TaskResult.Error = &domain.Failure{Kind: domain.ErrorKindBusiness, Message: "任务失败"}
	result, err := session.CommitExecution(ctx, commit)
	must(t, err)
	commit.TaskResult.Output["nested"].(map[string]any)["values"].([]any)[0] = "输入已改"
	commit.TaskResult.Error.Message = "输入已改"
	result.TaskResult.Output["nested"].(map[string]any)["values"].([]any)[0] = "返回值已改"
	result.TaskResult.Error.Message = "返回值已改"
	want := domain.SubagentResult{Status: domain.SubagentStatusFailed,
		Output: map[string]any{"nested": map[string]any{"values": []any{"完整部分结果"}}},
		Error:  &domain.Failure{Kind: domain.ErrorKindBusiness, Message: "任务失败"}}
	saved, err := store.LoadSubagentTask(ctx, task.ID)
	must(t, err)
	sameJSON(t, saved.Result, want)
	if saved.CompletionExecutionID != claim.Token.ExecutionID {
		t.Fatalf("task没有保存完成execution: %+v", saved)
	}
	saved.Result.Output["nested"].(map[string]any)["values"].([]any)[0] = "查询值已改"
	saved.Result.Error.Message = "查询值已改"
	listed, err := store.ListSubagentTasks(ctx)
	must(t, err)
	sameJSON(t, listed[0].Result, want)
	listed[0].Result.Error.Message = "列表值已改"
	child, err := session.LoadAgent(ctx, task.ChildAgentID)
	must(t, err)
	storedExecution, err := session.LoadExecution(ctx, claim.Token.ExecutionID)
	must(t, err)
	delivery, err := session.LoadDelivery(ctx, claim.Token.Delivery)
	must(t, err)
	if child.Status != domain.AgentStatusTerminated || child.StateVersion != 1 || delivery.Status != domain.DeliveryStatusCompleted ||
		storedExecution.Execution.Status != domain.ExecutionStatusCompleted || storedExecution.Execution.Result == nil ||
		storedExecution.Attempts[0].Status != domain.AttemptStatusSucceeded {
		t.Fatalf("任务终态未完整提交: child=%+v execution=%+v delivery=%+v", child, storedExecution, delivery)
	}
	sameJSON(t, storedExecution.Execution.Result.TaskResult, want)
	if _, err := session.CommitExecution(ctx, subagentCommit(claim)); !errors.Is(err, core.ErrStoreStaleClaim) {
		t.Fatalf("重复完成覆盖原结果: %v", err)
	}
	must(t, session.Close(ctx))
	reopened := readySession(t, backend)
	retained, err := subagentStore(t, reopened).LoadSubagentTask(ctx, task.ID)
	must(t, err)
	sameJSON(t, retained.Result, want)
}

func testSubagentCompletionValidation(t *testing.T, backend core.RecoveryStore) {
	ctx := context.Background()
	session := readySession(t, backend)
	store := subagentStore(t, session)
	spawn := subagentSpawn(t, session, "parent", "child")
	task, err := store.SpawnSubagent(ctx, spawn)
	must(t, err)
	claim := subagentInitial(t, session, task)
	for _, taskResult := range []*domain.SubagentResult{
		{Status: "running", Output: map[string]any{}},
		{Status: domain.SubagentStatusSucceeded},
		{Status: domain.SubagentStatusFailed, Output: map[string]any{}},
		{Status: domain.SubagentStatusSucceeded, Output: map[string]any{}, Error: &domain.Failure{Kind: domain.ErrorKindBusiness, Message: "错误"}},
	} {
		if _, err := session.CommitExecution(ctx, core.ExecutionCommit{Token: claim.Token, StateUpdate: map[string]any{"invalid": true}, TaskResult: taskResult}); err == nil {
			t.Fatalf("非法typed结果被提交: %+v", taskResult)
		}
	}
	commit := subagentCommit(claim)
	action := newAction(claim)
	commit.Actions = []domain.ActionRecord{action}
	if _, err := session.CommitExecution(ctx, commit); err == nil {
		t.Fatal("typed完成提交了新action")
	}
	if _, err := session.LoadAction(ctx, action.Request.ID); !errors.Is(err, core.ErrStoreNotFound) {
		t.Fatalf("被拒绝的typed完成留下action: %v", err)
	}
	saved, err := store.LoadSubagentTask(ctx, task.ID)
	must(t, err)
	child, err := session.LoadAgent(ctx, task.ChildAgentID)
	must(t, err)
	execution, err := session.LoadExecution(ctx, claim.Token.ExecutionID)
	must(t, err)
	if saved.Result != nil || child.Status != domain.AgentStatusActive || child.StateVersion != 0 || child.State["invalid"] != nil ||
		execution.Execution.Status != domain.ExecutionStatusRunning || execution.Execution.Result != nil {
		t.Fatalf("被拒绝的完成发布了部分结果: task=%+v child=%+v execution=%+v", saved, child, execution)
	}
	parentClaim := newExecution(t, session, task.ParentAgentID)
	if _, err := session.CommitExecution(ctx, subagentCommit(parentClaim)); !errors.Is(err, core.ErrStoreConflict) {
		t.Fatalf("普通agent提交了task_result: %v", err)
	}
	_, err = session.CommitExecution(ctx, subagentCommit(claim))
	must(t, err)
}

func testSubagentCompletionPendingAction(t *testing.T, backend core.RecoveryStore) {
	ctx := context.Background()
	session := readySession(t, backend)
	store := subagentStore(t, session)
	spawn := subagentSpawn(t, session, "parent", "child")
	task, err := store.SpawnSubagent(ctx, spawn)
	must(t, err)
	initial := subagentInitial(t, session, task)
	action := newAction(initial)
	action.Request.Type = core.SubagentSpawnActionType
	_, err = session.CommitExecution(ctx, core.ExecutionCommit{Token: initial.Token, Actions: []domain.ActionRecord{action}})
	must(t, err)
	final := newExecution(t, session, task.ChildAgentID)
	if _, err := session.CommitExecution(ctx, subagentCommit(final)); !errors.Is(err, core.ErrSubagentBusy) {
		t.Fatalf("未完成action没有阻止task终态: %v", err)
	}
	claim := claimAction(t, session, action.Request.ID)
	nested := spawn
	nested.Token, nested.Child.ID, nested.Event.ID = claim.Token, "grandchild", domain.NewEvent("subagent.request", nil).ID
	if _, err := store.SpawnSubagent(ctx, nested); !errors.Is(err, core.ErrStoreConflict) {
		t.Fatalf("child递归创建任务: %v", err)
	}
	if _, err := session.LoadAgent(ctx, "grandchild"); !errors.Is(err, core.ErrStoreNotFound) {
		t.Fatalf("递归创建留下child: %v", err)
	}
	_, err = session.CompleteAction(ctx, completion(claim, "确认结果"))
	must(t, err)
	_, err = session.CommitExecution(ctx, subagentCommit(final))
	must(t, err)
}

func testSubagentFailureInterruption(t *testing.T, backend core.RecoveryStore) {
	ctx := context.Background()
	session := readySession(t, backend)
	store := subagentStore(t, session)
	for _, interrupted := range []bool{false, true} {
		parentID, childID := domain.ID("parent-failed"), domain.ID("child-failed")
		if interrupted {
			parentID, childID = "parent-interrupted", "child-interrupted"
		}
		spawn := subagentSpawn(t, session, parentID, childID)
		task, err := store.SpawnSubagent(ctx, spawn)
		must(t, err)
		claim := subagentInitial(t, session, task)
		failure := domain.Failure{Kind: domain.ErrorKindBusiness, Message: "任务失败"}
		if interrupted {
			failure.Kind = domain.ErrorKindInterrupted
		}
		must(t, session.FailExecution(ctx, core.ExecutionFailure{Token: claim.Token, Failure: failure, Interrupted: interrupted}))
		saved, err := store.LoadSubagentTask(ctx, task.ID)
		must(t, err)
		child, err := session.LoadAgent(ctx, childID)
		must(t, err)
		if interrupted {
			if saved.Result != nil || child.Status != domain.AgentStatusActive {
				t.Fatalf("中断伪造了task终态: task=%+v child=%+v", saved, child)
			}
			must(t, session.RequeueDelivery(ctx, claim.Token.Delivery))
		} else if saved.Result == nil || saved.Result.Status != domain.SubagentStatusFailed || saved.CompletionExecutionID != claim.Token.ExecutionID ||
			child.Status != domain.AgentStatusTerminated || saved.Result.Error == nil || *saved.Result.Error != failure {
			t.Fatalf("明确失败没有提交task终态: task=%+v child=%+v", saved, child)
		}
		if !interrupted {
			if err := session.RequeueDelivery(ctx, claim.Token.Delivery); err == nil {
				t.Fatal("已结束的child重新排队并改变task完成execution")
			}
			execution, err := session.LoadExecution(ctx, claim.Token.ExecutionID)
			must(t, err)
			if execution.Execution.Status != domain.ExecutionStatusFailed || execution.Execution.Error != failure.Message {
				t.Fatalf("拒绝重排仍改变了task完成execution: %+v", execution)
			}
		}
	}
}

func testSubagentCancelBusy(t *testing.T, backend core.RecoveryStore) {
	ctx := context.Background()
	session := readySession(t, backend)
	store := subagentStore(t, session)
	spawn := subagentSpawn(t, session, "parent", "child")
	task, err := store.SpawnSubagent(ctx, spawn)
	must(t, err)
	if _, err := store.RequestSubagentCancel(ctx, "other", task.ID); !errors.Is(err, core.ErrStoreConflict) {
		t.Fatalf("其他parent取消了task: %v", err)
	}
	if err := store.FinishSubagentCancel(ctx, task.ID); !errors.Is(err, core.ErrStoreConflict) {
		t.Fatalf("未请求取消即结束task: %v", err)
	}
	initial := subagentInitial(t, session, task)
	requested, err := store.RequestSubagentCancel(ctx, task.ParentAgentID, task.ID)
	must(t, err)
	if !requested.CancelRequested || requested.Result != nil {
		t.Fatalf("取消请求被错误记成终态: %+v", requested)
	}
	if err := store.FinishSubagentCancel(ctx, task.ID); !errors.Is(err, core.ErrSubagentBusy) {
		t.Fatalf("running execution没有阻止取消完成: %v", err)
	}
	action := newAction(initial)
	_, err = session.CommitExecution(ctx, core.ExecutionCommit{Token: initial.Token, Actions: []domain.ActionRecord{action}})
	must(t, err)
	if err := store.FinishSubagentCancel(ctx, task.ID); !errors.Is(err, core.ErrSubagentBusy) {
		t.Fatalf("pending action没有阻止取消完成: %v", err)
	}
	claim, err := store.ClaimSubagentCancelAction(ctx, action.Request.ID)
	must(t, err)
	must(t, session.RecordActionUnknown(ctx, claim.Token, domain.Failure{Kind: domain.ErrorKindUnknown, Message: "结果未知"}))
	if err := store.FinishSubagentCancel(ctx, task.ID); !errors.Is(err, core.ErrSubagentBusy) {
		t.Fatalf("unknown action没有阻止取消完成: %v", err)
	}
	if _, err := session.ClaimAction(ctx, action.Request.ID); !errors.Is(err, core.ErrActionNotReady) {
		t.Fatalf("取消后普通入口重试了unknown action: %v", err)
	}
	unknown, err := session.LoadAction(ctx, action.Request.ID)
	must(t, err)
	if unknown.Action.Status != domain.ActionStatusUnknown || unknown.Action.AttemptCount != 1 || len(unknown.Attempts) != 1 {
		t.Fatalf("取消门禁改变了unknown action或预算: %+v", unknown)
	}
	saved, err := store.LoadSubagentTask(ctx, task.ID)
	must(t, err)
	child, err := session.LoadAgent(ctx, task.ChildAgentID)
	must(t, err)
	if saved.Result != nil || child.Status != domain.AgentStatusActive {
		t.Fatalf("在途操作未确认时提前结束child: task=%+v child=%+v", saved, child)
	}
	retry, err := store.ClaimSubagentCancelAction(ctx, action.Request.ID)
	must(t, err)
	_, err = session.CompleteAction(ctx, completion(retry, "已确认"))
	must(t, err)
	must(t, store.FinishSubagentCancel(ctx, task.ID))
	must(t, store.FinishSubagentCancel(ctx, task.ID))
	finished, err := store.LoadSubagentTask(ctx, task.ID)
	must(t, err)
	child, err = session.LoadAgent(ctx, task.ChildAgentID)
	must(t, err)
	if finished.Result == nil || finished.Result.Status != domain.SubagentStatusCancelled || finished.CompletionExecutionID != "" || child.Status != domain.AgentStatusTerminated {
		t.Fatalf("取消未提交独立终态: task=%+v child=%+v", finished, child)
	}
	must(t, session.Close(ctx))
	reopened := readySession(t, backend)
	retained, err := subagentStore(t, reopened).LoadSubagentTask(ctx, task.ID)
	must(t, err)
	sameJSON(t, retained, finished)
}

func testSubagentCancelClaims(t *testing.T, backend core.RecoveryStore) {
	ctx := context.Background()
	session := readySession(t, backend)
	store := subagentStore(t, session)
	spawn := subagentSpawn(t, session, "parent", "child")
	task, err := store.SpawnSubagent(ctx, spawn)
	must(t, err)
	initial := subagentInitial(t, session, task)
	action := newAction(initial)
	_, err = session.CommitExecution(ctx, core.ExecutionCommit{Token: initial.Token, Actions: []domain.ActionRecord{action}})
	must(t, err)
	if _, err := store.ClaimSubagentCancelAction(ctx, action.Request.ID); !errors.Is(err, core.ErrStoreConflict) {
		t.Fatalf("未请求取消时专用领取成功: %v", err)
	}
	_, err = store.RequestSubagentCancel(ctx, task.ParentAgentID, task.ID)
	must(t, err)
	next := receive(t, session, task.ChildAgentID, domain.NewEvent("test", nil))
	if _, err := session.ClaimExecution(ctx, next.Delivery.Key); !errors.Is(err, core.ErrDeliveryNotReady) {
		t.Fatalf("取消后普通execution领取未被拒绝: %v", err)
	}
	delivery, err := session.LoadDelivery(ctx, next.Delivery.Key)
	must(t, err)
	sameJSON(t, delivery, next.Delivery)
	if _, err := session.LoadExecution(ctx, next.Delivery.ExecutionID); !errors.Is(err, core.ErrStoreNotFound) {
		t.Fatalf("取消门禁新增了execution或attempt: %v", err)
	}
	if _, err := session.ClaimAction(ctx, action.Request.ID); !errors.Is(err, core.ErrActionNotReady) {
		t.Fatalf("取消后普通action领取未被拒绝: %v", err)
	}
	saved, err := session.LoadAction(ctx, action.Request.ID)
	must(t, err)
	if saved.Action.Status != domain.ActionStatusPending || saved.Action.AttemptCount != 0 || len(saved.Attempts) != 0 {
		t.Fatalf("取消门禁改变了action或新增attempt: %+v", saved)
	}
	parent := newExecution(t, session, task.ParentAgentID)
	parentAction := newAction(parent)
	_, err = session.CommitExecution(ctx, core.ExecutionCommit{Token: parent.Token, Actions: []domain.ActionRecord{parentAction}})
	must(t, err)
	if _, err := store.ClaimSubagentCancelAction(ctx, parentAction.Request.ID); !errors.Is(err, core.ErrStoreConflict) {
		t.Fatalf("取消专用入口领取了parent action: %v", err)
	}
	claimAction(t, session, parentAction.Request.ID)
	cleanup, err := store.ClaimSubagentCancelAction(ctx, action.Request.ID)
	must(t, err)
	if cleanup.Token.AttemptNumber != 1 || cleanup.Record.Request.ID != action.Request.ID {
		t.Fatalf("取消专用领取没有保持action身份和预算: %+v", cleanup)
	}
	if _, err := store.ClaimSubagentCancelAction(ctx, action.Request.ID); err == nil {
		t.Fatal("取消专用入口重复领取running action")
	}
	_, err = session.CompleteAction(ctx, completion(cleanup, "已确认"))
	must(t, err)
	must(t, store.FinishSubagentCancel(ctx, task.ID))
	if _, err := store.ClaimSubagentCancelAction(ctx, action.Request.ID); !errors.Is(err, core.ErrStoreConflict) {
		t.Fatalf("任务终态后取消专用入口仍可领取: %v", err)
	}
}

func testSubagentSessionGates(t *testing.T, backend core.RecoveryStore) {
	ctx := context.Background()
	session := openSession(t, backend)
	store := subagentStore(t, session)
	writes := []operation{
		{"spawn", func() error { _, err := store.SpawnSubagent(ctx, core.SubagentSpawn{}); return err }},
		{"cancel", func() error { _, err := store.RequestSubagentCancel(ctx, "parent", "task"); return err }},
		{"finish_cancel", func() error { return store.FinishSubagentCancel(ctx, "task") }},
		{"claim_cancel", func() error { _, err := store.ClaimSubagentCancelAction(ctx, "action"); return err }},
	}
	reads := []operation{
		{"load", func() error { _, err := store.LoadSubagentTask(ctx, "task"); return err }},
		{"list", func() error { _, err := store.ListSubagentTasks(ctx); return err }},
	}
	for _, op := range writes {
		if err := op.call(); !errors.Is(err, core.ErrRecoveryRequired) {
			t.Fatalf("恢复前%s未受门禁约束: %v", op.name, err)
		}
	}
	for _, op := range reads {
		if err := op.call(); err != nil && !errors.Is(err, core.ErrStoreNotFound) {
			t.Fatalf("恢复前%s无法只读查询: %v", op.name, err)
		}
	}
	_, err := session.Recover(ctx)
	must(t, err)
	spawn := subagentSpawn(t, session, "parent", "child")
	task, err := store.SpawnSubagent(ctx, spawn)
	must(t, err)
	must(t, session.Close(ctx))
	for _, op := range append(writes, reads...) {
		if err := op.call(); !errors.Is(err, core.ErrStoreClosed) {
			t.Fatalf("关闭后%s未被拒绝: %v", op.name, err)
		}
	}
	reopened := openSession(t, backend)
	reopenedStore := subagentStore(t, reopened)
	if _, err := reopenedStore.RequestSubagentCancel(ctx, task.ParentAgentID, task.ID); !errors.Is(err, core.ErrRecoveryRequired) {
		t.Fatalf("重开后恢复门禁失效: %v", err)
	}
	_, err = reopened.Recover(ctx)
	must(t, err)
	retained, err := reopenedStore.LoadSubagentTask(ctx, task.ID)
	must(t, err)
	sameJSON(t, retained, task)
}
