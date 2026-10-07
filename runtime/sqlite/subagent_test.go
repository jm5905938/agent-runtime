package sqlite

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"agent-runtime/core"
	"agent-runtime/domain"
)

func subagentFixture(t *testing.T) (*Session, *core.ActionClaim, core.SubagentSpawn) {
	t.Helper()
	ctx := context.Background()
	session, _ := newTestSession(t)
	parent := domain.AgentInstance{ID: "subagent-parent", Name: "parent", Definition: domain.DefinitionRef{ID: "main", Version: "1"},
		Status: domain.AgentStatusActive, State: map[string]any{}}
	if err := session.CreateAgent(ctx, parent); err != nil {
		t.Fatal(err)
	}
	claim := subagentActionFixture(t, session, parent.ID, core.SubagentSpawnActionType, domain.RecoveryPolicySafeRetry)
	spawn := core.SubagentSpawn{Token: claim.Token,
		Child: domain.AgentInstance{ID: "subagent-child", Name: "child", Definition: domain.DefinitionRef{ID: "main", Version: "1"},
			Status: domain.AgentStatusActive, State: map[string]any{"initial": true}},
		Event: domain.Event{ID: "subagent-initial", Type: "main.request", Payload: map[string]any{"message": "完成子任务"}, CreatedAt: time.Now().UTC()}}
	return session, claim, spawn
}

func subagentActionFixture(t *testing.T, session *Session, agentID domain.ID, actionType string, policy domain.RecoveryPolicy) *core.ActionClaim {
	t.Helper()
	ctx := context.Background()
	received, err := session.ReceiveEvent(ctx, agentID, domain.NewEvent("main.request", map[string]any{"message": "准备action"}))
	if err != nil {
		t.Fatal(err)
	}
	execution, err := session.ClaimExecution(ctx, received.Delivery.Key)
	if err != nil {
		t.Fatal(err)
	}
	request := domain.NewAction(actionType, map[string]any{"task": "完成子任务"})
	request.BindExecution(execution.Token.ExecutionID)
	action := domain.ActionRecord{Request: request, AgentID: agentID, HandlerVersion: "1", RecoveryPolicy: policy,
		MaxAttempts: 3, Status: domain.ActionStatusPending, ResultEventID: domain.NewEvent("action.result", nil).ID}
	if _, err := session.CommitExecution(ctx, core.ExecutionCommit{Token: execution.Token, Actions: []domain.ActionRecord{action}}); err != nil {
		t.Fatal(err)
	}
	claimed, err := session.ClaimAction(ctx, request.ID)
	if err != nil {
		t.Fatal(err)
	}
	return claimed
}

func spawnSubagentFixture(t *testing.T, session *Session, spawn core.SubagentSpawn) *domain.SubagentTask {
	t.Helper()
	task, err := session.SpawnSubagent(context.Background(), spawn)
	if err != nil {
		t.Fatal(err)
	}
	return task
}

func claimSubagentInitial(t *testing.T, session *Session, task *domain.SubagentTask) *core.ExecutionClaim {
	t.Helper()
	claim, err := session.ClaimExecution(context.Background(), domain.DeliveryKey{AgentID: task.ChildAgentID, EventID: task.InitialEventID})
	if err != nil {
		t.Fatal(err)
	}
	return claim
}

func subagentCompletion(claim *core.ExecutionClaim) core.ExecutionCommit {
	return core.ExecutionCommit{Token: claim.Token, StateUpdate: map[string]any{"finished": true},
		TaskResult: &domain.SubagentResult{Status: domain.SubagentStatusSucceeded, Output: map[string]any{"text": "子任务完成"}}}
}

func reopenSubagentDatabase(t *testing.T, session *Session) *Session {
	t.Helper()
	if err := session.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	path := session.backend.Path()
	if err := session.backend.Close(); err != nil {
		t.Fatal(err)
	}
	backend, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = backend.Close() })
	return openTestSession(t, backend)
}

func TestSubagentSpawnRollsBackAllBusinessWrites(t *testing.T) {
	ctx := context.Background()
	session, _, spawn := subagentFixture(t)
	if _, err := session.backend.db.Exec(`CREATE TRIGGER fail_subagent_spawn BEFORE INSERT ON subagent_tasks
		BEGIN SELECT CASE WHEN
			(SELECT COUNT(*) FROM agents WHERE id = NEW.child_agent_id) = 1 AND
			(SELECT COUNT(*) FROM events WHERE id = NEW.initial_event_id) = 1 AND
			(SELECT COUNT(*) FROM deliveries WHERE agent_id = NEW.child_agent_id AND event_id = NEW.initial_event_id) = 1
			THEN RAISE(ABORT, '创建写入已发生') ELSE RAISE(ABORT, '创建写入未发生') END; END`); err != nil {
		t.Fatal(err)
	}
	before := recoveryDatabaseSnapshot(t, session.backend)
	if _, err := session.SpawnSubagent(ctx, spawn); err == nil || !strings.Contains(err.Error(), "创建写入已发生") {
		t.Fatalf("未在创建中途注入失败: %v", err)
	}
	if after := recoveryDatabaseSnapshot(t, session.backend); !reflect.DeepEqual(before, after) {
		t.Fatal("subagent创建失败留下部分业务写入")
	}
	if _, err := session.backend.db.Exec(`DROP TRIGGER fail_subagent_spawn`); err != nil {
		t.Fatal(err)
	}
	task := spawnSubagentFixture(t, session, spawn)
	duplicate := spawn
	duplicate.Event.CreatedAt = duplicate.Event.CreatedAt.Add(time.Hour)
	if saved, err := session.SpawnSubagent(ctx, duplicate); err != nil || !reflect.DeepEqual(task, saved) {
		t.Fatalf("重复创建没有返回原task: %+v %v", saved, err)
	}
	duplicate.Event.Payload = map[string]any{"message": "不同任务"}
	if _, err := session.SpawnSubagent(ctx, duplicate); !errors.Is(err, core.ErrStoreConflict) {
		t.Fatalf("重复创建接受不同输入: %v", err)
	}
	duplicate = spawn
	duplicate.Token.AttemptNumber++
	if _, err := session.SpawnSubagent(ctx, duplicate); !errors.Is(err, core.ErrStoreStaleClaim) {
		t.Fatalf("过期token创建了subagent: %v", err)
	}
}

func TestSubagentCompletionRollsBackAndPersistsAcrossReopen(t *testing.T) {
	ctx := context.Background()
	session, _, spawn := subagentFixture(t)
	task := spawnSubagentFixture(t, session, spawn)
	claim := claimSubagentInitial(t, session, task)
	commit := subagentCompletion(claim)
	if _, err := session.backend.db.Exec(`CREATE TRIGGER fail_subagent_finish BEFORE UPDATE ON agents
		WHEN NEW.id = 'subagent-child' AND NEW.status = 'terminated'
		BEGIN SELECT CASE WHEN
			(SELECT result_json IS NOT NULL FROM subagent_tasks WHERE child_agent_id = NEW.id) AND
			(SELECT status FROM executions WHERE agent_id = NEW.id) = 'completed' AND
			(SELECT status FROM deliveries WHERE agent_id = NEW.id) = 'completed' AND NEW.state_version = '1'
			THEN RAISE(ABORT, '完成写入已发生') ELSE RAISE(ABORT, '完成写入未发生') END; END`); err != nil {
		t.Fatal(err)
	}
	before := recoveryDatabaseSnapshot(t, session.backend)
	if _, err := session.CommitExecution(ctx, commit); err == nil || !strings.Contains(err.Error(), "完成写入已发生") {
		t.Fatalf("未在终止前注入失败: %v", err)
	}
	if after := recoveryDatabaseSnapshot(t, session.backend); !reflect.DeepEqual(before, after) {
		t.Fatal("subagent完成失败留下部分业务写入")
	}
	if _, err := session.backend.db.Exec(`DROP TRIGGER fail_subagent_finish`); err != nil {
		t.Fatal(err)
	}
	result, err := session.CommitExecution(ctx, commit)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result.TaskResult, commit.TaskResult) {
		t.Fatalf("execution未保留task结果: %+v", result)
	}
	agent, err := session.LoadAgent(ctx, task.ChildAgentID)
	if err != nil || agent.Status != domain.AgentStatusTerminated || agent.StateVersion != 1 || agent.State["finished"] != true {
		t.Fatalf("child终态与状态没有一起提交: %+v %v", agent, err)
	}
	saved, err := session.LoadSubagentTask(ctx, task.ID)
	if err != nil || saved.CompletionExecutionID != claim.Token.ExecutionID || !reflect.DeepEqual(saved.Result, commit.TaskResult) {
		t.Fatalf("task完成记录错误: %+v %v", saved, err)
	}
	saved.Result.Output["text"] = "修改查询结果"
	saved, err = session.LoadSubagentTask(ctx, task.ID)
	if err != nil || saved.Result.Output["text"] != "子任务完成" {
		t.Fatalf("查询结果不是独立快照: %+v %v", saved, err)
	}
	reopened := reopenSubagentDatabase(t, session)
	if _, err := reopened.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	retained, err := reopened.LoadSubagentTask(ctx, task.ID)
	if err != nil || !reflect.DeepEqual(retained, saved) {
		t.Fatalf("重启改变了task终态: %+v %v", retained, err)
	}
	if _, err := reopened.CommitExecution(ctx, commit); !errors.Is(err, core.ErrStoreStaleClaim) {
		t.Fatalf("旧完成token没有失效: %v", err)
	}
}

func TestSubagentCreationAndRunningExecutionRecoverWithStableIDs(t *testing.T) {
	ctx := context.Background()
	session, source, spawn := subagentFixture(t)
	task := spawnSubagentFixture(t, session, spawn)
	oldExecution := claimSubagentInitial(t, session, task)
	reopened := reopenSubagentDatabase(t, session)
	report, err := reopened.Recover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.RequeuedDeliveries) != 1 || report.RequeuedDeliveries[0].AgentID != task.ChildAgentID {
		t.Fatalf("child执行未重新排队: %+v", report)
	}
	if _, err := reopened.SpawnSubagent(ctx, spawn); !errors.Is(err, core.ErrStoreStaleClaim) {
		t.Fatalf("旧spawn token没有失效: %v", err)
	}
	if _, err := reopened.CommitExecution(ctx, subagentCompletion(oldExecution)); !errors.Is(err, core.ErrStoreStaleClaim) {
		t.Fatalf("旧child token没有失效: %v", err)
	}
	retried, err := reopened.ClaimAction(ctx, source.Token.ActionID)
	if err != nil {
		t.Fatal(err)
	}
	spawn.Token = retried.Token
	if recovered, err := reopened.SpawnSubagent(ctx, spawn); err != nil || !reflect.DeepEqual(recovered, task) {
		t.Fatalf("spawn重试改变了任务身份: %+v %v", recovered, err)
	}
	newExecution := claimSubagentInitial(t, reopened, task)
	if newExecution.Token.ExecutionID != oldExecution.Token.ExecutionID || newExecution.Token.AttemptID == oldExecution.Token.AttemptID {
		t.Fatalf("child恢复没有保留execution并替换attempt: %+v", newExecution.Token)
	}
	if _, err := reopened.CommitExecution(ctx, subagentCompletion(newExecution)); err != nil {
		t.Fatal(err)
	}
}

func TestSubagentFailureAndInterruptionHaveDistinctTaskOutcomes(t *testing.T) {
	for _, interrupted := range []bool{false, true} {
		t.Run(fmt.Sprint(interrupted), func(t *testing.T) {
			ctx := context.Background()
			session, _, spawn := subagentFixture(t)
			task := spawnSubagentFixture(t, session, spawn)
			claim := claimSubagentInitial(t, session, task)
			failure := domain.Failure{Kind: domain.ErrorKindBusiness, Message: "任务失败"}
			if interrupted {
				failure.Kind = domain.ErrorKindInterrupted
			}
			if err := session.FailExecution(ctx, core.ExecutionFailure{Token: claim.Token, Failure: failure, Interrupted: interrupted}); err != nil {
				t.Fatal(err)
			}
			saved, err := session.LoadSubagentTask(ctx, task.ID)
			if err != nil {
				t.Fatal(err)
			}
			agent, err := session.LoadAgent(ctx, task.ChildAgentID)
			if err != nil {
				t.Fatal(err)
			}
			if interrupted {
				if saved.Result != nil || agent.Status != domain.AgentStatusActive {
					t.Fatalf("中断被记成task终态: %+v %+v", saved, agent)
				}
				if err := session.RequeueDelivery(ctx, claim.Token.Delivery); err != nil {
					t.Fatal(err)
				}
			} else if saved.Result == nil || saved.Result.Status != domain.SubagentStatusFailed || saved.CompletionExecutionID != claim.Token.ExecutionID || agent.Status != domain.AgentStatusTerminated {
				t.Fatalf("明确失败没有结束task与child: %+v %+v", saved, agent)
			} else {
				before := recoveryDatabaseSnapshot(t, session.backend)
				if err := session.RequeueDelivery(ctx, claim.Token.Delivery); !errors.Is(err, core.ErrStoreConflict) {
					t.Fatalf("终态child被重新排队: %v", err)
				}
				if after := recoveryDatabaseSnapshot(t, session.backend); !reflect.DeepEqual(before, after) {
					t.Fatal("被拒绝的终态重排改变了持久记录")
				}
				if _, err := reopenSubagentDatabase(t, session).Recover(ctx); err != nil {
					t.Fatalf("被拒绝的终态重排破坏恢复: %v", err)
				}
			}
		})
	}
}

func TestSubagentCancelWaitsForRunningExecutionAndUnknownAction(t *testing.T) {
	ctx := context.Background()
	session, _, spawn := subagentFixture(t)
	task := spawnSubagentFixture(t, session, spawn)
	claim := claimSubagentInitial(t, session, task)
	if _, err := session.RequestSubagentCancel(ctx, "another-parent", task.ID); !errors.Is(err, core.ErrStoreConflict) {
		t.Fatalf("其他parent取消了任务: %v", err)
	}
	if err := session.FinishSubagentCancel(ctx, task.ID); !errors.Is(err, core.ErrStoreConflict) {
		t.Fatalf("没有请求也接受取消: %v", err)
	}
	if _, err := session.RequestSubagentCancel(ctx, task.ParentAgentID, task.ID); err != nil {
		t.Fatal(err)
	}
	if err := session.FinishSubagentCancel(ctx, task.ID); !errors.Is(err, core.ErrSubagentBusy) {
		t.Fatalf("在途execution没有阻止取消完成: %v", err)
	}
	request := domain.NewAction("model.generate", nil)
	request.BindExecution(claim.Token.ExecutionID)
	action := domain.ActionRecord{Request: request, AgentID: task.ChildAgentID, HandlerVersion: "1", RecoveryPolicy: domain.RecoveryPolicyManual,
		MaxAttempts: 1, Status: domain.ActionStatusPending, ResultEventID: domain.NewEvent("action.result", nil).ID}
	if _, err := session.CommitExecution(ctx, core.ExecutionCommit{Token: claim.Token, Actions: []domain.ActionRecord{action}}); err != nil {
		t.Fatal(err)
	}
	if err := session.FinishSubagentCancel(ctx, task.ID); !errors.Is(err, core.ErrSubagentBusy) {
		t.Fatalf("pending action没有阻止取消完成: %v", err)
	}
	actionClaim, err := session.ClaimSubagentCancelAction(ctx, request.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := session.RecordActionUnknown(ctx, actionClaim.Token, domain.Failure{Kind: domain.ErrorKindUnknown, Message: "外部结果未知"}); err != nil {
		t.Fatal(err)
	}
	if err := session.FinishSubagentCancel(ctx, task.ID); !errors.Is(err, core.ErrSubagentBusy) {
		t.Fatalf("unknown action没有阻止取消完成: %v", err)
	}
	event := domain.Event{ID: domain.ID(fmt.Sprintf("action-resolution/%x", sha256.Sum256([]byte(request.ID)))), Type: core.ActionResolutionEventType,
		Payload: map[string]any{"action_id": string(request.ID), "action_type": request.Type, "execution_id": string(*request.ExecutionID), "decision": "abandon", "reason": "取消子任务"}, CreatedAt: time.Now().UTC()}
	if _, err := session.ReceiveEvent(ctx, task.ChildAgentID, event); err != nil {
		t.Fatal(err)
	}
	if err := session.FinishSubagentCancel(ctx, task.ID); err != nil {
		t.Fatal(err)
	}
	saved, err := session.LoadSubagentTask(ctx, task.ID)
	if err != nil || saved.Result == nil || saved.Result.Status != domain.SubagentStatusCancelled || saved.CompletionExecutionID != "" {
		t.Fatalf("取消没有保存正确终态: %+v %v", saved, err)
	}
	if err := session.FinishSubagentCancel(ctx, task.ID); err != nil {
		t.Fatalf("重复取消完成不幂等: %v", err)
	}
	if err := session.Close(ctx); err != nil {
		t.Fatal(err)
	}
	reopened := openTestSession(t, session.backend)
	if _, err := reopened.Recover(ctx); err != nil {
		t.Fatalf("取消与人工放弃结果不能恢复: %v", err)
	}
}

func TestSubagentCancelUnknownClaimRetainsPolicyBudgetAndResult(t *testing.T) {
	for _, scenario := range []struct {
		name        string
		policy      domain.RecoveryPolicy
		maxAttempts uint64
		allowed     bool
	}{
		{"safe_retry", domain.RecoveryPolicySafeRetry, 2, true},
		{"exhausted", domain.RecoveryPolicySafeRetry, 1, false},
		{"manual", domain.RecoveryPolicyManual, 3, false},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			ctx := context.Background()
			session, _, spawn := subagentFixture(t)
			task := spawnSubagentFixture(t, session, spawn)
			initial := claimSubagentInitial(t, session, task)
			actionType := "echo"
			if scenario.policy == domain.RecoveryPolicyManual {
				actionType = "model.generate"
			}
			request := domain.NewAction(actionType, nil)
			request.BindExecution(initial.Token.ExecutionID)
			action := domain.ActionRecord{Request: request, AgentID: task.ChildAgentID, HandlerVersion: "1", RecoveryPolicy: scenario.policy,
				MaxAttempts: scenario.maxAttempts, Status: domain.ActionStatusPending, ResultEventID: domain.NewEvent("action.result", nil).ID}
			if _, err := session.CommitExecution(ctx, core.ExecutionCommit{Token: initial.Token, Actions: []domain.ActionRecord{action}}); err != nil {
				t.Fatal(err)
			}
			first, err := session.ClaimAction(ctx, request.ID)
			if err != nil {
				t.Fatal(err)
			}
			if err := session.RecordActionUnknown(ctx, first.Token, domain.Failure{Kind: domain.ErrorKindUnknown, Message: "结果提交前中断"}); err != nil {
				t.Fatal(err)
			}
			if _, err := session.RequestSubagentCancel(ctx, task.ParentAgentID, task.ID); err != nil {
				t.Fatal(err)
			}
			before := recoveryDatabaseSnapshot(t, session.backend)
			confirmed, err := session.ClaimSubagentCancelAction(ctx, request.ID)
			if !scenario.allowed {
				if !errors.Is(err, core.ErrStoreConflict) {
					t.Fatalf("取消领取绕过重试策略或预算: %+v %v", confirmed, err)
				}
				if after := recoveryDatabaseSnapshot(t, session.backend); !reflect.DeepEqual(before, after) {
					t.Fatal("被拒绝的取消领取留下新attempt")
				}
				if err := session.FinishSubagentCancel(ctx, task.ID); !errors.Is(err, core.ErrSubagentBusy) {
					t.Fatalf("未确认unknown被记成取消完成: %v", err)
				}
			} else {
				if err != nil || confirmed.Token.AttemptNumber != 2 {
					t.Fatalf("取消无法领取safe_retry未知结果: %+v %v", confirmed, err)
				}
				if _, err := session.CompleteAction(ctx, actionRegressionCompletion(confirmed)); err != nil {
					t.Fatal(err)
				}
				if err := session.FinishSubagentCancel(ctx, task.ID); err != nil {
					t.Fatal(err)
				}
			}
			retained, err := session.LoadSubagentTask(ctx, task.ID)
			if err != nil {
				t.Fatal(err)
			}
			reopened := reopenSubagentDatabase(t, session)
			if _, err := reopened.Recover(ctx); err != nil {
				t.Fatal(err)
			}
			if saved, err := reopened.LoadSubagentTask(ctx, task.ID); err != nil || !reflect.DeepEqual(saved, retained) {
				t.Fatalf("恢复改变取消与结果记录: %+v %v", saved, err)
			}
			stored, err := reopened.LoadAction(ctx, request.ID)
			if err != nil {
				t.Fatal(err)
			}
			if scenario.allowed {
				if retained.Result == nil || retained.Result.Status != domain.SubagentStatusCancelled || stored.Action.Result == nil ||
					stored.Action.Result.Output["text"] != "hello" || stored.Action.AttemptCount != 2 || len(stored.Attempts) != 2 {
					t.Fatalf("取消丢失已确认action结果: %+v %+v", retained, stored)
				}
			} else if retained.Result != nil || stored.Action.Status != domain.ActionStatusUnknown || stored.Action.AttemptCount != 1 || len(stored.Attempts) != 1 {
				t.Fatalf("取消改变未确认unknown或预算: %+v %+v", retained, stored)
			}
		})
	}
}

func TestSubagentNaturalCompletionMayWinAfterCancelRequest(t *testing.T) {
	ctx := context.Background()
	session, _, spawn := subagentFixture(t)
	task := spawnSubagentFixture(t, session, spawn)
	claim := claimSubagentInitial(t, session, task)
	if _, err := session.RequestSubagentCancel(ctx, task.ParentAgentID, task.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := session.CommitExecution(ctx, subagentCompletion(claim)); err != nil {
		t.Fatal(err)
	}
	if err := session.FinishSubagentCancel(ctx, task.ID); err != nil {
		t.Fatal(err)
	}
	saved, err := session.RequestSubagentCancel(ctx, task.ParentAgentID, task.ID)
	if err != nil || saved.Result == nil || saved.Result.Status != domain.SubagentStatusSucceeded {
		t.Fatalf("取消覆盖了已经提交的自然结果: %+v %v", saved, err)
	}
}

func TestSubagentCompletionRejectsUnfinishedActionAndRecursiveSpawn(t *testing.T) {
	ctx := context.Background()
	session, _, spawn := subagentFixture(t)
	task := spawnSubagentFixture(t, session, spawn)
	initial := claimSubagentInitial(t, session, task)
	request := domain.NewAction(core.SubagentSpawnActionType, nil)
	request.BindExecution(initial.Token.ExecutionID)
	action := domain.ActionRecord{Request: request, AgentID: task.ChildAgentID, HandlerVersion: "1", RecoveryPolicy: domain.RecoveryPolicySafeRetry,
		MaxAttempts: 3, Status: domain.ActionStatusPending, ResultEventID: domain.NewEvent("action.result", nil).ID}
	commit := subagentCompletion(initial)
	commit.Actions = []domain.ActionRecord{action}
	if _, err := session.CommitExecution(ctx, commit); !errors.Is(err, core.ErrStoreConflict) {
		t.Fatalf("完成task时创建了新action: %v", err)
	}
	if _, err := session.CommitExecution(ctx, core.ExecutionCommit{Token: initial.Token, Actions: []domain.ActionRecord{action}}); err != nil {
		t.Fatal(err)
	}
	claim, err := session.ClaimAction(ctx, request.ID)
	if err != nil {
		t.Fatal(err)
	}
	nested := spawn
	nested.Token = claim.Token
	nested.Child.ID = "nested-child"
	nested.Event.ID = "nested-event"
	if _, err := session.SpawnSubagent(ctx, nested); !errors.Is(err, core.ErrStoreConflict) {
		t.Fatalf("child递归创建了subagent: %v", err)
	}
	received, err := session.ReceiveEvent(ctx, task.ChildAgentID, domain.NewEvent("main.request", nil))
	if err != nil {
		t.Fatal(err)
	}
	final, err := session.ClaimExecution(ctx, received.Delivery.Key)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.CommitExecution(ctx, subagentCompletion(final)); !errors.Is(err, core.ErrSubagentBusy) {
		t.Fatalf("未完成action没有阻止task结束: %v", err)
	}
}

func TestSubagentFailureAndCancelRollbackAfterTaskWrite(t *testing.T) {
	for _, cancel := range []bool{false, true} {
		t.Run(fmt.Sprint(cancel), func(t *testing.T) {
			ctx := context.Background()
			session, _, spawn := subagentFixture(t)
			task := spawnSubagentFixture(t, session, spawn)
			var claim *core.ExecutionClaim
			if cancel {
				if _, err := session.RequestSubagentCancel(ctx, task.ParentAgentID, task.ID); err != nil {
					t.Fatal(err)
				}
			} else {
				claim = claimSubagentInitial(t, session, task)
			}
			if _, err := session.backend.db.Exec(`CREATE TRIGGER fail_subagent_terminal BEFORE UPDATE ON agents
				WHEN NEW.id = 'subagent-child' AND NEW.status = 'terminated' AND
				(SELECT result_json IS NOT NULL FROM subagent_tasks WHERE child_agent_id = NEW.id)
				BEGIN SELECT RAISE(ABORT, 'task终态已写入'); END`); err != nil {
				t.Fatal(err)
			}
			before := recoveryDatabaseSnapshot(t, session.backend)
			var err error
			if cancel {
				err = session.FinishSubagentCancel(ctx, task.ID)
			} else {
				err = session.FailExecution(ctx, core.ExecutionFailure{Token: claim.Token, Failure: domain.Failure{Kind: domain.ErrorKindBusiness, Message: "任务失败"}})
			}
			if err == nil || !strings.Contains(err.Error(), "task终态已写入") {
				t.Fatalf("没有在task终态后注入故障: %v", err)
			}
			if after := recoveryDatabaseSnapshot(t, session.backend); !reflect.DeepEqual(before, after) {
				t.Fatal("终止失败留下部分task或execution写入")
			}
		})
	}
}

func TestSubagentRecoveryRejectsDamagedTaskWithoutPartialRecovery(t *testing.T) {
	for _, damage := range []string{"same_parent_child", "wrong_initial_event", "completion_without_result", "terminated_without_result", "result_without_termination", "invalid_result", "invalid_source_type"} {
		t.Run(damage, func(t *testing.T) {
			ctx := context.Background()
			session, source, spawn := subagentFixture(t)
			task := spawnSubagentFixture(t, session, spawn)
			childClaim := claimSubagentInitial(t, session, task)
			var query string
			var args []any
			switch damage {
			case "same_parent_child":
				query = `UPDATE subagent_tasks SET child_agent_id = parent_agent_id WHERE id = ?`
				args = []any{string(task.ID)}
			case "wrong_initial_event":
				query = `UPDATE subagent_tasks SET initial_event_id = (SELECT event_id FROM executions WHERE id = ?) WHERE id = ?`
				args = []any{string(*source.Record.Request.ExecutionID), string(task.ID)}
			case "completion_without_result":
				query = `UPDATE subagent_tasks SET completion_execution_id = ? WHERE id = ?`
				args = []any{string(childClaim.Token.ExecutionID), string(task.ID)}
			case "terminated_without_result":
				query = `UPDATE agents SET status = 'terminated' WHERE id = ?`
				args = []any{string(task.ChildAgentID)}
			case "result_without_termination":
				query = `UPDATE subagent_tasks SET result_json = '{"status":"succeeded","output":{}}' WHERE id = ?`
				args = []any{string(task.ID)}
			case "invalid_result":
				query = `UPDATE subagent_tasks SET result_json = 'null' WHERE id = ?`
				args = []any{string(task.ID)}
			case "invalid_source_type":
				query = `UPDATE actions SET request_json = json_set(request_json, '$.type', 'echo') WHERE id = ?`
				args = []any{string(task.ID)}
			}
			if _, err := session.backend.db.ExecContext(ctx, query, args...); err != nil {
				t.Fatal(err)
			}
			if err := session.Close(ctx); err != nil {
				t.Fatal(err)
			}
			reopened := openTestSession(t, session.backend)
			before := recoveryDatabaseSnapshot(t, session.backend)
			if _, err := reopened.Recover(ctx); err == nil {
				t.Fatal("损坏task通过了启动恢复")
			}
			if after := recoveryDatabaseSnapshot(t, session.backend); !reflect.DeepEqual(before, after) {
				t.Fatal("task恢复校验失败留下部分转换")
			}
			if _, err := reopened.RequestSubagentCancel(ctx, task.ParentAgentID, task.ID); !errors.Is(err, core.ErrRecoveryRequired) {
				t.Fatalf("恢复失败后开启了task写入口: %v", err)
			}
		})
	}
}

func TestSubagentCrashHelper(t *testing.T) {
	path := os.Getenv("AGENT_RUNTIME_SUBAGENT_CRASH_DB")
	if path == "" {
		return
	}
	ctx := context.Background()
	backend, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	session, err := backend.OpenSession(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := session.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	parent := domain.AgentInstance{ID: "crash-parent", Definition: domain.DefinitionRef{ID: "main", Version: "1"}, Status: domain.AgentStatusActive}
	if err := session.CreateAgent(ctx, parent); err != nil {
		t.Fatal(err)
	}
	source := subagentActionFixture(t, session, parent.ID, core.SubagentSpawnActionType, domain.RecoveryPolicySafeRetry)
	spawn := core.SubagentSpawn{Token: source.Token,
		Child: domain.AgentInstance{ID: "crash-child", Definition: domain.DefinitionRef{ID: "main", Version: "1"}, Status: domain.AgentStatusActive},
		Event: domain.Event{ID: "crash-initial", Type: "main.request", CreatedAt: time.Now().UTC()}}
	task := spawnSubagentFixture(t, session, spawn)
	_ = claimSubagentInitial(t, session, task)
	os.Exit(0)
}

func TestSubagentAbruptProcessExitRecoversOriginalTask(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "crash.db")
	command := exec.Command(os.Args[0], "-test.run=^TestSubagentCrashHelper$")
	command.Env = append(os.Environ(), "AGENT_RUNTIME_SUBAGENT_CRASH_DB="+path)
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("子进程未保存任务: %v %s", err, output)
	}
	backend, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = backend.Close() })
	session := openTestSession(t, backend)
	report, err := session.Recover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	tasks, err := session.ListSubagentTasks(ctx)
	if err != nil || len(tasks) != 1 {
		t.Fatalf("进程退出丢失任务: %+v %v", tasks, err)
	}
	task := tasks[0]
	if task.ParentAgentID != "crash-parent" || task.ChildAgentID != "crash-child" || task.InitialEventID != "crash-initial" || task.Result != nil ||
		len(report.RequeuedDeliveries) != 1 || len(report.UnknownActions) != 1 || report.UnknownActions[0] != task.ID {
		t.Fatalf("崩溃后恢复结果错误: %+v %+v", task, report)
	}
	spawnClaim, err := session.ClaimAction(ctx, task.ID)
	if err != nil || spawnClaim.Token.AttemptNumber != 2 {
		t.Fatalf("spawn尝试未恢复: %+v %v", spawnClaim, err)
	}
	event, err := session.LoadEvent(ctx, task.InitialEventID)
	if err != nil {
		t.Fatal(err)
	}
	spawn := core.SubagentSpawn{Token: spawnClaim.Token,
		Child: domain.AgentInstance{ID: task.ChildAgentID, Definition: domain.DefinitionRef{ID: "main", Version: "1"}, Status: domain.AgentStatusActive}, Event: *event}
	if retained, err := session.SpawnSubagent(ctx, spawn); err != nil || !reflect.DeepEqual(retained, &task) {
		t.Fatalf("spawn重试生成了不同任务: %+v %v", retained, err)
	}
	claim := claimSubagentInitial(t, session, &task)
	if claim.Attempt.Number != 2 {
		t.Fatalf("child尝试未恢复: %+v", claim)
	}
	if _, err := session.CommitExecution(ctx, subagentCompletion(claim)); err != nil {
		t.Fatal(err)
	}
	retained, err := session.LoadSubagentTask(ctx, task.ID)
	if err != nil || retained.Result == nil || retained.Result.Status != domain.SubagentStatusSucceeded {
		t.Fatalf("恢复后不能完成原任务: %+v %v", retained, err)
	}
}

func TestSubagentWaitResultHasIndependentIdempotentTransaction(t *testing.T) {
	ctx := context.Background()
	session, spawnClaim, spawn := subagentFixture(t)
	task := spawnSubagentFixture(t, session, spawn)
	spawnCompletion := actionRegressionCompletion(spawnClaim)
	handle := map[string]any{"task_id": string(task.ID), "child_id": string(task.ChildAgentID)}
	spawnCompletion.Result.Output = handle
	spawnCompletion.Event.Payload["result"] = handle
	if _, err := session.CompleteAction(ctx, spawnCompletion); err != nil {
		t.Fatal(err)
	}
	childClaim := claimSubagentInitial(t, session, task)
	if _, err := session.CommitExecution(ctx, subagentCompletion(childClaim)); err != nil {
		t.Fatal(err)
	}
	childBefore, err := session.LoadAgent(ctx, task.ChildAgentID)
	if err != nil {
		t.Fatal(err)
	}
	taskBefore, err := session.LoadSubagentTask(ctx, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	waitClaim := subagentActionFixture(t, session, task.ParentAgentID, core.SubagentWaitActionType, domain.RecoveryPolicySafeRetry)
	waitCompletion := actionRegressionCompletion(waitClaim)
	output := map[string]any{"task_id": string(task.ID), "child_id": string(task.ChildAgentID), "status": "succeeded", "result": taskBefore.Result.Output}
	waitCompletion.Result.Output = output
	waitCompletion.Event.Payload["result"] = output
	if _, err := session.backend.db.Exec(fmt.Sprintf(`CREATE TRIGGER fail_subagent_wait BEFORE INSERT ON deliveries
		WHEN NEW.event_id = '%s'
		BEGIN SELECT CASE WHEN
			(SELECT status FROM actions WHERE result_event_id = NEW.event_id) = 'succeeded' AND
			(SELECT COUNT(*) FROM events WHERE id = NEW.event_id) = 1
			THEN RAISE(ABORT, 'wait结果已写入') ELSE RAISE(ABORT, 'wait结果未写入') END; END`, waitClaim.Record.ResultEventID)); err != nil {
		t.Fatal(err)
	}
	if _, err := session.CompleteAction(ctx, waitCompletion); err == nil || !strings.Contains(err.Error(), "wait结果已写入") {
		t.Fatalf("wait结果未在投递前注入故障: %v", err)
	}
	childAfter, err := session.LoadAgent(ctx, task.ChildAgentID)
	if err != nil {
		t.Fatal(err)
	}
	taskAfter, err := session.LoadSubagentTask(ctx, task.ID)
	if err != nil || !reflect.DeepEqual(taskBefore, taskAfter) || !reflect.DeepEqual(childBefore, childAfter) {
		t.Fatalf("wait提交失败影响了已完成child/task: %+v %+v %v", taskAfter, childAfter, err)
	}
	if _, err := session.backend.db.Exec(`DROP TRIGGER fail_subagent_wait`); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, err := session.CompleteAction(ctx, waitCompletion); err != nil {
			t.Fatalf("wait结果重复提交失败: %v", err)
		}
	}
	var deliveries int
	if err := session.backend.db.QueryRow(`SELECT COUNT(*) FROM deliveries WHERE event_id = ?`, string(waitClaim.Record.ResultEventID)).Scan(&deliveries); err != nil || deliveries != 1 {
		t.Fatalf("wait结果重复投递: %d %v", deliveries, err)
	}
	reopened := reopenSubagentDatabase(t, session)
	if _, err := reopened.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.CompleteAction(ctx, waitCompletion); err != nil {
		t.Fatalf("恢复后wait最终结果不幂等: %v", err)
	}
}
