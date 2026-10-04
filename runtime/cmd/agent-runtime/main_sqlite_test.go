package main

import (
	"agent-runtime/core"
	"agent-runtime/domain"
	pythonrunner "agent-runtime/python"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

func TestMainSQLiteCommandsAcrossProcesses(t *testing.T) {
	pythonArgs := chatTestPython(t)
	binary := filepath.Join(t.TempDir(), "agent-runtime")
	if runtime.GOOS == "windows" {
		binary += ".exe"
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if output, err := exec.CommandContext(ctx, "go", "build", "-o", binary, ".").CombinedOutput(); err != nil {
		t.Fatalf("构建main持久化cli: %v\n%s", err, output)
	}
	p := sqliteCommandProcess{binary: binary, python: pythonArgs[1], source: pythonArgs[3]}
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method != http.MethodPost || r.URL.Path != "/v1/chat/completions" || r.Header.Get("Authorization") != "Bearer cli-test-secret" {
			t.Error("持久main模型请求方法、路径或认证错误")
		}
		var request struct {
			Model    string                           `json:"model"`
			Messages []struct{ Role, Content string } `json:"messages"`
			Stream   bool                             `json:"stream"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil || request.Model != "test-model" || request.Stream ||
			len(request.Messages) != 1 || request.Messages[0].Role != "user" {
			t.Error("持久main没有构造有效的模型输入")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]any{
			"choices": []any{map[string]any{"message": map[string]any{"role": "assistant", "content": "回复：" + request.Messages[0].Content}}},
		}); err != nil {
			t.Error(err)
		}
	}))
	defer server.Close()
	config := chatTestConfig(t, server.URL)
	missingConfig := filepath.Join(t.TempDir(), "missing.env")

	t.Run("round_trip_duplicate_and_second_request", func(t *testing.T) {
		directory := filepath.Join(t.TempDir(), "持久 main # %")
		created, _ := p.call(t, directory, 0, "init", "--definition", "main", "--name", "持久main")
		agentID := string(created.Agent.ID)
		definition := domain.DefinitionRef{ID: "main", Version: "1"}
		if created.Agent.Definition != definition || created.Agent.Name != "持久main" {
			t.Fatalf("创建了错误的定义: %+v", created.Agent)
		}
		first, _ := p.call(t, directory, 0, "submit", "--agent", agentID, "--event-id", "first", "--message", "你好\nhello")
		duplicate, _ := p.call(t, directory, 0, "submit", "--agent", agentID, "--event-id", "first", "--message", "你好\nhello")
		if first.Submission.Duplicate || !duplicate.Submission.Duplicate || first.Submission.Delivery != duplicate.Submission.Delivery {
			t.Fatal("main重复事件改变了delivery身份")
		}
		_, stderr := p.call(t, directory, 1, "submit", "--agent", agentID, "--event-id", "first", "--message", "different")
		assertCommandError(t, stderr, "conflict", 1)
		pending, _ := p.call(t, directory, 0, "status", "--agent", agentID, "--python", "/missing-python")
		if pending.Query.Agent.ID != created.Agent.ID || pending.Query.Agent.StateVersion != 0 || len(pending.Query.Deliveries) != 1 ||
			pending.Query.Deliveries[0].Event.Type != "main.request" || pending.Query.Deliveries[0].Execution != nil {
			t.Fatalf("main未持久化请求或status执行了业务: %+v", pending.Query)
		}
		beforeCalls := calls.Load()
		ran, _ := p.call(t, directory, 0, "run", "--env-file", config)
		status, _ := p.call(t, directory, 0, "status", "--agent", agentID)
		if status.Query.Agent.ID != created.Agent.ID || status.Query.Agent.Name != created.Agent.Name || status.Query.Agent.Definition != definition ||
			status.Query.Agent.State["result"] != "回复：你好\nhello" || status.Query.Agent.State["request_status"] != "succeeded" || status.Query.Agent.StateVersion != 2 ||
			len(status.Query.Actions) != 1 || len(status.Query.Deliveries) != 2 || ran.Run.After.ActionAttempts != 1 || ran.Run.After.ExecutionAttempts != 2 || calls.Load() != beforeCalls+1 {
			t.Fatalf("跨进程main闭环未持久化: status=%+v run=%+v calls=%d", status.Query, ran.Run, calls.Load()-beforeCalls)
		}
		action := status.Query.Actions[0].Action
		if action.Request.Type != "model.generate" || action.Status != domain.ActionStatusSucceeded || action.Result == nil ||
			action.Result.Output["message"] != "回复：你好\nhello" || action.RecoveryPolicy != domain.RecoveryPolicyManual || action.MaxAttempts != 1 {
			t.Fatalf("模型结果或恢复策略丢失: %+v", action)
		}
		idle, _ := p.call(t, directory, 0, "run", "--env-file", missingConfig)
		if !reflect.DeepEqual(idle.Run.After, ran.Run.After) || calls.Load() != beforeCalls+1 {
			t.Fatal("idle重启重复执行已完成模型action或要求模型配置")
		}
		p.call(t, directory, 0, "submit", "--agent", agentID, "--event-id", "second", "--message", "第二轮")
		p.call(t, directory, 0, "run", "--env-file", config)
		status, _ = p.call(t, directory, 0, "status", "--agent", agentID)
		if status.Query.Agent.ID != created.Agent.ID || status.Query.Agent.StateVersion != 4 || status.Query.Agent.State["result"] != "回复：第二轮" ||
			status.Query.Agent.State["request_event_id"] != "second" || len(status.Query.Actions) != 2 || len(status.Query.Deliveries) != 4 || calls.Load() != beforeCalls+2 {
			t.Fatalf("同一main实例没有完成第二轮: %+v calls=%d", status.Query, calls.Load()-beforeCalls)
		}
	})

	t.Run("missing_config_preserves_pending_request", func(t *testing.T) {
		directory := filepath.Join(t.TempDir(), "data")
		created, _ := p.call(t, directory, 0, "init", "--definition", "main")
		agentID := string(created.Agent.ID)
		p.call(t, directory, 0, "submit", "--agent", agentID, "--event-id", "request", "--message", "保留请求")
		before, _ := p.call(t, directory, 0, "status", "--agent", agentID)
		beforeCalls := calls.Load()
		_, stderr := p.call(t, directory, 1, "run", "--env-file", missingConfig)
		assertCommandError(t, stderr, "operation", 1)
		after, _ := p.call(t, directory, 0, "status", "--agent", agentID)
		if !reflect.DeepEqual(after.Query, before.Query) || calls.Load() != beforeCalls {
			t.Fatalf("缺少模型配置推进了业务或破坏了status: before=%+v after=%+v", before.Query, after.Query)
		}
		p.call(t, directory, 0, "run", "--env-file", config)
		after, _ = p.call(t, directory, 0, "status", "--agent", agentID)
		if after.Query.Agent.State["result"] != "回复：保留请求" || after.Query.Agent.StateVersion != 2 || calls.Load() != beforeCalls+1 {
			t.Fatalf("恢复模型配置后没有完成保留的请求: %+v", after.Query)
		}
	})

	t.Run("resume_saved_action_without_repeating_request", func(t *testing.T) {
		for _, mode := range []string{"pending", "completed", "interrupted"} {
			t.Run(mode, func(t *testing.T) {
				directory := filepath.Join(t.TempDir(), "data")
				created, _ := p.call(t, directory, 0, "init", "--definition", "main")
				agentID := string(created.Agent.ID)
				p.call(t, directory, 0, "submit", "--agent", agentID, "--event-id", "resume", "--message", "恢复消息")
				receipt := mainSQLiteSaveAction(t, p, directory, created.Agent.ID, "resume", mode)
				beforeCalls := calls.Load()
				before, _ := p.call(t, directory, 0, "status", "--agent", agentID)
				if before.Query.Agent.State["request_status"] != "waiting" || before.Query.Agent.StateVersion != 1 || len(before.Query.Actions) != 1 {
					t.Fatalf("恢复fixture未保存waiting状态: %+v", before.Query)
				}
				if mode == "pending" {
					_, stderr := p.call(t, directory, 1, "run", "--env-file", missingConfig)
					assertCommandError(t, stderr, "operation", 1)
					unchanged, _ := p.call(t, directory, 0, "status", "--agent", agentID)
					if !reflect.DeepEqual(before.Query, unchanged.Query) || calls.Load() != beforeCalls {
						t.Fatal("缺少配置改变了已提交未执行的模型action")
					}
					p.call(t, directory, 0, "run", "--env-file", config)
				} else {
					p.call(t, directory, 0, "run", "--env-file", missingConfig)
				}
				if mode == "interrupted" {
					if len(before.StartupRecovery.UnknownActions) != 1 || before.StartupRecovery.UnknownActions[0] != receipt.ActionID || len(before.StartupRecovery.RetryableActions) != 0 {
						t.Fatalf("中断模型action没有恢复为人工确认: %+v", before.StartupRecovery)
					}
					p.call(t, directory, 0, "run", "--env-file", config)
				}
				after, _ := p.call(t, directory, 0, "status", "--agent", agentID)
				action := after.Query.Actions[0]
				if after.Query.Deliveries[0].Execution.ID != receipt.ExecutionID || after.Query.Deliveries[0].Execution.AttemptCount != 1 ||
					action.Action.Request.ID != receipt.ActionID || action.Action.AttemptCount != 1 || action.Action.ResultEventID != receipt.ResultEventID {
					t.Fatalf("恢复重复了来源execution或丢失action身份: %+v", after.Query)
				}
				if mode == "interrupted" {
					blocked := false
					for _, reason := range action.BlockedBy {
						blocked = blocked || reason.Code == core.BlockManualUnknown
					}
					if action.Action.Status != domain.ActionStatusUnknown || action.Ready || !blocked || after.Query.Agent.State["request_status"] != "waiting" ||
						after.Query.Agent.StateVersion != 1 || len(after.Query.Deliveries) != 1 || calls.Load() != beforeCalls {
						t.Fatalf("模型结果未知时自动重试或丢失waiting状态: %+v", after.Query)
					}
					return
				}
				wantReply, wantCalls := "已保存的回复", beforeCalls
				if mode == "pending" {
					wantReply, wantCalls = "回复：恢复消息", beforeCalls+1
				}
				if action.Action.Status != domain.ActionStatusSucceeded || after.Query.Agent.State["result"] != wantReply || after.Query.Agent.StateVersion != 2 ||
					len(after.Query.Deliveries) != 2 || after.Query.Agent.State["result_event_id"] != string(receipt.ResultEventID) || calls.Load() != wantCalls {
					t.Fatalf("恢复已保存模型工作未闭环或重复调用模型: %+v calls=%d", after.Query, calls.Load()-beforeCalls)
				}
			})
		}
	})
}

// 保存真实Python request execution的提交，再停在外部模型action的不同边界。
func mainSQLiteSaveAction(t *testing.T, p sqliteCommandProcess, directory string, agentID, eventID domain.ID, mode string) sqliteClaimReceipt {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	handle, err := openCommandBackend(ctx, directory)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := handle.Close(); err != nil {
			t.Error(err)
		}
	}()
	session, err := handle.Store.OpenSession(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := session.Close(context.Background()); err != nil {
			t.Error(err)
		}
	}()
	if _, err := session.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	claim, err := session.ClaimExecution(ctx, domain.DeliveryKey{AgentID: agentID, EventID: eventID})
	if err != nil {
		t.Fatal(err)
	}
	runner, err := pythonrunner.NewRunner(pythonrunner.Options{Python: p.python, SourceDir: p.source})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := runner.Close(); err != nil {
			t.Error(err)
		}
	}()
	output, err := runner.RunContext(ctx, core.ExecutionContext{
		Agent: core.AgentSnapshot{ID: claim.Agent.ID, Name: claim.Agent.Name, Definition: claim.Agent.Definition,
			Status: claim.Agent.Status, State: claim.Agent.State, StateVersion: claim.Agent.StateVersion},
		Event: claim.Event, ExecutionID: claim.Token.ExecutionID, AttemptID: claim.Token.AttemptID,
	})
	if err != nil || len(output.Actions) != 1 || output.Actions[0].Type != "model.generate" {
		t.Fatalf("真实Python没有产生模型action: %+v %v", output, err)
	}
	action := output.Actions[0]
	action.BindExecution(claim.Token.ExecutionID)
	record := domain.ActionRecord{
		Request: action, AgentID: agentID, HandlerVersion: "1", RecoveryPolicy: domain.RecoveryPolicyManual,
		IdempotencyKey: string(action.ID), MaxAttempts: 1, Status: domain.ActionStatusPending, ResultEventID: domain.NewEvent("action.result", nil).ID,
	}
	if _, err := session.CommitExecution(ctx, core.ExecutionCommit{Token: claim.Token, StateUpdate: output.StateUpdate, Actions: []domain.ActionRecord{record}}); err != nil {
		t.Fatal(err)
	}
	receipt := sqliteClaimReceipt{ExecutionID: claim.Token.ExecutionID, ActionID: action.ID, ResultEventID: record.ResultEventID}
	if mode == "pending" {
		return receipt
	}
	actionClaim, err := session.ClaimAction(ctx, action.ID)
	if err != nil {
		t.Fatal(err)
	}
	if mode == "completed" {
		result := domain.ActionResult{ActionID: action.ID, EventID: record.ResultEventID, Status: domain.ActionStatusSucceeded, Output: map[string]any{"message": "已保存的回复"}}
		event := domain.NewEvent("action.result", map[string]any{
			"action_id": string(action.ID), "execution_id": string(claim.Token.ExecutionID), "action_type": action.Type,
			"status": "succeeded", "result": result.Output,
		})
		event.ID = record.ResultEventID
		if _, err := session.CompleteAction(ctx, core.ActionCompletion{Token: actionClaim.Token, Result: result, Event: event}); err != nil {
			t.Fatal(err)
		}
	}
	return receipt
}
