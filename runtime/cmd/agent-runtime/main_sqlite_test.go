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
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type mainSQLiteMessage struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

func mainSQLiteAssertMessages(t *testing.T, value any, want []mainSQLiteMessage) {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	var got []mainSQLiteMessage
	if err := json.Unmarshal(encoded, &got); err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("对话消息错误: got=%s want=%+v err=%v", encoded, want, err)
	}
}

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
	var requestMu sync.Mutex
	var requests [][]mainSQLiteMessage
	lastRequest := func(t *testing.T) []mainSQLiteMessage {
		t.Helper()
		requestMu.Lock()
		defer requestMu.Unlock()
		if len(requests) == 0 {
			t.Fatal("模型没有收到请求")
		}
		return append([]mainSQLiteMessage(nil), requests[len(requests)-1]...)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method != http.MethodPost || r.URL.Path != "/v1/chat/completions" || r.Header.Get("Authorization") != "Bearer cli-test-secret" {
			t.Error("持久main模型请求方法、路径或认证错误")
		}
		var request struct {
			Model    string              `json:"model"`
			Messages []mainSQLiteMessage `json:"messages"`
			Stream   bool                `json:"stream"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil || request.Model != "test-model" || request.Stream ||
			len(request.Messages) == 0 || len(request.Messages)%2 != 1 {
			t.Error("持久main没有构造有效的模型输入")
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		for i, message := range request.Messages {
			wantRole := "user"
			if i%2 == 1 {
				wantRole = "assistant"
			}
			if message.Role != wantRole {
				t.Errorf("模型消息%d角色错误: got=%s want=%s", i, message.Role, wantRole)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
		}
		requestMu.Lock()
		requests = append(requests, append([]mainSQLiteMessage(nil), request.Messages...))
		requestMu.Unlock()
		message := request.Messages[len(request.Messages)-1].Content
		if message == "模型失败" {
			w.WriteHeader(http.StatusBadGateway)
			if err := json.NewEncoder(w).Encode(map[string]any{"error": map[string]any{"message": "测试模型失败"}}); err != nil {
				t.Error(err)
			}
			return
		}
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(map[string]any{
			"choices": []any{map[string]any{"message": map[string]any{"role": "assistant", "content": "回复：" + message}}},
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
		firstInput := []mainSQLiteMessage{{Role: "user", Content: "你好\nhello"}}
		firstHistory := append(append([]mainSQLiteMessage(nil), firstInput...), mainSQLiteMessage{Role: "assistant", Content: "回复：你好\nhello"})
		mainSQLiteAssertMessages(t, lastRequest(t), firstInput)
		mainSQLiteAssertMessages(t, action.Request.Payload["messages"], firstInput)
		mainSQLiteAssertMessages(t, status.Query.Agent.State["messages"], firstHistory)
		if status.Query.Agent.State["pending_message"] != nil {
			t.Fatalf("完成后pending消息未清空: %+v", status.Query.Agent.State)
		}
		idle, _ := p.call(t, directory, 0, "run", "--env-file", missingConfig)
		if !reflect.DeepEqual(idle.Run.After, ran.Run.After) || calls.Load() != beforeCalls+1 {
			t.Fatal("idle重启重复执行已完成模型action或要求模型配置")
		}
		duplicate, _ = p.call(t, directory, 0, "submit", "--agent", agentID, "--event-id", "first", "--message", "你好\nhello")
		if !duplicate.Submission.Duplicate {
			t.Fatal("完成后再次submit没有识别已完成事件")
		}
		p.call(t, directory, 0, "run", "--env-file", missingConfig)
		unchanged, _ := p.call(t, directory, 0, "status", "--agent", agentID)
		if !reflect.DeepEqual(status.Query, unchanged.Query) || calls.Load() != beforeCalls+1 {
			t.Fatal("重复事件或idle运行重复追加了历史")
		}
		p.call(t, directory, 0, "submit", "--agent", agentID, "--event-id", "second", "--message", "第二轮")
		p.call(t, directory, 0, "run", "--env-file", config)
		status, _ = p.call(t, directory, 0, "status", "--agent", agentID)
		if status.Query.Agent.ID != created.Agent.ID || status.Query.Agent.StateVersion != 4 || status.Query.Agent.State["result"] != "回复：第二轮" ||
			status.Query.Agent.State["request_event_id"] != "second" || len(status.Query.Actions) != 2 || len(status.Query.Deliveries) != 4 || calls.Load() != beforeCalls+2 {
			t.Fatalf("同一main实例没有完成第二轮: %+v calls=%d", status.Query, calls.Load()-beforeCalls)
		}
		secondInput := append(append([]mainSQLiteMessage(nil), firstHistory...), mainSQLiteMessage{Role: "user", Content: "第二轮"})
		secondHistory := append(append([]mainSQLiteMessage(nil), secondInput...), mainSQLiteMessage{Role: "assistant", Content: "回复：第二轮"})
		mainSQLiteAssertMessages(t, lastRequest(t), secondInput)
		mainSQLiteAssertMessages(t, status.Query.Agent.State["messages"], secondHistory)
		for _, savedAction := range status.Query.Actions {
			if savedAction.Action.Request.ID == action.Request.ID {
				mainSQLiteAssertMessages(t, savedAction.Action.Request.Payload["messages"], firstInput)
			} else {
				mainSQLiteAssertMessages(t, savedAction.Action.Request.Payload["messages"], secondInput)
			}
		}
		if status.Query.Agent.State["pending_message"] != nil {
			t.Fatal("第二轮完成后pending消息未清空")
		}
		other, _ := p.call(t, directory, 0, "init", "--definition", "main", "--name", "独立main")
		otherID := string(other.Agent.ID)
		if other.Agent.ID == created.Agent.ID {
			t.Fatal("第二个实例复用了第一个实例ID")
		}
		p.call(t, directory, 0, "submit", "--agent", otherID, "--event-id", "other", "--message", "独立消息")
		p.call(t, directory, 0, "run", "--env-file", config)
		mainSQLiteAssertMessages(t, lastRequest(t), []mainSQLiteMessage{{Role: "user", Content: "独立消息"}})
		otherStatus, _ := p.call(t, directory, 0, "status", "--agent", otherID)
		mainSQLiteAssertMessages(t, otherStatus.Query.Agent.State["messages"], []mainSQLiteMessage{
			{Role: "user", Content: "独立消息"}, {Role: "assistant", Content: "回复：独立消息"},
		})
		unchanged, _ = p.call(t, directory, 0, "status", "--agent", agentID)
		if !reflect.DeepEqual(status.Query, unchanged.Query) || calls.Load() != beforeCalls+3 {
			t.Fatal("第二个实例的对话改变了第一个实例")
		}
	})

	t.Run("failed_round_keeps_history_for_next_request", func(t *testing.T) {
		directory := filepath.Join(t.TempDir(), "data")
		created, _ := p.call(t, directory, 0, "init", "--definition", "main")
		agentID := string(created.Agent.ID)
		beforeCalls := calls.Load()
		p.call(t, directory, 0, "submit", "--agent", agentID, "--event-id", "success", "--message", "已完成消息")
		p.call(t, directory, 0, "run", "--env-file", config)
		history := []mainSQLiteMessage{{Role: "user", Content: "已完成消息"}, {Role: "assistant", Content: "回复：已完成消息"}}
		p.call(t, directory, 0, "submit", "--agent", agentID, "--event-id", "failure", "--message", "模型失败")
		p.call(t, directory, 0, "run", "--env-file", config)
		failed, _ := p.call(t, directory, 0, "status", "--agent", agentID)
		if failed.Query.Agent.ID != created.Agent.ID || failed.Query.Agent.StateVersion != 4 || failed.Query.Agent.State["request_status"] != "failed" ||
			failed.Query.Agent.State["pending_message"] != nil || failed.Query.Agent.State["result"] != nil || failed.Query.Agent.State["error"] == nil || calls.Load() != beforeCalls+2 {
			t.Fatalf("失败模型没有清理pending或保存失败状态: %+v", failed.Query)
		}
		mainSQLiteAssertMessages(t, failed.Query.Agent.State["messages"], history)
		mainSQLiteAssertMessages(t, lastRequest(t), append(append([]mainSQLiteMessage(nil), history...), mainSQLiteMessage{Role: "user", Content: "模型失败"}))
		p.call(t, directory, 0, "run", "--env-file", missingConfig)
		unchanged, _ := p.call(t, directory, 0, "status", "--agent", agentID)
		if !reflect.DeepEqual(failed.Query, unchanged.Query) || calls.Load() != beforeCalls+2 {
			t.Fatal("重启后自动重试失败模型或改变历史")
		}
		p.call(t, directory, 0, "submit", "--agent", agentID, "--event-id", "after_failure", "--message", "继续对话")
		p.call(t, directory, 0, "run", "--env-file", config)
		input := append(append([]mainSQLiteMessage(nil), history...), mainSQLiteMessage{Role: "user", Content: "继续对话"})
		mainSQLiteAssertMessages(t, lastRequest(t), input)
		after, _ := p.call(t, directory, 0, "status", "--agent", agentID)
		if after.Query.Agent.ID != created.Agent.ID || after.Query.Agent.StateVersion != 6 || after.Query.Agent.State["request_status"] != "succeeded" ||
			after.Query.Agent.State["pending_message"] != nil || after.Query.Agent.State["error"] != nil || calls.Load() != beforeCalls+3 {
			t.Fatalf("失败后新请求没有正常完成: %+v", after.Query)
		}
		mainSQLiteAssertMessages(t, after.Query.Agent.State["messages"], append(input, mainSQLiteMessage{Role: "assistant", Content: "回复：继续对话"}))
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
		mainSQLiteAssertMessages(t, after.Query.Agent.State["messages"], []mainSQLiteMessage{
			{Role: "user", Content: "保留请求"}, {Role: "assistant", Content: "回复：保留请求"},
		})
		if after.Query.Agent.State["pending_message"] != nil {
			t.Fatal("恢复配置完成后pending消息未清空")
		}
	})

	t.Run("resume_saved_action_without_repeating_request", func(t *testing.T) {
		for _, mode := range []string{"pending", "completed", "interrupted"} {
			t.Run(mode, func(t *testing.T) {
				directory := filepath.Join(t.TempDir(), "data")
				created, _ := p.call(t, directory, 0, "init", "--definition", "main")
				agentID := string(created.Agent.ID)
				p.call(t, directory, 0, "submit", "--agent", agentID, "--event-id", "previous", "--message", "先前消息")
				p.call(t, directory, 0, "run", "--env-file", config)
				history := []mainSQLiteMessage{{Role: "user", Content: "先前消息"}, {Role: "assistant", Content: "回复：先前消息"}}
				input := append(append([]mainSQLiteMessage(nil), history...), mainSQLiteMessage{Role: "user", Content: "恢复消息"})
				p.call(t, directory, 0, "submit", "--agent", agentID, "--event-id", "resume", "--message", "恢复消息")
				receipt := mainSQLiteSaveAction(t, p, directory, created.Agent.ID, "resume", mode)
				beforeCalls := calls.Load()
				before, _ := p.call(t, directory, 0, "status", "--agent", agentID)
				if before.Query.Agent.State["request_status"] != "waiting" || before.Query.Agent.StateVersion != 3 || len(before.Query.Actions) != 2 ||
					before.Query.Agent.State["pending_message"] != "恢复消息" {
					t.Fatalf("恢复fixture未保存waiting状态: %+v", before.Query)
				}
				mainSQLiteAssertMessages(t, before.Query.Agent.State["messages"], history)
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
				var action core.ActionQuery
				for _, savedAction := range after.Query.Actions {
					if savedAction.Action.Request.ID == receipt.ActionID {
						action = savedAction
					}
				}
				var execution *domain.Execution
				for _, delivery := range after.Query.Deliveries {
					if delivery.Event.ID == "resume" {
						execution = delivery.Execution
					}
				}
				if execution == nil || execution.ID != receipt.ExecutionID || execution.AttemptCount != 1 ||
					action.Action.Request.ID != receipt.ActionID || action.Action.AttemptCount != 1 || action.Action.ResultEventID != receipt.ResultEventID {
					t.Fatalf("恢复重复了来源execution或丢失action身份: %+v", after.Query)
				}
				mainSQLiteAssertMessages(t, action.Action.Request.Payload["messages"], input)
				if mode == "interrupted" {
					blocked := false
					for _, reason := range action.BlockedBy {
						blocked = blocked || reason.Code == core.BlockManualUnknown
					}
					if action.Action.Status != domain.ActionStatusUnknown || action.Ready || !blocked || after.Query.Agent.State["request_status"] != "waiting" ||
						after.Query.Agent.StateVersion != 3 || len(after.Query.Deliveries) != 3 || calls.Load() != beforeCalls ||
						after.Query.Agent.State["pending_message"] != "恢复消息" {
						t.Fatalf("模型结果未知时自动重试或丢失waiting状态: %+v", after.Query)
					}
					mainSQLiteAssertMessages(t, after.Query.Agent.State["messages"], history)
					return
				}
				wantReply, wantCalls := "已保存的回复", beforeCalls
				if mode == "pending" {
					wantReply, wantCalls = "回复：恢复消息", beforeCalls+1
				}
				if action.Action.Status != domain.ActionStatusSucceeded || after.Query.Agent.State["result"] != wantReply || after.Query.Agent.StateVersion != 4 ||
					len(after.Query.Deliveries) != 4 || after.Query.Agent.State["result_event_id"] != string(receipt.ResultEventID) || calls.Load() != wantCalls ||
					after.Query.Agent.State["pending_message"] != nil {
					t.Fatalf("恢复已保存模型工作未闭环或重复调用模型: %+v calls=%d", after.Query, calls.Load()-beforeCalls)
				}
				mainSQLiteAssertMessages(t, after.Query.Agent.State["messages"], append(input, mainSQLiteMessage{Role: "assistant", Content: wantReply}))
				if mode == "pending" {
					mainSQLiteAssertMessages(t, lastRequest(t), input)
				}
				p.call(t, directory, 0, "run", "--env-file", missingConfig)
				unchanged, _ := p.call(t, directory, 0, "status", "--agent", agentID)
				if !reflect.DeepEqual(after.Query, unchanged.Query) || calls.Load() != wantCalls {
					t.Fatal("恢复完成后再次run重复追加历史或重新调用模型")
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
