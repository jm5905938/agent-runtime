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
	"testing"
	"time"
)

type mainToolsModelRequest struct {
	Model    string           `json:"model"`
	Messages []map[string]any `json:"messages"`
	Tools    []map[string]any `json:"tools"`
	Stream   bool             `json:"stream"`
}

type mainToolsModelCapture struct {
	mu       sync.Mutex
	requests []mainToolsModelRequest
}

func (c *mainToolsModelCapture) append(request mainToolsModelRequest) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.requests = append(c.requests, request)
}

func (c *mainToolsModelCapture) all() []mainToolsModelRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]mainToolsModelRequest(nil), c.requests...)
}

func mainToolsCall(id, arguments string) map[string]any {
	return map[string]any{"id": id, "type": "function",
		"function": map[string]any{"name": "agent_status", "arguments": arguments}}
}

func mainToolsReply(t *testing.T, w http.ResponseWriter, content string, calls ...map[string]any) {
	t.Helper()
	message := map[string]any{"role": "assistant", "content": content}
	if len(calls) > 0 {
		message["tool_calls"] = calls
	}
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]any{
		"choices": []any{map[string]any{"message": message}},
	}); err != nil {
		t.Error(err)
	}
}

func mainToolsReadRequest(t *testing.T, r *http.Request) mainToolsModelRequest {
	t.Helper()
	if r.Method != http.MethodPost || r.URL.Path != "/v1/chat/completions" || r.Header.Get("Authorization") != "Bearer cli-test-secret" {
		t.Error("工具流程模型请求方法、路径或认证错误")
	}
	var request mainToolsModelRequest
	if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
		t.Errorf("读取模型请求: %v", err)
	}
	if request.Model != "test-model" || request.Stream || (len(request.Tools) != 1 && len(request.Tools) != 8) {
		t.Errorf("模型请求缺少工具声明: %+v", request)
		return request
	}
	function, ok := request.Tools[0]["function"].(map[string]any)
	if request.Tools[0]["type"] != "function" || !ok || function["name"] != "agent_status" {
		t.Errorf("agent_status工具声明错误: %+v", request.Tools)
		return request
	}
	if len(request.Tools) == 8 {
		for i, name := range []string{"spawn_subagent", "wait_subagent", "cancel_subagent", "get_current_time", "get_current_date", "read_file", "write_file"} {
			function, ok := request.Tools[i+1]["function"].(map[string]any)
			if !ok || function["name"] != name {
				t.Errorf("main工具声明错误: %+v", request.Tools)
			}
		}
	}
	parameters, ok := function["parameters"].(map[string]any)
	if !ok || parameters["type"] != "object" {
		t.Errorf("agent_status缺少对象参数定义: %+v", function)
		return request
	}
	properties, ok := parameters["properties"].(map[string]any)
	agentID, idOK := properties["agent_id"].(map[string]any)
	if !ok || !idOK || agentID["type"] != "string" {
		t.Errorf("agent_status缺少可选agent_id参数: %+v", parameters)
	}
	if required, exists := parameters["required"]; exists {
		if fields, ok := required.([]any); !ok || len(fields) != 0 {
			t.Errorf("agent_id应该可以省略: %+v", parameters)
		}
	}
	return request
}

func mainToolsAssertJSON(t *testing.T, got, want any) {
	t.Helper()
	normalize := func(value any) any {
		encoded, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		var result any
		if err := json.Unmarshal(encoded, &result); err != nil {
			t.Fatal(err)
		}
		return result
	}
	if !reflect.DeepEqual(normalize(got), normalize(want)) {
		t.Fatalf("工具对话或快照不一致:\ngot=%+v\nwant=%+v", got, want)
	}
}

func mainToolsOutput(t *testing.T, message map[string]any, callID string) map[string]any {
	t.Helper()
	if message["role"] != "tool" || message["tool_call_id"] != callID {
		t.Fatalf("工具结果没有关联tool_call_id: %+v", message)
	}
	content, ok := message["content"].(string)
	if !ok {
		t.Fatalf("工具消息content不是字符串: %+v", message)
	}
	var output map[string]any
	if err := json.Unmarshal([]byte(content), &output); err != nil {
		t.Fatalf("工具结果不是JSON: %+v %v", message, err)
	}
	if _, exists := output["state"]; exists {
		t.Fatalf("查询工具向模型暴露完整state: %+v", output)
	}
	return output
}

func mainToolsAssertFinished(t *testing.T, query *core.AgentQuery, reply string, actions int) {
	t.Helper()
	state := query.Agent.State
	if state["request_status"] != "succeeded" || state["result"] != reply || state["error"] != nil ||
		state["pending_message"] != nil || state["waiting_action_id"] != nil || len(query.Actions) != actions {
		t.Fatalf("工具流程未完成或清理等待状态: %+v", query)
	}
	for _, field := range []string{"pending_messages", "pending_tool_calls"} {
		encoded, err := json.Marshal(state[field])
		if err != nil || string(encoded) != "[]" {
			t.Fatalf("完成后%s未清空: %s %v", field, encoded, err)
		}
	}
	for _, delivery := range query.Deliveries {
		if delivery.Execution == nil || delivery.Execution.AttemptCount != 1 || delivery.Delivery.Status != domain.DeliveryStatusCompleted {
			t.Fatalf("工具流程重复执行来源或留下待办: %+v", delivery)
		}
	}
	for _, action := range query.Actions {
		if action.Action.AttemptCount != 1 || action.Action.Result == nil || action.Action.Request.ExecutionID == nil {
			t.Fatalf("工具流程重复执行操作或丢失来源与结果: %+v", action)
		}
		found := false
		for _, delivery := range query.Deliveries {
			found = found || delivery.Execution.ID == *action.Action.Request.ExecutionID
		}
		if !found {
			t.Fatalf("action引用未保存的execution: %+v", action)
		}
	}
}

func TestMainToolsSQLiteCommandsAcrossProcesses(t *testing.T) {
	pythonArgs := chatTestPython(t)
	binary := filepath.Join(t.TempDir(), "agent-runtime")
	if runtime.GOOS == "windows" {
		binary += ".exe"
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if output, err := exec.CommandContext(ctx, "go", "build", "-o", binary, ".").CombinedOutput(); err != nil {
		t.Fatalf("构建main工具流程cli: %v\n%s", err, output)
	}
	p := sqliteCommandProcess{binary: binary, python: pythonArgs[1], source: pythonArgs[3]}

	t.Run("multiple_tools_and_duplicate_requests", func(t *testing.T) {
		directory := filepath.Join(t.TempDir(), "工具持久化 # %")
		echo, _ := p.call(t, directory, 0, "init", "--name", "查询对象")
		echoID := string(echo.Agent.ID)
		p.call(t, directory, 0, "submit", "--agent", echoID, "--event-id", "echo-event", "--message", "内部状态不应透传")
		p.call(t, directory, 0, "run")
		created, _ := p.call(t, directory, 0, "init", "--definition", "main", "--name", "工具main")
		agentID := string(created.Agent.ID)
		arguments, err := json.Marshal(map[string]any{"agent_id": echoID})
		if err != nil {
			t.Fatal(err)
		}
		calls := []map[string]any{
			mainToolsCall("named-agent", string(arguments)), mainToolsCall("self-agent", "{}"),
			mainToolsCall("bad-arguments", "{invalid"),
		}
		var captured mainToolsModelCapture
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			request := mainToolsReadRequest(t, r)
			captured.append(request)
			switch len(captured.all()) {
			case 1:
				mainToolsReply(t, w, "", calls...)
			case 2:
				mainToolsReply(t, w, "已查询两个Agent")
			default:
				t.Error("重复调用工具流程模型")
				w.WriteHeader(http.StatusBadRequest)
			}
		}))
		defer server.Close()
		config := chatTestConfig(t, server.URL)
		missingConfig := filepath.Join(t.TempDir(), "missing.env")
		first, _ := p.call(t, directory, 0, "submit", "--agent", agentID, "--event-id", "tool-request", "--message", "查询对象与自身")
		duplicate, _ := p.call(t, directory, 0, "submit", "--agent", agentID, "--event-id", "tool-request", "--message", "查询对象与自身")
		if first.Submission.Duplicate || !duplicate.Submission.Duplicate {
			t.Fatal("工具流程未识别重复输入")
		}
		p.call(t, directory, 0, "run", "--env-file", config)
		status, _ := p.call(t, directory, 0, "status", "--agent", agentID)
		mainToolsAssertFinished(t, status.Query, "已查询两个Agent", 4)
		requests := captured.all()
		if len(requests) != 2 || len(requests[0].Messages) != 1 || len(requests[1].Messages) != 5 ||
			requests[0].Messages[0]["content"] != "查询对象与自身" || requests[1].Messages[1]["role"] != "assistant" {
			t.Fatalf("多工具调用未顺序完成后继续模型: %+v", requests)
		}
		mainToolsAssertJSON(t, requests[1].Messages[1]["tool_calls"], calls)
		target := mainToolsOutput(t, requests[1].Messages[2], "named-agent")
		if target["id"] != echoID || target["name"] != "查询对象" || target["status"] != "active" || target["state_version"] != float64(2) {
			t.Fatalf("工具未返回持久化目标状态: %+v", target)
		}
		mainToolsAssertJSON(t, target["definition"], domain.DefinitionRef{ID: "echo", Version: "1"})
		self := mainToolsOutput(t, requests[1].Messages[3], "self-agent")
		if self["id"] != agentID || self["name"] != "工具main" || self["request_status"] != "waiting" {
			t.Fatalf("省略agent_id未查询当前Agent: %+v", self)
		}
		invalid := mainToolsOutput(t, requests[1].Messages[4], "bad-arguments")
		if message, ok := invalid["error"].(string); !ok || message == "" {
			t.Fatalf("非法参数未回传工具错误: %+v", invalid)
		}
		history := append(append([]map[string]any(nil), requests[1].Messages...), map[string]any{"role": "assistant", "content": "已查询两个Agent"})
		mainToolsAssertJSON(t, status.Query.Agent.State["messages"], history)
		modelActions, toolActions := 0, 0
		for _, saved := range status.Query.Actions {
			switch saved.Action.Request.Type {
			case "model.generate":
				modelActions++
				encoded, err := json.Marshal(saved.Action.Request.Payload["messages"])
				if err != nil {
					t.Fatal(err)
				}
				var messages []map[string]any
				if err := json.Unmarshal(encoded, &messages); err != nil {
					t.Fatal(err)
				}
				input := requests[0].Messages
				if len(messages) > 1 {
					input = requests[1].Messages
				}
				mainToolsAssertJSON(t, saved.Action.Request.Payload["messages"], input)
				mainToolsAssertJSON(t, saved.Action.Request.Payload["tools"], requests[0].Tools)
			case "tool.agent_status":
				toolActions++
				if saved.Action.RecoveryPolicy != domain.RecoveryPolicySafeRetry || saved.Action.MaxAttempts <= 1 {
					t.Fatalf("只读工具未保存重试策略: %+v", saved.Action)
				}
			default:
				t.Fatalf("产生未知action: %+v", saved.Action)
			}
		}
		if modelActions != 2 || toolActions != 2 {
			t.Fatalf("action数量错误: model=%d tools=%d", modelActions, toolActions)
		}
		duplicate, _ = p.call(t, directory, 0, "submit", "--agent", agentID, "--event-id", "tool-request", "--message", "查询对象与自身")
		if !duplicate.Submission.Duplicate {
			t.Fatal("完成后未识别重复输入")
		}
		p.call(t, directory, 0, "run", "--env-file", missingConfig)
		unchanged, _ := p.call(t, directory, 0, "status", "--agent", agentID)
		if !reflect.DeepEqual(status.Query, unchanged.Query) || len(captured.all()) != 2 {
			t.Fatal("重复输入或重启重复执行工具或追加历史")
		}
	})

	t.Run("failed_tool_is_returned_to_model", func(t *testing.T) {
		directory := filepath.Join(t.TempDir(), "data")
		created, _ := p.call(t, directory, 0, "init", "--definition", "main")
		agentID := string(created.Agent.ID)
		var captured mainToolsModelCapture
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			request := mainToolsReadRequest(t, r)
			captured.append(request)
			if len(captured.all()) == 1 {
				mainToolsReply(t, w, "", mainToolsCall("missing-agent", "{\"agent_id\":\"does-not-exist\"}"))
			} else {
				mainToolsReply(t, w, "该Agent不存在")
			}
		}))
		defer server.Close()
		config := chatTestConfig(t, server.URL)
		p.call(t, directory, 0, "submit", "--agent", agentID, "--event-id", "failure", "--message", "查不存在的Agent")
		p.call(t, directory, 0, "run", "--env-file", config)
		status, _ := p.call(t, directory, 0, "status", "--agent", agentID)
		mainToolsAssertFinished(t, status.Query, "该Agent不存在", 3)
		requests := captured.all()
		if len(requests) != 2 || len(requests[1].Messages) != 3 {
			t.Fatalf("工具失败未继续模型: %+v", requests)
		}
		output := mainToolsOutput(t, requests[1].Messages[2], "missing-agent")
		if message, ok := output["error"].(string); !ok || message == "" {
			t.Fatalf("工具失败未转为模型可读错误: %+v", output)
		}
		failed := false
		for _, saved := range status.Query.Actions {
			if saved.Action.Request.Type == "tool.agent_status" {
				failed = saved.Action.Status == domain.ActionStatusFailed && saved.Action.Result.Error != nil
			}
		}
		if !failed {
			t.Fatal("工具失败未保存失败action记录")
		}
		history := append(append([]map[string]any(nil), requests[1].Messages...), map[string]any{"role": "assistant", "content": "该Agent不存在"})
		mainToolsAssertJSON(t, status.Query.Agent.State["messages"], history)
	})

	t.Run("resume_saved_boundaries", func(t *testing.T) {
		for _, boundary := range []string{"saved_model_calls", "pending_tool", "saved_tool_result", "saved_final_text"} {
			t.Run(boundary, func(t *testing.T) {
				directory := filepath.Join(t.TempDir(), "data")
				target, _ := p.call(t, directory, 0, "init", "--name", "恢复目标")
				created, _ := p.call(t, directory, 0, "init", "--definition", "main")
				agentID := string(created.Agent.ID)
				p.call(t, directory, 0, "submit", "--agent", agentID, "--event-id", "resume-tools", "--message", "恢复工具查询")
				arguments, err := json.Marshal(map[string]any{"agent_id": target.Agent.ID})
				if err != nil {
					t.Fatal(err)
				}
				calls := []map[string]any{mainToolsCall("saved-status", string(arguments))}
				firstModel := mainToolsSQLiteCommitExecution(t, p, directory, created.Agent.ID, "resume-tools")
				mainToolsSQLiteCompleteAction(t, directory, firstModel, map[string]any{"message": "", "tool_calls": calls})
				var toolAction, finalModel domain.ActionRecord
				if boundary != "saved_model_calls" {
					toolAction = mainToolsSQLiteCommitExecution(t, p, directory, created.Agent.ID, firstModel.ResultEventID)
					if toolAction.Request.Type != "tool.agent_status" {
						t.Fatalf("真实Python未产生工具action: %+v", toolAction)
					}
				}
				if boundary == "saved_tool_result" || boundary == "saved_final_text" {
					mainToolsSQLiteCompleteAction(t, directory, toolAction, map[string]any{
						"id": string(target.Agent.ID), "name": "恢复目标",
						"definition": map[string]any{"id": "echo", "version": "1"}, "status": "active", "state_version": 0,
					})
				}
				if boundary == "saved_final_text" {
					finalModel = mainToolsSQLiteCommitExecution(t, p, directory, created.Agent.ID, toolAction.ResultEventID)
					if finalModel.Request.Type != "model.generate" {
						t.Fatalf("真实Python未继续模型: %+v", finalModel)
					}
					mainToolsSQLiteCompleteAction(t, directory, finalModel, map[string]any{"message": "已保存的最终回复"})
				}
				var captured mainToolsModelCapture
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					request := mainToolsReadRequest(t, r)
					captured.append(request)
					mainToolsReply(t, w, "恢复完成")
				}))
				defer server.Close()
				config := chatTestConfig(t, server.URL)
				missingConfig := filepath.Join(t.TempDir(), "missing.env")
				before, _ := p.call(t, directory, 0, "status", "--agent", agentID)
				pending, _ := before.Query.Agent.State["pending_messages"].([]any)
				if before.Query.Agent.State["request_status"] != "waiting" || len(pending) == 0 {
					t.Fatalf("fixture未保存真实等待状态: %+v", before.Query)
				}
				mainToolsAssertJSON(t, pending[0], map[string]any{"role": "user", "content": "恢复工具查询"})
				if boundary != "saved_final_text" {
					_, stderr := p.call(t, directory, 1, "run", "--env-file", missingConfig)
					assertCommandError(t, stderr, "operation", 1)
					unchanged, _ := p.call(t, directory, 0, "status", "--agent", agentID)
					if !reflect.DeepEqual(before.Query, unchanged.Query) || len(captured.all()) != 0 {
						t.Fatal("缺模型配置推进续轮工具流程")
					}
					p.call(t, directory, 0, "run", "--env-file", config)
				} else {
					p.call(t, directory, 0, "run", "--env-file", missingConfig)
				}
				after, _ := p.call(t, directory, 0, "status", "--agent", agentID)
				reply, wantCalls := "恢复完成", 1
				if boundary == "saved_final_text" {
					reply, wantCalls = "已保存的最终回复", 0
				}
				mainToolsAssertFinished(t, after.Query, reply, 3)
				requests := captured.all()
				if len(requests) != wantCalls {
					t.Fatalf("恢复重做已保存模型结果: got=%d want=%d", len(requests), wantCalls)
				}
				var input any = finalModel.Request.Payload["messages"]
				if wantCalls == 1 {
					if len(requests[0].Messages) != 3 || requests[0].Messages[0]["content"] != "恢复工具查询" {
						t.Fatalf("恢复未继续冻结上下文: %+v", requests)
					}
					mainToolsAssertJSON(t, requests[0].Messages[1]["tool_calls"], calls)
					output := mainToolsOutput(t, requests[0].Messages[2], "saved-status")
					if output["id"] != string(target.Agent.ID) || output["name"] != "恢复目标" {
						t.Fatalf("恢复工具结果丢失: %+v", output)
					}
					input = requests[0].Messages
				}
				encoded, err := json.Marshal(input)
				if err != nil {
					t.Fatal(err)
				}
				var messages []map[string]any
				if err := json.Unmarshal(encoded, &messages); err != nil {
					t.Fatal(err)
				}
				mainToolsAssertJSON(t, after.Query.Agent.State["messages"], append(messages, map[string]any{"role": "assistant", "content": reply}))
				seenFirst := false
				for _, saved := range after.Query.Actions {
					if saved.Action.Request.ID == firstModel.Request.ID {
						seenFirst = true
						if saved.Action.ResultEventID != firstModel.ResultEventID {
							t.Fatal("恢复改变首个模型action身份或快照")
						}
						mainToolsAssertJSON(t, saved.Action.Request, firstModel.Request)
					}
					if toolAction.Request.ID != "" && saved.Action.Request.ID == toolAction.Request.ID {
						if saved.Action.ResultEventID != toolAction.ResultEventID {
							t.Fatal("恢复改变已保存工具action身份或快照")
						}
						mainToolsAssertJSON(t, saved.Action.Request, toolAction.Request)
					}
				}
				if !seenFirst {
					t.Fatal("恢复丢失已保存模型action")
				}
				p.call(t, directory, 0, "run", "--env-file", missingConfig)
				unchanged, _ := p.call(t, directory, 0, "status", "--agent", agentID)
				if !reflect.DeepEqual(after.Query, unchanged.Query) || len(captured.all()) != wantCalls {
					t.Fatal("完成后run重复执行工具或追加历史")
				}
			})
		}
	})
}

// 运行并提交真实Python execution，保留action给后续CLI进程恢复。
func mainToolsSQLiteCommitExecution(t *testing.T, p sqliteCommandProcess, directory string, agentID, eventID domain.ID) domain.ActionRecord {
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
	if err != nil || len(output.Actions) != 1 {
		t.Fatalf("真实Python未产生单个可恢复action: %+v %v", output, err)
	}
	action := output.Actions[0]
	action.BindExecution(claim.Token.ExecutionID)
	record := domain.ActionRecord{
		Request: action, AgentID: agentID, HandlerVersion: "1", RecoveryPolicy: domain.RecoveryPolicyManual,
		MaxAttempts: 1, Status: domain.ActionStatusPending,
		ResultEventID: domain.NewEvent("action.result", nil).ID,
	}
	if action.Type == "tool.agent_status" {
		record.RecoveryPolicy, record.MaxAttempts = domain.RecoveryPolicySafeRetry, 3
	}
	if _, err := session.CommitExecution(ctx, core.ExecutionCommit{
		Token: claim.Token, StateUpdate: output.StateUpdate, Actions: []domain.ActionRecord{record},
	}); err != nil {
		t.Fatal(err)
	}
	return record
}

func mainToolsSQLiteCompleteAction(t *testing.T, directory string, record domain.ActionRecord, output map[string]any) {
	t.Helper()
	encoded, err := json.Marshal(output)
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(encoded, &output); err != nil {
		t.Fatal(err)
	}
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
	claim, err := session.ClaimAction(ctx, record.Request.ID)
	if err != nil {
		t.Fatal(err)
	}
	result := domain.ActionResult{ActionID: record.Request.ID, EventID: record.ResultEventID, Status: domain.ActionStatusSucceeded, Output: output}
	event := domain.NewEvent("action.result", map[string]any{
		"action_id": string(record.Request.ID), "execution_id": string(*record.Request.ExecutionID),
		"action_type": record.Request.Type, "status": "succeeded", "result": output,
	})
	event.ID = record.ResultEventID
	if _, err := session.CompleteAction(ctx, core.ActionCompletion{Token: claim.Token, Result: result, Event: event}); err != nil {
		t.Fatal(err)
	}
}
