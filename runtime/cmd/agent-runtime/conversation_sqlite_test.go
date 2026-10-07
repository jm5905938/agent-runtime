package main

import (
	"agent-runtime/cli"
	"agent-runtime/core"
	"agent-runtime/domain"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

func conversationCommand(t *testing.T, directory string, input io.Reader, expectedCode int, args ...string) (string, string) {
	t.Helper()
	args = append([]string{"--data-dir", directory}, args...)
	var stdout, stderr bytes.Buffer
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	code := runCommandWithInput(ctx, args, input, &stdout, &stderr, openCommandBackend, openCommandIngress)
	if code != expectedCode || code == 0 && stderr.Len() != 0 {
		t.Fatalf("对话命令失败: args=%q code=%d stdout=%s stderr=%s", args, code, stdout.String(), stderr.String())
	}
	if strings.Contains(stdout.String()+stderr.String(), "cli-test-secret") {
		t.Fatal("对话命令输出泄露了配置")
	}
	return stdout.String(), stderr.String()
}

func conversationQuery(t *testing.T, directory string, agentID domain.ID) *core.AgentQuery {
	t.Helper()
	output, _ := conversationCommand(t, directory, strings.NewReader(""), 0, "status", "--agent", string(agentID), "--json")
	var result cli.Result
	if err := json.Unmarshal([]byte(output), &result); err != nil || result.Query == nil {
		t.Fatalf("对话查询结果无效: %s %v", output, err)
	}
	return result.Query
}

func conversationOnlyAgent(t *testing.T, directory string) domain.ID {
	t.Helper()
	output, _ := conversationCommand(t, directory, strings.NewReader(""), 0, "status", "--json")
	var result cli.Result
	if err := json.Unmarshal([]byte(output), &result); err != nil || len(result.Agents) != 1 || result.Agents[0].Definition.ID != "main" {
		t.Fatalf("持久对话没有复用唯一main agent: %s %v", output, err)
	}
	return result.Agents[0].ID
}

func conversationUnknown(t *testing.T, directory string, pythonArgs []string) (domain.ID, core.ActionQuery) {
	t.Helper()
	output, _ := conversationCommand(t, directory, strings.NewReader(""), 0, "init", "--definition", "main", "--json")
	var created cli.Result
	if err := json.Unmarshal([]byte(output), &created); err != nil || created.Agent == nil {
		t.Fatalf("创建恢复fixture: %s %v", output, err)
	}
	agentID := created.Agent.ID
	conversationCommand(t, directory, strings.NewReader(""), 0, "submit", "--agent", string(agentID), "--event-id", "interrupted", "--message", "中断的消息")
	p := sqliteCommandProcess{python: pythonArgs[1], source: pythonArgs[3]}
	receipt := mainSQLiteSaveAction(t, p, directory, agentID, "interrupted", "interrupted")
	query := conversationQuery(t, directory, agentID)
	unknown := mainResolutionAction(t, query, receipt.ActionID)
	if query.Agent.State["request_status"] != "waiting" || unknown.Action.Status != domain.ActionStatusUnknown {
		t.Fatalf("恢复fixture没有等待未知结果: %+v", query)
	}
	return agentID, unknown
}

func TestConversationSQLiteCommandsAcrossProcesses(t *testing.T) {
	pythonArgs := chatTestPython(t)
	binary := filepath.Join(t.TempDir(), "agent-runtime")
	if runtime.GOOS == "windows" {
		binary += ".exe"
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if output, err := exec.CommandContext(ctx, "go", "build", "-o", binary, ".").CombinedOutput(); err != nil {
		t.Fatalf("构建持久对话cli: %v\n%s", err, output)
	}
	p := sqliteCommandProcess{binary: binary, python: pythonArgs[1], source: pythonArgs[3]}
	var captured mainToolsModelCapture
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := mainToolsReadRequest(t, r)
		captured.append(request)
		mainToolsReply(t, w, "回复："+mainQueueUser(t, request))
	}))
	defer server.Close()
	config := chatTestConfig(t, server.URL)
	directory := filepath.Join(t.TempDir(), "持久对话 # %")
	var agentID domain.ID
	var history []mainSQLiteMessage
	for _, message := range []string{"第一轮", "第二轮"} {
		args := append([]string{"chat", "--data-dir", directory, "--env-file", config, "--message", message, "--json"}, pythonArgs...)
		command := exec.CommandContext(ctx, binary, args...)
		command.Dir = filepath.Dir(directory)
		var stdout, stderr bytes.Buffer
		command.Stdout, command.Stderr = &stdout, &stderr
		if err := command.Run(); err != nil || stderr.Len() != 0 {
			t.Fatalf("独立进程对话失败: %v stdout=%s stderr=%s", err, stdout.String(), stderr.String())
		}
		var result chatResult
		if err := json.Unmarshal(stdout.Bytes(), &result); err != nil || result.Status != "succeeded" || result.Result != "回复："+message || result.AgentID == "" {
			t.Fatalf("独立进程对话结果错误: %+v %v", result, err)
		}
		if agentID == "" {
			agentID = result.AgentID
		} else if result.AgentID != agentID {
			t.Fatalf("再次启动没有恢复同一main agent: got=%s want=%s", result.AgentID, agentID)
		}
		history = append(history, mainSQLiteMessage{Role: "user", Content: message}, mainSQLiteMessage{Role: "assistant", Content: "回复：" + message})
	}
	listed, _ := p.call(t, directory, 0, "status")
	if len(listed.Agents) != 1 || listed.Agents[0].ID != agentID {
		t.Fatalf("自动创建重复main agent: %+v", listed.Agents)
	}
	status, _ := p.call(t, directory, 0, "status", "--agent", string(agentID))
	mainSQLiteAssertMessages(t, status.Query.Agent.State["messages"], history)
	requests := captured.all()
	if len(requests) != 2 {
		t.Fatalf("两次输入模型调用数量错误: %d", len(requests))
	}
	mainSQLiteAssertMessages(t, requests[1].Messages, history[:3])
}

func TestConversationInteractivePersistsInputs(t *testing.T) {
	pythonArgs := chatTestPython(t)
	for _, scenario := range []struct {
		name  string
		input string
	}{
		{"exit", "第一轮\n/status\n第二轮\n/exit\n不应提交\n"},
		{"eof", "第一轮\n第二轮"},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			directory := t.TempDir()
			var captured mainToolsModelCapture
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				request := mainToolsReadRequest(t, r)
				captured.append(request)
				mainToolsReply(t, w, "回复："+mainQueueUser(t, request))
			}))
			defer server.Close()
			config := chatTestConfig(t, server.URL)
			args := append([]string{"chat", "--env-file", config}, pythonArgs...)
			output, _ := conversationCommand(t, directory, strings.NewReader(scenario.input), 0, args...)
			for _, reply := range []string{"回复：第一轮", "回复：第二轮"} {
				if !strings.Contains(output, reply) {
					t.Fatalf("交互对话没有显示回复: %q", output)
				}
			}
			requests := captured.all()
			if len(requests) != 2 || mainQueueUser(t, requests[0]) != "第一轮" || mainQueueUser(t, requests[1]) != "第二轮" {
				t.Fatalf("交互命令被提交为用户消息或输入丢失: %+v", requests)
			}
			agentID := conversationOnlyAgent(t, directory)
			query := conversationQuery(t, directory, agentID)
			mainSQLiteAssertMessages(t, query.Agent.State["messages"], []mainSQLiteMessage{
				{Role: "user", Content: "第一轮"}, {Role: "assistant", Content: "回复：第一轮"},
				{Role: "user", Content: "第二轮"}, {Role: "assistant", Content: "回复：第二轮"},
			})
		})
	}
}

func TestConversationResumeAbandonProcessesQueuedInputs(t *testing.T) {
	pythonArgs := chatTestPython(t)
	directory := t.TempDir()
	agentID, original := conversationUnknown(t, directory, pythonArgs)
	for _, eventID := range []string{"queued-first", "queued-second"} {
		conversationCommand(t, directory, strings.NewReader(""), 0, "submit", "--agent", string(agentID), "--event-id", eventID, "--message", eventID)
	}
	var captured mainToolsModelCapture
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := mainToolsReadRequest(t, r)
		captured.append(request)
		mainToolsReply(t, w, "回复："+mainQueueUser(t, request))
	}))
	defer server.Close()
	config := chatTestConfig(t, server.URL)
	args := append([]string{"resume", "--abandon", "--reason", "不再需要旧回复", "--env-file", config}, pythonArgs...)
	conversationCommand(t, directory, strings.NewReader(""), 0, args...)
	query := conversationQuery(t, directory, agentID)
	old := mainResolutionAction(t, query, original.Action.Request.ID)
	mainResolutionAssertUnknown(t, original, old)
	if old.Resolution == nil || old.Resolution.Decision != core.ResolutionAbandon || old.Resolution.Reason != "不再需要旧回复" {
		t.Fatalf("resume没有持久化放弃决定: %+v", old.Resolution)
	}
	requests := captured.all()
	if len(requests) != 2 || mainQueueUser(t, requests[0]) != "queued-first" || mainQueueUser(t, requests[1]) != "queued-second" {
		t.Fatalf("resume没有自动继续排队输入或顺序错误: %+v", requests)
	}
	mainSQLiteAssertMessages(t, query.Agent.State["messages"], []mainSQLiteMessage{
		{Role: "user", Content: "queued-first"}, {Role: "assistant", Content: "回复：queued-first"},
		{Role: "user", Content: "queued-second"}, {Role: "assistant", Content: "回复：queued-second"},
	})
}

func TestConversationResumeProcessesPendingInputs(t *testing.T) {
	pythonArgs := chatTestPython(t)
	directory := t.TempDir()
	output, _ := conversationCommand(t, directory, strings.NewReader(""), 0, "init", "--definition", "main", "--json")
	var created cli.Result
	if err := json.Unmarshal([]byte(output), &created); err != nil || created.Agent == nil {
		t.Fatalf("创建待恢复agent: %s %v", output, err)
	}
	conversationCommand(t, directory, strings.NewReader(""), 0, "submit", "--agent", string(created.Agent.ID), "--event-id", "pending", "--message", "已排队输入")
	var captured mainToolsModelCapture
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := mainToolsReadRequest(t, r)
		captured.append(request)
		mainToolsReply(t, w, "继续完成")
	}))
	defer server.Close()
	config := chatTestConfig(t, server.URL)
	args := append([]string{"resume", "--env-file", config}, pythonArgs...)
	conversationCommand(t, directory, strings.NewReader(""), 0, args...)
	query := conversationQuery(t, directory, created.Agent.ID)
	if query.Agent.State["request_status"] != "succeeded" || query.Agent.State["result"] != "继续完成" || len(captured.all()) != 1 {
		t.Fatalf("resume没有推进已有输入: %+v calls=%d", query, len(captured.all()))
	}
	conversationCommand(t, directory, strings.NewReader(""), 0, "resume", "--env-file", filepath.Join(t.TempDir(), "missing.env"))
	unchanged := conversationQuery(t, directory, created.Agent.ID)
	mainQueueAssertUnchanged(t, query, unchanged)
	if len(captured.all()) != 1 {
		t.Fatal("空闲resume重复执行已完成输入")
	}
}

func conversationFailDelivery(t *testing.T, directory string, key domain.DeliveryKey) domain.ID {
	t.Helper()
	ctx := context.Background()
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
		if err := session.Close(ctx); err != nil {
			t.Error(err)
		}
	}()
	if _, err := session.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	claim, err := session.ClaimExecution(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := session.FailExecution(ctx, core.ExecutionFailure{Token: claim.Token, Failure: domain.Failure{
		Kind: domain.ErrorKindRuntime, Message: "测试Agent执行失败，外部结果已经保存",
	}}); err != nil {
		t.Fatal(err)
	}
	return claim.Token.ExecutionID
}

func TestConversationResumeRetriesFailedSavedDecision(t *testing.T) {
	pythonArgs := chatTestPython(t)
	directory := t.TempDir()
	agentID, original := conversationUnknown(t, directory, pythonArgs)
	output, _ := conversationCommand(t, directory, strings.NewReader(""), 0, "resolve", "--action", string(original.Action.Request.ID), "--retry", "--json")
	var resolved cli.Result
	if err := json.Unmarshal([]byte(output), &resolved); err != nil || resolved.Resolution == nil {
		t.Fatalf("人工决定没有保存: %s %v", output, err)
	}
	controlID := resolved.Resolution.Resolution.EventID
	executionID := conversationFailDelivery(t, directory, domain.DeliveryKey{AgentID: agentID, EventID: controlID})
	before := conversationQuery(t, directory, agentID)
	control := mainQueueDelivery(t, before, controlID)
	if control.Delivery.Status != domain.DeliveryStatusFailed || control.Execution == nil || len(control.Attempts) != 1 || control.Attempts[0].Status != domain.AttemptStatusFailed {
		t.Fatalf("已保存人工决定没有进入失败执行fixture: %+v", control)
	}
	var captured mainToolsModelCapture
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := mainToolsReadRequest(t, r)
		captured.append(request)
		mainToolsReply(t, w, "重排决定后的回复")
	}))
	defer server.Close()
	config := chatTestConfig(t, server.URL)
	args := append([]string{"resume", "--env-file", config}, pythonArgs...)
	output, _ = conversationCommand(t, directory, strings.NewReader(""), 0, args...)
	after := conversationQuery(t, directory, agentID)
	control = mainQueueDelivery(t, after, controlID)
	if !strings.Contains(output, "重排决定后的回复") || after.Agent.State["request_status"] != "succeeded" || len(captured.all()) != 1 || len(after.Actions) != 2 {
		t.Fatalf("resume未继续已存决定或重复创建调用: output=%q query=%+v calls=%d", output, after, len(captured.all()))
	}
	if control.Delivery.Status != domain.DeliveryStatusCompleted || control.Execution == nil || control.Execution.ID != executionID ||
		control.Execution.AttemptCount != 2 || len(control.Attempts) != 2 || control.Attempts[0].Status != domain.AttemptStatusFailed || control.Attempts[1].Status != domain.AttemptStatusSucceeded {
		t.Fatalf("恢复没有重新排队原控制事件并保留失败尝试: %+v", control)
	}
	old := mainResolutionAction(t, after, original.Action.Request.ID)
	mainResolutionAssertUnknown(t, original, old)
	if old.Resolution == nil || old.Resolution.EventID != controlID || old.Resolution.Decision != core.ResolutionRetry {
		t.Fatalf("恢复丢失或重建了已保存决定: %+v", old.Resolution)
	}
}

func TestConversationResumeRetriesSavedModelResultWithoutModelConfig(t *testing.T) {
	pythonArgs := chatTestPython(t)
	directory := t.TempDir()
	output, _ := conversationCommand(t, directory, strings.NewReader(""), 0, "init", "--definition", "main", "--json")
	var created cli.Result
	if err := json.Unmarshal([]byte(output), &created); err != nil || created.Agent == nil {
		t.Fatalf("创建已保存结果fixture: %s %v", output, err)
	}
	agentID := created.Agent.ID
	conversationCommand(t, directory, strings.NewReader(""), 0, "submit", "--agent", string(agentID), "--event-id", "saved-result", "--message", "已完成的模型输入")
	p := sqliteCommandProcess{python: pythonArgs[1], source: pythonArgs[3]}
	receipt := mainSQLiteSaveAction(t, p, directory, agentID, "saved-result", "completed")
	executionID := conversationFailDelivery(t, directory, domain.DeliveryKey{AgentID: agentID, EventID: receipt.ResultEventID})
	before := conversationQuery(t, directory, agentID)
	original := mainResolutionAction(t, before, receipt.ActionID)
	if original.Action.Status != domain.ActionStatusSucceeded || original.Action.AttemptCount != 1 || len(original.Attempts) != 1 {
		t.Fatalf("fixture没有保存成功模型结果: %+v", original)
	}
	var captured mainToolsModelCapture
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captured.append(mainToolsReadRequest(t, r))
		mainToolsReply(t, w, "不应重新调用模型")
	}))
	defer server.Close()
	chatTestConfig(t, server.URL)
	args := append([]string{"resume", "--env-file", filepath.Join(t.TempDir(), "missing.env")}, pythonArgs...)
	output, _ = conversationCommand(t, directory, strings.NewReader(""), 0, args...)
	after := conversationQuery(t, directory, agentID)
	if !strings.Contains(output, "已保存的回复") || after.Agent.State["request_status"] != "succeeded" || after.Agent.State["result"] != "已保存的回复" || len(captured.all()) != 0 {
		t.Fatalf("resume未交付持久结果或重新请求模型: output=%q query=%+v calls=%d", output, after, len(captured.all()))
	}
	completed := mainResolutionAction(t, after, receipt.ActionID)
	mainToolsAssertJSON(t, completed.Action, original.Action)
	mainToolsAssertJSON(t, completed.Attempts, original.Attempts)
	result := mainQueueDelivery(t, after, receipt.ResultEventID)
	if result.Delivery.Status != domain.DeliveryStatusCompleted || result.Execution == nil || result.Execution.ID != executionID || result.Execution.AttemptCount != 2 ||
		len(result.Attempts) != 2 || result.Attempts[0].Status != domain.AttemptStatusFailed || result.Attempts[1].Status != domain.AttemptStatusSucceeded {
		t.Fatalf("resume没有保留并重试原结果交付记录: %+v", result)
	}
	mainSQLiteAssertMessages(t, after.Agent.State["messages"], []mainSQLiteMessage{
		{Role: "user", Content: "已完成的模型输入"}, {Role: "assistant", Content: "已保存的回复"},
	})
}

func TestConversationResumeJSONContainsOneResult(t *testing.T) {
	pythonArgs := chatTestPython(t)
	for _, decision := range []core.ResolutionDecision{core.ResolutionRetry, core.ResolutionAbandon} {
		t.Run(string(decision), func(t *testing.T) {
			directory := t.TempDir()
			agentID, original := conversationUnknown(t, directory, pythonArgs)
			var captured mainToolsModelCapture
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				request := mainToolsReadRequest(t, r)
				captured.append(request)
				mainToolsReply(t, w, "JSON恢复回复")
			}))
			defer server.Close()
			config := chatTestConfig(t, server.URL)
			args := append([]string{"resume", "--" + string(decision), "--json", "--env-file", config}, pythonArgs...)
			output, _ := conversationCommand(t, directory, strings.NewReader(""), 0, args...)
			var result struct {
				Command    string                        `json:"command"`
				Agent      core.AgentSnapshot            `json:"agent"`
				Resolution *core.ActionResolutionReceipt `json:"resolution"`
			}
			if err := json.Unmarshal([]byte(output), &result); err != nil || result.Command != "resume" || result.Agent.ID != agentID ||
				result.Resolution == nil || result.Resolution.Resolution.ActionID != original.Action.Request.ID || result.Resolution.Resolution.Decision != decision {
				t.Fatalf("resume的JSON混入回复文本或缺少恢复结果: %q %v", output, err)
			}
			expectedCalls := 0
			if decision == core.ResolutionRetry {
				expectedCalls = 1
				if result.Agent.State["result"] != "JSON恢复回复" || result.Agent.State["request_status"] != "succeeded" {
					t.Fatalf("JSON恢复没有完成重试: %+v", result.Agent)
				}
			} else if result.Agent.State["request_status"] != "failed" {
				t.Fatalf("JSON恢复没有结束放弃轮次: %+v", result.Agent)
			}
			if len(captured.all()) != expectedCalls {
				t.Fatalf("JSON恢复调用数量错误: got=%d want=%d", len(captured.all()), expectedCalls)
			}
			query := conversationQuery(t, directory, agentID)
			mainResolutionAssertUnknown(t, original, mainResolutionAction(t, query, original.Action.Request.ID))
		})
	}
}

func TestConversationInteractiveContinuesAfterModelFailure(t *testing.T) {
	pythonArgs := chatTestPython(t)
	directory := t.TempDir()
	var captured mainToolsModelCapture
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := mainToolsReadRequest(t, r)
		captured.append(request)
		if mainQueueUser(t, request) == "会失败的输入" {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		mainToolsReply(t, w, "继续输入的回复")
	}))
	defer server.Close()
	config := chatTestConfig(t, server.URL)
	args := append([]string{"chat", "--env-file", config}, pythonArgs...)
	output, _ := conversationCommand(t, directory, strings.NewReader("会失败的输入\n继续输入\n/exit\n"), 0, args...)
	if !strings.Contains(output, "失败") || !strings.Contains(output, "继续输入的回复") {
		t.Fatalf("交互没有显示失败并继续后续输入: %q", output)
	}
	requests := captured.all()
	if len(requests) != 2 || mainQueueUser(t, requests[0]) != "会失败的输入" || mainQueueUser(t, requests[1]) != "继续输入" {
		t.Fatalf("已知模型失败导致交互退出或重复输入: %+v", requests)
	}
	mainSQLiteAssertMessages(t, requests[1].Messages, []mainSQLiteMessage{{Role: "user", Content: "继续输入"}})
	query := conversationQuery(t, directory, conversationOnlyAgent(t, directory))
	if query.Agent.State["request_status"] != "succeeded" || len(query.Actions) != 2 {
		t.Fatalf("失败后交互没有持久化后续成功: %+v", query)
	}
	failed, succeeded := 0, 0
	for _, action := range query.Actions {
		if action.Action.Status == domain.ActionStatusFailed {
			failed++
		}
		if action.Action.Status == domain.ActionStatusSucceeded {
			succeeded++
		}
	}
	if failed != 1 || succeeded != 1 {
		t.Fatalf("已知模型失败或后续成功记录丢失: %+v", query.Actions)
	}
	mainSQLiteAssertMessages(t, query.Agent.State["messages"], []mainSQLiteMessage{
		{Role: "user", Content: "继续输入"}, {Role: "assistant", Content: "继续输入的回复"},
	})
}

func TestConversationSingleMessageAcceptsExplicitEmptyInput(t *testing.T) {
	pythonArgs := chatTestPython(t)
	directory := t.TempDir()
	var captured mainToolsModelCapture
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := mainToolsReadRequest(t, r)
		captured.append(request)
		mainToolsReply(t, w, "空输入回复")
	}))
	defer server.Close()
	config := chatTestConfig(t, server.URL)
	args := append([]string{"chat", "--message=", "--env-file", config, "--json"}, pythonArgs...)
	output, _ := conversationCommand(t, directory, strings.NewReader("不应读取stdin\n"), 0, args...)
	var result chatResult
	if err := json.Unmarshal([]byte(output), &result); err != nil || result.Result != "空输入回复" || result.Status != "succeeded" || len(captured.all()) != 1 {
		t.Fatalf("显式空message没有执行单轮对话: %s %v", output, err)
	}
	mainSQLiteAssertMessages(t, captured.all()[0].Messages, []mainSQLiteMessage{{Role: "user", Content: ""}})
}

func TestConversationSingleMessageQueuesBehindUnknown(t *testing.T) {
	pythonArgs := chatTestPython(t)
	directory := t.TempDir()
	agentID, original := conversationUnknown(t, directory, pythonArgs)
	args := append([]string{"chat", "--message", "排队的新输入", "--json", "--env-file", filepath.Join(t.TempDir(), "missing.env")}, pythonArgs...)
	output, _ := conversationCommand(t, directory, strings.NewReader("2\n"), 0, args...)
	var result struct {
		chatResult
		RequestEventID  domain.ID `json:"request_event_id"`
		WaitingActionID domain.ID `json:"waiting_action_id"`
		Queued          bool      `json:"queued"`
	}
	if err := json.Unmarshal([]byte(output), &result); err != nil || result.AgentID != agentID || result.Status != "pending" || result.Result != "" ||
		!result.Queued || result.RequestEventID == "" || result.WaitingActionID != original.Action.Request.ID {
		t.Fatalf("单轮waiting输出没有说明输入已保存: %s %v", output, err)
	}
	query := conversationQuery(t, directory, agentID)
	old := mainResolutionAction(t, query, original.Action.Request.ID)
	mainResolutionAssertUnknown(t, original, old)
	if old.Resolution != nil {
		t.Fatal("单轮chat擅自读取stdin并处理了旧unknown")
	}
	queued := mainQueueDelivery(t, query, result.RequestEventID)
	if queued.Event.Payload["message"] != "排队的新输入" || queued.Delivery.Status != domain.DeliveryStatusPending || queued.Execution != nil || len(queued.Attempts) != 0 {
		t.Fatalf("单轮chat没有持久化新输入或提前执行: %+v", queued)
	}
	var captured mainToolsModelCapture
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := mainToolsReadRequest(t, r)
		captured.append(request)
		mainToolsReply(t, w, "新输入回复")
	}))
	defer server.Close()
	config := chatTestConfig(t, server.URL)
	args = append([]string{"resume", "--abandon", "--env-file", config}, pythonArgs...)
	conversationCommand(t, directory, strings.NewReader(""), 0, args...)
	after := conversationQuery(t, directory, agentID)
	if len(captured.all()) != 1 || after.Agent.State["result"] != "新输入回复" {
		t.Fatalf("恢复没有继续此前单轮保存的输入: %+v", after)
	}
	mainSQLiteAssertMessages(t, captured.all()[0].Messages, []mainSQLiteMessage{{Role: "user", Content: "排队的新输入"}})
}

func TestConversationUnknownInteractiveRecovery(t *testing.T) {
	pythonArgs := chatTestPython(t)
	for _, scenario := range []struct {
		name     string
		command  string
		input    string
		decision core.ResolutionDecision
		calls    int
	}{
		{"chat_retry", "chat", "无效选择\n1\n/exit\n", core.ResolutionRetry, 1},
		{"chat_abandon", "chat", "2\n/exit\n", core.ResolutionAbandon, 0},
		{"chat_defer", "chat", "3\n/exit\n", "", 0},
		{"resume_retry", "resume", "1\n", core.ResolutionRetry, 1},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			directory := t.TempDir()
			agentID, original := conversationUnknown(t, directory, pythonArgs)
			var captured mainToolsModelCapture
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				request := mainToolsReadRequest(t, r)
				captured.append(request)
				mainToolsReply(t, w, "恢复后的回复")
			}))
			defer server.Close()
			config := chatTestConfig(t, server.URL)
			args := append([]string{scenario.command, "--env-file", config}, pythonArgs...)
			output, _ := conversationCommand(t, directory, strings.NewReader(scenario.input), 0, args...)
			for _, text := range []string{"1", "2", "重试", "放弃"} {
				if !strings.Contains(output, text) {
					t.Fatalf("未知调用没有就地显示人工选择: %q", output)
				}
			}
			query := conversationQuery(t, directory, agentID)
			old := mainResolutionAction(t, query, original.Action.Request.ID)
			mainResolutionAssertUnknown(t, original, old)
			if scenario.decision == "" {
				if old.Resolution != nil || query.Agent.State["request_status"] != "waiting" || len(query.Deliveries) != 1 {
					t.Fatalf("暂不处理仍改变未知调用或接受命令为输入: %+v", query)
				}
			} else if old.Resolution == nil || old.Resolution.Decision != scenario.decision {
				t.Fatalf("交互选择没有保存正确决定: %+v", old.Resolution)
			}
			if len(captured.all()) != scenario.calls {
				t.Fatalf("人工处理的模型调用数量错误: got=%d want=%d", len(captured.all()), scenario.calls)
			}
			if scenario.decision == core.ResolutionRetry {
				if query.Agent.State["request_status"] != "succeeded" || query.Agent.State["result"] != "恢复后的回复" || len(query.Actions) != 2 {
					t.Fatalf("重试没有自动运行并显示回复: %+v", query)
				}
				for _, action := range query.Actions {
					if action.Action.Request.ID != original.Action.Request.ID && action.Action.Request.Payload["retry_of"] != string(original.Action.Request.ID) {
						t.Fatalf("重试新调用丢失来源关联: %+v", action.Action)
					}
				}
				mainSQLiteAssertMessages(t, captured.all()[0].Messages, []mainSQLiteMessage{{Role: "user", Content: "中断的消息"}})
			}
		})
	}
}

type conversationBlockingInput struct {
	*io.PipeReader
	started  chan struct{}
	finished chan struct{}
	once     sync.Once
}

func (r *conversationBlockingInput) Read(buffer []byte) (int, error) {
	r.once.Do(func() { close(r.started) })
	n, err := r.PipeReader.Read(buffer)
	if err != nil {
		close(r.finished)
	}
	return n, err
}

func TestConversationBlockedInputCancellation(t *testing.T) {
	directory := t.TempDir()
	reader, writer := io.Pipe()
	defer reader.Close()
	defer writer.Close()
	input := &conversationBlockingInput{PipeReader: reader, started: make(chan struct{}), finished: make(chan struct{})}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var stdout, stderr bytes.Buffer
	done := make(chan int, 1)
	go func() {
		done <- runCommandWithInput(ctx, []string{"chat", "--data-dir", directory}, input, &stdout, &stderr, openCommandBackend, openCommandIngress)
	}()
	select {
	case <-input.started:
	case <-time.After(5 * time.Second):
		t.Fatal("交互chat没有开始等待输入")
	}
	cancel()
	select {
	case code := <-done:
		if code != 1 || !strings.Contains(stderr.String(), "canceled") {
			t.Fatalf("取消等待输入没有结束chat: code=%d stderr=%s", code, stderr.String())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("取消后chat仍阻塞在stdin")
	}
	select {
	case <-input.finished:
	case <-time.After(time.Second):
		t.Fatal("取消后stdin读取仍存活")
	}
	conversationOnlyAgent(t, directory)
}

func TestDefaultDataDirFindsProjectRoot(t *testing.T) {
	projectData, err := filepath.Abs("../../../.agent-runtime")
	if err != nil {
		t.Fatal(err)
	}
	t.Chdir("../..")
	if got := defaultDataDir(); got != projectData {
		t.Fatalf("没有从runtime定位项目数据目录: got=%q want=%q", got, projectData)
	}
	directory := t.TempDir()
	t.Chdir(directory)
	if got, err := filepath.Abs(defaultDataDir()); err != nil || got != filepath.Join(directory, ".agent-runtime") {
		t.Fatalf("独立目录默认数据位置错误: %q %v", got, err)
	}
}

func TestConversationDefaultStatusDoesNotCreateMain(t *testing.T) {
	directory := t.TempDir()
	t.Chdir(directory)
	var stdout, stderr bytes.Buffer
	if code := runCommandWithInput(context.Background(), []string{"status"}, strings.NewReader(""), &stdout, &stderr, openCommandBackend, openCommandIngress); code != 0 || stderr.Len() != 0 {
		t.Fatalf("空环境status失败: code=%d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	if stdout.Len() == 0 {
		t.Fatal("空环境status没有说明如何开始")
	}
	dataDir := filepath.Join(directory, ".agent-runtime")
	if _, err := os.Stat(filepath.Join(dataDir, "store.db")); errors.Is(err, os.ErrNotExist) {
		return
	} else if err != nil {
		t.Fatal(err)
	}
	output, _ := conversationCommand(t, dataDir, strings.NewReader(""), 0, "status", "--json")
	var result cli.Result
	if err := json.Unmarshal([]byte(output), &result); err != nil || len(result.Agents) != 0 {
		t.Fatalf("status擅自创建了main agent: %s %v", output, err)
	}
}
