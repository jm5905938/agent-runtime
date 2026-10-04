package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strconv"
	"testing"
	"time"
	"unicode/utf8"
)

func promptTestClearEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{"LLM_SYSTEM_PROMPT", "LLM_MAX_PROMPT_CHARS"} {
		t.Setenv(key, "")
		if err := os.Unsetenv(key); err != nil {
			t.Fatal(err)
		}
	}
}

func promptTestAppendConfig(t *testing.T, path, systemPrompt string, maxChars int) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fmt.Fprintf(file, "\nLLM_SYSTEM_PROMPT=%s\nLLM_MAX_PROMPT_CHARS=%d\n", strconv.Quote(systemPrompt), maxChars); err != nil {
		file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
}

func promptTestCharacters(t *testing.T, messages []map[string]any) int {
	t.Helper()
	count := 0
	for _, message := range messages {
		if content, ok := message["content"].(string); ok {
			count += utf8.RuneCountInString(content)
		}
		if calls, exists := message["tool_calls"]; exists {
			encoded, err := json.Marshal(calls)
			if err != nil {
				t.Fatal(err)
			}
			count += utf8.RuneCount(encoded)
		}
		if id, ok := message["tool_call_id"].(string); ok {
			count += utf8.RuneCountInString(id)
		}
	}
	return count
}

func promptTestAssertSystem(t *testing.T, messages []map[string]any, systemPrompt string) {
	t.Helper()
	if len(messages) == 0 || messages[0]["role"] != "system" || messages[0]["content"] != systemPrompt {
		t.Fatalf("system prompt没有放在输入开头: %+v", messages)
	}
	for _, message := range messages[1:] {
		if message["role"] == "system" {
			t.Fatalf("system prompt重复进入模型输入: %+v", messages)
		}
	}
}

func TestPromptSQLiteCommandsAcrossProcesses(t *testing.T) {
	promptTestClearEnv(t)
	pythonArgs := chatTestPython(t)
	binary := filepath.Join(t.TempDir(), "agent-runtime")
	if runtime.GOOS == "windows" {
		binary += ".exe"
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if output, err := exec.CommandContext(ctx, "go", "build", "-o", binary, ".").CombinedOutput(); err != nil {
		t.Fatalf("构建prompt流程cli: %v\n%s", err, output)
	}
	p := sqliteCommandProcess{binary: binary, python: pythonArgs[1], source: pythonArgs[3]}

	t.Run("env_file_unicode_newline_and_character_boundary", func(t *testing.T) {
		directory := filepath.Join(t.TempDir(), "data")
		created, _ := p.call(t, directory, 0, "init", "--definition", "main")
		agentID := string(created.Agent.ID)
		var captured mainToolsModelCapture
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			captured.append(mainToolsReadRequest(t, r))
			mainToolsReply(t, w, "收到")
		}))
		defer server.Close()
		config := chatTestConfig(t, server.URL)
		systemPrompt := "你好\n第二行 😀"
		promptTestAppendConfig(t, config, systemPrompt, 10)
		p.call(t, directory, 0, "submit", "--agent", agentID, "--event-id", "unicode", "--message", "世界")
		p.call(t, directory, 0, "run", "--env-file", config)
		requests := captured.all()
		if len(requests) != 1 {
			t.Fatalf("Unicode边界请求未执行一次: %+v", requests)
		}
		mainToolsAssertJSON(t, requests[0].Messages, []map[string]any{
			{"role": "system", "content": systemPrompt}, {"role": "user", "content": "世界"},
		})
		if promptTestCharacters(t, requests[0].Messages) != 10 {
			t.Fatal("测试未到达Unicode字符预算边界")
		}
		status, _ := p.call(t, directory, 0, "status", "--agent", agentID)
		mainSQLiteAssertMessages(t, status.Query.Agent.State["messages"], []mainSQLiteMessage{
			{Role: "user", Content: "世界"}, {Role: "assistant", Content: "收到"},
		})
		mainToolsAssertJSON(t, status.Query.Actions[0].Action.Request.Payload["messages"], requests[0].Messages)
	})

	t.Run("operating_system_environment_overrides_file", func(t *testing.T) {
		directory := filepath.Join(t.TempDir(), "data")
		created, _ := p.call(t, directory, 0, "init", "--definition", "main")
		agentID := string(created.Agent.ID)
		var captured mainToolsModelCapture
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			captured.append(mainToolsReadRequest(t, r))
			mainToolsReply(t, w, "收到")
		}))
		defer server.Close()
		config := chatTestConfig(t, server.URL)
		promptTestAppendConfig(t, config, "文件指令", 1)
		systemPrompt := "OS指令\n🙂"
		t.Setenv("LLM_SYSTEM_PROMPT", systemPrompt)
		t.Setenv("LLM_MAX_PROMPT_CHARS", "8")
		p.call(t, directory, 0, "submit", "--agent", agentID, "--event-id", "override", "--message", "输入")
		p.call(t, directory, 0, "run", "--env-file", config)
		requests := captured.all()
		if len(requests) != 1 {
			t.Fatalf("环境变量覆盖没有产生模型请求: %+v", requests)
		}
		promptTestAssertSystem(t, requests[0].Messages, systemPrompt)
		if promptTestCharacters(t, requests[0].Messages) != 8 {
			t.Fatal("环境变量未覆盖文件中的字符预算")
		}

		// 显式空的OS环境变量可以关闭文件中的system prompt。
		t.Setenv("LLM_SYSTEM_PROMPT", "")
		p.call(t, directory, 0, "submit", "--agent", agentID, "--event-id", "empty-override", "--message", "后续")
		p.call(t, directory, 0, "run", "--env-file", config)
		requests = captured.all()
		if len(requests) != 2 {
			t.Fatalf("关闭system prompt后未继续对话: %+v", requests)
		}
		mainToolsAssertJSON(t, requests[1].Messages, []map[string]any{
			{"role": "user", "content": "输入"}, {"role": "assistant", "content": "收到"},
			{"role": "user", "content": "后续"},
		})
	})

	t.Run("oldest_turn_trim_accounts_for_system_and_current_input", func(t *testing.T) {
		directory := filepath.Join(t.TempDir(), "data")
		created, _ := p.call(t, directory, 0, "init", "--definition", "main")
		agentID := string(created.Agent.ID)
		var captured mainToolsModelCapture
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			captured.append(mainToolsReadRequest(t, r))
			mainToolsReply(t, w, "收到")
		}))
		defer server.Close()
		config := chatTestConfig(t, server.URL)
		promptTestAppendConfig(t, config, "规则规则", 18)
		for i, input := range []string{"一轮", "二轮", "三轮", "最后输入六字"} {
			p.call(t, directory, 0, "submit", "--agent", agentID, "--event-id", fmt.Sprintf("turn-%d", i), "--message", input)
			p.call(t, directory, 0, "run", "--env-file", config)
		}
		requests := captured.all()
		if len(requests) != 4 {
			t.Fatalf("多轮prompt没有各请求一次: %+v", requests)
		}
		want := []map[string]any{
			{"role": "system", "content": "规则规则"},
			{"role": "user", "content": "二轮"}, {"role": "assistant", "content": "收到"},
			{"role": "user", "content": "三轮"}, {"role": "assistant", "content": "收到"},
			{"role": "user", "content": "最后输入六字"},
		}
		mainToolsAssertJSON(t, requests[3].Messages, want)
		if promptTestCharacters(t, requests[3].Messages) != 18 {
			t.Fatal("最终prompt没有保留预算允许的最近轮次")
		}
		status, _ := p.call(t, directory, 0, "status", "--agent", agentID)
		mainSQLiteAssertMessages(t, status.Query.Agent.State["messages"], []mainSQLiteMessage{
			{Role: "user", Content: "一轮"}, {Role: "assistant", Content: "收到"},
			{Role: "user", Content: "二轮"}, {Role: "assistant", Content: "收到"},
			{Role: "user", Content: "三轮"}, {Role: "assistant", Content: "收到"},
			{Role: "user", Content: "最后输入六字"}, {Role: "assistant", Content: "收到"},
		})
	})

	t.Run("mandatory_prompt_overflow_does_not_call_provider", func(t *testing.T) {
		directory := filepath.Join(t.TempDir(), "data")
		created, _ := p.call(t, directory, 0, "init", "--definition", "main")
		agentID := string(created.Agent.ID)
		var captured mainToolsModelCapture
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			captured.append(mainToolsReadRequest(t, r))
			mainToolsReply(t, w, "收到")
		}))
		defer server.Close()
		config := chatTestConfig(t, server.URL)
		promptTestAppendConfig(t, config, "规则", 6)
		p.call(t, directory, 0, "submit", "--agent", agentID, "--event-id", "completed", "--message", "好")
		p.call(t, directory, 0, "run", "--env-file", config)
		before, _ := p.call(t, directory, 0, "status", "--agent", agentID)
		p.call(t, directory, 0, "submit", "--agent", agentID, "--event-id", "overflow", "--message", "五个字输入")
		p.call(t, directory, 0, "run", "--env-file", config)
		after, _ := p.call(t, directory, 0, "status", "--agent", agentID)
		if len(captured.all()) != 1 || len(after.Query.Actions) != 1 || after.Query.Agent.State["request_status"] != "failed" ||
			after.Query.Agent.State["error"] == nil || after.Query.Agent.State["pending_message"] != nil {
			t.Fatalf("必要输入超限仍调用模型或未保存失败: %+v requests=%+v", after.Query, captured.all())
		}
		mainToolsAssertJSON(t, after.Query.Agent.State["messages"], before.Query.Agent.State["messages"])
	})

	t.Run("tool_continuation_keeps_one_system_and_trims_whole_old_tool_turn", func(t *testing.T) {
		directory := filepath.Join(t.TempDir(), "data")
		created, _ := p.call(t, directory, 0, "init", "--definition", "main")
		agentID := string(created.Agent.ID)
		var captured mainToolsModelCapture
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			captured.append(mainToolsReadRequest(t, r))
			switch len(captured.all()) {
			case 1:
				mainToolsReply(t, w, "", mainToolsCall("old-call", "{}"))
			case 2:
				mainToolsReply(t, w, "完成")
			case 3:
				mainToolsReply(t, w, "收到")
			case 4:
				mainToolsReply(t, w, "", mainToolsCall("new-call", "{}"))
			case 5:
				mainToolsReply(t, w, "结束")
			default:
				t.Error("工具prompt多调用模型")
				w.WriteHeader(http.StatusBadRequest)
			}
		}))
		defer server.Close()
		config := chatTestConfig(t, server.URL)
		systemPrompt := "sys"
		promptTestAppendConfig(t, config, systemPrompt, 100000)
		for i, input := range []string{"早", "近"} {
			p.call(t, directory, 0, "submit", "--agent", agentID, "--event-id", fmt.Sprintf("setup-%d", i), "--message", input)
			p.call(t, directory, 0, "run", "--env-file", config)
		}
		requests := captured.all()
		if len(requests) != 3 {
			t.Fatalf("没有建立工具与普通历史轮次: %+v", requests)
		}
		// 第三个请求在预算边界保留全部历史；本轮工具结果回来后，
		// 最早工具轮次需要整体移除，最近普通轮次仍然可以保留。
		maxChars := promptTestCharacters(t, requests[2].Messages) + utf8.RuneCountInString("收到新")
		promptTestAppendConfig(t, config, systemPrompt, maxChars)
		p.call(t, directory, 0, "submit", "--agent", agentID, "--event-id", "current-tool", "--message", "新")
		p.call(t, directory, 0, "run", "--env-file", config)
		requests = captured.all()
		if len(requests) != 5 {
			t.Fatalf("工具续轮没有完成: %+v", requests)
		}
		for _, request := range requests {
			promptTestAssertSystem(t, request.Messages, systemPrompt)
		}
		if len(requests[3].Messages) != 8 || requests[3].Messages[1]["content"] != "早" ||
			promptTestCharacters(t, requests[3].Messages) != maxChars {
			t.Fatalf("工具调用前没有保留预算允许的全部历史: %+v budget=%d", requests[3].Messages, maxChars)
		}
		continuation := requests[4].Messages
		if len(continuation) != 6 || continuation[1]["content"] != "近" || continuation[2]["content"] != "收到" ||
			continuation[3]["content"] != "新" || continuation[4]["role"] != "assistant" ||
			continuation[5]["role"] != "tool" || promptTestCharacters(t, continuation) > maxChars {
			t.Fatalf("工具续轮裁剪拆开轮次或删除较新的历史: %+v budget=%d", continuation, maxChars)
		}
		mainToolsAssertJSON(t, continuation[4]["tool_calls"], []map[string]any{mainToolsCall("new-call", "{}")})
		mainToolsOutput(t, continuation[5], "new-call")
		status, _ := p.call(t, directory, 0, "status", "--agent", agentID)
		mainToolsAssertFinished(t, status.Query, "结束", 7)
		var history []map[string]any
		encoded, err := json.Marshal(status.Query.Agent.State["messages"])
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(encoded, &history); err != nil {
			t.Fatal(err)
		}
		if len(history) != 10 || history[0]["content"] != "早" || history[2]["tool_call_id"] != "old-call" {
			t.Fatalf("prompt裁剪改写了已保存完整历史: %+v", history)
		}
		for _, message := range history {
			if message["role"] == "system" {
				t.Fatalf("system prompt进入了已保存对话历史: %+v", history)
			}
		}
	})

	t.Run("pending_action_retains_old_prompt_after_environment_change", func(t *testing.T) {
		directory := filepath.Join(t.TempDir(), "data")
		created, _ := p.call(t, directory, 0, "init", "--definition", "main")
		agentID := string(created.Agent.ID)
		p.call(t, directory, 0, "submit", "--agent", agentID, "--event-id", "saved-prompt", "--message", "旧输入")
		oldPrompt := "旧指令\n🙂"
		t.Setenv("LLM_SYSTEM_PROMPT", oldPrompt)
		t.Setenv("LLM_MAX_PROMPT_CHARS", "100")
		saved := mainToolsSQLiteCommitExecution(t, p, directory, created.Agent.ID, "saved-prompt")
		want := []map[string]any{{"role": "system", "content": oldPrompt}, {"role": "user", "content": "旧输入"}}
		mainToolsAssertJSON(t, saved.Request.Payload["messages"], want)
		promptTestClearEnv(t)
		var captured mainToolsModelCapture
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			captured.append(mainToolsReadRequest(t, r))
			mainToolsReply(t, w, "恢复完成")
		}))
		defer server.Close()
		config := chatTestConfig(t, server.URL)
		// 新预算连原user都容纳不了，仍必须发送已保存的旧Action快照。
		promptTestAppendConfig(t, config, "新", 1)
		p.call(t, directory, 0, "run", "--env-file", config)
		requests := captured.all()
		if len(requests) != 1 {
			t.Fatalf("恢复没有执行已保存模型Action: %+v", requests)
		}
		mainToolsAssertJSON(t, requests[0].Messages, want)
		status, _ := p.call(t, directory, 0, "status", "--agent", agentID)
		mainToolsAssertFinished(t, status.Query, "恢复完成", 1)
		if !reflect.DeepEqual(status.Query.Actions[0].Action.Request, saved.Request) {
			t.Fatal("配置变化改写了已保存Action的身份或prompt快照")
		}
		mainSQLiteAssertMessages(t, status.Query.Agent.State["messages"], []mainSQLiteMessage{
			{Role: "user", Content: "旧输入"}, {Role: "assistant", Content: "恢复完成"},
		})
	})
}
