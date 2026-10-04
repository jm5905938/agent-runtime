package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

func chatTestConfig(t *testing.T, baseURL string) string {
	t.Helper()
	for _, key := range []string{"LLM_BASE_URL", "LLM_API_KEY", "LLM_MODEL", "LLM_TIMEOUT", "LLM_SYSTEM_PROMPT", "LLM_MAX_PROMPT_CHARS"} {
		t.Setenv(key, "")
		if err := os.Unsetenv(key); err != nil {
			t.Fatal(err)
		}
	}
	path := filepath.Join(t.TempDir(), ".env")
	config := fmt.Sprintf("LLM_BASE_URL=%s/v1\nLLM_API_KEY=cli-test-secret\nLLM_MODEL=test-model\nLLM_TIMEOUT=2s\n", baseURL)
	if err := os.WriteFile(path, []byte(config), 0600); err != nil {
		t.Fatal(err)
	}
	return path
}

func chatTestPython(t *testing.T) []string {
	t.Helper()
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("运行main agent需要python3")
	}
	source, err := filepath.Abs("../../../python/src")
	if err != nil {
		t.Fatal(err)
	}
	return []string{"--python", python, "--python-source", source}
}

func TestChatCommandUsesEnvAndRealPython(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Method != http.MethodPost || r.URL.Path != "/v1/chat/completions" || r.Header.Get("Authorization") != "Bearer cli-test-secret" {
			t.Error("模型请求方法、路径或认证错误")
		}
		var request struct {
			Model    string                           `json:"model"`
			Messages []struct{ Role, Content string } `json:"messages"`
			Stream   bool                             `json:"stream"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil || request.Model != "test-model" || request.Stream ||
			len(request.Messages) != 1 || request.Messages[0].Role != "user" || request.Messages[0].Content != "你好\n世界" {
			t.Error("模型没有收到main agent构造的输入")
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"choices":[{"message":{"role":"assistant","content":"你好，收到你的消息"},"finish_reason":"stop"}]}`)
	}))
	defer server.Close()
	config := chatTestConfig(t, server.URL)
	pythonArgs := chatTestPython(t)
	for _, asJSON := range []bool{false, true} {
		args := append([]string{"chat", "--message", "你好\n世界", "--env-file", config}, pythonArgs...)
		if asJSON {
			args = append(args, "--json")
		}
		var stdout, stderr bytes.Buffer
		code := runCommandWithBackend(context.Background(), args, &stdout, &stderr, nil)
		if code != 0 || stderr.Len() != 0 {
			t.Fatalf("chat失败: code=%d stderr=%s", code, stderr.String())
		}
		if asJSON {
			var result chatResult
			if err := json.Unmarshal(stdout.Bytes(), &result); err != nil || result.Result != "你好，收到你的消息" ||
				result.Status != "succeeded" || result.AgentID == "" || result.Executions != 2 || result.Actions != 1 {
				t.Fatalf("chat闭环不完整: %+v %v", result, err)
			}
		} else if stdout.String() != "你好，收到你的消息\n" {
			t.Fatalf("chat没有显示回复: %q", stdout.String())
		}
		if strings.Contains(stdout.String()+stderr.String(), "cli-test-secret") {
			t.Fatal("chat输出泄露了配置")
		}
	}
	if calls.Load() != 2 {
		t.Fatalf("每次输入的模型请求数量错误: %d", calls.Load())
	}
}

func TestChatCommandReportsModelFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprint(w, `{"error":{"message":"invalid cli-test-secret"}}`)
	}))
	defer server.Close()
	config := chatTestConfig(t, server.URL)
	args := append([]string{"chat", "--message", "你好", "--env-file", config, "--json"}, chatTestPython(t)...)
	var stdout, stderr bytes.Buffer
	if code := runCommand(context.Background(), args, &stdout, &stderr); code != 1 || stdout.Len() != 0 {
		t.Fatalf("模型失败仍报告成功: code=%d stdout=%s", code, stdout.String())
	}
	assertCommandError(t, stderr.Bytes(), "operation", 1)
	if !strings.Contains(stderr.String(), "401") || strings.Contains(stderr.String(), "cli-test-secret") {
		t.Fatal("模型失败缺少状态码或泄露了配置")
	}
}

func TestChatCommandValidatesArgumentsAndConfiguration(t *testing.T) {
	for _, args := range [][]string{
		{"chat"}, {"chat", "--message", "hello", "--data-dir", "data"},
		{"chat", "--message", "hello", "--agent", "agent"},
		{"chat", "--message", "hello", "--event-id", "event"},
		{"chat", "--message", "hello", "--name", "main"},
		{"--env-file", "config", "--message", "hello"},
	} {
		var stdout, stderr bytes.Buffer
		if code := runCommand(context.Background(), args, &stdout, &stderr); code != 2 || stdout.Len() != 0 {
			t.Fatalf("chat参数未校验: args=%q code=%d", args, code)
		}
	}
	config := chatTestConfig(t, "https://example.com")
	if err := os.WriteFile(config, []byte("LLM_BASE_URL=https://example.com/v1\n"), 0600); err != nil {
		t.Fatal(err)
	}
	var stdout, stderr bytes.Buffer
	if code := runCommand(context.Background(), []string{"chat", "--message", "hello", "--env-file", config, "--json"}, &stdout, &stderr); code != 1 || stdout.Len() != 0 {
		t.Fatalf("缺少配置仍成功: code=%d", code)
	}
	assertCommandError(t, stderr.Bytes(), "operation", 1)
	options, err := parseCommand([]string{"--env-file", "--json", "chat", "--message=", "--json"})
	if err != nil || options.envFile != "--json" || options.request.Message != "" || !options.asJSON ||
		requestedJSON([]string{"chat", "--env-file", "--json", "--message", "hello"}) {
		t.Fatalf("chat混淆参数值: %+v %v", options, err)
	}
}

func TestDefaultEnvFileFindsProjectRoot(t *testing.T) {
	projectEnv, err := filepath.Abs("../../../.env")
	if err != nil {
		t.Fatal(err)
	}
	t.Chdir("../..")
	if got := defaultEnvFile(); got != projectEnv {
		t.Fatalf("没有从runtime定位项目配置: %q", got)
	}
	t.Chdir(t.TempDir())
	if err := os.WriteFile(".env", []byte(""), 0600); err != nil {
		t.Fatal(err)
	}
	if got := defaultEnvFile(); got != ".env" {
		t.Fatalf("没有优先使用当前目录配置: %q", got)
	}
}
