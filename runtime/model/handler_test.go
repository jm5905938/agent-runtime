package model

import (
	"agent-runtime/core"
	"agent-runtime/domain"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

var _ core.ActionHandler = (*Handler)(nil)

const testKey = "test-model-key"

func modelAction(content string) domain.Action {
	return domain.Action{Type: "model.chat", Payload: map[string]any{
		"messages": []any{map[string]any{"role": "user", "content": content}},
	}}
}

func testHandler(t *testing.T, endpoint string) *Handler {
	t.Helper()
	handler, err := NewHandler(Config{BaseURL: endpoint, APIKey: testKey, Model: "test-model"})
	if err != nil {
		t.Fatal(err)
	}
	return handler
}

func TestHandlerChatCompletion(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != "/v1/chat/completions" {
			t.Errorf("请求方法或路径不符合chat completions接口")
		}
		if request.Header.Get("Authorization") != "Bearer "+testKey || request.Header.Get("Content-Type") != "application/json" {
			t.Errorf("请求认证或json类型不符合约定")
		}
		var body struct {
			Model    string    `json:"model"`
			Messages []message `json:"messages"`
			Stream   *bool     `json:"stream"`
		}
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Errorf("请求json无效: %v", err)
		}
		if body.Model != "test-model" || body.Stream == nil || *body.Stream || len(body.Messages) != 2 ||
			body.Messages[0].Role != "system" || body.Messages[0].Content != "请简短回复" ||
			body.Messages[1].Role != "user" || body.Messages[1].Content != "你好" {
			t.Errorf("请求没有保留模型配置和消息内容")
		}
		io.WriteString(writer, `{"choices":[{"message":{"role":"assistant","content":"  你好\n"}},{"message":{"content":"第二个候选"}}]}`)
	}))
	defer server.Close()
	action := modelAction("你好")
	action.Payload["messages"] = []any{
		map[string]any{"role": "system", "content": "请简短回复"},
		map[string]any{"role": "user", "content": "你好"},
	}
	action.Payload["model"] = "ignored-model"
	result, err := testHandler(t, server.URL+"/v1/").Execute(action)
	if err != nil || result["message"] != "  你好\n" || len(result) != 1 {
		t.Fatalf("模型回复=%v，错误=%v", result, err)
	}
}

func TestHandlerRejectsInvalidMessagesBeforeRequest(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls.Add(1) }))
	defer server.Close()
	handler := testHandler(t, server.URL)
	cases := []struct {
		name     string
		messages any
	}{
		{"missing", nil},
		{"not-list", "private-input"},
		{"empty-list", []any{}},
		{"not-object", []any{"private-input"}},
		{"missing-role", []any{map[string]any{"content": "private-input"}}},
		{"unknown-role", []any{map[string]any{"role": "private-role", "content": "private-input"}}},
		{"tool-role", []any{map[string]any{"role": "tool", "content": "private-input"}}},
		{"non-text", []any{map[string]any{"role": "user", "content": []any{}}}},
		{"invalid-utf8", []any{map[string]any{"role": "user", "content": string([]byte{0xff})}}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			result, err := handler.Execute(domain.Action{Payload: map[string]any{"messages": test.messages}})
			if err == nil || result != nil {
				t.Fatal("非法消息未被拒绝")
			}
			if strings.Contains(err.Error(), "private-input") || strings.Contains(err.Error(), "private-role") {
				t.Fatal("错误包含原始消息数据")
			}
		})
	}
	if calls.Load() != 0 {
		t.Fatal("非法消息触发了模型请求")
	}
}

func TestHandlerPreservesEmptyAndBlankInput(t *testing.T) {
	for _, content := range []string{"", " \t\n"} {
		t.Run(map[bool]string{true: "empty", false: "blank"}[content == ""], func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				var body struct {
					Messages []message `json:"messages"`
				}
				if err := json.NewDecoder(request.Body).Decode(&body); err != nil || len(body.Messages) != 1 || body.Messages[0].Content != content {
					t.Error("模型请求没有保留空输入或空白输入")
				}
				io.WriteString(writer, `{"choices":[{"message":{"content":"请提供输入"}}]}`)
			}))
			defer server.Close()
			result, err := testHandler(t, server.URL).Execute(modelAction(content))
			if err != nil || result["message"] != "请提供输入" {
				t.Fatalf("空输入无法交给模型: %v", err)
			}
		})
	}
}

func TestNewHandlerRejectsUnsafeConfigWithoutValues(t *testing.T) {
	cases := []struct {
		name   string
		modify func(*Config)
	}{
		{"empty-url", func(config *Config) { config.BaseURL = "" }},
		{"relative-url", func(config *Config) { config.BaseURL = "/v1" }},
		{"unsupported-scheme", func(config *Config) { config.BaseURL = "ftp://host/v1" }},
		{"missing-host", func(config *Config) { config.BaseURL = "http:///v1" }},
		{"invalid-url", func(config *Config) { config.BaseURL = "http://host/%" + testKey }},
		{"userinfo", func(config *Config) { config.BaseURL = "https://user:" + testKey + "@host/v1" }},
		{"query", func(config *Config) { config.BaseURL = "https://host/v1?key=" + testKey }},
		{"empty-query", func(config *Config) { config.BaseURL = "https://host/v1?" }},
		{"fragment", func(config *Config) { config.BaseURL = "https://host/v1#" + testKey }},
		{"empty-key", func(config *Config) { config.APIKey = " \t" }},
		{"header-key", func(config *Config) { config.APIKey += "\r\nInjected: value" }},
		{"invalid-key", func(config *Config) { config.APIKey = string([]byte{0xff}) }},
		{"empty-model", func(config *Config) { config.Model = " \n" }},
		{"invalid-model", func(config *Config) { config.Model = string([]byte{0xff}) }},
		{"negative-timeout", func(config *Config) { config.Timeout = -time.Second }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			config := Config{BaseURL: "https://host/v1", APIKey: testKey, Model: "test-model"}
			test.modify(&config)
			handler, err := NewHandler(config)
			if err == nil || handler != nil {
				t.Fatal("非法配置未被拒绝")
			}
			if strings.Contains(err.Error(), testKey) || strings.Contains(err.Error(), config.BaseURL) && config.BaseURL != "" {
				t.Fatal("错误包含配置值")
			}
		})
	}
}

func TestHandlerRejectsInvalidResponsesWithoutRawData(t *testing.T) {
	cases := []struct {
		name   string
		status int
		body   string
	}{
		{"unauthorized", http.StatusUnauthorized, `{"error":{"message":"` + testKey + `"}}`},
		{"server-error", http.StatusBadGateway, "private-provider-response " + testKey},
		{"non-200", http.StatusCreated, `{"choices":[{"message":{"content":"text"}}]}`},
		{"invalid-json", http.StatusOK, "private-provider-response " + testKey},
		{"trailing-json", http.StatusOK, `{"choices":[{"message":{"content":"text"}}]} {}`},
		{"null-response", http.StatusOK, `null`},
		{"missing-choices", http.StatusOK, `{}`},
		{"null-choices", http.StatusOK, `{"choices":null}`},
		{"empty-choices", http.StatusOK, `{"choices":[]}`},
		{"null-message", http.StatusOK, `{"choices":[{"message":null}]}`},
		{"null-content", http.StatusOK, `{"choices":[{"message":{"content":null}}]}`},
		{"blank-content", http.StatusOK, `{"choices":[{"message":{"content":" \n\t"}}]}`},
		{"non-text-content", http.StatusOK, `{"choices":[{"message":{"content":[]}}]}`},
		{"tool-only", http.StatusOK, `{"choices":[{"message":{"content":null,"tool_calls":[{"id":"private-provider-response"}]}}]}`},
		{"invalid-utf8", http.StatusOK, "{\"choices\":[{\"message\":{\"content\":\"" + string([]byte{0xff}) + "\"}}]}"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				writer.WriteHeader(test.status)
				io.WriteString(writer, test.body)
			}))
			defer server.Close()
			result, err := testHandler(t, server.URL).Execute(modelAction("private-input"))
			if err == nil || result != nil {
				t.Fatal("非法响应未被拒绝")
			}
			for _, sensitive := range []string{testKey, server.URL, "private-input", "private-provider-response"} {
				if strings.Contains(err.Error(), sensitive) {
					t.Fatal("错误包含请求或响应中的敏感数据")
				}
			}
		})
	}
}

func TestHandlerReplyLimits(t *testing.T) {
	cases := []struct {
		name    string
		content string
		body    string
		wantErr string
	}{
		{name: "maximum-text", content: strings.Repeat("x", maxMessageBytes-2)},
		{name: "oversized-text", content: strings.Repeat("x", maxMessageBytes-1), wantErr: "256KiB"},
		{name: "escaped-text", content: strings.Repeat("<", maxMessageBytes/6+1), wantErr: "256KiB"},
		{name: "oversized-body", body: strings.Repeat("x", maxResponseBytes+1), wantErr: "1MiB"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			body := test.body
			if body == "" {
				encoded, err := json.Marshal(map[string]any{"choices": []any{map[string]any{"message": map[string]any{"content": test.content}}}})
				if err != nil {
					t.Fatal(err)
				}
				body = string(encoded)
			}
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) { io.WriteString(writer, body) }))
			defer server.Close()
			result, err := testHandler(t, server.URL).Execute(modelAction("你好"))
			if test.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErr) || result != nil {
					t.Fatalf("回复限制错误=%v", err)
				}
			} else if err != nil || result["message"] != test.content {
				t.Fatalf("合法长度回复未被接受: %v", err)
			}
		})
	}
}

func TestHandlerTimeoutCoversHeadersAndBody(t *testing.T) {
	for _, flushHeader := range []bool{false, true} {
		t.Run(map[bool]string{false: "headers", true: "body"}[flushHeader], func(t *testing.T) {
			release := make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				if flushHeader {
					writer.WriteHeader(http.StatusOK)
					writer.(http.Flusher).Flush()
				}
				select {
				case <-request.Context().Done():
				case <-release:
				}
			}))
			defer server.Close()
			defer close(release)
			handler, err := NewHandler(Config{BaseURL: server.URL, APIKey: testKey, Model: "test-model", Timeout: 30 * time.Millisecond})
			if err != nil {
				t.Fatal(err)
			}
			start := time.Now()
			result, err := handler.Execute(modelAction("你好"))
			if err == nil || !strings.Contains(err.Error(), "超时") || result != nil || time.Since(start) > time.Second {
				t.Fatalf("请求没有在限制时间内结束: %v", err)
			}
			if strings.Contains(err.Error(), testKey) || strings.Contains(err.Error(), server.URL) {
				t.Fatal("超时错误包含配置值")
			}
		})
	}
}

func TestHandlerDoesNotFollowRedirect(t *testing.T) {
	var targetCalls atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { targetCalls.Add(1) }))
	defer target.Close()
	source := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		http.Redirect(writer, request, target.URL+"/completion", http.StatusTemporaryRedirect)
	}))
	defer source.Close()
	result, err := testHandler(t, source.URL).Execute(modelAction("你好"))
	if err == nil || result != nil || targetCalls.Load() != 0 || !strings.Contains(err.Error(), "307") {
		t.Fatalf("重定向没有被拒绝: %v", err)
	}
}

func TestHandlerTransportErrorDoesNotExposeURL(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	handler := testHandler(t, server.URL)
	server.Close()
	result, err := handler.Execute(modelAction("private-input"))
	if err == nil || result != nil || strings.Contains(err.Error(), server.URL) || strings.Contains(err.Error(), testKey) {
		t.Fatalf("连接失败错误没有正确脱敏: %v", err)
	}
}
