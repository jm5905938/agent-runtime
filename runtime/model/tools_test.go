package model

import (
	"agent-runtime/codec"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
)

func testToolCall(id string) map[string]any {
	return map[string]any{
		"id": id, "type": "function",
		"function": map[string]any{"name": "agent_status", "arguments": "{\"agent_id\":\"target\"}"},
	}
}

func testToolDefinition() map[string]any {
	return map[string]any{
		"type": "function",
		"function": map[string]any{
			"name": "agent_status", "description": "查询 Agent 状态", "strict": false,
			"parameters": map[string]any{
				"type": "object", "properties": map[string]any{"agent_id": map[string]any{"type": "string"}},
				"required": []any{"agent_id"}, "additionalProperties": false,
			},
		},
	}
}

func testToolReply(t *testing.T, message map[string]any) string {
	t.Helper()
	encoded, err := json.Marshal(map[string]any{"choices": []any{map[string]any{"message": message}}})
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}

func TestHandlerToolTranscriptAndDefinitions(t *testing.T) {
	calls := []any{testToolCall("call-a"), testToolCall("call-b")}
	messages := []any{
		map[string]any{"role": "user", "content": "查看状态"},
		map[string]any{"role": "assistant", "content": "", "tool_calls": calls},
		map[string]any{"role": "tool", "tool_call_id": "call-b", "content": "{\"status\":\"ready\"}"},
		map[string]any{"role": "tool", "tool_call_id": "call-a", "content": "{\"status\":\"waiting\"}"},
	}
	tools := []any{testToolDefinition()}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if !reflect.DeepEqual(body["messages"], messages) || !reflect.DeepEqual(body["tools"], tools) ||
			body["model"] != "test-model" || body["stream"] != false {
			t.Errorf("请求没有保留工具声明和调用轨迹: %v", body)
		}
		io.WriteString(writer, testToolReply(t, map[string]any{"content": "状态已读取"}))
	}))
	defer server.Close()
	action := modelAction("ignored")
	action.Payload["messages"], action.Payload["tools"] = messages, tools
	result, err := testHandler(t, server.URL).Execute(action)
	if err != nil || !reflect.DeepEqual(result, map[string]any{"message": "状态已读取"}) {
		t.Fatalf("工具结果后的模型回复=%v, 错误=%v", result, err)
	}
}

func TestHandlerOmitsAbsentTools(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Error(err)
		}
		if _, exists := body["tools"]; exists {
			t.Error("没有工具声明时请求不应包含tools")
		}
		io.WriteString(writer, testToolReply(t, map[string]any{"content": "回复"}))
	}))
	defer server.Close()
	if _, err := testHandler(t, server.URL).Execute(modelAction("你好")); err != nil {
		t.Fatal(err)
	}
}

func TestHandlerPreservesExistingSingleMessageRoles(t *testing.T) {
	for _, role := range []string{"system", "developer", "user", "assistant"} {
		t.Run(role, func(t *testing.T) {
			messages := []any{map[string]any{"role": role, "content": "保留原有输入"}}
			var calls atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				calls.Add(1)
				var body map[string]any
				if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
					t.Error(err)
				}
				if !reflect.DeepEqual(body["messages"], messages) {
					t.Errorf("旧消息角色没有原样交给provider: %v", body["messages"])
				}
				io.WriteString(writer, testToolReply(t, map[string]any{"content": "兼容回复"}))
			}))
			defer server.Close()
			action := modelAction("ignored")
			action.Payload["messages"] = messages
			result, err := testHandler(t, server.URL).Execute(action)
			if err != nil || !reflect.DeepEqual(result, map[string]any{"message": "兼容回复"}) || calls.Load() != 1 {
				t.Fatalf("旧%s消息输入不应被拒绝: result=%v error=%v calls=%d", role, result, err, calls.Load())
			}
		})
	}
}

func TestHandlerReturnsStructuredToolCalls(t *testing.T) {
	for _, variant := range []string{"null", "missing", "empty", "caption"} {
		t.Run(variant, func(t *testing.T) {
			call := testToolCall("call-1")
			call["function"].(map[string]any)["name"] = "another_registered_tool"
			calls := []any{call}
			reply := map[string]any{"tool_calls": calls}
			content := ""
			switch variant {
			case "null":
				reply["content"] = nil
			case "empty":
				reply["content"] = ""
			case "caption":
				content = "先查询状态"
				reply["content"] = content
			}
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				io.WriteString(writer, testToolReply(t, reply))
			}))
			defer server.Close()
			result, err := testHandler(t, server.URL).Execute(modelAction("状态"))
			if err != nil || !reflect.DeepEqual(result, map[string]any{"message": content, "tool_calls": calls}) {
				t.Fatalf("工具回复=%v, 错误=%v", result, err)
			}
			if err := codec.ValidateData(result); err != nil {
				t.Fatalf("工具回复不是可持久化JSON数据: %v", err)
			}
		})
	}
}

func TestHandlerRejectsInvalidToolResponses(t *testing.T) {
	var manyCalls []any
	for index := 0; index < 9; index++ {
		manyCalls = append(manyCalls, testToolCall(string(rune('a'+index))))
	}
	cases := []struct {
		name   string
		calls  any
		modify func(map[string]any)
	}{
		{name: "duplicate-id", calls: []any{testToolCall("same"), testToolCall("same")}},
		{name: "too-many", calls: manyCalls},
		{name: "wrong-type", modify: func(call map[string]any) { call["type"] = "custom" }},
		{name: "missing-id", modify: func(call map[string]any) { delete(call, "id") }},
		{name: "blank-id", modify: func(call map[string]any) { call["id"] = " \t" }},
		{name: "oversized-id", modify: func(call map[string]any) { call["id"] = strings.Repeat("x", maxToolIDBytes+1) }},
		{name: "invalid-name", modify: func(call map[string]any) { call["function"].(map[string]any)["name"] = "private-name/invalid" }},
		{name: "oversized-name", modify: func(call map[string]any) {
			call["function"].(map[string]any)["name"] = strings.Repeat("x", maxToolNameBytes+1)
		}},
		{name: "missing-function", modify: func(call map[string]any) { delete(call, "function") }},
		{name: "arguments-not-string", modify: func(call map[string]any) { call["function"].(map[string]any)["arguments"] = map[string]any{} }},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			calls := test.calls
			if calls == nil {
				call := testToolCall("private-call")
				test.modify(call)
				calls = []any{call}
			}
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				io.WriteString(writer, testToolReply(t, map[string]any{"content": "private-content", "tool_calls": calls}))
			}))
			defer server.Close()
			result, err := testHandler(t, server.URL).Execute(modelAction("private-input"))
			if err == nil || result != nil {
				t.Fatalf("非法工具调用未被拒绝: %v", result)
			}
			for _, sensitive := range []string{"private-content", "private-input", "private-call", "private-name", "private-argument"} {
				if strings.Contains(err.Error(), sensitive) {
					t.Fatal("错误包含原始模型数据")
				}
			}
		})
	}
}

func TestHandlerToolReplyLimits(t *testing.T) {
	calls := []any{testToolCall("call-1")}
	base, err := json.Marshal(map[string]any{"message": "", "tool_calls": calls})
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name    string
		content string
		wantErr bool
	}{
		{"maximum-combined", strings.Repeat("x", maxMessageBytes-len(base)), false},
		{"oversized-combined", strings.Repeat("x", maxMessageBytes-len(base)+1), true},
		{"escaped-combined", strings.Repeat("<", maxMessageBytes/6), true},
	} {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
				io.WriteString(writer, testToolReply(t, map[string]any{"content": test.content, "tool_calls": calls}))
			}))
			defer server.Close()
			result, err := testHandler(t, server.URL).Execute(modelAction("状态"))
			if test.wantErr {
				if result != nil || err == nil || !strings.Contains(err.Error(), "256KiB") {
					t.Fatalf("工具回复限制错误=%v", err)
				}
			} else if err != nil || result["message"] != test.content {
				t.Fatalf("最大合法工具回复未通过: %v", err)
			}
		})
	}
}

func TestHandlerRejectsInvalidToolInputsBeforeRequest(t *testing.T) {
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { calls.Add(1) }))
	defer server.Close()
	handler := testHandler(t, server.URL)
	cases := []struct {
		name   string
		mutate func(map[string]any)
	}{
		{"orphan-tool", func(payload map[string]any) {
			payload["messages"] = []any{map[string]any{"role": "tool", "content": "private-content", "tool_call_id": "missing"}}
		}},
		{"unclosed-call", func(payload map[string]any) {
			payload["messages"] = []any{map[string]any{"role": "assistant", "content": "", "tool_calls": []any{testToolCall("a")}}}
		}},
		{"missing-tool-result", func(payload map[string]any) {
			payload["messages"] = []any{
				map[string]any{"role": "assistant", "content": "", "tool_calls": []any{testToolCall("a"), testToolCall("b")}},
				map[string]any{"role": "tool", "content": "result", "tool_call_id": "a"},
			}
		}},
		{"interrupted-call", func(payload map[string]any) {
			payload["messages"] = []any{
				map[string]any{"role": "assistant", "content": "", "tool_calls": []any{testToolCall("a")}},
				map[string]any{"role": "user", "content": "private-content"},
			}
		}},
		{"duplicate-result", func(payload map[string]any) {
			payload["messages"] = []any{
				map[string]any{"role": "assistant", "content": "", "tool_calls": []any{testToolCall("a")}},
				map[string]any{"role": "tool", "content": "first", "tool_call_id": "a"},
				map[string]any{"role": "tool", "content": "duplicate", "tool_call_id": "a"},
			}
		}},
		{"wrong-result-id", func(payload map[string]any) {
			payload["messages"] = []any{
				map[string]any{"role": "assistant", "content": "", "tool_calls": []any{testToolCall("a")}},
				map[string]any{"role": "tool", "content": "result", "tool_call_id": "b"},
			}
		}},
		{"calls-on-user", func(payload map[string]any) {
			payload["messages"].([]any)[0].(map[string]any)["tool_calls"] = []any{testToolCall("a")}
		}},
		{"id-on-user", func(payload map[string]any) {
			payload["messages"].([]any)[0].(map[string]any)["tool_call_id"] = "private-id"
		}},
		{"duplicate-calls", func(payload map[string]any) {
			payload["messages"] = []any{map[string]any{"role": "assistant", "content": "", "tool_calls": []any{testToolCall("a"), testToolCall("a")}}}
		}},
		{"empty-calls", func(payload map[string]any) {
			payload["messages"] = []any{map[string]any{"role": "assistant", "content": "", "tool_calls": []any{}}}
		}},
		{"arguments-not-string", func(payload map[string]any) {
			call := testToolCall("a")
			call["function"].(map[string]any)["arguments"] = map[string]any{}
			payload["messages"] = []any{map[string]any{"role": "assistant", "content": "", "tool_calls": []any{call}}}
		}},
		{"arguments-invalid-utf8", func(payload map[string]any) {
			call := testToolCall("a")
			call["function"].(map[string]any)["arguments"] = string([]byte{0xff})
			payload["messages"] = []any{map[string]any{"role": "assistant", "content": "", "tool_calls": []any{call}}}
		}},
		{"tools-null", func(payload map[string]any) { payload["tools"] = nil }},
		{"tools-empty", func(payload map[string]any) { payload["tools"] = []any{} }},
		{"tools-not-list", func(payload map[string]any) { payload["tools"] = "private-tools" }},
		{"tool-not-object", func(payload map[string]any) { payload["tools"] = []any{"private-tool"} }},
		{"wrong-definition-type", func(payload map[string]any) {
			payload["tools"].([]any)[0].(map[string]any)["type"] = "custom"
		}},
		{"function-not-object", func(payload map[string]any) {
			payload["tools"].([]any)[0].(map[string]any)["function"] = "private-function"
		}},
		{"bad-name", func(payload map[string]any) {
			payload["tools"].([]any)[0].(map[string]any)["function"].(map[string]any)["name"] = "private/name"
		}},
		{"duplicate-definitions", func(payload map[string]any) {
			payload["tools"] = []any{testToolDefinition(), testToolDefinition()}
		}},
		{"bad-parameters", func(payload map[string]any) {
			payload["tools"].([]any)[0].(map[string]any)["function"].(map[string]any)["parameters"] = map[string]any{"type": "array"}
		}},
		{"non-json-parameters", func(payload map[string]any) {
			payload["tools"].([]any)[0].(map[string]any)["function"].(map[string]any)["parameters"] = map[string]any{"type": "object", "maximum": math.NaN()}
		}},
		{"invalid-utf8-parameters", func(payload map[string]any) {
			payload["tools"].([]any)[0].(map[string]any)["function"].(map[string]any)["parameters"] = map[string]any{"type": "object", "description": string([]byte{0xff})}
		}},
		{"invalid-description", func(payload map[string]any) {
			payload["tools"].([]any)[0].(map[string]any)["function"].(map[string]any)["description"] = 1
		}},
		{"invalid-strict", func(payload map[string]any) {
			payload["tools"].([]any)[0].(map[string]any)["function"].(map[string]any)["strict"] = "true"
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			action := modelAction("private-input")
			action.Payload["tools"] = []any{testToolDefinition()}
			test.mutate(action.Payload)
			result, err := handler.Execute(action)
			if err == nil || result != nil {
				t.Fatal("非法工具输入未被拒绝")
			}
			if strings.Contains(err.Error(), "private-") {
				t.Fatal("错误包含原始输入数据")
			}
		})
	}
	if calls.Load() != 0 {
		t.Fatal("非法工具输入触发了模型请求")
	}
}

func TestHandlerAllowsReusedIDAfterCompletedBatch(t *testing.T) {
	action := modelAction("第二轮")
	assistant := map[string]any{"role": "assistant", "content": nil, "tool_calls": []any{testToolCall("call-1")}}
	action.Payload["messages"] = []any{
		map[string]any{"role": "user", "content": "第一轮"},
		assistant,
		map[string]any{"role": "tool", "tool_call_id": "call-1", "content": "first"},
		map[string]any{"role": "assistant", "content": "第一轮回复"},
		map[string]any{"role": "user", "content": "第二轮"},
		assistant,
		map[string]any{"role": "tool", "tool_call_id": "call-1", "content": "second"},
	}
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		var body struct {
			Messages []message `json:"messages"`
		}
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil || len(body.Messages) != 7 ||
			body.Messages[1].Content != "" || body.Messages[5].Content != "" {
			t.Error("请求未保留按批次闭合的工具调用轨迹")
		}
		io.WriteString(writer, testToolReply(t, map[string]any{"content": "完成"}))
	}))
	defer server.Close()
	if _, err := testHandler(t, server.URL).Execute(action); err != nil {
		t.Fatalf("已闭合批次后的调用ID不应冲突: %v", err)
	}
}

func TestHandlerPreservesInvalidArgumentsThroughToolError(t *testing.T) {
	for _, arguments := range []string{"not-json", "null", "[]", "\"text\"", "{} {}", ""} {
		t.Run(arguments, func(t *testing.T) {
			call := testToolCall("call-invalid")
			call["function"].(map[string]any)["arguments"] = arguments
			calls := []any{call}
			firstMessages := []any{map[string]any{"role": "user", "content": "查询状态"}}
			toolError := "{\"error\":\"arguments需要JSON对象\"}"
			expectedMessages := []any{
				firstMessages[0],
				map[string]any{"role": "assistant", "content": "", "tool_calls": calls},
				map[string]any{"role": "tool", "content": toolError, "tool_call_id": "call-invalid"},
			}
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
				var body map[string]any
				if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
					t.Error(err)
				}
				switch requests.Add(1) {
				case 1:
					if !reflect.DeepEqual(body["messages"], firstMessages) {
						t.Error("初始请求消息被改动")
					}
					io.WriteString(writer, testToolReply(t, map[string]any{"content": nil, "tool_calls": calls}))
				case 2:
					if !reflect.DeepEqual(body["messages"], expectedMessages) {
						t.Errorf("工具错误轨迹没有原样回传: %v", body["messages"])
					}
					io.WriteString(writer, testToolReply(t, map[string]any{"content": "参数错误，已停止查询"}))
				default:
					t.Error("模型请求次数超出预期")
				}
			}))
			defer server.Close()
			handler := testHandler(t, server.URL)
			action := modelAction("查询状态")
			action.Payload["tools"] = []any{testToolDefinition()}
			result, err := handler.Execute(action)
			if err != nil || !reflect.DeepEqual(result, map[string]any{"message": "", "tool_calls": calls}) {
				t.Fatalf("transport不应验证工具arguments业务格式: result=%v, error=%v", result, err)
			}
			messages := append(firstMessages,
				map[string]any{"role": "assistant", "content": "", "tool_calls": result["tool_calls"]},
				map[string]any{"role": "tool", "content": toolError, "tool_call_id": "call-invalid"},
			)
			action.Payload["messages"] = messages
			result, err = handler.Execute(action)
			if err != nil || !reflect.DeepEqual(result, map[string]any{"message": "参数错误，已停止查询"}) {
				t.Fatalf("含原始arguments的工具错误轨迹应可继续调用模型: result=%v, error=%v", result, err)
			}
			if requests.Load() != 2 {
				t.Fatal("工具错误轨迹没有交回模型")
			}
		})
	}
}
