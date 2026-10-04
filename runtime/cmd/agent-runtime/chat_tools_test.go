package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestChatCommandCompletesToolLoopWithRealPython(t *testing.T) {
	var captured mainToolsModelCapture
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := mainToolsReadRequest(t, r)
		captured.append(request)
		switch len(captured.all()) {
		case 1:
			mainToolsReply(t, w, "", mainToolsCall("chat-self", "{}"))
		case 2:
			mainToolsReply(t, w, "当前Agent正在运行")
		default:
			t.Error("chat工具流程重复调用模型")
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()
	config := chatTestConfig(t, server.URL)
	args := append([]string{"chat", "--message", "查询自己的状态", "--env-file", config, "--json"}, chatTestPython(t)...)
	var stdout, stderr bytes.Buffer
	if code := runCommand(context.Background(), args, &stdout, &stderr); code != 0 || stderr.Len() != 0 {
		t.Fatalf("chat工具闭环失败: code=%d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	var result chatResult
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil || result.Result != "当前Agent正在运行" ||
		result.Status != "succeeded" || result.AgentID == "" || result.Actions != 3 || result.Executions != 4 {
		t.Fatalf("chat工具闭环结果错误: %+v %v", result, err)
	}
	requests := captured.all()
	if len(requests) != 2 || len(requests[0].Messages) != 1 || len(requests[1].Messages) != 3 {
		t.Fatalf("chat没有根据工具结果继续模型: %+v", requests)
	}
	mainToolsAssertJSON(t, requests[1].Messages[1]["tool_calls"], []map[string]any{mainToolsCall("chat-self", "{}")})
	output := mainToolsOutput(t, requests[1].Messages[2], "chat-self")
	if output["id"] != string(result.AgentID) || output["request_status"] != "waiting" || output["state_version"] != float64(2) {
		t.Fatalf("chat只读工具没有查询同一个内存Agent: %+v", output)
	}
}
