package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestChatCommandCompletesFileAndClockToolsWithRealPython(t *testing.T) {
	pythonArgs := chatTestPython(t)
	source := t.TempDir()
	if err := os.CopyFS(source, os.DirFS(pythonArgs[3])); err != nil {
		t.Fatal(err)
	}
	pythonArgs[3] = source
	var captured mainToolsModelCapture
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := mainToolsReadRequest(t, r)
		captured.append(request)
		switch len(captured.all()) {
		case 1:
			var calls []map[string]any
			for _, tool := range []struct{ name, arguments string }{
				{"write_file", `{"path":"result.txt","content":"合并验证"}`},
				{"read_file", `{"path":"result.txt"}`},
				{"get_current_time", `{}`},
				{"get_current_date", `{}`},
			} {
				calls = append(calls, map[string]any{"id": tool.name, "type": "function",
					"function": map[string]any{"name": tool.name, "arguments": tool.arguments}})
			}
			mainToolsReply(t, w, "", calls...)
		case 2:
			mainToolsReply(t, w, "工具执行完成")
		default:
			t.Error("文件与时间工具流程重复调用模型")
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()
	config := chatTestConfig(t, server.URL)
	args := append([]string{"chat", "--data-dir", t.TempDir(), "--message", "创建文件并读取时间", "--env-file", config, "--json"}, pythonArgs...)
	var stdout, stderr bytes.Buffer
	if code := runCommand(context.Background(), args, &stdout, &stderr); code != 0 || stderr.Len() != 0 {
		t.Fatalf("工具闭环失败: code=%d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	var result chatResult
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil || result.Status != "succeeded" || result.Actions != 6 {
		t.Fatalf("工具闭环结果错误: %+v %v", result, err)
	}
	requests := captured.all()
	if len(requests) != 2 || len(requests[1].Messages) != 6 {
		t.Fatalf("工具轨迹不完整: %+v", requests)
	}
	messages := requests[1].Messages
	if output := mainToolsOutput(t, messages[2], "write_file"); output["bytes"] != float64(len("合并验证")) {
		t.Fatalf("写入工具结果错误: %+v", output)
	}
	if output := mainToolsOutput(t, messages[3], "read_file"); output["content"] != "合并验证" {
		t.Fatalf("读取工具结果错误: %+v", output)
	}
	if output := mainToolsOutput(t, messages[4], "get_current_time"); output["datetime"] == nil {
		t.Fatalf("时间工具结果错误: %+v", output)
	}
	if output := mainToolsOutput(t, messages[5], "get_current_date"); output["date"] == nil {
		t.Fatalf("日期工具结果错误: %+v", output)
	}
	content, err := os.ReadFile(filepath.Join(source, "result.txt"))
	if err != nil || string(content) != "合并验证" {
		t.Fatalf("文件未正确落盘: %q %v", content, err)
	}
}
