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
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func runCommandWithBackend(ctx context.Context, args []string, stdout, stderr io.Writer, open backendOpener) int {
	return runCommandWithStorage(ctx, args, stdout, stderr, open, nil)
}

func TestPersistentCommandsRejectMissingBackendOpener(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "not-created")
	for _, command := range [][]string{
		{"init"}, {"status"}, {"run"}, {"submit", "--agent", "agent", "--event-id", "event", "--message", ""},
		{"retry", "--agent", "agent", "--event-id", "event"},
	} {
		args := append([]string{"--data-dir", directory, "--json"}, command...)
		var stdout, stderr bytes.Buffer
		if code := runCommandWithBackend(context.Background(), args, &stdout, &stderr, nil); code != 1 || stdout.Len() != 0 {
			t.Fatalf("未配置后端却成功: code=%d, stdout=%q, stderr=%q", code, stdout.String(), stderr.String())
		}
		assertCommandError(t, stderr.Bytes(), "backend_unavailable", 1)
	}
	if _, err := os.Stat(directory); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("不可用后端创建了目录: %v", err)
	}
}

func TestCommandValidationPrecedesBackendOpen(t *testing.T) {
	calls := 0
	open := func(context.Context, string) (backendHandle, error) { calls++; return backendHandle{}, nil }
	for _, args := range [][]string{
		{"--message", string([]byte{0xff})},
		{"--data-dir", "data"}, {"run"}, {"unknown", "--data-dir", "data"},
		{"run", "--data-dir", "data", "--agent", "agent"},
		{"run", "--data-dir", "data", "extra"}, {"init", "--data-dir="},
		{"init", "--data-dir", "data", "--message="}, {"init", "--data-dir", "data", "--name", " "},
		{"init", "--data-dir", "data", "--definition", "unknown"},
		{"init", "--data-dir", "data", "--definition="},
		{"submit", "--data-dir", "data", "--definition", "main", "--agent", "agent", "--event-id", "event", "--message", "hello"},
		{"status", "--data-dir", "data", "--env-file", "config"},
		{"submit", "--data-dir", "data", "--agent", "agent", "--event-id", "event"},
		{"submit", "--data-dir", "data", "--agent", " ", "--event-id", "event", "--message", "hello"},
		{"retry", "--data-dir", "data", "--agent", "agent"},
		{"status", "--data-dir", "data", "--event-id", "event"},
		{"status", "--data-dir", "data", "--timeout=0s"},
	} {
		var stdout, stderr bytes.Buffer
		args = append([]string{"--json"}, args...)
		if code := runCommandWithBackend(context.Background(), args, &stdout, &stderr, open); code != 2 || stdout.Len() != 0 {
			t.Fatalf("无效参数返回错误: args=%q code=%d stdout=%q stderr=%q", args, code, stdout.String(), stderr.String())
		}
		assertCommandError(t, stderr.Bytes(), "usage", 2)
	}
	for _, args := range [][]string{{"--help"}, {"status", "--help"}, {"--data-dir", "data", "submit", "-h"}} {
		var stdout, stderr bytes.Buffer
		if code := runCommandWithBackend(context.Background(), args, &stdout, &stderr, open); code != 0 ||
			stderr.Len() != 0 || !strings.Contains(stdout.String(), "retry") {
			t.Fatalf("帮助失败: code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
		}
	}
	if calls != 0 {
		t.Fatalf("参数错误或帮助打开了后端: %d", calls)
	}
}

func TestParseCommandDistinguishesFlagValuesFromCommands(t *testing.T) {
	for _, args := range [][]string{
		{"--json", "--data-dir", "a path", "submit", "--message", "init", "--agent", "run", "--event-id", "status"},
		{"submit", "--data-dir=a path", "--json=true", "--message=init", "--agent=run", "--event-id=status"},
	} {
		options, err := parseCommand(args)
		if err != nil || options.request.Command != "submit" || options.request.Message != "init" || options.request.AgentID != "run" ||
			options.request.EventID != "status" || options.dataDir != "a path" || !options.asJSON {
			t.Fatalf("混淆参数与命令: %+v, %v", options, err)
		}
	}
	options, err := parseCommand([]string{"--json", "status", "--json=false", "--data-dir", "data"})
	if err != nil || options.asJSON {
		t.Fatalf("未采用最后的json设置: %+v, %v", options, err)
	}
	options, err = parseCommand([]string{"--message", "status"})
	if err != nil || options.request.Command != "" || options.request.Message != "status" {
		t.Fatalf("echo文本被当作命令: %+v, %v", options, err)
	}
	if requestedJSON([]string{"--message", "--json"}) || !requestedJSON([]string{"--message=--json", "--json"}) ||
		requestedJSON([]string{"--json", "status", "--json=false"}) {
		t.Fatal("json错误格式被普通参数值影响")
	}
}

func TestParsePersistentMainOptions(t *testing.T) {
	for _, args := range [][]string{
		{"init", "--data-dir", "data", "--definition", "main"},
		{"--definition=main", "--data-dir=data", "init"},
	} {
		options, err := parseCommand(args)
		if err != nil || options.request.Definition != "main" || options.request.Name != "main" {
			t.Fatalf("main定义或默认名称不正确: %+v %v", options, err)
		}
	}
	options, err := parseCommand([]string{"init", "--data-dir", "data", "--definition", "main", "--name", "助手"})
	if err != nil || options.request.Name != "助手" {
		t.Fatalf("实例名称未保留: %+v %v", options, err)
	}
	options, err = parseCommand([]string{"init", "--data-dir", "data"})
	if err != nil || options.request.Name != "echo" || options.request.Definition != "" {
		t.Fatalf("默认echo命令改变: %+v %v", options, err)
	}
	options, err = parseCommand([]string{"run", "--data-dir", "data", "--env-file", "模型配置"})
	if err != nil || options.envFile != "模型配置" {
		t.Fatalf("run没有接受模型配置文件: %+v %v", options, err)
	}
	if requestedJSON([]string{"init", "--definition", "--json"}) ||
		!requestedJSON([]string{"init", "--definition=main", "--json"}) {
		t.Fatal("definition参数值影响了错误输出格式")
	}
}

func assertCommandError(t *testing.T, data []byte, kind string, code int) {
	t.Helper()
	var result struct {
		Error struct{ Kind, Message string }
		Code  int `json:"exit_code"`
	}
	if err := json.Unmarshal(data, &result); err != nil || result.Error.Kind != kind || result.Error.Message == "" || result.Code != code {
		t.Fatalf("错误输出不稳定: %s, %v", data, err)
	}
}

func TestCommandsUseSharedBackendAndRealPython(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("需要python3")
	}
	source, err := filepath.Abs("../../../python/src")
	if err != nil {
		t.Fatal(err)
	}
	backend := core.NewMemoryRecoveryStore()
	opens, closes := 0, 0
	open := func(ctx context.Context, directory string) (backendHandle, error) {
		if directory != "shared-data" {
			t.Fatalf("数据目录没有传入工厂: %q", directory)
		}
		opens++
		return backendHandle{Store: backend, Close: func() error {
			closes++
			session, err := backend.OpenSession(context.Background())
			if err != nil {
				return err
			}
			return session.Close(context.Background())
		}}, nil
	}
	call := func(wantCode int, args ...string) (cli.Result, []byte) {
		t.Helper()
		args = append([]string{"--data-dir", "shared-data", "--python", python, "--python-source", source, "--json"}, args...)
		var stdout, stderr bytes.Buffer
		if code := runCommandWithBackend(context.Background(), args, &stdout, &stderr, open); code != wantCode {
			t.Fatalf("命令错误: args=%q code=%d stdout=%q stderr=%q", args, code, stdout.String(), stderr.String())
		}
		if opens != closes {
			t.Fatalf("后端没有关闭: open=%d close=%d", opens, closes)
		}
		var result cli.Result
		if wantCode == 0 {
			if err := json.Unmarshal(stdout.Bytes(), &result); err != nil || stderr.Len() != 0 {
				t.Fatalf("成功输出错误: %q %q %v", stdout.String(), stderr.String(), err)
			}
		} else if stdout.Len() != 0 {
			t.Fatalf("失败打印了成功结果: %q", stdout.String())
		}
		return result, stderr.Bytes()
	}
	created, _ := call(0, "init", "--name", "中文echo")
	agentID := string(created.Agent.ID)
	second, _ := call(0, "init")
	if second.Agent.ID == created.Agent.ID {
		t.Fatal("init重复使用了旧实例")
	}
	list, _ := call(0, "status", "--python", "/missing-python")
	if len(list.Agents) != 2 {
		t.Fatalf("status列表错误: %+v", list)
	}
	first, _ := call(0, "submit", "--agent", agentID, "--event-id", "first", "--message", "你好\nhello")
	duplicate, _ := call(0, "submit", "--agent", agentID, "--event-id", "first", "--message", "你好\nhello")
	if first.Submission.Duplicate || !duplicate.Submission.Duplicate || first.Submission.Delivery != duplicate.Submission.Delivery {
		t.Fatal("重复输入没有复用投递")
	}
	_, output := call(1, "submit", "--agent", agentID, "--event-id", "first", "--message", "different")
	assertCommandError(t, output, "conflict", 1)
	pending, _ := call(0, "status", "--agent", agentID, "--python", "/missing-python")
	if len(pending.Query.Deliveries) != 1 || pending.Query.Deliveries[0].Execution != nil || pending.Query.Agent.StateVersion != 0 {
		t.Fatalf("status执行了业务或漏掉pending: %+v", pending.Query)
	}
	call(0, "submit", "--agent", agentID, "--event-id", "second", "--message", "next")
	ran, _ := call(0, "run")
	if ran.Run.Before.Deliveries[domain.DeliveryStatusPending] != 2 || ran.Run.After.Deliveries[domain.DeliveryStatusFailed] != 1 ||
		ran.Run.After.Actions[domain.ActionStatusSucceeded] != 1 {
		t.Fatalf("busy失败或本轮统计错误: %+v", ran.Run)
	}
	status, _ := call(0, "status", "--agent", agentID)
	if status.Query.Agent.State["result"] != "你好\nhello" || status.Query.Agent.State["request_status"] != "succeeded" {
		t.Fatalf("echo未完成: %+v", status.Query.Agent)
	}
	retried, _ := call(0, "retry", "--agent", agentID, "--event-id", "second")
	if retried.Retry.EventID != "second" {
		t.Fatal("重试目标错误")
	}
	ran, _ = call(0, "run")
	status, _ = call(0, "status", "--agent", agentID)
	if status.Query.Agent.State["result"] != "next" || status.Query.Deliveries[1].Execution.AttemptCount != 2 ||
		ran.Run.After.Deliveries[domain.DeliveryStatusFailed] != 0 {
		t.Fatalf("显式重试未恢复原请求: %+v", status.Query)
	}
	idle, _ := call(0, "run")
	if idle.Run.After.ExecutionAttempts != ran.Run.After.ExecutionAttempts || idle.Run.After.ActionAttempts != ran.Run.After.ActionAttempts {
		t.Fatal("无待办时增加了尝试次数")
	}
	_, output = call(1, "retry", "--agent", agentID, "--event-id", "first")
	assertCommandError(t, output, "conflict", 1)
	_, output = call(1, "status", "--agent", "missing")
	assertCommandError(t, output, "not_found", 1)
	call(0, "submit", "--agent", string(second.Agent.ID), "--event-id", "empty", "--message", "")
	call(0, "run")
	status, _ = call(0, "status", "--agent", string(second.Agent.ID))
	if status.Query.Agent.State["result"] != "" || status.Query.Agent.State["request_status"] != "succeeded" {
		t.Fatalf("空消息未通过: %+v", status.Query.Agent)
	}
	var text, errorsText bytes.Buffer
	if code := runCommandWithBackend(context.Background(), []string{"status", "--data-dir", "shared-data", "--agent", agentID}, &text, &errorsText, open); code != 0 ||
		!strings.Contains(text.String(), "state:") || !strings.Contains(text.String(), "delivery:") || !strings.Contains(text.String(), "action:") {
		t.Fatalf("文字查询不完整: code=%d %q %q", code, text.String(), errorsText.String())
	}
}

type commandFailWriter struct{}

func (commandFailWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestCommandOutputAndBackendCleanupFailures(t *testing.T) {
	backend := core.NewMemoryRecoveryStore()
	closed := false
	open := func(context.Context, string) (backendHandle, error) {
		return backendHandle{Store: backend, Close: func() error { closed = true; return nil }}, nil
	}
	var stderr bytes.Buffer
	if code := runCommandWithBackend(context.Background(), []string{"status", "--data-dir", "data", "--json"}, commandFailWriter{}, &stderr, open); code != 1 || !closed {
		t.Fatalf("输出失败未报告或未关闭: code=%d closed=%t %s", code, closed, stderr.String())
	}
	assertCommandError(t, stderr.Bytes(), "operation", 1)
	open = func(context.Context, string) (backendHandle, error) {
		return backendHandle{Store: backend, Close: func() error { return errors.New("关闭后端失败") }}, nil
	}
	stderr.Reset()
	var stdout bytes.Buffer
	if code := runCommandWithBackend(context.Background(), []string{"status", "--data-dir", "data", "--json"}, &stdout, &stderr, open); code != 1 || stdout.Len() != 0 {
		t.Fatalf("关闭失败却输出成功: code=%d %s %s", code, stdout.String(), stderr.String())
	}
	assertCommandError(t, stderr.Bytes(), "cleanup", 1)
}

func TestJSONWorkerDiagnosticsDoNotCorruptErrorOutput(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("需要python3")
	}
	source := t.TempDir()
	packageDir := filepath.Join(source, "agent_runtime")
	if err := os.Mkdir(packageDir, 0700); err != nil {
		t.Fatal(err)
	}
	for name, content := range map[string]string{
		"__init__.py": "",
		"worker.py":   "import sys\nsys.stderr.write('worker diagnostic\\n' * 5000)\nsys.stderr.flush()\nsys.exit(3)\n",
	} {
		if err := os.WriteFile(filepath.Join(packageDir, name), []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
	}
	var stdout, stderr bytes.Buffer
	code := runCommand(context.Background(), []string{"--python", python, "--python-source", source, "--json"}, &stdout, &stderr)
	if code != 1 || stdout.Len() != 0 {
		t.Fatalf("worker故障返回错误: code=%d %s %s", code, stdout.String(), stderr.String())
	}
	assertCommandError(t, stderr.Bytes(), "operation", 1)
	var result struct {
		Diagnostics string `json:"worker_diagnostics"`
	}
	if err := json.Unmarshal(stderr.Bytes(), &result); err != nil || !strings.HasPrefix(result.Diagnostics, "worker diagnostic") ||
		!strings.HasSuffix(result.Diagnostics, "[worker诊断已截断]") || len(result.Diagnostics) > 66*1024 {
		t.Fatalf("worker诊断未隔离或未限制: bytes=%d, %v", len(result.Diagnostics), err)
	}
}
