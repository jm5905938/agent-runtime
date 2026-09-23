package main

import (
	"bytes"
	"context"
	"encoding/json"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestCommandRejectsUnsupportedOrInvalidArguments(t *testing.T) {
	for _, args := range [][]string{{"status"}, {"demo"}, {"--reopen"}, {"--data-dir", "data"}, {"--timeout", "0s"}, {"extra"}} {
		var stdout, stderr bytes.Buffer
		if code := runCommand(context.Background(), args, &stdout, &stderr); code != 2 || stdout.Len() != 0 || stderr.Len() == 0 {
			t.Fatalf("args=%v code=%d stdout=%q stderr=%q", args, code, stdout.String(), stderr.String())
		}
	}
}

func TestCommandRunsPythonAndPrintsJSON(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("运行echo需要python3")
	}
	source, err := filepath.Abs("../../../python/src")
	if err != nil {
		t.Fatal(err)
	}
	for _, message := range []string{"你好\nhello", ""} {
		args := []string{"--python", python, "--python-source", source, "--message", message, "--json"}
		var stdout, stderr bytes.Buffer
		if code := runCommand(context.Background(), args, &stdout, &stderr); code != 0 {
			t.Fatalf("code=%d stderr=%q", code, stderr.String())
		}
		var result echoResult
		if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
			t.Fatalf("stdout not json: %q, %v", stdout.String(), err)
		}
		if result.Result != message || result.Status != "succeeded" || result.Executions != 2 || result.Actions != 1 || result.AgentID == "" || result.ActionID == "" || result.ResultEventID == "" || result.RequestExecutionID == "" {
			t.Fatalf("incorrect result: %+v", result)
		}
		var fields map[string]any
		if err := json.Unmarshal(stdout.Bytes(), &fields); err != nil {
			t.Fatal(err)
		}
		if _, exists := fields["reopened"]; exists {
			t.Fatal("输出仍包含恢复演示字段")
		}
	}
}

func TestCommandWithoutArgumentsRunsEcho(t *testing.T) {
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("运行echo需要python3")
	}
	t.Chdir("../..")
	var stdout, stderr bytes.Buffer
	code := runCommand(context.Background(), nil, &stdout, &stderr)
	if code != 0 || !strings.HasPrefix(stdout.String(), "hello\nstatus: succeeded\n") || stderr.Len() != 0 {
		t.Fatalf("默认启动失败: code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
}

func TestCommandHelpDescribesDirectEntry(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := runCommand(context.Background(), []string{"--help"}, &stdout, &stderr)
	if code != 0 || stderr.Len() != 0 || !strings.Contains(stdout.String(), "--message") || strings.Contains(stdout.String(), "demo") || strings.Contains(stdout.String(), "reopen") {
		t.Fatalf("帮助信息不匹配: code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
}

func TestMissingPythonFailsInsteadOfReportingSuccess(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := runCommand(context.Background(), []string{"--python", filepath.Join(t.TempDir(), "missing-python")}, &stdout, &stderr)
	if code != 1 || stdout.Len() != 0 || !strings.Contains(stderr.String(), "运行失败:") {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
}
