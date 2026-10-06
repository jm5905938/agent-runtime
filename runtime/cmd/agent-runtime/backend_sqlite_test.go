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
	"runtime"
	"testing"
	"time"
)

func TestSQLiteBackendFailuresReleaseResources(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	directory := filepath.Join(t.TempDir(), "canceled")
	if _, err := openCommandBackend(ctx, directory); !errors.Is(err, context.Canceled) {
		t.Fatalf("已取消的打开请求: %v", err)
	}
	if _, err := os.Stat(directory); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("已取消的请求创建了目录: %v", err)
	}
	file := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(file, []byte("保留"), 0600); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{file, filepath.Join(file, "child")} {
		var stdout, stderr bytes.Buffer
		if code := runCommand(context.Background(), []string{"status", "--data-dir", path, "--json"}, &stdout, &stderr); code != 1 || stdout.Len() != 0 {
			t.Fatalf("无效目录返回成功: code=%d %s %s", code, stdout.String(), stderr.String())
		}
		assertCommandError(t, stderr.Bytes(), "operation", 1)
	}
	directory = filepath.Join(t.TempDir(), "data")
	for range 2 {
		var stdout, stderr bytes.Buffer
		if code := runCommand(context.Background(), []string{"init", "--data-dir", directory, "--python-source", filepath.Join(directory, "missing"), "--json"}, &stdout, &stderr); code != 1 || stdout.Len() != 0 {
			t.Fatalf("绑定失败返回成功: code=%d %s %s", code, stdout.String(), stderr.String())
		}
		assertCommandError(t, stderr.Bytes(), "operation", 1)
		handle, err := openCommandBackend(context.Background(), directory)
		if err != nil {
			t.Fatalf("绑定失败未释放后端: %v", err)
		}
		session, err := handle.Store.OpenSession(context.Background())
		if err != nil {
			handle.Close()
			t.Fatalf("绑定失败未释放session: %v", err)
		}
		agents, err := session.ListAgents(context.Background())
		if err != nil || len(agents) != 0 {
			t.Errorf("绑定失败留下agent: %+v %v", agents, err)
		}
		if err := session.Close(context.Background()); err != nil {
			t.Error(err)
		}
		if err := handle.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

type sqliteCommandProcess struct {
	binary string
	python string
	source string
}

func (p sqliteCommandProcess) call(t *testing.T, directory string, code int, args ...string) (cli.Result, []byte) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	args = append([]string{"--data-dir", directory, "--python", p.python, "--python-source", p.source, "--json"}, args...)
	cmd := exec.CommandContext(ctx, p.binary, args...)
	cmd.Dir = filepath.Dir(directory)
	var stdout, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	err := cmd.Run()
	if ctx.Err() != nil {
		t.Fatalf("命令超时: %q", args)
	}
	actual := 0
	if err != nil {
		var exit *exec.ExitError
		if !errors.As(err, &exit) {
			t.Fatal(err)
		}
		actual = exit.ExitCode()
	}
	if actual != code {
		t.Fatalf("命令退出码错误: args=%q code=%d stdout=%s stderr=%s", args, actual, stdout.String(), stderr.String())
	}
	var result cli.Result
	if code == 0 {
		if err := json.Unmarshal(stdout.Bytes(), &result); err != nil || stderr.Len() != 0 {
			t.Fatalf("命令输出无效: %s %s %v", stdout.String(), stderr.String(), err)
		}
	} else if stdout.Len() != 0 {
		t.Fatalf("失败命令输出了成功结果: %s", stdout.String())
	}
	return result, stderr.Bytes()
}

func TestSQLiteCommandsAcrossProcesses(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("需要python3")
	}
	source, err := filepath.Abs("../../../python/src")
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "agent-runtime")
	if runtime.GOOS == "windows" {
		binary += ".exe"
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if output, err := exec.CommandContext(ctx, "go", "build", "-o", binary, ".").CombinedOutput(); err != nil {
		t.Fatalf("构建cli: %v\n%s", err, output)
	}
	p := sqliteCommandProcess{binary: binary, python: python, source: source}
	t.Run("round_trip_retry_and_ownership", func(t *testing.T) {
		directory := filepath.Join(t.TempDir(), "数据 # %")
		created, _ := p.call(t, directory, 0, "init", "--name", "持久echo")
		agentID := string(created.Agent.ID)
		first, _ := p.call(t, directory, 0, "submit", "--agent", agentID, "--event-id", "first", "--message", "你好\nhello")
		duplicate, _ := p.call(t, directory, 0, "submit", "--agent", agentID, "--event-id", "first", "--message", "你好\nhello")
		if first.Submission.Duplicate || !duplicate.Submission.Duplicate || first.Submission.Delivery != duplicate.Submission.Delivery {
			t.Fatal("跨进程重复投递改变身份")
		}
		_, stderr := p.call(t, directory, 1, "submit", "--agent", agentID, "--event-id", "first", "--message", "different")
		assertCommandError(t, stderr, "conflict", 1)
		pending, _ := p.call(t, directory, 0, "status", "--agent", agentID, "--python", "/missing-python")
		if pending.Query.Agent.StateVersion != 0 || len(pending.Query.Deliveries) != 1 || pending.Query.Deliveries[0].Execution != nil {
			t.Fatalf("status未保留待办或执行了业务: %+v", pending.Query)
		}
		p.call(t, directory, 0, "submit", "--agent", agentID, "--event-id", "second", "--message", "")
		ran, _ := p.call(t, directory, 0, "run")
		if ran.Run.After.Deliveries[domain.DeliveryStatusFailed] != 1 || ran.Run.After.Actions[domain.ActionStatusSucceeded] != 1 {
			t.Fatalf("未保存busy失败: %+v", ran.Run)
		}
		status, _ := p.call(t, directory, 0, "status", "--agent", agentID)
		if status.Query.Agent.State["result"] != "你好\nhello" || status.Query.Agent.StateVersion != 2 {
			t.Fatalf("跨进程echo结果丢失: %+v", status.Query)
		}
		p.call(t, directory, 0, "retry", "--agent", agentID, "--event-id", "second")
		pending, _ = p.call(t, directory, 0, "status", "--agent", agentID)
		if pending.Query.Deliveries[1].Delivery.Status != domain.DeliveryStatusPending || pending.Query.Deliveries[1].Execution.AttemptCount != 1 || pending.Query.Agent.StateVersion != 2 {
			t.Fatalf("retry执行了业务或丢失历史: %+v", pending.Query)
		}
		ran, _ = p.call(t, directory, 0, "run")
		status, _ = p.call(t, directory, 0, "status", "--agent", agentID)
		if status.Query.Agent.State["result"] != "" || status.Query.Agent.State["request_status"] != "succeeded" || status.Query.Agent.StateVersion != 4 || status.Query.Deliveries[1].Execution.AttemptCount != 2 {
			t.Fatalf("跨进程retry失败: %+v", status.Query)
		}
		idle, _ := p.call(t, directory, 0, "run")
		if idle.Run.After.ExecutionAttempts != ran.Run.After.ExecutionAttempts || idle.Run.After.ActionAttempts != ran.Run.After.ActionAttempts {
			t.Fatal("重开后重复执行已完成工作")
		}
		handle, err := openCommandBackend(context.Background(), directory)
		if err != nil {
			t.Fatal(err)
		}
		defer handle.Close()
		session, err := handle.Store.OpenSession(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer session.Close(context.Background())
		_, stderr = p.call(t, directory, 1, "status")
		assertCommandError(t, stderr, "store_owned", 1)
		if err := session.Close(context.Background()); err != nil {
			t.Fatal(err)
		}
		list, _ := p.call(t, directory, 0, "status")
		if len(list.Agents) != 1 || string(list.Agents[0].ID) != agentID {
			t.Fatalf("所有权释放后数据改变: %+v", list)
		}
	})
	for _, mode := range []string{"execution", "action"} {
		t.Run("killed_"+mode, func(t *testing.T) {
			directory := filepath.Join(t.TempDir(), "data")
			created, _ := p.call(t, directory, 0, "init")
			agentID := string(created.Agent.ID)
			p.call(t, directory, 0, "submit", "--agent", agentID, "--event-id", "interrupted", "--message", "恢复消息")
			receipt := killSQLiteClaimProcess(t, directory, mode, agentID)
			status, _ := p.call(t, directory, 0, "status", "--agent", agentID)
			if mode == "execution" {
				if len(status.StartupRecovery.RequeuedDeliveries) != 1 || status.Query.Deliveries[0].Execution.ID != receipt.ExecutionID || status.Query.Deliveries[0].Attempts[0].Status != domain.AttemptStatusInterrupted || status.Query.Agent.StateVersion != 0 {
					t.Fatalf("强杀后execution未恢复: %+v", status)
				}
			} else {
				if len(status.StartupRecovery.UnknownActions) != 1 || status.StartupRecovery.UnknownActions[0] != receipt.ActionID || len(status.StartupRecovery.RetryableActions) != 1 || status.Query.Actions[0].Action.Status != domain.ActionStatusUnknown || status.Query.Agent.State["request_status"] != "waiting" {
					t.Fatalf("强杀后action未恢复: %+v", status)
				}
			}
			p.call(t, directory, 0, "run")
			status, _ = p.call(t, directory, 0, "status", "--agent", agentID)
			if status.Query.Agent.State["request_status"] != "succeeded" || status.Query.Agent.State["result"] != "恢复消息" || status.Query.Agent.StateVersion != 2 || len(status.Query.Actions) != 1 || len(status.Query.Deliveries) != 2 {
				t.Fatalf("恢复后闭环未完成: %+v", status.Query)
			}
			if mode == "execution" && (status.Query.Deliveries[0].Execution.ID != receipt.ExecutionID || status.Query.Deliveries[0].Execution.AttemptCount != 2) {
				t.Fatalf("恢复丢失execution身份或历史: %+v", status.Query.Deliveries[0])
			}
			if mode == "action" && (status.Query.Actions[0].Action.Request.ID != receipt.ActionID || status.Query.Actions[0].Action.AttemptCount != 2 || status.Query.Actions[0].Action.ResultEventID != receipt.ResultEventID || status.Query.Deliveries[0].Execution.AttemptCount != 1) {
				t.Fatalf("恢复丢失action身份或重跑来源execution: %+v", status.Query)
			}
		})
	}
}

type sqliteClaimReceipt struct {
	ExecutionID   domain.ID
	ActionID      domain.ID
	ResultEventID domain.ID
}

func killSQLiteClaimProcess(t *testing.T, directory, mode, agentID string) sqliteClaimReceipt {
	t.Helper()
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary, "-test.run=^TestSQLiteClaimProcess$", "--", directory, mode, agentID)
	cmd.Env = append(os.Environ(), "AGENT_RUNTIME_CLI_CLAIM_HELPER=1")
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	defer stdin.Close()
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if cmd.ProcessState == nil {
			cmd.Process.Kill()
			cmd.Wait()
		}
	}()
	var receipt sqliteClaimReceipt
	if err := json.NewDecoder(stdout).Decode(&receipt); err != nil {
		cmd.Process.Kill()
		cmd.Wait()
		t.Fatalf("等待claim提交: %v %s", err, stderr.String())
	}
	if err := cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	if err := cmd.Wait(); err == nil {
		t.Fatal("测试进程正常退出，未验证强杀恢复")
	}
	return receipt
}

func TestSQLiteClaimProcess(t *testing.T) {
	if os.Getenv("AGENT_RUNTIME_CLI_CLAIM_HELPER") != "1" {
		return
	}
	args := os.Args[len(os.Args)-3:]
	ctx := context.Background()
	must := func(err error) {
		t.Helper()
		if err != nil {
			t.Fatal(err)
		}
	}
	handle, err := openCommandBackend(ctx, args[0])
	must(err)
	s, err := handle.Store.OpenSession(ctx)
	must(err)
	_, err = s.Recover(ctx)
	must(err)
	claim, err := s.ClaimExecution(ctx, domain.DeliveryKey{AgentID: domain.ID(args[2]), EventID: "interrupted"})
	must(err)
	receipt := sqliteClaimReceipt{ExecutionID: claim.Token.ExecutionID}
	if args[1] == "action" {
		action := domain.NewAction("echo", claim.Event.Payload)
		action.BindExecution(claim.Token.ExecutionID)
		record := domain.ActionRecord{Request: action, AgentID: claim.Agent.ID, HandlerVersion: "1", RecoveryPolicy: domain.RecoveryPolicySafeRetry,
			MaxAttempts: 3, Status: domain.ActionStatusPending, ResultEventID: domain.NewEvent("action.result", nil).ID}
		_, err = s.CommitExecution(ctx, core.ExecutionCommit{Token: claim.Token, Actions: []domain.ActionRecord{record}, StateUpdate: map[string]any{
			"request_status": "waiting", "request_event_id": string(claim.Event.ID), "request_execution_id": string(claim.Token.ExecutionID), "waiting_action_id": string(action.ID),
		}})
		must(err)
		_, err = s.ClaimAction(ctx, action.ID)
		must(err)
		receipt.ActionID, receipt.ResultEventID = action.ID, record.ResultEventID
	}
	must(json.NewEncoder(os.Stdout).Encode(receipt))
	_, _ = io.Copy(io.Discard, os.Stdin)
	os.Exit(0)
}
