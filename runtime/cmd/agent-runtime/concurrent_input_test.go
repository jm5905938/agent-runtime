package main

import (
	"agent-runtime/cli"
	"agent-runtime/domain"
	"agent-runtime/sqlite"
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

type concurrentCommand struct {
	cmd            *exec.Cmd
	stdout, stderr bytes.Buffer
	done           chan struct{}
	err            error
}

func concurrentStartRun(t *testing.T, p sqliteCommandProcess, directory, config string) *concurrentCommand {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	process := &concurrentCommand{done: make(chan struct{})}
	process.cmd = exec.CommandContext(ctx, p.binary,
		"--data-dir", directory, "--python", p.python, "--python-source", p.source,
		"--json", "run", "--env-file", config)
	process.cmd.Dir = filepath.Dir(directory)
	process.cmd.Stdout, process.cmd.Stderr = &process.stdout, &process.stderr
	if err := process.cmd.Start(); err != nil {
		cancel()
		t.Fatal(err)
	}
	go func() {
		process.err = process.cmd.Wait()
		close(process.done)
	}()
	t.Cleanup(func() {
		cancel()
		<-process.done
	})
	return process
}

func concurrentWaitRun(t *testing.T, process *concurrentCommand, code int) cli.Result {
	t.Helper()
	select {
	case <-process.done:
	case <-time.After(10 * time.Second):
		t.Fatal("run未在预期时间内退出")
	}
	actual := 0
	if process.err != nil {
		var exit *exec.ExitError
		if !errors.As(process.err, &exit) {
			t.Fatal(process.err)
		}
		actual = exit.ExitCode()
	}
	if actual != code {
		t.Fatalf("run退出码错误: code=%d stdout=%s stderr=%s", actual, process.stdout.String(), process.stderr.String())
	}
	var result cli.Result
	if code == 0 {
		if err := json.Unmarshal(process.stdout.Bytes(), &result); err != nil || process.stderr.Len() != 0 {
			t.Fatalf("run成功结果无效: %s %s %v", process.stdout.String(), process.stderr.String(), err)
		}
	} else if process.stdout.Len() != 0 {
		t.Fatalf("失败run输出了成功结果: %s", process.stdout.String())
	}
	return result
}

func concurrentAwait(t *testing.T, signal <-chan struct{}, message string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatal(message)
	}
}

func concurrentConfig(t *testing.T, serverURL string) string {
	t.Helper()
	config := chatTestConfig(t, serverURL)
	content, err := os.ReadFile(config)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(config, []byte(strings.ReplaceAll(string(content), "LLM_TIMEOUT=2s", "LLM_TIMEOUT=30s")), 0600); err != nil {
		t.Fatal(err)
	}
	return config
}

type concurrentBusinessSnapshot struct {
	State         string
	StateVersion  string
	ActionID      string
	ActionStatus  string
	ActionAttempt string
	AttemptStatus string
}

func concurrentReadDB(t *testing.T, directory string) *sql.DB {
	t.Helper()
	path := filepath.Join(directory, "store.db")
	dsnPath := filepath.ToSlash(path)
	if filepath.VolumeName(path) != "" && !strings.HasPrefix(dsnPath, "/") {
		dsnPath = "/" + dsnPath
	}
	dsn := (&url.URL{Scheme: "file", Path: dsnPath, RawQuery: "mode=ro"}).String()
	db, err := sql.Open(sqlite.Name, dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	return db
}

func concurrentReadBusiness(t *testing.T, db *sql.DB, agentID string) concurrentBusinessSnapshot {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var snapshot concurrentBusinessSnapshot
	if err := db.QueryRowContext(ctx, `SELECT state_json, state_version FROM agents WHERE id = ?`, agentID).
		Scan(&snapshot.State, &snapshot.StateVersion); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT a.id, a.status, a.attempt_count, aa.status
		FROM actions a JOIN action_attempts aa ON aa.action_id = a.id WHERE a.agent_id = ?`, agentID).
		Scan(&snapshot.ActionID, &snapshot.ActionStatus, &snapshot.ActionAttempt, &snapshot.AttemptStatus); err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func concurrentAssertPending(t *testing.T, db *sql.DB, agentID, eventID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var status string
	var executions int
	if err := db.QueryRowContext(ctx, `SELECT d.status,
		(SELECT COUNT(*) FROM executions e WHERE e.agent_id = d.agent_id AND e.event_id = d.event_id)
		FROM deliveries d WHERE d.agent_id = ? AND d.event_id = ?`, agentID, eventID).Scan(&status, &executions); err != nil {
		t.Fatal(err)
	}
	if status != string(domain.DeliveryStatusPending) || executions != 0 {
		t.Fatalf("并发submit执行了队列输入: event=%s status=%s executions=%d", eventID, status, executions)
	}
}

func TestConcurrentMainCommandsAcrossProcesses(t *testing.T) {
	python := chatTestPython(t)
	binary := filepath.Join(t.TempDir(), "agent-runtime")
	if runtime.GOOS == "windows" {
		binary += ".exe"
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if output, err := exec.CommandContext(ctx, "go", "build", "-o", binary, ".").CombinedOutput(); err != nil {
		t.Fatalf("构建并发输入cli: %v\n%s", err, output)
	}
	p := sqliteCommandProcess{binary: binary, python: python[1], source: python[3]}

	t.Run("submit_while_model_running_preserves_owner_and_fifo", func(t *testing.T) {
		directory := filepath.Join(t.TempDir(), "data")
		created, _ := p.call(t, directory, 0, "init", "--definition", "main")
		agentID := string(created.Agent.ID)
		p.call(t, directory, 0, "submit", "--agent", agentID, "--event-id", "first", "--message", "第一轮")
		entered, release := make(chan struct{}), make(chan struct{})
		var once sync.Once
		releaseProvider := func() { once.Do(func() { close(release) }) }
		var captured mainToolsModelCapture
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			request := mainToolsReadRequest(t, r)
			captured.append(request)
			if mainQueueUser(t, request) == "第一轮" {
				close(entered)
				select {
				case <-release:
				case <-r.Context().Done():
					return
				}
			}
			mainToolsReply(t, w, "回复："+mainQueueUser(t, request))
		}))
		t.Cleanup(server.Close)
		t.Cleanup(releaseProvider)
		process := concurrentStartRun(t, p, directory, concurrentConfig(t, server.URL))
		concurrentAwait(t, entered, "模型请求未开始")
		db := concurrentReadDB(t, directory)
		before := concurrentReadBusiness(t, db, agentID)
		if before.ActionStatus != string(domain.ActionStatusRunning) || before.ActionAttempt != "1" || before.AttemptStatus != string(domain.AttemptStatusRunning) {
			t.Fatalf("fixture没有暂停执行中的模型操作: %+v", before)
		}
		second, _ := p.call(t, directory, 0, "submit", "--agent", agentID, "--event-id", "second", "--message", "第二轮")
		third, _ := p.call(t, directory, 0, "submit", "--agent", agentID, "--event-id", "third", "--message", "第三轮")
		duplicate, _ := p.call(t, directory, 0, "submit", "--agent", agentID, "--event-id", "second", "--message", "第二轮")
		if second.Submission.Duplicate || third.Submission.Duplicate || !duplicate.Submission.Duplicate || second.Submission.Delivery != duplicate.Submission.Delivery {
			t.Fatal("运行期间submit没有保留去重身份")
		}
		_, stderr := p.call(t, directory, 1, "submit", "--agent", agentID, "--event-id", "second", "--message", "冲突消息")
		assertCommandError(t, stderr, "conflict", 1)
		concurrentAssertPending(t, db, agentID, "second")
		concurrentAssertPending(t, db, agentID, "third")
		after := concurrentReadBusiness(t, db, agentID)
		if !reflect.DeepEqual(before, after) {
			t.Fatalf("并发submit恢复或改变了执行中的业务: before=%+v after=%+v", before, after)
		}
		if len(captured.all()) != 1 {
			t.Fatal("首轮等待期间开始了后续模型请求")
		}
		releaseProvider()
		ran := concurrentWaitRun(t, process, 0)
		status, _ := p.call(t, directory, 0, "status", "--agent", agentID)
		mainToolsAssertFinished(t, status.Query, "回复：第三轮", 3)
		if len(status.Query.Deliveries) != 6 || status.Query.Agent.StateVersion != 6 || ran.Run.After.ExecutionAttempts != 6 || ran.Run.After.ActionAttempts != 3 {
			t.Fatalf("原run未恰好完成同时接收的三个输入: %+v %+v", status.Query, ran.Run)
		}
		requests := captured.all()
		if len(requests) != 3 {
			t.Fatalf("并发输入模型调用次数错误: %d", len(requests))
		}
		var history []mainSQLiteMessage
		for i, message := range []string{"第一轮", "第二轮", "第三轮"} {
			input := append(append([]mainSQLiteMessage(nil), history...), mainSQLiteMessage{Role: "user", Content: message})
			mainSQLiteAssertMessages(t, requests[i].Messages, input)
			history = append(input, mainSQLiteMessage{Role: "assistant", Content: "回复：" + message})
		}
		mainSQLiteAssertMessages(t, status.Query.Agent.State["messages"], history)
	})

	t.Run("interrupt_cancels_http_and_keeps_unknown_without_replay", func(t *testing.T) {
		if runtime.GOOS == "windows" {
			t.Skip("需要可发送SIGINT的进程")
		}
		directory := filepath.Join(t.TempDir(), "data")
		created, _ := p.call(t, directory, 0, "init", "--definition", "main")
		agentID := string(created.Agent.ID)
		p.call(t, directory, 0, "submit", "--agent", agentID, "--event-id", "cancel-first", "--message", "取消轮次")
		p.call(t, directory, 0, "submit", "--agent", agentID, "--event-id", "cancel-next", "--message", "等待下一轮")
		entered, canceled := make(chan struct{}), make(chan struct{})
		var captured mainToolsModelCapture
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			captured.append(mainToolsReadRequest(t, r))
			close(entered)
			<-r.Context().Done()
			close(canceled)
		}))
		t.Cleanup(server.Close)
		config := concurrentConfig(t, server.URL)
		process := concurrentStartRun(t, p, directory, config)
		concurrentAwait(t, entered, "取消测试的模型请求未开始")
		if err := process.cmd.Process.Signal(os.Interrupt); err != nil {
			t.Fatal(err)
		}
		concurrentAwait(t, canceled, "SIGINT没有及时取消模型HTTP请求")
		concurrentWaitRun(t, process, 1)
		assertCommandError(t, process.stderr.Bytes(), "canceled", 1)
		before, _ := p.call(t, directory, 0, "status", "--agent", agentID)
		mainQueueAssertWaiting(t, before.Query, "cancel-next")
		if len(before.Query.Actions) != 1 || before.Query.Actions[0].Action.Status != domain.ActionStatusUnknown || before.Query.Actions[0].Action.AttemptCount != 1 || len(before.Query.Actions[0].Attempts) != 1 {
			t.Fatalf("取消后的模型操作未保存为unknown: %+v", before.Query)
		}
		p.call(t, directory, 0, "run", "--env-file", filepath.Join(t.TempDir(), "missing.env"))
		after, _ := p.call(t, directory, 0, "status", "--agent", agentID)
		mainQueueAssertUnchanged(t, before.Query, after.Query)
		if len(captured.all()) != 1 {
			t.Fatal("重启重复调用了已取消且结果未知的模型操作")
		}
	})
}
