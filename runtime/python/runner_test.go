package python

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"agent-runtime/core"
	"agent-runtime/domain"
)

const fixture = `import json, os, sys, time
for line in sys.stdin:
    request = json.loads(line)
    context = request["context"]
    mode = context["event"]["type"]
    response = {"version": 1, "id": request["id"], "result": {"state_update": {"pid": os.getpid(), "attempt": request["id"]}, "actions": []}}
    if mode == "business" or mode == "runtime":
        response.pop("result")
        response["error"] = {"kind": mode, "message": "fixture error"}
    elif mode == "wrong_id": response["id"] = "stale"
    elif mode == "version": response["version"] = 2
    elif mode == "missing_version": response.pop("version")
    elif mode == "null": response["result"] = None
    elif mode == "missing_state": response["result"].pop("state_update")
    elif mode == "null_state": response["result"]["state_update"] = None
    elif mode == "missing_actions": response["result"].pop("actions")
    elif mode == "null_actions": response["result"]["actions"] = None
    elif mode == "bad_action": response["result"]["actions"] = [{"id": "a", "type": "echo"}]
    elif mode == "null_action": response["result"]["actions"] = [None]
    elif mode == "execution": response["result"]["actions"] = [{"id": "a", "type": "echo", "payload": {}, "execution_id": "other"}]
    elif mode == "duplicate": response["result"]["actions"] = [{"id": "a", "type": "echo", "payload": {}}] * 2
    elif mode == "unknown": response["extra"] = True
    elif mode == "both": response["error"] = None
    elif mode == "empty_error": response.pop("result"); response["error"] = {"kind": "business"}
    elif mode == "bad_error": response.pop("result"); response["error"] = {"kind": "unknown", "message": "fixture"}
    elif mode == "nan": response["result"]["state_update"]["value"] = float("nan")
    elif mode == "exit": os._exit(17)
    elif mode == "oversized": sys.stdout.write("x" * (1024 * 1024)); sys.stdout.flush(); time.sleep(60)
    elif mode == "unterminated": sys.stdout.write(json.dumps(response)); sys.stdout.flush(); sys.exit(0)
    elif mode == "malformed": sys.stdout.write("not json\n"); sys.stdout.flush(); continue
    elif mode == "sleep": print("started", file=sys.stderr, flush=True); time.sleep(60)
    elif mode == "short_sleep": print("started", file=sys.stderr, flush=True); time.sleep(0.2)
    elif mode == "precision":
        sys.stdout.write('{"version":1,"id":' + json.dumps(request["id"]) + ',"result":{"state_update":{"big":900719925474099312345,"decimal":0.12345678901234567890123456789},"actions":[]}}\n')
        sys.stdout.flush()
        continue
    print(json.dumps(response), flush=True)
`

func newTestRunner(t *testing.T, source string, options Options) *Runner {
	t.Helper()
	if _, err := exec.LookPath("python3"); err != nil {
		t.Skip("python3 is required for worker integration tests")
	}
	dir := t.TempDir()
	pkg := filepath.Join(dir, "agent_runtime")
	if err := os.Mkdir(pkg, 0o700); err != nil {
		t.Fatal(err)
	}
	for name, data := range map[string]string{"__init__.py": "", "worker.py": source} {
		if err := os.WriteFile(filepath.Join(pkg, name), []byte(data), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	options.SourceDir = dir
	if options.Stderr == nil {
		options.Stderr = io.Discard
	}
	runner, err := NewRunner(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { runner.Close() })
	return runner
}

func input(mode, attempt string) core.ExecutionContext {
	return core.ExecutionContext{
		Agent:       core.AgentSnapshot{ID: "agent", State: map[string]any{}},
		Event:       domain.Event{ID: "event", Type: mode, Payload: map[string]any{}},
		ExecutionID: "execution", AttemptID: domain.ID(attempt),
	}
}

func assertKind(t *testing.T, err error, expected domain.ErrorKind) {
	t.Helper()
	var failure *Error
	if !errors.As(err, &failure) || failure.FailureKind() != expected {
		t.Fatalf("error = %v; want kind %q", err, expected)
	}
}

func TestPersistentWorkerAndAgentErrors(t *testing.T) {
	runner := newTestRunner(t, fixture, Options{})
	first, err := runner.Run(input("ok", "first"))
	if err != nil {
		t.Fatal(err)
	}
	for _, kind := range []domain.ErrorKind{domain.ErrorKindBusiness, domain.ErrorKindRuntime} {
		_, err := runner.Run(input(string(kind), "error"))
		assertKind(t, err, kind)
		if err.Error() != "fixture error" {
			t.Fatalf("unexpected error: %v", err)
		}
	}
	last, err := runner.Run(input("ok", "last"))
	if err != nil {
		t.Fatal(err)
	}
	if first.StateUpdate["pid"] != last.StateUpdate["pid"] || last.StateUpdate["attempt"] != "last" {
		t.Fatalf("worker was replaced or response mismatched: %#v %#v", first, last)
	}
}

func TestInvalidResponsesDiscardWorker(t *testing.T) {
	for _, mode := range []string{
		"wrong_id", "version", "missing_version", "null", "missing_state", "null_state",
		"missing_actions", "null_actions", "bad_action", "null_action", "execution", "duplicate",
		"unknown", "both", "empty_error", "bad_error", "nan", "exit", "oversized", "unterminated", "malformed",
	} {
		t.Run(mode, func(t *testing.T) {
			runner := newTestRunner(t, fixture, Options{})
			if _, err := runner.Run(input("ok", "first")); err != nil {
				t.Fatal(err)
			}
			process := runner.worker
			result, err := runner.Run(input(mode, "bad"))
			assertKind(t, err, domain.ErrorKindRuntime)
			if result.StateUpdate != nil || result.Actions != nil {
				t.Fatalf("invalid response leaked a result: %#v", result)
			}
			if runner.worker != nil || process.cmd.ProcessState == nil {
				t.Fatal("invalid worker was not discarded and reaped")
			}
			if _, err := runner.Run(input("ok", "new")); err != nil {
				t.Fatalf("later call could not start a new worker: %v", err)
			}
		})
	}
}

func TestNumericPrecision(t *testing.T) {
	runner := newTestRunner(t, fixture, Options{})
	result, err := runner.Run(input("precision", "number"))
	if err != nil {
		t.Fatal(err)
	}
	if result.StateUpdate["big"] != json.Number("900719925474099312345") ||
		result.StateUpdate["decimal"] != json.Number("0.12345678901234567890123456789") {
		t.Fatalf("numeric precision lost: %#v", result.StateUpdate)
	}
}

type startedWriter struct {
	ready chan struct{}
	once  sync.Once
}

func (w *startedWriter) Write(data []byte) (int, error) {
	if strings.Contains(string(data), "started") {
		w.once.Do(func() { close(w.ready) })
	}
	return len(data), nil
}

func awaitStarted(t *testing.T, writer *startedWriter) {
	t.Helper()
	select {
	case <-writer.ready:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not start")
	}
}

func TestCancellationKillsAndReaps(t *testing.T) {
	writer := &startedWriter{ready: make(chan struct{})}
	runner := newTestRunner(t, fixture, Options{Stderr: writer})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { _, err := runner.RunContext(ctx, input("sleep", "sleep")); result <- err }()
	awaitStarted(t, writer)
	runner.mu.Lock()
	process := runner.worker
	runner.mu.Unlock()
	cancel()
	select {
	case err := <-result:
		assertKind(t, err, domain.ErrorKindInterrupted)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation cause missing: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("cancellation did not unblock call")
	}
	if process.cmd.ProcessState == nil || runner.worker != nil {
		t.Fatal("canceled worker was not reaped")
	}
	if result, err := runner.Run(input("ok", "next")); err != nil || result.StateUpdate["attempt"] != "next" {
		t.Fatalf("next call consumed a stale response: %#v %v", result, err)
	}
}

func TestTimeout(t *testing.T) {
	runner := newTestRunner(t, fixture, Options{Timeout: 100 * time.Millisecond})
	_, err := runner.Run(input("sleep", "timeout"))
	assertKind(t, err, domain.ErrorKindInterrupted)
	if !errors.Is(err, context.DeadlineExceeded) || runner.worker != nil {
		t.Fatalf("deadline not enforced: %v", err)
	}
}

func TestCanceledWaitDoesNotKillActiveWorker(t *testing.T) {
	writer := &startedWriter{ready: make(chan struct{})}
	runner := newTestRunner(t, fixture, Options{Stderr: writer})
	result := make(chan error, 1)
	go func() { _, err := runner.Run(input("short_sleep", "active")); result <- err }()
	awaitStarted(t, writer)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := runner.RunContext(ctx, input("ok", "canceled"))
	assertKind(t, err, domain.ErrorKindInterrupted)
	if err := <-result; err != nil {
		t.Fatalf("waiting call interrupted active call: %v", err)
	}
	if runner.worker == nil {
		t.Fatal("waiting call discarded active worker")
	}
}

func TestConcurrentCalls(t *testing.T) {
	runner := newTestRunner(t, fixture, Options{})
	var pending sync.WaitGroup
	for i := range 12 {
		pending.Go(func() {
			id := fmt.Sprintf("attempt-%d", i)
			result, err := runner.Run(input("ok", id))
			if err != nil || result.StateUpdate["attempt"] != id {
				t.Errorf("concurrent response mismatch: %#v %v", result, err)
			}
		})
	}
	pending.Wait()
}

func TestCloseInterruptsCallAndRejectsFutureCalls(t *testing.T) {
	writer := &startedWriter{ready: make(chan struct{})}
	runner := newTestRunner(t, fixture, Options{Stderr: writer})
	result := make(chan error, 1)
	go func() { _, err := runner.Run(input("sleep", "active")); result <- err }()
	awaitStarted(t, writer)
	if err := runner.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		assertKind(t, err, domain.ErrorKindInterrupted)
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("close cause missing: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("close did not interrupt active call")
	}
	for range 5 {
		_, err := runner.Run(input("ok", "closed"))
		if !errors.Is(err, ErrClosed) {
			t.Fatalf("closed runner accepted a call: %v", err)
		}
	}
	if err := runner.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestTimeoutWhileWorkerDoesNotRead(t *testing.T) {
	runner := newTestRunner(t, "import time\ntime.sleep(60)\n", Options{Timeout: 100 * time.Millisecond})
	request := input("ok", "blocked-write")
	request.Event.Payload["large"] = strings.Repeat("a", MaxFrameBytes/2)
	_, err := runner.Run(request)
	assertKind(t, err, domain.ErrorKindInterrupted)
	if !errors.Is(err, context.DeadlineExceeded) || runner.worker != nil {
		t.Fatalf("blocked write did not honor deadline: %v", err)
	}
}

func TestRejectInvalidRequestsBeforeStarting(t *testing.T) {
	runner := newTestRunner(t, fixture, Options{})
	for _, request := range []core.ExecutionContext{input("ok", ""), input("ok", "large"), input("ok", "invalid")} {
		if request.AttemptID == "large" {
			request.Event.Payload["large"] = strings.Repeat("a", MaxFrameBytes)
		}
		if request.AttemptID == "invalid" {
			request.Event.Payload["invalid"] = make(chan int)
		}
		_, err := runner.Run(request)
		assertKind(t, err, domain.ErrorKindRuntime)
		if runner.worker != nil {
			t.Fatal("worker started for invalid request")
		}
	}
}

func TestMissingInterpreter(t *testing.T) {
	runner := newTestRunner(t, fixture, Options{Python: filepath.Join(t.TempDir(), "missing-python")})
	_, err := runner.Run(input("ok", "missing"))
	assertKind(t, err, domain.ErrorKindRuntime)
	if runner.worker != nil {
		t.Fatal("worker was retained after start failure")
	}
}
