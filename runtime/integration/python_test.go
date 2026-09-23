package integration_test

import (
	"agent-runtime/core"
	"agent-runtime/domain"
	"agent-runtime/python"
	"context"
	"errors"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"
)

type observedStore struct {
	core.RecoveryStore
	session core.RecoverySession
}

func (s *observedStore) OpenSession(ctx context.Context) (core.RecoverySession, error) {
	session, err := s.RecoveryStore.OpenSession(ctx)
	s.session = session
	return session, err
}

type pythonRuntime struct {
	runtime *core.Runtime
	runner  *python.Runner
	store   core.RecoverySession
}

func openPythonRuntime(t *testing.T, backend core.RecoveryStore, handler core.ActionHandler, policy domain.RecoveryPolicy) *pythonRuntime {
	t.Helper()
	executable, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("real Python integration requires python3 on PATH")
	}
	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot locate Python sources")
	}
	runner, err := python.NewRunner(python.Options{
		Python: executable, SourceDir: filepath.Join(filepath.Dir(sourceFile), "..", "..", "python", "src"),
		Timeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := runner.Close(); err != nil {
			t.Errorf("close Python worker: %v", err)
		}
	})
	observed := &observedStore{RecoveryStore: backend}
	rt, err := core.OpenRuntime(context.Background(), observed)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := rt.Close(ctx); err != nil {
			t.Errorf("close runtime: %v", err)
		}
	})
	if err := rt.RegisterDefinition(domain.DefinitionRef{ID: "echo", Version: "1"}, runner); err != nil {
		t.Fatal(err)
	}
	if err := rt.Executor().RegisterWithOptions("echo", handler, core.HandlerOptions{
		Version: "1", RecoveryPolicy: policy, MaxAttempts: 3,
	}); err != nil {
		t.Fatal(err)
	}
	return &pythonRuntime{runtime: rt, runner: runner, store: observed.session}
}

func (p *pythonRuntime) createAgent(t *testing.T) core.AgentSnapshot {
	t.Helper()
	agent, err := p.runtime.CreateAgent("Python echo", domain.DefinitionRef{ID: "echo", Version: "1"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	return agent
}

func snapshot(t *testing.T, rt *core.Runtime, agentID domain.ID) core.AgentSnapshot {
	t.Helper()
	agent, err := rt.Agent(agentID)
	if err != nil {
		t.Fatal(err)
	}
	return agent
}

func startEcho(t *testing.T, p *pythonRuntime, agentID domain.ID, message string) (domain.Event, domain.Action) {
	t.Helper()
	event := domain.NewEvent("echo.request", map[string]any{"message": message})
	result, err := p.runtime.Process(agentID, event)
	if err != nil || len(result.Actions) != 1 {
		t.Fatalf("Python request: result=%+v, err=%v", result, err)
	}
	action := result.Actions[0]
	waiting := snapshot(t, p.runtime, agentID)
	if waiting.State["request_status"] != "waiting" || waiting.State["request_event_id"] != string(event.ID) ||
		waiting.State["waiting_action_id"] != string(action.ID) || action.ExecutionID == nil ||
		waiting.State["request_execution_id"] != string(*action.ExecutionID) {
		t.Fatalf("request lost its correlation: agent=%+v, action=%+v", waiting, action)
	}
	return event, action
}

func assertSucceeded(t *testing.T, p *pythonRuntime, agentID domain.ID, message string, actionID domain.ID) core.AgentSnapshot {
	t.Helper()
	agent := snapshot(t, p.runtime, agentID)
	action, err := p.store.LoadAction(context.Background(), actionID)
	if err != nil {
		t.Fatal(err)
	}
	if agent.Status != domain.AgentStatusActive || agent.State["request_status"] != "succeeded" ||
		agent.State["result"] != message || agent.State["result_event_id"] != string(action.Action.ResultEventID) ||
		action.Action.Status != domain.ActionStatusSucceeded || len(action.Attempts) != 1 {
		t.Fatalf("echo did not complete: agent=%+v, action=%+v", agent, action)
	}
	return agent
}

func assertActionCount(t *testing.T, p *pythonRuntime, want int) {
	t.Helper()
	actions, err := p.store.ListActions(context.Background())
	if err != nil || len(actions) != want {
		t.Fatalf("actions: count=%d, want=%d, err=%v", len(actions), want, err)
	}
}

func TestPythonEchoRoundTripAndDuplicateDelivery(t *testing.T) {
	for _, message := range []string{"hello", "你好，世界 🌍\nsecond line", ""} {
		t.Run(message, func(t *testing.T) {
			p := openPythonRuntime(t, core.NewMemoryRecoveryStore(), core.EchoHandler{}, domain.RecoveryPolicySafeRetry)
			agent := p.createAgent(t)
			request, action := startEcho(t, p, agent.ID, message)
			if err := p.runtime.RunUntilIdle(); err != nil {
				t.Fatal(err)
			}
			completed := assertSucceeded(t, p, agent.ID, message, action.ID)
			if completed.StateVersion != 2 || len(p.runtime.Executions()) != 2 {
				t.Fatalf("expected request and result executions: state version=%d, executions=%d", completed.StateVersion, len(p.runtime.Executions()))
			}
			assertActionCount(t, p, 1)

			stored, err := p.store.LoadAction(context.Background(), action.ID)
			if err != nil {
				t.Fatal(err)
			}
			resultEvent, err := p.store.LoadEvent(context.Background(), stored.Action.ResultEventID)
			if err != nil {
				t.Fatal(err)
			}
			for _, event := range []domain.Event{request, *resultEvent} {
				received, err := p.runtime.SubmitContext(context.Background(), agent.ID, event)
				if err != nil || !received.Duplicate {
					t.Fatalf("duplicate delivery: received=%+v, err=%v", received, err)
				}
				if _, err := p.runtime.Process(agent.ID, event); err != nil {
					t.Fatalf("reuse committed execution: %v", err)
				}
			}
			if err := p.runtime.RunUntilIdle(); err != nil {
				t.Fatal(err)
			}
			if got := snapshot(t, p.runtime, agent.ID); !reflect.DeepEqual(got, completed) || len(p.runtime.Executions()) != 2 {
				t.Fatalf("duplicate changed completed agent: %+v", got)
			}
			assertActionCount(t, p, 1)
		})
	}
}

func TestPythonEchoBusyRequestCanBeRetried(t *testing.T) {
	p := openPythonRuntime(t, core.NewMemoryRecoveryStore(), core.EchoHandler{}, domain.RecoveryPolicySafeRetry)
	agent := p.createAgent(t)
	_, firstAction := startEcho(t, p, agent.ID, "first")
	waiting := snapshot(t, p.runtime, agent.ID)
	second := domain.NewEvent("echo.request", map[string]any{"message": "second"})
	if _, err := p.runtime.Process(agent.ID, second); !errors.Is(err, core.ErrExecutionFailed) {
		t.Fatalf("busy request should fail its execution: %v", err)
	}
	if got := snapshot(t, p.runtime, agent.ID); !reflect.DeepEqual(got, waiting) {
		t.Fatalf("busy request changed first request: %+v", got)
	}
	assertActionCount(t, p, 1)
	if err := p.runtime.RunUntilIdle(); err != nil {
		t.Fatal(err)
	}
	assertSucceeded(t, p, agent.ID, "first", firstAction.ID)
	key := domain.DeliveryKey{AgentID: agent.ID, EventID: second.ID}
	if err := p.runtime.Retry(context.Background(), key); err != nil {
		t.Fatal(err)
	}
	if err := p.runtime.RunUntilIdle(); err != nil {
		t.Fatal(err)
	}
	agent = snapshot(t, p.runtime, agent.ID)
	if agent.State["request_status"] != "succeeded" || agent.State["result"] != "second" ||
		agent.State["request_event_id"] != string(second.ID) || agent.StateVersion != 4 {
		t.Fatalf("retried request did not complete: %+v", agent)
	}
	assertActionCount(t, p, 2)
	delivery, err := p.store.LoadDelivery(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	execution, err := p.store.LoadExecution(context.Background(), delivery.ExecutionID)
	if err != nil || execution.Execution.AttemptCount != 2 || len(execution.Attempts) != 2 ||
		execution.Attempts[0].Error == nil || execution.Attempts[0].Error.Kind != domain.ErrorKindBusiness ||
		execution.Attempts[1].Status != domain.AttemptStatusSucceeded {
		t.Fatalf("busy retry history: %+v, err=%v", execution, err)
	}
}

func TestPythonEchoInstancesShareWorkerWithoutSharingState(t *testing.T) {
	p := openPythonRuntime(t, core.NewMemoryRecoveryStore(), core.EchoHandler{}, domain.RecoveryPolicySafeRetry)
	first, second := p.createAgent(t), p.createAgent(t)
	_, firstAction := startEcho(t, p, first.ID, "first instance")
	_, secondAction := startEcho(t, p, second.ID, "second instance")
	if firstAction.ID == secondAction.ID {
		t.Fatal("distinct instances emitted the same action identity")
	}
	if err := p.runtime.RunUntilIdle(); err != nil {
		t.Fatal(err)
	}
	assertSucceeded(t, p, first.ID, "first instance", firstAction.ID)
	assertSucceeded(t, p, second.ID, "second instance", secondAction.ID)
	if len(p.runtime.Executions()) != 4 {
		t.Fatalf("expected independent request and result executions: %+v", p.runtime.Executions())
	}
	assertActionCount(t, p, 2)
}

func TestPythonEchoUnexpectedExceptionPreservesStateAndWorker(t *testing.T) {
	p := openPythonRuntime(t, core.NewMemoryRecoveryStore(), core.EchoHandler{}, domain.RecoveryPolicySafeRetry)
	initialState := map[string]any{"request_status": "corrupt", "result": "preserve me"}
	agent, err := p.runtime.CreateAgent("invalid Python echo state", domain.DefinitionRef{ID: "echo", Version: "1"}, initialState)
	if err != nil {
		t.Fatal(err)
	}
	event := domain.NewEvent("echo.request", map[string]any{"message": "hello"})
	if _, err := p.runtime.Process(agent.ID, event); !errors.Is(err, core.ErrExecutionFailed) {
		t.Fatalf("unexpected Python exception should fail its execution: %v", err)
	}
	if got := snapshot(t, p.runtime, agent.ID); got.StateVersion != 0 || !reflect.DeepEqual(got.State, initialState) {
		t.Fatalf("Python exception changed saved state: %+v", got)
	}
	assertActionCount(t, p, 0)
	delivery, err := p.store.LoadDelivery(context.Background(), domain.DeliveryKey{AgentID: agent.ID, EventID: event.ID})
	if err != nil {
		t.Fatal(err)
	}
	execution, err := p.store.LoadExecution(context.Background(), delivery.ExecutionID)
	if err != nil {
		t.Fatal(err)
	}
	if delivery.Status != domain.DeliveryStatusFailed || execution.Execution.Status != domain.ExecutionStatusFailed ||
		len(execution.Attempts) != 1 || execution.Attempts[0].Status != domain.AttemptStatusFailed ||
		execution.Attempts[0].Error == nil || execution.Attempts[0].Error.Kind != domain.ErrorKindRuntime ||
		!strings.Contains(execution.Attempts[0].Error.Message, "request_status") {
		t.Fatalf("Python runtime exception was not persisted: delivery=%+v, execution=%+v", delivery, execution)
	}

	healthy := p.createAgent(t)
	_, action := startEcho(t, p, healthy.ID, "still works")
	if err := p.runtime.RunUntilIdle(); err != nil {
		t.Fatal(err)
	}
	assertSucceeded(t, p, healthy.ID, "still works", action.ID)
	assertActionCount(t, p, 1)
}

func TestPythonEchoReopenPreservesPendingAction(t *testing.T) {
	backend := core.NewMemoryRecoveryStore()
	first := openPythonRuntime(t, backend, core.EchoHandler{}, domain.RecoveryPolicySafeRetry)
	agent := first.createAgent(t)
	request, action := startEcho(t, first, agent.ID, "restore me")
	pending, err := first.store.LoadAction(context.Background(), action.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.runtime.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := first.runner.Close(); err != nil {
		t.Fatal(err)
	}

	reopened := openPythonRuntime(t, backend, core.EchoHandler{}, domain.RecoveryPolicySafeRetry)
	if err := reopened.runtime.RunUntilIdle(); err != nil {
		t.Fatal(err)
	}
	completed := assertSucceeded(t, reopened, agent.ID, "restore me", action.ID)
	restored, err := reopened.store.LoadAction(context.Background(), action.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(restored.Action.Request, pending.Action.Request) ||
		restored.Action.ResultEventID != pending.Action.ResultEventID || completed.StateVersion != 2 {
		t.Fatalf("reopen replaced pending action: before=%+v, after=%+v", pending, restored)
	}
	executions := reopened.runtime.Executions()
	if len(executions) != 2 || executions[*action.ExecutionID].EventID != request.ID || executions[*action.ExecutionID].AttemptCount != 1 {
		t.Fatalf("reopen reran original request: %+v", executions)
	}
	assertActionCount(t, reopened, 1)
}

func TestPythonEchoRejectsStaleAndMalformedResults(t *testing.T) {
	p := openPythonRuntime(t, core.NewMemoryRecoveryStore(), core.EchoHandler{}, domain.RecoveryPolicySafeRetry)
	agent := p.createAgent(t)
	_, action := startEcho(t, p, agent.ID, "keep this")
	waiting := snapshot(t, p.runtime, agent.ID)
	for _, scenario := range []string{"wrong action", "wrong execution", "wrong type", "malformed output", "unknown status"} {
		t.Run(scenario, func(t *testing.T) {
			payload := map[string]any{
				"action_id": string(action.ID), "execution_id": string(*action.ExecutionID),
				"action_type": "echo", "status": "succeeded", "result": map[string]any{"message": "unexpected"},
			}
			switch scenario {
			case "wrong action":
				payload["action_id"] = string(domain.NewAction("echo", nil).ID)
			case "wrong execution":
				payload["execution_id"] = string(domain.NewExecution(agent.ID, domain.NewEvent("unused", nil).ID).ID)
			case "wrong type":
				payload["action_type"] = "other"
			case "malformed output":
				payload["result"] = map[string]any{"message": 42}
			case "unknown status":
				payload["status"] = "unknown"
			}
			if _, err := p.runtime.Process(agent.ID, domain.NewEvent("action.result", payload)); !errors.Is(err, core.ErrExecutionFailed) {
				t.Fatalf("invalid result should fail its execution: %v", err)
			}
			if got := snapshot(t, p.runtime, agent.ID); !reflect.DeepEqual(got, waiting) {
				t.Fatalf("invalid result changed waiting state: %+v", got)
			}
			assertActionCount(t, p, 1)
		})
	}
	if err := p.runtime.RunUntilIdle(); err != nil {
		t.Fatal(err)
	}
	assertSucceeded(t, p, agent.ID, "keep this", action.ID)
}

type handlerFunc func(domain.Action) (map[string]any, error)

func (f handlerFunc) Execute(action domain.Action) (map[string]any, error) { return f(action) }

func TestPythonEchoDistinguishesFailedAndUnknownAction(t *testing.T) {
	for _, scenario := range []string{"failed", "unknown"} {
		t.Run(scenario, func(t *testing.T) {
			calls := 0
			handler := handlerFunc(func(domain.Action) (map[string]any, error) {
				calls++
				if scenario == "unknown" {
					panic("outcome uncertain")
				}
				return nil, errors.New("echo refused")
			})
			p := openPythonRuntime(t, core.NewMemoryRecoveryStore(), handler, domain.RecoveryPolicyManual)
			agent := p.createAgent(t)
			_, action := startEcho(t, p, agent.ID, "hello")
			for range 2 {
				if err := p.runtime.RunUntilIdle(); err != nil {
					t.Fatal(err)
				}
			}
			agent = snapshot(t, p.runtime, agent.ID)
			saved, err := p.store.LoadAction(context.Background(), action.ID)
			if err != nil {
				t.Fatal(err)
			}
			if calls != 1 || agent.Status != domain.AgentStatusActive || string(saved.Action.Status) != scenario {
				t.Fatalf("action outcome: calls=%d, agent=%+v, action=%+v", calls, agent, saved)
			}
			if scenario == "failed" {
				message, _ := agent.State["error"].(string)
				if agent.State["request_status"] != "failed" || !strings.Contains(message, "echo refused") ||
					agent.StateVersion != 2 || len(p.runtime.Executions()) != 2 || saved.Action.Result == nil {
					t.Fatalf("explicit failure was not delivered: agent=%+v, action=%+v", agent, saved)
				}
			} else if agent.State["request_status"] != "waiting" || agent.StateVersion != 1 ||
				len(p.runtime.Executions()) != 1 || saved.Action.Result != nil {
				t.Fatalf("unknown outcome completed business request: agent=%+v, action=%+v", agent, saved)
			}
			assertActionCount(t, p, 1)
		})
	}
}
