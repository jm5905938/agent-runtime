package core

import (
	"agent-runtime/domain"
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var p4StorageFault = errors.New("injected storage fault")

type p4FaultStore struct {
	RecoveryStore
	stage string
}

func (s p4FaultStore) OpenSession(ctx context.Context) (RecoverySession, error) {
	session, err := s.RecoveryStore.OpenSession(ctx)
	if err != nil {
		return nil, err
	}
	return &p4FaultSession{RecoverySession: session, stage: s.stage}, nil
}

type p4FaultSession struct {
	RecoverySession
	stage string
	fired atomic.Bool
}

func (s *p4FaultSession) Recover(ctx context.Context) (RecoveryReport, error) {
	if s.stage == "recover" {
		return RecoveryReport{}, p4StorageFault
	}
	return s.RecoverySession.Recover(ctx)
}

func (s *p4FaultSession) CommitExecution(ctx context.Context, commit ExecutionCommit) (domain.ExecutionResult, error) {
	if s.stage == "execution before" && !s.fired.Swap(true) {
		return domain.ExecutionResult{}, p4StorageFault
	}
	result, err := s.RecoverySession.CommitExecution(ctx, commit)
	if err == nil && s.stage == "execution after" && !s.fired.Swap(true) {
		return domain.ExecutionResult{}, p4StorageFault
	}
	return result, err
}

func (s *p4FaultSession) CompleteAction(ctx context.Context, completion ActionCompletion) (domain.ActionResult, error) {
	if s.stage == "action before" && !s.fired.Swap(true) {
		return domain.ActionResult{}, p4StorageFault
	}
	result, err := s.RecoverySession.CompleteAction(ctx, completion)
	if err == nil && s.stage == "action after" && !s.fired.Swap(true) {
		return domain.ActionResult{}, p4StorageFault
	}
	return result, err
}

func p4Open(t *testing.T, backend RecoveryStore) *Runtime {
	t.Helper()
	runtime, err := OpenRuntime(context.Background(), backend)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { p4Close(t, runtime) })
	return runtime
}

func p4Close(t *testing.T, runtime *Runtime) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := runtime.Close(ctx); err != nil {
		t.Errorf("close: %v", err)
	}
}

func p4Run(t *testing.T, runtime *Runtime) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- runtime.RunUntilIdle() }()
	return p3Await(t, done)
}

func p4EchoRunner(calls *atomic.Int32) AgentRunner {
	return functionRunner(func(ctx ExecutionContext) (ExecutionResult, error) {
		calls.Add(1)
		if ctx.Event.Type == "action.result" {
			return ExecutionResult{StateUpdate: map[string]any{
				"echoed": ctx.Event.Payload["result"], "result_event_id": string(ctx.Event.ID),
			}}, nil
		}
		return ExecutionResult{StateUpdate: map[string]any{"received": true},
			Actions: []domain.Action{domain.NewAction("echo", ctx.Event.Payload)}}, nil
	})
}

func p4Bind(t *testing.T, runtime *Runtime, ref domain.DefinitionRef, runner AgentRunner, handler ActionHandler, policy domain.RecoveryPolicy) {
	t.Helper()
	if err := runtime.RegisterDefinition(ref, runner); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Executor().RegisterWithOptions("echo", handler, HandlerOptions{
		Version: "1", RecoveryPolicy: policy, MaxAttempts: 3,
	}); err != nil {
		t.Fatal(err)
	}
}

func p4Agent(t *testing.T, runtime *Runtime, runner AgentRunner, handler ActionHandler, policy domain.RecoveryPolicy) AgentSnapshot {
	t.Helper()
	ref := domain.DefinitionRef{ID: "p4-echo", Version: "1"}
	p4Bind(t, runtime, ref, runner, handler, policy)
	agent, err := runtime.CreateAgent("p4 echo", ref, nil)
	if err != nil {
		t.Fatal(err)
	}
	return agent
}

func TestP4CloseWaitsForCallbacksAndKeepsOwnership(t *testing.T) {
	for _, callback := range []string{"runner", "handler"} {
		for _, interrupted := range []string{"canceled", "deadline"} {
			t.Run(callback+"/"+interrupted, func(t *testing.T) {
				backend := NewMemoryRecoveryStore()
				runtime := p4Open(t, backend)
				entered, release := make(chan struct{}, 1), make(chan struct{})
				var once sync.Once
				unblock := func() { once.Do(func() { close(release) }) }
				t.Cleanup(unblock)
				wait := func() { entered <- struct{}{}; <-release }
				runner := functionRunner(func(ctx ExecutionContext) (ExecutionResult, error) {
					if ctx.Event.Type == "action.result" {
						return ExecutionResult{}, nil
					}
					if callback == "runner" {
						wait()
					}
					return ExecutionResult{Actions: []domain.Action{domain.NewAction("echo", nil)}}, nil
				})
				handler := handlerFunc(func(domain.Action) (map[string]any, error) {
					wait()
					return nil, nil
				})
				agent := p4Agent(t, runtime, runner, handler, domain.RecoveryPolicyManual)
				event := domain.NewEvent("request", nil)
				done := make(chan error, 1)
				if callback == "runner" {
					go func() { _, err := runtime.Process(agent.ID, event); done <- err }()
				} else {
					if _, err := runtime.Process(agent.ID, event); err != nil {
						t.Fatal(err)
					}
					go func() { done <- runtime.RunUntilIdle() }()
				}
				p3Await(t, entered)
				ctx, cancel := context.WithCancel(context.Background())
				want := error(context.Canceled)
				if interrupted == "deadline" {
					cancel()
					ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
					want = context.DeadlineExceeded
				} else {
					cancel()
				}
				defer cancel()
				if err := runtime.Close(ctx); !errors.Is(err, want) {
					t.Fatalf("interrupted close: %v", err)
				}
				if other, err := OpenRuntime(context.Background(), backend); !errors.Is(err, ErrStoreOwned) {
					if other != nil {
						p4Close(t, other)
					}
					t.Fatalf("callback still running but owner changed: %v", err)
				}
				p4AssertClosedEntrypoints(t, runtime, agent)
				unblock()
				if err := p3Await(t, done); callback == "runner" && err != nil {
					t.Fatalf("accepted process did not finish: %v", err)
				}
				if other, err := OpenRuntime(context.Background(), backend); !errors.Is(err, ErrStoreOwned) {
					if other != nil {
						p4Close(t, other)
					}
					t.Fatalf("owner released without another close: %v", err)
				}
				if callback == "handler" {
					actions, err := runtime.store.ListActions(context.Background())
					if err != nil || len(actions) != 1 || actions[0].Status != domain.ActionStatusSucceeded {
						t.Fatalf("close did not let handler save its result: actions=%+v err=%v", actions, err)
					}
					delivery, err := runtime.store.LoadDelivery(context.Background(), domain.DeliveryKey{AgentID: agent.ID, EventID: actions[0].ResultEventID})
					if err != nil || delivery.Status != domain.DeliveryStatusPending {
						t.Fatalf("closing loop dispatched new result delivery: %+v, %v", delivery, err)
					}
				}
				p4Close(t, runtime)
				_ = p4Open(t, backend)
				p4AssertClosedEntrypoints(t, runtime, agent)
			})
		}
	}
}

func p4AssertClosedEntrypoints(t *testing.T, runtime *Runtime, agent AgentSnapshot) {
	t.Helper()
	event := domain.NewEvent("new request", nil)
	for name, operation := range map[string]func() error{
		"register": func() error { a := domain.NewAgentInstance("new"); return runtime.Register(&a, resultRunner{}) },
		"definition": func() error {
			return runtime.RegisterDefinition(domain.DefinitionRef{ID: "new", Version: "1"}, resultRunner{})
		},
		"handler": func() error { return runtime.Executor().Register("new", EchoHandler{}) },
		"create":  func() error { _, err := runtime.CreateAgent("new", agent.Definition, nil); return err },
		"restore": func() error {
			a := domain.NewAgentInstance("new")
			a.Definition = agent.Definition
			return runtime.RestoreAgent(a)
		},
		"submit":  func() error { return runtime.Submit(agent.ID, event) },
		"process": func() error { _, err := runtime.Process(agent.ID, event); return err },
		"retry": func() error {
			return runtime.Retry(context.Background(), domain.DeliveryKey{AgentID: agent.ID, EventID: event.ID})
		},
		"loop": runtime.RunUntilIdle,
	} {
		done := make(chan error, 1)
		go func() { done <- operation() }()
		if err := p3Await(t, done); !errors.Is(err, ErrStoreClosed) {
			t.Errorf("%s during/after close: %v", name, err)
		}
	}
}

func TestP4EchoResumesAtStoredBoundary(t *testing.T) {
	for _, scenario := range []struct {
		stage  string
		policy domain.RecoveryPolicy
	}{
		{"execution before", domain.RecoveryPolicySafeRetry},
		{"execution after", domain.RecoveryPolicySafeRetry},
		{"pending action", domain.RecoveryPolicySafeRetry},
		{"action before", domain.RecoveryPolicySafeRetry},
		{"action before", domain.RecoveryPolicyManual},
		{"action after", domain.RecoveryPolicySafeRetry},
	} {
		t.Run(scenario.stage+"/"+string(scenario.policy), func(t *testing.T) {
			backend := NewMemoryRecoveryStore()
			original := p4Open(t, p4FaultStore{RecoveryStore: backend, stage: scenario.stage})
			var runnerCalls, handlerCalls atomic.Int32
			runner := p4EchoRunner(&runnerCalls)
			handler := handlerFunc(func(action domain.Action) (map[string]any, error) {
				handlerCalls.Add(1)
				return EchoHandler{}.Execute(action)
			})
			agent := p4Agent(t, original, runner, handler, scenario.policy)
			event := domain.NewEvent("request", map[string]any{"text": "recover me"})
			_, err := original.Process(agent.ID, event)
			if scenario.stage == "execution before" || scenario.stage == "execution after" {
				if !errors.Is(err, p4StorageFault) {
					t.Fatalf("execution fault: %v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			if scenario.stage == "action before" || scenario.stage == "action after" {
				if err := p4Run(t, original); !errors.Is(err, p4StorageFault) {
					t.Fatalf("action fault: %v", err)
				}
			}
			firstDelivery, firstExecution := p3StoredExecution(t, original.store, agent.ID, event)
			beforeActions, err := original.store.ListActions(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			p4Close(t, original)
			resumed := p4Open(t, backend)
			//新绑定策略不改写已保存的action策略
			p4Bind(t, resumed, agent.Definition, runner, handler, domain.RecoveryPolicyManual)
			report := resumed.RecoveryReport()
			wantRequeued, wantUnknown, wantRetryable := 0, 0, 0
			if scenario.stage == "execution before" {
				wantRequeued = 1
				if len(report.RequeuedDeliveries) != 1 || report.RequeuedDeliveries[0] != firstDelivery.Key {
					t.Fatalf("requeued identity: %+v", report)
				}
			}
			if scenario.stage == "action before" {
				wantUnknown = 1
				if scenario.policy == domain.RecoveryPolicySafeRetry {
					wantRetryable = 1
				}
			}
			if len(report.RequeuedDeliveries) != wantRequeued || len(report.UnknownActions) != wantUnknown || len(report.RetryableActions) != wantRetryable {
				t.Fatalf("unexpected recovery report: %+v", report)
			}
			p4AssertReportIsolation(t, resumed)
			if err := p4Run(t, resumed); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(report, resumed.RecoveryReport()) {
				t.Fatal("startup report changed after running")
			}
			finalDelivery, finalExecution := p3StoredExecution(t, resumed.store, agent.ID, event)
			wantAttempts := 1
			if scenario.stage == "execution before" {
				wantAttempts = 2
				first := finalExecution.Attempts[0]
				if first.ID != firstExecution.Attempts[0].ID || first.Status != domain.AttemptStatusInterrupted || first.FinishedAt == nil || first.Error == nil || first.Error.Kind != domain.ErrorKindInterrupted {
					t.Fatalf("interrupted attempt lost: %+v", first)
				}
			}
			if finalDelivery.ExecutionID != firstDelivery.ExecutionID || finalDelivery.Status != domain.DeliveryStatusCompleted || len(finalExecution.Attempts) != wantAttempts || finalExecution.Execution.AttemptCount != uint64(wantAttempts) {
				t.Fatalf("execution identity/history: delivery=%+v, execution=%+v", finalDelivery, finalExecution)
			}
			allActions, err := resumed.store.ListActions(context.Background())
			if err != nil || len(allActions) != 1 {
				t.Fatalf("actions=%+v, err=%v", allActions, err)
			}
			action := allActions[0]
			if len(beforeActions) > 0 {
				before := beforeActions[0]
				if !reflect.DeepEqual(action.Request, before.Request) || action.IdempotencyKey != before.IdempotencyKey || action.ResultEventID != before.ResultEventID || action.HandlerVersion != before.HandlerVersion || action.RecoveryPolicy != before.RecoveryPolicy || action.MaxAttempts != before.MaxAttempts {
					t.Fatalf("action identity/metadata replaced: before=%+v, after=%+v", before, action)
				}
			}
			manualUnknown := scenario.stage == "action before" && scenario.policy == domain.RecoveryPolicyManual
			wantExecutions, wantHandlerCalls := 2, int32(1)
			if manualUnknown {
				wantExecutions = 1
				if action.Status != domain.ActionStatusUnknown || action.Result != nil || action.LastError == nil || action.LastError.Kind != domain.ErrorKindInterrupted {
					t.Fatalf("manual action lost uncertainty: %+v", action)
				}
				if _, err := resumed.store.LoadEvent(context.Background(), action.ResultEventID); !errors.Is(err, ErrStoreNotFound) {
					t.Fatalf("manual unknown published result: %v", err)
				}
			} else {
				if scenario.stage == "action before" {
					wantHandlerCalls = 2
				}
				if action.Status != domain.ActionStatusSucceeded || action.Result == nil || action.Result.EventID != action.ResultEventID {
					t.Fatalf("echo did not finish: %+v", action)
				}
				resultDelivery, resultExecution := p3StoredExecution(t, resumed.store, agent.ID, domain.Event{ID: action.ResultEventID})
				if resultDelivery.Status != domain.DeliveryStatusCompleted || len(resultExecution.Attempts) != 1 {
					t.Fatalf("result delivery=%+v execution=%+v", resultDelivery, resultExecution)
				}
			}
			agents, err := resumed.store.ListAgents(context.Background())
			if err != nil || len(agents) != 1 || agents[0].ID != agent.ID || agents[0].StateVersion != uint64(wantExecutions) || len(resumed.Executions()) != wantExecutions || handlerCalls.Load() != wantHandlerCalls || action.AttemptCount != uint64(wantHandlerCalls) {
				t.Fatalf("resumed state: agents=%+v, executions=%d, handler calls=%d, action=%+v, err=%v", agents, len(resumed.Executions()), handlerCalls.Load(), action, err)
			}
			if !manualUnknown && (!reflect.DeepEqual(agents[0].State["echoed"], event.Payload) || agents[0].State["result_event_id"] != string(action.ResultEventID)) {
				t.Fatalf("echo payload not delivered: %+v", agents[0].State)
			}
			wantRunnerCalls := int32(wantExecutions)
			if scenario.stage == "execution before" {
				wantRunnerCalls++
			}
			if runnerCalls.Load() != wantRunnerCalls {
				t.Fatalf("unexpected runner calls: %d, want %d", runnerCalls.Load(), wantRunnerCalls)
			}
			p4Close(t, resumed)
			again := p4Open(t, backend)
			p4Bind(t, again, agent.Definition, runner, handler, domain.RecoveryPolicySafeRetry)
			if err := p4Run(t, again); err != nil || handlerCalls.Load() != wantHandlerCalls || runnerCalls.Load() != wantRunnerCalls {
				t.Fatalf("second recovery repeated finished/manual work: err=%v runner=%d handler=%d", err, runnerCalls.Load(), handlerCalls.Load())
			}
		})
	}
}

func p4AssertReportIsolation(t *testing.T, runtime *Runtime) {
	t.Helper()
	want := runtime.RecoveryReport()
	changed := runtime.RecoveryReport()
	if len(changed.RequeuedDeliveries) > 0 {
		changed.RequeuedDeliveries[0].AgentID = "mutated"
	}
	if len(changed.UnknownActions) > 0 {
		changed.UnknownActions[0] = "mutated"
	}
	if len(changed.RetryableActions) > 0 {
		changed.RetryableActions[0] = "mutated"
	}
	if got := runtime.RecoveryReport(); !reflect.DeepEqual(got, want) {
		t.Fatalf("mutable recovery report: got=%+v want=%+v", got, want)
	}
}

func TestP4SafeRetryStopsAtSavedAttemptLimit(t *testing.T) {
	backend := NewMemoryRecoveryStore()
	runtime := p4Open(t, backend)
	var runnerCalls, handlerCalls atomic.Int32
	runner := p4EchoRunner(&runnerCalls)
	handler := handlerFunc(func(domain.Action) (map[string]any, error) {
		handlerCalls.Add(1)
		panic("external outcome unknown")
	})
	agent := p4Agent(t, runtime, runner, handler, domain.RecoveryPolicySafeRetry)
	if err := runtime.Submit(agent.ID, domain.NewEvent("request", nil)); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := p4Run(t, runtime); err != nil {
			t.Fatal(err)
		}
	}
	actions, err := runtime.store.ListActions(context.Background())
	if err != nil || len(actions) != 1 {
		t.Fatalf("actions=%+v err=%v", actions, err)
	}
	action, err := runtime.store.LoadAction(context.Background(), actions[0].Request.ID)
	if err != nil || action.Action.Status != domain.ActionStatusUnknown || action.Action.AttemptCount != 3 || len(action.Attempts) != 3 || action.Action.Result != nil || handlerCalls.Load() != 3 {
		t.Fatalf("exhausted action=%+v calls=%d err=%v", action, handlerCalls.Load(), err)
	}
	for _, attempt := range action.Attempts {
		if attempt.Status != domain.ActionStatusUnknown || attempt.Error == nil || attempt.FinishedAt == nil {
			t.Fatalf("attempt did not retain unknown: %+v", attempt)
		}
	}
	p4Close(t, runtime)
	resumed := p4Open(t, backend)
	if err := resumed.RegisterDefinition(agent.Definition, runner); err != nil {
		t.Fatal(err)
	}
	if err := resumed.Executor().RegisterWithOptions("echo", handler, HandlerOptions{
		Version: "1", RecoveryPolicy: domain.RecoveryPolicySafeRetry, MaxAttempts: 100,
	}); err != nil {
		t.Fatal(err)
	}
	if report := resumed.RecoveryReport(); len(report.UnknownActions) != 1 || len(report.RetryableActions) != 0 {
		t.Fatalf("exhausted recovery report: %+v", report)
	}
	if err := p4Run(t, resumed); err != nil || handlerCalls.Load() != 3 {
		t.Fatalf("re-registration reset attempt limit: calls=%d err=%v", handlerCalls.Load(), err)
	}
}

func TestP4MissingBindingsDoNotBlockOtherAgents(t *testing.T) {
	backend := NewMemoryRecoveryStore()
	original := p4Open(t, p4FaultStore{RecoveryStore: backend, stage: "action before"})
	var calls atomic.Int32
	agent := p4Agent(t, original, p4EchoRunner(&calls), EchoHandler{}, domain.RecoveryPolicySafeRetry)
	if err := original.Submit(agent.ID, domain.NewEvent("request", nil)); err != nil {
		t.Fatal(err)
	}
	if err := p4Run(t, original); !errors.Is(err, p4StorageFault) {
		t.Fatalf("action fault: %v", err)
	}
	if err := original.Submit(agent.ID, domain.NewEvent("missing definition", nil)); err != nil {
		t.Fatal(err)
	}
	p4Close(t, original)
	resumed := p4Open(t, backend)
	ref := domain.DefinitionRef{ID: "healthy", Version: "1"}
	if err := resumed.RegisterDefinition(ref, functionRunner(func(ctx ExecutionContext) (ExecutionResult, error) {
		if ctx.Event.Type == "action.result" {
			return ExecutionResult{StateUpdate: map[string]any{"finished": true}}, nil
		}
		return ExecutionResult{Actions: []domain.Action{domain.NewAction("healthy-echo", nil)}}, nil
	})); err != nil {
		t.Fatal(err)
	}
	if err := resumed.Executor().Register("healthy-echo", EchoHandler{}); err != nil {
		t.Fatal(err)
	}
	healthy, err := resumed.CreateAgent("healthy", ref, nil)
	if err != nil {
		t.Fatal(err)
	}
	event := domain.NewEvent("healthy request", nil)
	if err := resumed.Submit(healthy.ID, event); err != nil {
		t.Fatal(err)
	}
	if err := p4Run(t, resumed); err != nil {
		t.Fatal(err)
	}
	delivery, _ := p3StoredExecution(t, resumed.store, healthy.ID, event)
	if delivery.Status != domain.DeliveryStatusCompleted {
		t.Fatalf("healthy delivery blocked: %+v", delivery)
	}
	healthySnapshot, err := resumed.Agent(healthy.ID)
	if err != nil || healthySnapshot.State["finished"] != true || healthySnapshot.StateVersion != 2 {
		t.Fatalf("healthy action/result blocked: %+v, %v", healthySnapshot, err)
	}
	unknown, err := resumed.store.ListActions(context.Background(), domain.ActionStatusUnknown)
	if err != nil || len(unknown) != 1 || unknown[0].AttemptCount != 1 {
		t.Fatalf("missing handler consumed retry: %+v, %v", unknown, err)
	}
	snapshot, err := resumed.Agent(agent.ID)
	if err != nil || snapshot.BindingError == "" || snapshot.StateVersion != 1 {
		t.Fatalf("missing definition state: %+v err=%v", snapshot, err)
	}
}

func TestP4PendingActionResumesBeforeDefinitionBinding(t *testing.T) {
	backend := NewMemoryRecoveryStore()
	original := p4Open(t, backend)
	var runnerCalls, handlerCalls atomic.Int32
	runner := p4EchoRunner(&runnerCalls)
	handler := handlerFunc(func(action domain.Action) (map[string]any, error) {
		handlerCalls.Add(1)
		return EchoHandler{}.Execute(action)
	})
	agent := p4Agent(t, original, runner, handler, domain.RecoveryPolicySafeRetry)
	result, err := original.Process(agent.ID, domain.NewEvent("request", map[string]any{"text": "stored action"}))
	if err != nil || len(result.Actions) != 1 {
		t.Fatalf("request result=%+v err=%v", result, err)
	}
	id := result.Actions[0].ID
	before, err := original.ActionContext(context.Background(), id)
	if err != nil || before.Action.Status != domain.ActionStatusPending {
		t.Fatalf("pending action=%+v err=%v", before, err)
	}
	p4Close(t, original)
	resumed := p4Open(t, backend)
	if err := resumed.Executor().Register("echo", handler); err != nil {
		t.Fatal(err)
	}
	if err := p4Run(t, resumed); err != nil {
		t.Fatal(err)
	}
	saved, err := resumed.ActionContext(context.Background(), id)
	if err != nil || saved.Action.Status != domain.ActionStatusSucceeded || saved.Action.Result == nil || saved.Action.ResultEventID != before.Action.ResultEventID || len(saved.Attempts) != 1 || handlerCalls.Load() != 1 || runnerCalls.Load() != 1 {
		t.Fatalf("pending action required definition: action=%+v handler=%d runner=%d err=%v", saved, handlerCalls.Load(), runnerCalls.Load(), err)
	}
	key := domain.DeliveryKey{AgentID: agent.ID, EventID: saved.Action.ResultEventID}
	delivery, err := resumed.store.LoadDelivery(context.Background(), key)
	if err != nil || delivery.Status != domain.DeliveryStatusPending {
		t.Fatalf("unbound result delivery=%+v err=%v", delivery, err)
	}
	snapshot, err := resumed.Agent(agent.ID)
	if err != nil || snapshot.BindingError == "" || snapshot.StateVersion != 1 {
		t.Fatalf("unbound agent changed: %+v err=%v", snapshot, err)
	}
	if err := resumed.RegisterDefinition(agent.Definition, runner); err != nil {
		t.Fatal(err)
	}
	if err := p4Run(t, resumed); err != nil {
		t.Fatal(err)
	}
	delivery, err = resumed.store.LoadDelivery(context.Background(), key)
	if err != nil || delivery.Status != domain.DeliveryStatusCompleted || handlerCalls.Load() != 1 || runnerCalls.Load() != 2 {
		t.Fatalf("rebound result repeated action: delivery=%+v handler=%d runner=%d err=%v", delivery, handlerCalls.Load(), runnerCalls.Load(), err)
	}
	snapshot, err = resumed.Agent(agent.ID)
	if err != nil || snapshot.BindingError != "" || snapshot.StateVersion != 2 || snapshot.State["result_event_id"] != string(key.EventID) {
		t.Fatalf("rebound agent=%+v err=%v", snapshot, err)
	}
}

func TestP4OpenFailureReleasesSession(t *testing.T) {
	backend := NewMemoryRecoveryStore()
	runtime, err := OpenRuntime(context.Background(), p4FaultStore{RecoveryStore: backend, stage: "recover"})
	if !errors.Is(err, p4StorageFault) || runtime != nil {
		t.Fatalf("open with failed recovery: runtime=%v err=%v", runtime, err)
	}
	_ = p4Open(t, backend)
}

func TestP4LegacyRuntimeDoesNotRetrySafeUnknown(t *testing.T) {
	runtime := NewRuntime()
	var runnerCalls, handlerCalls atomic.Int32
	agent := p4Agent(t, runtime, p4EchoRunner(&runnerCalls), handlerFunc(func(domain.Action) (map[string]any, error) {
		handlerCalls.Add(1)
		panic("unknown")
	}), domain.RecoveryPolicySafeRetry)
	if err := runtime.Submit(agent.ID, domain.NewEvent("request", nil)); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := p4Run(t, runtime); err != nil {
			t.Fatal(err)
		}
	}
	if handlerCalls.Load() != 1 {
		t.Fatalf("legacy runtime began recovery retries: %d", handlerCalls.Load())
	}
}

func TestP4ExecutorCannotBypassRecoveryStore(t *testing.T) {
	runtime := p4Open(t, NewMemoryRecoveryStore())
	var calls atomic.Int32
	if err := runtime.Executor().Register("echo", handlerFunc(func(domain.Action) (map[string]any, error) {
		calls.Add(1)
		return nil, nil
	})); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Executor().Execute(domain.NewAction("echo", nil)); err == nil || calls.Load() != 0 {
		t.Fatalf("direct executor bypassed recovery store: calls=%d err=%v", calls.Load(), err)
	}
}

func TestP4ActionContextReturnsIsolatedHistory(t *testing.T) {
	for _, status := range []domain.ActionStatus{domain.ActionStatusUnknown, domain.ActionStatusSucceeded} {
		t.Run(string(status), func(t *testing.T) {
			runtime := p4Open(t, NewMemoryRecoveryStore())
			var runnerCalls atomic.Int32
			handler := handlerFunc(func(action domain.Action) (map[string]any, error) {
				if status == domain.ActionStatusUnknown {
					panic("outcome unknown")
				}
				return EchoHandler{}.Execute(action)
			})
			agent := p4Agent(t, runtime, p4EchoRunner(&runnerCalls), handler, domain.RecoveryPolicyManual)
			result, err := runtime.Process(agent.ID, domain.NewEvent("request", map[string]any{
				"nested": map[string]any{"values": []any{"original"}},
			}))
			if err != nil || len(result.Actions) != 1 {
				t.Fatalf("request result=%+v err=%v", result, err)
			}
			if err := p4Run(t, runtime); err != nil {
				t.Fatal(err)
			}
			id := result.Actions[0].ID
			want, err := runtime.ActionContext(context.Background(), id)
			if err != nil || want.Action.Status != status || len(want.Attempts) != 1 || want.Attempts[0].FinishedAt == nil {
				t.Fatalf("action query=%+v err=%v", want, err)
			}
			changed, err := runtime.ActionContext(context.Background(), id)
			if err != nil {
				t.Fatal(err)
			}
			changed.Action.Request.Payload["nested"].(map[string]any)["values"].([]any)[0] = "mutated"
			*changed.Action.Request.ExecutionID = "mutated"
			changed.Action.Status = domain.ActionStatusPending
			changed.Attempts[0].Number = 99
			*changed.Attempts[0].FinishedAt = time.Time{}
			if status == domain.ActionStatusUnknown {
				if changed.Action.LastError == nil || changed.Attempts[0].Error == nil || changed.Action.Result != nil {
					t.Fatalf("unknown query missing cause: %+v", changed)
				}
				changed.Action.LastError.Message = "mutated"
				changed.Attempts[0].Error.Message = "mutated"
			} else {
				if changed.Action.Result == nil {
					t.Fatal("succeeded query missing result")
				}
				changed.Action.Result.Output["nested"].(map[string]any)["values"].([]any)[0] = "mutated"
			}
			got, err := runtime.ActionContext(context.Background(), id)
			if err != nil || !reflect.DeepEqual(got, want) {
				t.Fatalf("query leaked mutable records: got=%+v want=%+v err=%v", got, want, err)
			}
			if _, err := runtime.ActionContext(context.Background(), "missing"); !errors.Is(err, ErrStoreNotFound) {
				t.Fatalf("missing action: %v", err)
			}
			p4Close(t, runtime)
			if _, err := runtime.ActionContext(context.Background(), id); !errors.Is(err, ErrStoreClosed) {
				t.Fatalf("action query after close: %v", err)
			}
		})
	}
}
