package core

import (
	"agent-runtime/domain"
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func p3Runtime(t *testing.T, store StateStore, runner AgentRunner) (*Runtime, domain.AgentInstance) {
	t.Helper()
	runtime, err := NewRuntimeWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	agent := domain.NewAgentInstance("p3")
	agent.Definition = domain.DefinitionRef{ID: "p3", Version: "1"}
	agent.State = map[string]any{"value": "original"}
	if err := runtime.Register(&agent, runner); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Executor().Register("echo", EchoHandler{}); err != nil {
		t.Fatal(err)
	}
	return runtime, agent
}

func p3Await[T any](t *testing.T, channel <-chan T) T {
	t.Helper()
	select {
	case value := <-channel:
		return value
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for test barrier")
		var zero T
		return zero
	}
}

type p3Outcome struct {
	result ExecutionResult
	err    error
}

func p3Process(runtime *Runtime, agentID domain.ID, event domain.Event) <-chan p3Outcome {
	result := make(chan p3Outcome, 1)
	go func() {
		value, err := runtime.Process(agentID, event)
		result <- p3Outcome{value, err}
	}()
	return result
}

func p3StoredExecution(t *testing.T, store StateStore, agentID domain.ID, event domain.Event) (*domain.Delivery, *StoredExecution) {
	t.Helper()
	delivery, err := store.LoadDelivery(context.Background(), domain.DeliveryKey{AgentID: agentID, EventID: event.ID})
	if err != nil {
		t.Fatal(err)
	}
	execution, err := store.LoadExecution(context.Background(), delivery.ExecutionID)
	if err != nil {
		t.Fatal(err)
	}
	return delivery, execution
}

func TestP3CompletedProcessReturnsSavedIsolatedResult(t *testing.T) {
	var calls atomic.Int32
	runtime, agent := p3Runtime(t, NewMemoryStore(), functionRunner(func(ExecutionContext) (ExecutionResult, error) {
		calls.Add(1)
		return ExecutionResult{
			StateUpdate: map[string]any{"value": map[string]any{"nested": []any{"saved"}}},
			Actions:     []domain.Action{domain.NewAction("echo", map[string]any{"nested": []any{"saved"}})},
		}, nil
	}))
	event := domain.NewEvent("request", nil)
	first, err := runtime.Process(agent.ID, event)
	if err != nil {
		t.Fatal(err)
	}
	expected := cloneResult(first)
	first.StateUpdate["value"].(map[string]any)["nested"].([]any)[0] = "caller mutation"
	first.Actions[0].Payload["nested"].([]any)[0] = "caller mutation"
	*first.Actions[0].ExecutionID = "caller mutation"
	for range 3 {
		result, err := runtime.Process(agent.ID, event)
		if err != nil || !reflect.DeepEqual(result, expected) {
			t.Fatalf("replayed result=%+v, err=%v, want=%+v", result, err, expected)
		}
	}
	snapshot, err := runtime.Agent(agent.ID)
	if err != nil || calls.Load() != 1 || snapshot.StateVersion != 1 || len(runtime.Actions()) != 1 {
		t.Fatalf("duplicate committed: agent=%+v, calls=%d, err=%v", snapshot, calls.Load(), err)
	}
	delivery, execution := p3StoredExecution(t, runtime.store, agent.ID, event)
	if delivery.Status != domain.DeliveryStatusCompleted || len(execution.Attempts) != 1 || execution.Execution.Status != domain.ExecutionStatusCompleted {
		t.Fatalf("completion=%+v, attempts=%+v", delivery, execution)
	}
}

func TestP3ConcurrentDuplicateProcessCallsRunnerOnce(t *testing.T) {
	entered, release := make(chan struct{}, 1), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	var calls atomic.Int32
	runtime, agent := p3Runtime(t, NewMemoryStore(), functionRunner(func(ExecutionContext) (ExecutionResult, error) {
		calls.Add(1)
		entered <- struct{}{}
		<-release
		return ExecutionResult{StateUpdate: map[string]any{"value": "saved"}, Actions: []domain.Action{domain.NewAction("echo", nil)}}, nil
	}))
	event := domain.NewEvent("request", nil)
	first := p3Process(runtime, agent.ID, event)
	p3Await(t, entered)
	const duplicates = 16
	results := make(chan p3Outcome, duplicates)
	for range duplicates {
		go func() {
			value, err := runtime.Process(agent.ID, event)
			results <- p3Outcome{value, err}
		}()
	}
	for range duplicates {
		if result := p3Await(t, results); !errors.Is(result.err, ErrExecutionInProgress) {
			t.Fatalf("running duplicate returned %v", result.err)
		}
	}
	unblock()
	if result := p3Await(t, first); result.err != nil {
		t.Fatal(result.err)
	}
	if _, err := runtime.Process(agent.ID, event); err != nil {
		t.Fatal(err)
	}
	snapshot, err := runtime.Agent(agent.ID)
	if err != nil || calls.Load() != 1 || snapshot.StateVersion != 1 || len(runtime.Actions()) != 1 || len(runtime.Executions()) != 1 {
		t.Fatalf("duplicate execution: calls=%d, snapshot=%+v, err=%v", calls.Load(), snapshot, err)
	}
}

func TestP3SharedStoreSerializesAgentAcrossRuntimes(t *testing.T) {
	store := NewMemoryStore()
	entered, release := make(chan struct{}, 1), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	var calls atomic.Int32
	runner := functionRunner(func(ctx ExecutionContext) (ExecutionResult, error) {
		calls.Add(1)
		if ctx.Event.Type == "blocked" {
			entered <- struct{}{}
			<-release
		}
		return ExecutionResult{StateUpdate: map[string]any{"value": ctx.Event.Type}}, nil
	})
	firstRuntime, agent := p3Runtime(t, store, runner)
	secondRuntime, err := NewRuntimeWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	if err := secondRuntime.RegisterDefinition(agent.Definition, runner); err != nil {
		t.Fatal(err)
	}
	blocked := p3Process(firstRuntime, agent.ID, domain.NewEvent("blocked", nil))
	p3Await(t, entered)
	secondEvent := domain.NewEvent("second", nil)
	if result := p3Await(t, p3Process(secondRuntime, agent.ID, secondEvent)); !errors.Is(result.err, ErrExecutionInProgress) {
		t.Fatalf("same Agent ran concurrently: %v", result.err)
	}
	other, err := secondRuntime.CreateAgent("other", agent.Definition, nil)
	if err != nil {
		t.Fatal(err)
	}
	if result := p3Await(t, p3Process(secondRuntime, other.ID, domain.NewEvent("other", nil))); result.err != nil {
		t.Fatalf("unrelated Agent was blocked: %v", result.err)
	}
	if calls.Load() != 2 {
		t.Fatalf("unexpected calls while first Agent running: %d", calls.Load())
	}
	unblock()
	if result := p3Await(t, blocked); result.err != nil {
		t.Fatal(result.err)
	}
	if err := secondRuntime.RunUntilIdle(); err != nil {
		t.Fatal(err)
	}
	snapshot, err := firstRuntime.Agent(agent.ID)
	if err != nil || calls.Load() != 3 || snapshot.StateVersion != 2 || snapshot.State["value"] != "second" {
		t.Fatalf("pending work lost: agent=%+v, calls=%d, err=%v", snapshot, calls.Load(), err)
	}
}

func TestP3SubmitDeduplicatesContentAndPreservesFirstTime(t *testing.T) {
	runtime, agent := p3Runtime(t, NewMemoryStore(), resultRunner{})
	event := domain.NewEvent("request", map[string]any{"a": int64(7), "b": []any{"value"}})
	first, err := runtime.SubmitContext(context.Background(), agent.ID, event)
	if err != nil || first.Duplicate {
		t.Fatalf("first receive=%+v, err=%v", first, err)
	}
	duplicate := event
	duplicate.CreatedAt = event.CreatedAt.Add(time.Hour)
	duplicate.Payload = map[string]any{"b": []any{"value"}, "a": float64(7)}
	const submissions = 16
	results := make(chan error, submissions)
	for range submissions {
		go func() {
			received, err := runtime.SubmitContext(context.Background(), agent.ID, duplicate)
			if err == nil && (!received.Duplicate || received.Delivery != first.Delivery) {
				err = errors.New("duplicate did not reuse original Delivery")
			}
			results <- err
		}()
	}
	for range submissions {
		if err := p3Await(t, results); err != nil {
			t.Fatal(err)
		}
	}
	for _, conflict := range []domain.Event{
		{ID: event.ID, Type: "different", Payload: event.Payload, CreatedAt: event.CreatedAt},
		{ID: event.ID, Type: event.Type, Payload: map[string]any{"a": 8}, CreatedAt: event.CreatedAt},
	} {
		if _, err := runtime.Process(agent.ID, conflict); !errors.Is(err, ErrStoreConflict) {
			t.Fatalf("conflicting Process accepted: %v", err)
		}
	}
	saved, err := runtime.store.LoadEvent(context.Background(), event.ID)
	if err != nil || !saved.CreatedAt.Equal(event.CreatedAt) || !reflect.DeepEqual(saved.Payload, event.Payload) {
		t.Fatalf("first Event overwritten: event=%+v, err=%v", saved, err)
	}
	deliveries, err := runtime.store.ListDeliveries(context.Background())
	if err != nil || len(deliveries) != 1 || len(runtime.Executions()) != 0 {
		t.Fatalf("receive created work twice: deliveries=%+v, err=%v", deliveries, err)
	}
}

func TestP3BusinessFailureRequiresExplicitRetry(t *testing.T) {
	var calls atomic.Int32
	cause := errors.New("business rejected")
	runtime, agent := p3Runtime(t, NewMemoryStore(), functionRunner(func(ctx ExecutionContext) (ExecutionResult, error) {
		if calls.Add(1) == 1 {
			ctx.Agent.State["value"] = "leaked mutation"
			return ExecutionResult{StateUpdate: map[string]any{"value": "failed"}, Actions: []domain.Action{domain.NewAction("echo", nil)}}, cause
		}
		return ExecutionResult{StateUpdate: map[string]any{"value": "retried"}}, nil
	}))
	event := domain.NewEvent("request", nil)
	if _, err := runtime.Process(agent.ID, event); !errors.Is(err, cause) || !errors.Is(err, ErrExecutionFailed) {
		t.Fatalf("business failure lost: %v", err)
	}
	delivery, execution := p3StoredExecution(t, runtime.store, agent.ID, event)
	if delivery.Status != domain.DeliveryStatusFailed || len(execution.Attempts) != 1 ||
		execution.Attempts[0].Error == nil || execution.Attempts[0].Error.Kind != domain.ErrorKindBusiness {
		t.Fatalf("failure records: delivery=%+v, execution=%+v", delivery, execution)
	}
	if _, err := runtime.Process(agent.ID, event); !errors.Is(err, ErrDeliveryFailed) {
		t.Fatalf("failed delivery automatically retried: %v", err)
	}
	if err := runtime.RunUntilIdle(); err != nil {
		t.Fatal(err)
	}
	snapshot, err := runtime.Agent(agent.ID)
	if err != nil || calls.Load() != 1 || snapshot.StateVersion != 0 || snapshot.State["value"] != "original" || len(runtime.Actions()) != 0 {
		t.Fatalf("failed business changed state: calls=%d, agent=%+v, err=%v", calls.Load(), snapshot, err)
	}
	if err := runtime.Retry(context.Background(), delivery.Key); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Process(agent.ID, event); err != nil {
		t.Fatal(err)
	}
	retried, attempts := p3StoredExecution(t, runtime.store, agent.ID, event)
	if retried.ExecutionID != delivery.ExecutionID || len(attempts.Attempts) != 2 || calls.Load() != 2 ||
		attempts.Attempts[0].ID == attempts.Attempts[1].ID || attempts.Attempts[1].Number != 2 ||
		attempts.Attempts[0].Status != domain.AttemptStatusFailed || attempts.Attempts[1].Status != domain.AttemptStatusSucceeded {
		t.Fatalf("retry did not preserve history: delivery=%+v, attempts=%+v", retried, attempts)
	}
}

func TestP3NewRuntimeFindsAcceptedWorkInSharedStore(t *testing.T) {
	store := NewMemoryStore()
	var calls atomic.Int32
	runner := functionRunner(func(ExecutionContext) (ExecutionResult, error) {
		calls.Add(1)
		return ExecutionResult{StateUpdate: map[string]any{"value": "processed"}}, nil
	})
	original, agent := p3Runtime(t, store, runner)
	event := domain.NewEvent("request", nil)
	if err := original.Submit(agent.ID, event); err != nil {
		t.Fatal(err)
	}
	replacement, err := NewRuntimeWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	if err := replacement.RegisterDefinition(agent.Definition, runner); err != nil {
		t.Fatal(err)
	}
	if err := replacement.RunUntilIdle(); err != nil {
		t.Fatal(err)
	}
	snapshot, err := original.Agent(agent.ID)
	if err != nil || snapshot.StateVersion != 1 || snapshot.State["value"] != "processed" || calls.Load() != 1 {
		t.Fatalf("accepted work was lost: agent=%+v, calls=%d, err=%v", snapshot, calls.Load(), err)
	}
	if _, err := original.Process(agent.ID, event); err != nil || calls.Load() != 1 {
		t.Fatalf("old Runtime ignored stored completion: calls=%d, err=%v", calls.Load(), err)
	}
}

type p3ReceiveStore struct {
	StateStore
	receive func(context.Context, domain.ID, domain.Event) (ReceivedEvent, error)
}

func (s p3ReceiveStore) ReceiveEvent(ctx context.Context, id domain.ID, event domain.Event) (ReceivedEvent, error) {
	return s.receive(ctx, id, event)
}

type p3ClaimStore struct {
	StateStore
	claim func(context.Context, domain.DeliveryKey) (*ExecutionClaim, error)
}

func (s p3ClaimStore) ClaimExecution(ctx context.Context, key domain.DeliveryKey) (*ExecutionClaim, error) {
	return s.claim(ctx, key)
}

type p3CommitStore struct {
	StateStore
	commit func(context.Context, ExecutionCommit) (domain.ExecutionResult, error)
}

func (s p3CommitStore) CommitExecution(ctx context.Context, commit ExecutionCommit) (domain.ExecutionResult, error) {
	return s.commit(ctx, commit)
}

type p3FailStore struct {
	StateStore
	fail func(context.Context, ExecutionFailure) error
}

func (s p3FailStore) FailExecution(ctx context.Context, failure ExecutionFailure) error {
	return s.fail(ctx, failure)
}

func TestP3StorageFailuresStopWithoutPublishingBusinessData(t *testing.T) {
	for _, phase := range []string{"receive", "claim", "commit", "fail"} {
		t.Run(phase, func(t *testing.T) {
			base := NewMemoryStore()
			var store StateStore = base
			injected := fmt.Errorf("injected %s fault: %w", phase, ErrAgentUnavailable)
			switch phase {
			case "receive":
				store = p3ReceiveStore{base, func(context.Context, domain.ID, domain.Event) (ReceivedEvent, error) {
					return ReceivedEvent{}, injected
				}}
			case "claim":
				store = p3ClaimStore{base, func(context.Context, domain.DeliveryKey) (*ExecutionClaim, error) {
					return nil, injected
				}}
			case "commit":
				store = p3CommitStore{base, func(context.Context, ExecutionCommit) (domain.ExecutionResult, error) {
					return domain.ExecutionResult{}, injected
				}}
			case "fail":
				store = p3FailStore{base, func(context.Context, ExecutionFailure) error { return injected }}
			}
			var runnerCalls, handlerCalls atomic.Int32
			runtime, agent := p3Runtime(t, store, functionRunner(func(ExecutionContext) (ExecutionResult, error) {
				runnerCalls.Add(1)
				result := ExecutionResult{StateUpdate: map[string]any{"value": "changed"}, Actions: []domain.Action{domain.NewAction("probe", nil)}}
				if phase == "fail" {
					return result, errors.New("business error")
				}
				return result, nil
			}))
			if err := runtime.Executor().Register("probe", handlerFunc(func(domain.Action) (map[string]any, error) {
				handlerCalls.Add(1)
				return nil, nil
			})); err != nil {
				t.Fatal(err)
			}
			event := domain.NewEvent("request", nil)
			var err error
			if phase == "receive" {
				_, err = runtime.Process(agent.ID, event)
			} else {
				if err := runtime.Submit(agent.ID, event); err != nil {
					t.Fatal(err)
				}
				err = runtime.RunUntilIdle()
			}
			if !errors.Is(err, injected) {
				t.Fatalf("storage failure was swallowed: %v", err)
			}
			snapshot, err := runtime.Agent(agent.ID)
			if err != nil || snapshot.StateVersion != 0 || snapshot.State["value"] != "original" || len(runtime.Actions()) != 0 || handlerCalls.Load() != 0 {
				t.Fatalf("storage failure published data: agent=%+v, handler calls=%d, err=%v", snapshot, handlerCalls.Load(), err)
			}
			for _, execution := range runtime.Executions() {
				if execution.Status != domain.ExecutionStatusRunning || execution.Result != nil {
					t.Fatalf("storage failure published final execution: %+v", execution)
				}
			}
			for _, attempt := range runtime.Attempts() {
				if attempt.Status != domain.AttemptStatusRunning || attempt.FinishedAt != nil {
					t.Fatalf("storage failure published final attempt: %+v", attempt)
				}
			}
			wantCalls := int32(1)
			if phase == "receive" || phase == "claim" {
				wantCalls = 0
			}
			if runnerCalls.Load() != wantCalls {
				t.Fatalf("runner calls=%d, want=%d", runnerCalls.Load(), wantCalls)
			}
			if phase == "receive" {
				if _, err := base.LoadEvent(context.Background(), event.ID); !errors.Is(err, ErrStoreNotFound) {
					t.Fatalf("failed receive accepted Event: %v", err)
				}
			} else {
				delivery, err := base.LoadDelivery(context.Background(), domain.DeliveryKey{AgentID: agent.ID, EventID: event.ID})
				if err != nil || delivery.Status == domain.DeliveryStatusCompleted {
					t.Fatalf("failed operation completed Delivery: %+v, %v", delivery, err)
				}
			}
		})
	}
}

func TestP3CommitResponseErrorCannotRewriteSuccessfulTransaction(t *testing.T) {
	base := NewMemoryStore()
	responseError := errors.New("commit response lost")
	var failCalls, commitCalls, runnerCalls atomic.Int32
	tracked := p3FailStore{base, func(ctx context.Context, failure ExecutionFailure) error {
		failCalls.Add(1)
		return base.FailExecution(ctx, failure)
	}}
	store := p3CommitStore{tracked, func(ctx context.Context, commit ExecutionCommit) (domain.ExecutionResult, error) {
		commitCalls.Add(1)
		result, err := base.CommitExecution(ctx, commit)
		if err != nil {
			return result, err
		}
		return domain.ExecutionResult{}, responseError
	}}
	runtime, agent := p3Runtime(t, store, functionRunner(func(ExecutionContext) (ExecutionResult, error) {
		runnerCalls.Add(1)
		return ExecutionResult{StateUpdate: map[string]any{"value": "committed"}, Actions: []domain.Action{domain.NewAction("echo", nil)}}, nil
	}))
	event := domain.NewEvent("request", nil)
	if _, err := runtime.Process(agent.ID, event); !errors.Is(err, responseError) {
		t.Fatalf("commit response failure hidden: %v", err)
	}
	delivery, execution := p3StoredExecution(t, base, agent.ID, event)
	if delivery.Status != domain.DeliveryStatusCompleted || execution.Execution.Status != domain.ExecutionStatusCompleted || failCalls.Load() != 0 {
		t.Fatalf("commit failure overwrote successful transaction: delivery=%+v, execution=%+v, fail calls=%d", delivery, execution, failCalls.Load())
	}
	replayed, err := runtime.Process(agent.ID, event)
	if err != nil || !reflect.DeepEqual(replayed, *execution.Execution.Result) || runnerCalls.Load() != 1 || commitCalls.Load() != 1 {
		t.Fatalf("completed response not replayed: result=%+v, err=%v, calls=%d/%d", replayed, err, runnerCalls.Load(), commitCalls.Load())
	}
	snapshot, err := runtime.Agent(agent.ID)
	if err != nil || snapshot.StateVersion != 1 || snapshot.State["value"] != "committed" || len(runtime.Actions()) != 1 {
		t.Fatalf("successful transaction was lost: agent=%+v, err=%v", snapshot, err)
	}
}

func TestP3CancellationPreventsRunnerBeforeAndAfterClaim(t *testing.T) {
	for _, afterClaim := range []bool{false, true} {
		t.Run(fmt.Sprintf("after_claim_%t", afterClaim), func(t *testing.T) {
			base := NewMemoryStore()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var store StateStore = base
			if afterClaim {
				store = p3ClaimStore{base, func(ctx context.Context, key domain.DeliveryKey) (*ExecutionClaim, error) {
					claim, err := base.ClaimExecution(ctx, key)
					cancel()
					return claim, err
				}}
			} else {
				cancel()
			}
			var calls atomic.Int32
			runtime, agent := p3Runtime(t, store, functionRunner(func(ExecutionContext) (ExecutionResult, error) {
				calls.Add(1)
				return ExecutionResult{StateUpdate: map[string]any{"value": "changed"}}, nil
			}))
			event := domain.NewEvent("request", nil)
			if _, err := runtime.ProcessContext(ctx, agent.ID, event); !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled Process returned %v", err)
			}
			if err := runtime.RunUntilIdleContext(ctx); !errors.Is(err, context.Canceled) {
				t.Fatalf("canceled drain returned %v", err)
			}
			snapshot, err := runtime.Agent(agent.ID)
			if err != nil || calls.Load() != 0 || snapshot.StateVersion != 0 || snapshot.State["value"] != "original" {
				t.Fatalf("cancellation ran business: calls=%d, agent=%+v, err=%v", calls.Load(), snapshot, err)
			}
			if afterClaim {
				_, execution := p3StoredExecution(t, base, agent.ID, event)
				if len(execution.Attempts) != 1 || execution.Attempts[0].Status != domain.AttemptStatusInterrupted {
					t.Fatalf("canceled claim not recorded as interrupted: %+v", execution)
				}
			} else if _, err := base.LoadEvent(context.Background(), event.ID); !errors.Is(err, ErrStoreNotFound) {
				t.Fatalf("pre-canceled request was accepted: %v", err)
			}
		})
	}
}

func TestP3FailedAgentDoesNotBlockHealthyAgent(t *testing.T) {
	store := NewMemoryStore()
	runtime := newRuntime(store)
	ref := domain.DefinitionRef{ID: "isolated", Version: "1"}
	var failedCalls, healthyCalls atomic.Int32
	if err := runtime.RegisterDefinition(ref, functionRunner(func(ctx ExecutionContext) (ExecutionResult, error) {
		if ctx.Agent.ID == "a-failing" {
			failedCalls.Add(1)
			return ExecutionResult{}, fmt.Errorf("business rejected: %w", ErrAgentUnavailable)
		}
		healthyCalls.Add(1)
		return ExecutionResult{StateUpdate: map[string]any{"done": true}}, nil
	})); err != nil {
		t.Fatal(err)
	}
	for _, id := range []domain.ID{"a-failing", "z-healthy"} {
		agent := domain.NewAgentInstance(string(id))
		agent.ID, agent.Definition, agent.Status = id, ref, domain.AgentStatusActive
		if err := runtime.RestoreAgent(agent); err != nil {
			t.Fatal(err)
		}
		if err := runtime.Submit(id, domain.NewEvent("request", nil)); err != nil {
			t.Fatal(err)
		}
	}
	if err := runtime.RunUntilIdle(); err != nil {
		t.Fatal(err)
	}
	if err := runtime.RunUntilIdle(); err != nil {
		t.Fatal(err)
	}
	if failedCalls.Load() != 1 || healthyCalls.Load() != 1 {
		t.Fatalf("failure isolation calls=%d/%d", failedCalls.Load(), healthyCalls.Load())
	}
	healthy, err := runtime.Agent("z-healthy")
	if err != nil || healthy.StateVersion != 1 || healthy.State["done"] != true {
		t.Fatalf("healthy Agent did not complete: %+v, %v", healthy, err)
	}
}

func TestP3ClaimRaceReturnsConcurrentCompletion(t *testing.T) {
	base := NewMemoryStore()
	entered, release := make(chan struct{}, 1), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	delayedStore := p3ClaimStore{base, func(ctx context.Context, key domain.DeliveryKey) (*ExecutionClaim, error) {
		entered <- struct{}{}
		<-release
		return base.ClaimExecution(ctx, key)
	}}
	var calls atomic.Int32
	runner := functionRunner(func(ExecutionContext) (ExecutionResult, error) {
		calls.Add(1)
		return ExecutionResult{StateUpdate: map[string]any{"value": "saved"}}, nil
	})
	slow, agent := p3Runtime(t, delayedStore, runner)
	fast, err := NewRuntimeWithStore(base)
	if err != nil {
		t.Fatal(err)
	}
	if err := fast.RegisterDefinition(agent.Definition, runner); err != nil {
		t.Fatal(err)
	}
	event := domain.NewEvent("request", nil)
	pending := p3Process(slow, agent.ID, event)
	p3Await(t, entered)
	completed, err := fast.Process(agent.ID, event)
	if err != nil {
		t.Fatal(err)
	}
	unblock()
	replayed := p3Await(t, pending)
	if replayed.err != nil || !reflect.DeepEqual(replayed.result, completed) || calls.Load() != 1 {
		t.Fatalf("lost concurrent completion: result=%+v, err=%v, calls=%d", replayed.result, replayed.err, calls.Load())
	}
}

func TestP3SimultaneousClaimsCommitOnlyOnce(t *testing.T) {
	base := NewMemoryStore()
	const contenders = 16
	entered, release := make(chan struct{}, contenders), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	store := p3ClaimStore{base, func(ctx context.Context, key domain.DeliveryKey) (*ExecutionClaim, error) {
		entered <- struct{}{}
		<-release
		return base.ClaimExecution(ctx, key)
	}}
	var calls atomic.Int32
	runtime, agent := p3Runtime(t, store, functionRunner(func(ExecutionContext) (ExecutionResult, error) {
		calls.Add(1)
		return ExecutionResult{StateUpdate: map[string]any{"value": "saved"}, Actions: []domain.Action{domain.NewAction("echo", nil)}}, nil
	}))
	event := domain.NewEvent("request", nil)
	results := make([]<-chan p3Outcome, 0, contenders)
	for range contenders {
		results = append(results, p3Process(runtime, agent.ID, event))
	}
	for range contenders {
		p3Await(t, entered)
	}
	unblock()
	successes := 0
	for _, result := range results {
		outcome := p3Await(t, result)
		if outcome.err == nil {
			successes++
		} else if !errors.Is(outcome.err, ErrExecutionInProgress) {
			t.Fatalf("claim contender returned %v", outcome.err)
		}
	}
	snapshot, err := runtime.Agent(agent.ID)
	if err != nil || successes == 0 || calls.Load() != 1 || snapshot.StateVersion != 1 || len(runtime.Actions()) != 1 {
		t.Fatalf("claim race committed twice: successes=%d, calls=%d, agent=%+v, err=%v", successes, calls.Load(), snapshot, err)
	}
	_, execution := p3StoredExecution(t, base, agent.ID, event)
	if len(execution.Attempts) != 1 {
		t.Fatalf("duplicate claim added attempts: %+v", execution.Attempts)
	}
}

func TestP3CancellationBeforeCommitKeepsAttemptRetryable(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var calls int
	runtime, agent := p3Runtime(t, NewMemoryStore(), functionRunner(func(ExecutionContext) (ExecutionResult, error) {
		calls++
		if calls == 1 {
			cancel()
		}
		return ExecutionResult{StateUpdate: map[string]any{"value": "saved"}, Actions: []domain.Action{domain.NewAction("echo", nil)}}, nil
	}))
	event := domain.NewEvent("request", nil)
	if _, err := runtime.ProcessContext(ctx, agent.ID, event); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Runner result was committed: %v", err)
	}
	delivery, execution := p3StoredExecution(t, runtime.store, agent.ID, event)
	snapshot, err := runtime.Agent(agent.ID)
	if err != nil || snapshot.StateVersion != 0 || len(runtime.Actions()) != 0 ||
		delivery.Status != domain.DeliveryStatusFailed || execution.Attempts[0].Status != domain.AttemptStatusInterrupted {
		t.Fatalf("canceled output was published: agent=%+v execution=%+v err=%v", snapshot, execution, err)
	}
	if err := runtime.Retry(context.Background(), delivery.Key); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Process(agent.ID, event); err != nil {
		t.Fatal(err)
	}
	_, retried := p3StoredExecution(t, runtime.store, agent.ID, event)
	if retried.Execution.ID != execution.Execution.ID || len(retried.Attempts) != 2 || calls != 2 {
		t.Fatalf("interrupted execution was not retried in place: %+v calls=%d", retried, calls)
	}
}

type p3ReadFailureStore struct {
	StateStore
	failure error
}

func (s p3ReadFailureStore) ListDeliveries(context.Context, ...domain.DeliveryStatus) ([]domain.Delivery, error) {
	return nil, s.failure
}

func (s p3ReadFailureStore) ListActions(context.Context, ...domain.ActionStatus) ([]domain.ActionRecord, error) {
	return nil, s.failure
}

func TestP3ContextQueriesPropagateStoreErrors(t *testing.T) {
	want := errors.New("cannot read store")
	runtime, err := NewRuntimeWithStore(p3ReadFailureStore{NewMemoryStore(), want})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.ExecutionsContext(context.Background()); !errors.Is(err, want) {
		t.Fatalf("execution query error: %v", err)
	}
	if _, err := runtime.AttemptsContext(context.Background()); !errors.Is(err, want) {
		t.Fatalf("attempt query error: %v", err)
	}
	if _, err := runtime.ActionsContext(context.Background()); !errors.Is(err, want) {
		t.Fatalf("action query error: %v", err)
	}
}
