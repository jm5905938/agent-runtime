package core

import (
	"agent-runtime/domain"
	"context"
	"errors"
	"sync"
	"testing"
)

func scopeTestRuntime(t *testing.T, runner AgentRunner) (*Runtime, AgentSnapshot, AgentSnapshot) {
	t.Helper()
	runtime, err := OpenRuntime(context.Background(), NewMemoryRecoveryStore())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := runtime.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})
	ref := domain.DefinitionRef{ID: "scope-test", Version: "1"}
	if err := runtime.RegisterDefinition(ref, runner); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Executor().Register("echo", EchoHandler{}); err != nil {
		t.Fatal(err)
	}
	first, err := runtime.CreateAgent("first", ref, nil)
	if err != nil {
		t.Fatal(err)
	}
	second, err := runtime.CreateAgent("second", ref, nil)
	if err != nil {
		t.Fatal(err)
	}
	return runtime, first, second
}

func TestRunAgentUntilIdleLeavesOtherAgentWorkUntouched(t *testing.T) {
	ctx := context.Background()
	runtime, first, second := scopeTestRuntime(t, resultRunner{})
	result, err := runtime.Process(second.ID, domain.NewEvent("start", nil))
	if err != nil {
		t.Fatal(err)
	}
	completed, err := runtime.store.LoadAction(ctx, result.Actions[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.executeAction(ctx, completed.Action); err != nil {
		t.Fatal(err)
	}
	result, err = runtime.Process(second.ID, domain.NewEvent("start", nil))
	if err != nil {
		t.Fatal(err)
	}
	pendingActionID := result.Actions[0].ID
	queued := domain.NewEvent("start", nil)
	if err := runtime.Submit(second.ID, queued); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Submit(first.ID, domain.NewEvent("start", nil)); err != nil {
		t.Fatal(err)
	}
	if err := runtime.RunAgentUntilIdleContext(ctx, first.ID); err != nil {
		t.Fatal(err)
	}
	snapshot, err := runtime.Agent(first.ID)
	if err != nil || snapshot.StateVersion != 2 || snapshot.State["action_status"] != "succeeded" {
		t.Fatalf("selected agent did not finish its action/result: %+v, %v", snapshot, err)
	}
	snapshot, err = runtime.Agent(second.ID)
	if err != nil || snapshot.StateVersion != 2 || snapshot.State["action_status"] != nil {
		t.Fatalf("other agent changed: %+v, %v", snapshot, err)
	}
	for _, eventID := range []domain.ID{queued.ID, completed.Action.ResultEventID} {
		delivery, err := runtime.store.LoadDelivery(ctx, domain.DeliveryKey{AgentID: second.ID, EventID: eventID})
		if err != nil || delivery.Status != domain.DeliveryStatusPending {
			t.Fatalf("other agent delivery was claimed: %+v, %v", delivery, err)
		}
		if execution, err := runtime.store.LoadExecution(ctx, delivery.ExecutionID); !errors.Is(err, ErrStoreNotFound) {
			t.Fatalf("other agent delivery created an attempt: %+v, %v", execution, err)
		}
	}
	pending, err := runtime.store.LoadAction(ctx, pendingActionID)
	if err != nil || pending.Action.Status != domain.ActionStatusPending || pending.Action.AttemptCount != 0 || len(pending.Attempts) != 0 {
		t.Fatalf("other agent action was claimed: %+v, %v", pending, err)
	}
	if err := runtime.RunUntilIdle(); err != nil {
		t.Fatal(err)
	}
	snapshot, err = runtime.Agent(second.ID)
	if err != nil || snapshot.StateVersion != 6 || snapshot.State["action_status"] != "succeeded" {
		t.Fatalf("global drain did not finish remaining work: %+v, %v", snapshot, err)
	}
	remaining, err := runtime.store.ListDeliveries(ctx, domain.DeliveryStatusPending)
	if err != nil || len(remaining) != 0 {
		t.Fatalf("remaining deliveries=%+v, err=%v", remaining, err)
	}
}

func TestRunAgentUntilIdleValidatesScope(t *testing.T) {
	runtime, first, _ := scopeTestRuntime(t, resultRunner{})
	paused := domain.NewAgentInstance("paused")
	paused.Definition, paused.Status = first.Definition, domain.AgentStatusPaused
	if err := runtime.RestoreAgent(paused); err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name string
		id   domain.ID
		want error
	}{
		{"empty", "", nil},
		{"missing", "missing-agent", ErrStoreNotFound},
		{"paused", paused.ID, ErrAgentUnavailable},
	} {
		t.Run(test.name, func(t *testing.T) {
			err := runtime.RunAgentUntilIdleContext(context.Background(), test.id)
			if err == nil || test.want != nil && !errors.Is(err, test.want) {
				t.Fatalf("error=%v, want=%v", err, test.want)
			}
		})
	}
	if err := runtime.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := runtime.RunAgentUntilIdleContext(context.Background(), first.ID); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("closed scoped drain returned %v", err)
	}
}

func TestRunAgentUntilIdleCancellationWhileWaitingForDrain(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	runtime, first, second := scopeTestRuntime(t, functionRunner(func(input ExecutionContext) (ExecutionResult, error) {
		if input.Agent.Name == "first" {
			close(entered)
			<-release
		}
		return ExecutionResult{StateUpdate: map[string]any{"finished": true}}, nil
	}))
	t.Cleanup(unblock)
	for _, agentID := range []domain.ID{first.ID, second.ID} {
		if err := runtime.Submit(agentID, domain.NewEvent("start", nil)); err != nil {
			t.Fatal(err)
		}
	}
	done := make(chan error, 1)
	go func() { done <- runtime.RunAgentUntilIdleContext(context.Background(), first.ID) }()
	p3Await(t, entered)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := runtime.RunAgentUntilIdleContext(ctx, second.ID); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled drain waiter returned %v", err)
	}
	unblock()
	if err := p3Await(t, done); err != nil {
		t.Fatal(err)
	}
	snapshot, err := runtime.Agent(second.ID)
	if err != nil || snapshot.StateVersion != 0 {
		t.Fatalf("canceled waiter processed another agent: %+v, %v", snapshot, err)
	}
	if err := runtime.RunAgentUntilIdleContext(context.Background(), second.ID); err != nil {
		t.Fatal(err)
	}
	snapshot, err = runtime.Agent(second.ID)
	if err != nil || snapshot.State["finished"] != true {
		t.Fatalf("canceled waiter retained drain ownership: %+v, %v", snapshot, err)
	}
}

func TestRunAgentUntilIdleCloseWaitsForAcceptedWork(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	runtime, first, _ := scopeTestRuntime(t, functionRunner(func(ExecutionContext) (ExecutionResult, error) {
		close(entered)
		<-release
		return ExecutionResult{}, nil
	}))
	t.Cleanup(unblock)
	if err := runtime.Submit(first.ID, domain.NewEvent("start", nil)); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- runtime.RunAgentUntilIdleContext(context.Background(), first.ID) }()
	p3Await(t, entered)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := runtime.Close(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("interrupted close returned %v", err)
	}
	if err := runtime.RunAgentUntilIdleContext(context.Background(), first.ID); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("closing scoped drain accepted new work: %v", err)
	}
	unblock()
	if err := p3Await(t, done); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}
