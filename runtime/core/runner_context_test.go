package core

import (
	"agent-runtime/domain"
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

type contextTestRunner struct {
	run func(context.Context, ExecutionContext) (ExecutionResult, error)
}

func (r contextTestRunner) Run(ExecutionContext) (ExecutionResult, error) {
	panic("context runner used legacy entry")
}

func (r contextTestRunner) RunContext(ctx context.Context, input ExecutionContext) (ExecutionResult, error) {
	return r.run(ctx, input)
}

type classifiedRunnerError struct{ kind domain.ErrorKind }

func (e classifiedRunnerError) Error() string                 { return "worker failed" }
func (e classifiedRunnerError) FailureKind() domain.ErrorKind { return e.kind }

func TestContextRunnerCancellationRecordsInterruptionAndAllowsRetry(t *testing.T) {
	entered := make(chan struct{})
	calls := 0
	runner := contextTestRunner{run: func(ctx context.Context, input ExecutionContext) (ExecutionResult, error) {
		calls++
		if input.ExecutionID == "" || input.AttemptID == "" {
			t.Error("runner did not receive execution identity")
		}
		if calls == 1 {
			close(entered)
			<-ctx.Done()
			return ExecutionResult{}, ctx.Err()
		}
		return ExecutionResult{StateUpdate: map[string]any{"value": "saved"}}, nil
	}}
	store := NewMemoryStore()
	runtime, agent := p3Runtime(t, store, runner)
	event := domain.NewEvent("request", nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := runtime.ProcessContext(ctx, agent.ID, event); done <- err }()
	p3Await(t, entered)
	cancel()
	if err := p3Await(t, done); !errors.Is(err, context.Canceled) || !errors.Is(err, ErrExecutionFailed) {
		t.Fatalf("canceled execution: %v", err)
	}
	delivery, saved := p3StoredExecution(t, store, agent.ID, event)
	if saved.Attempts[0].Status != domain.AttemptStatusInterrupted || saved.Attempts[0].Error.Kind != domain.ErrorKindInterrupted {
		t.Fatalf("interruption lost: %+v", saved)
	}
	snapshot, err := runtime.Agent(agent.ID)
	if err != nil || snapshot.StateVersion != 0 || len(runtime.Actions()) != 0 {
		t.Fatalf("canceled output committed: %+v, %v", snapshot, err)
	}
	if err := runtime.Retry(context.Background(), delivery.Key); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Process(agent.ID, event); err != nil {
		t.Fatal(err)
	}
	_, retried := p3StoredExecution(t, store, agent.ID, event)
	if retried.Execution.ID != saved.Execution.ID || len(retried.Attempts) != 2 || retried.Attempts[1].Status != domain.AttemptStatusSucceeded {
		t.Fatalf("retry lost original identity: %+v", retried)
	}
	closeCtx, closeCancel := context.WithTimeout(context.Background(), time.Second)
	defer closeCancel()
	if err := runtime.Close(closeCtx); err != nil {
		t.Fatal(err)
	}
}

func TestRunnerFailureKindIsSavedWithoutCommittingOutput(t *testing.T) {
	for _, test := range []struct {
		name   string
		cause  error
		kind   domain.ErrorKind
		status domain.AttemptStatus
	}{
		{"business", errors.New("bad request"), domain.ErrorKindBusiness, domain.AttemptStatusFailed},
		{"worker", fmt.Errorf("bridge: %w", classifiedRunnerError{domain.ErrorKindRuntime}), domain.ErrorKindRuntime, domain.AttemptStatusFailed},
		{"deadline", fmt.Errorf("bridge: %w", context.DeadlineExceeded), domain.ErrorKindInterrupted, domain.AttemptStatusInterrupted},
		{"interrupted", classifiedRunnerError{domain.ErrorKindInterrupted}, domain.ErrorKindInterrupted, domain.AttemptStatusInterrupted},
		{"invalid kind", classifiedRunnerError{domain.ErrorKindUnknown}, domain.ErrorKindRuntime, domain.AttemptStatusFailed},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := NewMemoryStore()
			runtime, agent := p3Runtime(t, store, contextTestRunner{run: func(context.Context, ExecutionContext) (ExecutionResult, error) {
				return ExecutionResult{StateUpdate: map[string]any{"value": "must not commit"}}, test.cause
			}})
			event := domain.NewEvent("request", nil)
			if _, err := runtime.Process(agent.ID, event); !errors.Is(err, test.cause) {
				t.Fatalf("cause lost: %v", err)
			}
			_, saved := p3StoredExecution(t, store, agent.ID, event)
			attempt := saved.Attempts[0]
			if attempt.Error == nil || attempt.Error.Kind != test.kind || attempt.Status != test.status {
				t.Fatalf("wrong attempt: %+v", attempt)
			}
			snapshot, err := runtime.Agent(agent.ID)
			if err != nil || snapshot.StateVersion != 0 || snapshot.State["value"] != "original" {
				t.Fatalf("state changed: %+v, %v", snapshot, err)
			}
		})
	}
}
