package core

import (
	"agent-runtime/domain"
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

type contextHandlerFunc func(context.Context, domain.Action) (map[string]any, error)

func (handler contextHandlerFunc) Execute(action domain.Action) (map[string]any, error) {
	return handler(context.Background(), action)
}

func (handler contextHandlerFunc) ExecuteContext(ctx context.Context, action domain.Action) (map[string]any, error) {
	return handler(ctx, action)
}

func TestActionCancellationPreservesConfirmedResult(t *testing.T) {
	for _, contextual := range []bool{false, true} {
		for _, succeeded := range []bool{false, true} {
			t.Run(fmt.Sprintf("contextual=%t/succeeded=%t", contextual, succeeded), func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				var calls atomic.Int32
				invoke := func() (map[string]any, error) {
					calls.Add(1)
					cancel()
					if !succeeded {
						return nil, errors.New("provider refused the request")
					}
					return map[string]any{"message": "confirmed reply"}, nil
				}
				var handler ActionHandler = handlerFunc(func(domain.Action) (map[string]any, error) { return invoke() })
				if contextual {
					handler = contextHandlerFunc(func(passed context.Context, _ domain.Action) (map[string]any, error) {
						if passed != ctx {
							t.Error("runtime did not pass the caller context")
						}
						return invoke()
					})
				}
				store := NewMemoryStore()
				runtime, agent, original := actionBridgeRuntime(t, store, handler)
				if err := runtime.RunUntilIdleContext(ctx); !errors.Is(err, context.Canceled) {
					t.Fatalf("drain did not report cancellation: %v", err)
				}
				saved, err := store.LoadAction(context.Background(), original.Request.ID)
				if err != nil {
					t.Fatal(err)
				}
				wantStatus := domain.ActionStatusFailed
				if succeeded {
					wantStatus = domain.ActionStatusSucceeded
				}
				if saved.Action.Status != wantStatus || saved.Action.Result == nil || len(saved.Attempts) != 1 || saved.Attempts[0].FinishedAt == nil {
					t.Fatalf("confirmed result was lost: %+v", saved)
				}
				if succeeded && saved.Action.Result.Output["message"] != "confirmed reply" {
					t.Fatalf("reply changed: %+v", saved.Action.Result)
				}
				delivery, err := store.LoadDelivery(context.Background(), domain.DeliveryKey{AgentID: agent.ID, EventID: original.ResultEventID})
				if err != nil || delivery.Status != domain.DeliveryStatusPending {
					t.Fatalf("result should wait for the next drain: %+v, %v", delivery, err)
				}
				rebound := actionBridgeRebind(t, store, agent.Definition, handler)
				if err := rebound.RunUntilIdle(); err != nil {
					t.Fatal(err)
				}
				if calls.Load() != 1 {
					t.Fatalf("confirmed call repeated: %d", calls.Load())
				}
				snapshot, err := rebound.Agent(agent.ID)
				if err != nil || snapshot.State["action_status"] != string(wantStatus) {
					t.Fatalf("saved result was not consumed: %+v, %v", snapshot, err)
				}
			})
		}
	}
}

type actionCompletionContextKey struct{}

type cancelDuringCompletionStore struct {
	StateStore
	cancel context.CancelFunc
	fault  error
	t      *testing.T
}

func (store cancelDuringCompletionStore) CompleteAction(ctx context.Context, completion ActionCompletion) (domain.ActionResult, error) {
	store.cancel()
	if err := ctx.Err(); err != nil {
		store.t.Errorf("completion inherited cancellation: %v", err)
	}
	if ctx.Value(actionCompletionContextKey{}) != "preserved" {
		store.t.Error("completion lost caller context values")
	}
	deadline, ok := ctx.Deadline()
	if !ok || time.Until(deadline) > 5*time.Second || time.Until(deadline) <= 0 {
		store.t.Error("completion cleanup has no bounded timeout")
	}
	if store.fault != nil {
		return domain.ActionResult{}, store.fault
	}
	return store.StateStore.CompleteAction(ctx, completion)
}

func TestActionCompletionIgnoresConcurrentCancellation(t *testing.T) {
	for _, fault := range []error{nil, errors.New("completion save failed")} {
		t.Run(fmt.Sprint(fault), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.WithValue(context.Background(), actionCompletionContextKey{}, "preserved"))
			defer cancel()
			memory := NewMemoryStore()
			store := cancelDuringCompletionStore{StateStore: memory, cancel: cancel, fault: fault, t: t}
			runtime, _, original := actionBridgeRuntime(t, store, handlerFunc(func(domain.Action) (map[string]any, error) {
				return map[string]any{"message": "complete"}, nil
			}))
			err := runtime.RunUntilIdleContext(ctx)
			if !errors.Is(err, context.Canceled) || (fault != nil && !errors.Is(err, fault)) {
				t.Fatalf("completion error lost cancellation or storage fault: %v", err)
			}
			saved, err := memory.LoadAction(context.Background(), original.Request.ID)
			if err != nil {
				t.Fatal(err)
			}
			if fault == nil && (saved.Action.Status != domain.ActionStatusSucceeded || saved.Action.Result == nil) {
				t.Fatalf("completion lost result: %+v", saved)
			}
			if fault != nil && (saved.Action.Status != domain.ActionStatusRunning || saved.Action.Result != nil) {
				t.Fatalf("failed save produced a terminal result: %+v", saved)
			}
		})
	}
}

func TestContextActionCancellationIsUnknownAcrossRecovery(t *testing.T) {
	backend := NewMemoryRecoveryStore()
	runtime, err := OpenRuntime(context.Background(), backend)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { runtime.Close(context.Background()) })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var calls atomic.Int32
	handler := contextHandlerFunc(func(passed context.Context, _ domain.Action) (map[string]any, error) {
		calls.Add(1)
		if passed != ctx {
			t.Error("runtime did not pass the cancellation context")
		}
		cancel()
		return nil, fmt.Errorf("request result uncertain: %w", passed.Err())
	})
	agent := domain.NewAgentInstance("interrupted action")
	agent.Definition = domain.DefinitionRef{ID: "action-bridge", Version: "1"}
	if err := runtime.Register(&agent, functionRunner(actionBridgeRunner)); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Executor().Register("bridge-test", handler); err != nil {
		t.Fatal(err)
	}
	request, err := runtime.Process(agent.ID, domain.NewEvent("request", nil))
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.RunUntilIdleContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled drain: %v", err)
	}
	if err := runtime.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	resumed, err := OpenRuntime(context.Background(), backend)
	if err != nil {
		t.Fatal(err)
	}
	defer resumed.Close(context.Background())
	if err := resumed.RegisterDefinition(agent.Definition, functionRunner(actionBridgeRunner)); err != nil {
		t.Fatal(err)
	}
	if err := resumed.Executor().Register("bridge-test", handler); err != nil {
		t.Fatal(err)
	}
	if err := resumed.RunUntilIdle(); err != nil {
		t.Fatal(err)
	}
	saved, err := resumed.ActionContext(context.Background(), request.Actions[0].ID)
	if err != nil || saved.Action.Status != domain.ActionStatusUnknown || saved.Action.Result != nil || len(saved.Attempts) != 1 ||
		saved.Action.LastError == nil || saved.Action.LastError.Kind != domain.ErrorKindInterrupted || saved.Attempts[0].FinishedAt == nil || calls.Load() != 1 {
		t.Fatalf("uncertain canceled call was lost or repeated: %+v, calls=%d, err=%v", saved, calls.Load(), err)
	}
	if _, err := resumed.store.LoadEvent(context.Background(), saved.Action.ResultEventID); !errors.Is(err, ErrStoreNotFound) {
		t.Fatalf("uncertain call published a result event: %v", err)
	}
}
