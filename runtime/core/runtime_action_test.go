package core

import (
	"agent-runtime/domain"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

func actionBridgeRunner(ctx ExecutionContext) (ExecutionResult, error) {
	if ctx.Event.Type == "action.result" {
		return ExecutionResult{StateUpdate: map[string]any{
			"action_status": ctx.Event.Payload["status"], "result_event_id": string(ctx.Event.ID),
		}}, nil
	}
	return ExecutionResult{Actions: []domain.Action{domain.NewAction("bridge-test", nil)}}, nil
}

func actionBridgeRuntime(t *testing.T, store StateStore, handler ActionHandler) (*Runtime, domain.AgentInstance, domain.ActionRecord) {
	t.Helper()
	runtime, err := NewRuntimeWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	agent := domain.NewAgentInstance("action bridge")
	agent.Definition = domain.DefinitionRef{ID: "action-bridge", Version: "1"}
	if err := runtime.Register(&agent, functionRunner(actionBridgeRunner)); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Executor().Register("bridge-test", handler); err != nil {
		t.Fatal(err)
	}
	result, err := runtime.Process(agent.ID, domain.NewEvent("request", nil))
	if err != nil || len(result.Actions) != 1 {
		t.Fatalf("request result=%+v, err=%v", result, err)
	}
	saved, err := store.LoadAction(context.Background(), result.Actions[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	return runtime, agent, saved.Action
}

func actionBridgeRebind(t *testing.T, store StateStore, ref domain.DefinitionRef, handler ActionHandler) *Runtime {
	t.Helper()
	runtime, err := NewRuntimeWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	if err := runtime.RegisterDefinition(ref, functionRunner(actionBridgeRunner)); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Executor().Register("bridge-test", handler); err != nil {
		t.Fatal(err)
	}
	return runtime
}

func TestRuntimeActionFailureSavesStableResultOnce(t *testing.T) {
	store := NewMemoryStore()
	var calls atomic.Int32
	handler := handlerFunc(func(domain.Action) (map[string]any, error) {
		calls.Add(1)
		return nil, errors.New("handler refused")
	})
	runtime, agent, original := actionBridgeRuntime(t, store, handler)
	for range 2 {
		if err := runtime.RunUntilIdle(); err != nil {
			t.Fatal(err)
		}
	}
	rebound := actionBridgeRebind(t, store, agent.Definition, handler)
	if err := rebound.RunUntilIdle(); err != nil {
		t.Fatal(err)
	}
	saved, err := store.LoadAction(context.Background(), original.Request.ID)
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 || saved.Action.Status != domain.ActionStatusFailed || saved.Action.Result == nil ||
		saved.Action.Result.EventID != original.ResultEventID || len(saved.Attempts) != 1 ||
		saved.Attempts[0].Status != domain.ActionStatusFailed || saved.Attempts[0].FinishedAt == nil ||
		saved.Action.Result.Error == nil || saved.Action.Result.Error.Kind != domain.ErrorKindBusiness {
		t.Fatalf("calls=%d, saved action=%+v", calls.Load(), saved)
	}
	event, err := store.LoadEvent(context.Background(), original.ResultEventID)
	if err != nil || event.Payload["status"] != "failed" || event.Payload["action_id"] != string(original.Request.ID) ||
		event.Payload["error"] != saved.Action.Result.Error.Message {
		t.Fatalf("result event=%+v, err=%v", event, err)
	}
	delivery, err := store.LoadDelivery(context.Background(), domain.DeliveryKey{AgentID: agent.ID, EventID: original.ResultEventID})
	if err != nil || delivery.Status != domain.DeliveryStatusCompleted {
		t.Fatalf("result delivery=%+v, err=%v", delivery, err)
	}
	snapshot, err := rebound.Agent(agent.ID)
	if err != nil || snapshot.StateVersion != 2 || snapshot.State["action_status"] != "failed" ||
		snapshot.State["result_event_id"] != string(original.ResultEventID) || len(rebound.Executions()) != 2 {
		t.Fatalf("result processing=%+v, err=%v", snapshot, err)
	}
}

func TestRuntimeActionUnknownIsNotRetried(t *testing.T) {
	for _, scenario := range []string{"panic", "invalid output"} {
		t.Run(scenario, func(t *testing.T) {
			store := NewMemoryStore()
			var calls atomic.Int32
			handler := handlerFunc(func(domain.Action) (map[string]any, error) {
				calls.Add(1)
				if scenario == "panic" {
					panic("handler interrupted")
				}
				return map[string]any{"invalid": func() {}}, nil
			})
			runtime, agent, original := actionBridgeRuntime(t, store, handler)
			for range 2 {
				if err := runtime.RunUntilIdle(); err != nil {
					t.Fatal(err)
				}
			}
			rebound := actionBridgeRebind(t, store, agent.Definition, handler)
			if err := rebound.RunUntilIdle(); err != nil {
				t.Fatal(err)
			}
			saved, err := store.LoadAction(context.Background(), original.Request.ID)
			if err != nil {
				t.Fatal(err)
			}
			if calls.Load() != 1 || saved.Action.Status != domain.ActionStatusUnknown || saved.Action.Result != nil ||
				len(saved.Attempts) != 1 || saved.Attempts[0].Status != domain.ActionStatusUnknown ||
				saved.Attempts[0].FinishedAt == nil || saved.Action.LastError == nil ||
				saved.Action.LastError.Kind != domain.ErrorKindUnknown || saved.Action.LastError.Message == "" {
				t.Fatalf("calls=%d, unresolved action=%+v", calls.Load(), saved)
			}
			if _, err := store.LoadEvent(context.Background(), original.ResultEventID); !errors.Is(err, ErrStoreNotFound) {
				t.Fatalf("unknown action published a final event: %v", err)
			}
			snapshot, err := rebound.Agent(agent.ID)
			if err != nil || snapshot.StateVersion != 1 || len(rebound.Executions()) != 1 {
				t.Fatalf("unknown action advanced the agent: %+v, %v", snapshot, err)
			}
		})
	}
}

func TestRuntimeActionHandlerCanQueryRuntime(t *testing.T) {
	var runtime *Runtime
	var agent domain.AgentInstance
	handler := handlerFunc(func(action domain.Action) (map[string]any, error) {
		if _, err := runtime.Agent(agent.ID); err != nil {
			return nil, err
		}
		stored, err := runtime.store.LoadAction(context.Background(), action.ID)
		if err != nil {
			return nil, err
		}
		if stored.Action.Status != domain.ActionStatusRunning {
			return nil, fmt.Errorf("handler sees action status %s", stored.Action.Status)
		}
		if err := runtime.Executor().Register("handler-added", EchoHandler{}); err != nil {
			return nil, err
		}
		return map[string]any{"queried": true}, nil
	})
	runtime, agent, _ = actionBridgeRuntime(t, NewMemoryStore(), handler)
	done := make(chan error, 1)
	go func() { done <- runtime.RunUntilIdle() }()
	if err := p3Await(t, done); err != nil {
		t.Fatal(err)
	}
	snapshot, err := runtime.Agent(agent.ID)
	if err != nil || snapshot.State["action_status"] != "succeeded" {
		t.Fatalf("handler queries failed: %+v, %v", snapshot, err)
	}
}

type actionBridgeClaimBarrier struct {
	StateStore
	entered chan struct{}
	release <-chan struct{}
}

func (s *actionBridgeClaimBarrier) ClaimAction(ctx context.Context, actionID domain.ID) (*ActionClaim, error) {
	s.entered <- struct{}{}
	select {
	case <-s.release:
		return s.StateStore.ClaimAction(ctx, actionID)
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func TestRuntimeActionClaimRaceSkipsCompletedAction(t *testing.T) {
	store := NewMemoryStore()
	release := make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	barrier := &actionBridgeClaimBarrier{StateStore: store, entered: make(chan struct{}, 1), release: release}
	var calls atomic.Int32
	handler := handlerFunc(func(domain.Action) (map[string]any, error) {
		calls.Add(1)
		return nil, nil
	})
	first, agent, original := actionBridgeRuntime(t, barrier, handler)
	second := actionBridgeRebind(t, store, agent.Definition, handler)
	done := make(chan error, 1)
	go func() { done <- first.RunUntilIdle() }()
	p3Await(t, barrier.entered)
	if err := second.RunUntilIdle(); err != nil {
		t.Fatal(err)
	}
	unblock()
	if err := p3Await(t, done); err != nil {
		t.Fatalf("completed action race stopped the runtime: %v", err)
	}
	saved, err := store.LoadAction(context.Background(), original.Request.ID)
	if err != nil || calls.Load() != 1 || len(saved.Attempts) != 1 || saved.Action.Status != domain.ActionStatusSucceeded {
		t.Fatalf("calls=%d, action=%+v, err=%v", calls.Load(), saved, err)
	}
}
