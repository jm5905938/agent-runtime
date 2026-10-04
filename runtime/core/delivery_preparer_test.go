package core

import (
	"agent-runtime/domain"
	"context"
	"errors"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
)

type deliveryPreparerTestRunner struct {
	prepare func(context.Context, AgentSnapshot, domain.Event) error
	run     func(ExecutionContext) (ExecutionResult, error)
}

func (runner deliveryPreparerTestRunner) PrepareDelivery(ctx context.Context, agent AgentSnapshot, event domain.Event) error {
	return runner.prepare(ctx, agent, event)
}

func (runner deliveryPreparerTestRunner) Run(input ExecutionContext) (ExecutionResult, error) {
	if runner.run == nil {
		return ExecutionResult{}, nil
	}
	return runner.run(input)
}

type gatedDeliveryPreparerTestRunner struct {
	deliveryPreparerTestRunner
	gate func(AgentSnapshot, domain.Event, []domain.Event) []BlockReason
}

func (runner gatedDeliveryPreparerTestRunner) DeliveryBlockedBy(agent AgentSnapshot, event domain.Event, earlier []domain.Event) []BlockReason {
	return runner.gate(agent, event, earlier)
}

type deliveryPreparerClaimStore struct {
	StateStore
	claims atomic.Int32
	before func(context.Context, domain.DeliveryKey)
}

func (store *deliveryPreparerClaimStore) ClaimExecution(ctx context.Context, key domain.DeliveryKey) (*ExecutionClaim, error) {
	store.claims.Add(1)
	if store.before != nil {
		store.before(ctx, key)
	}
	return store.StateStore.ClaimExecution(ctx, key)
}

func deliveryPreparerTestAgent(t *testing.T, runtime *Runtime, runner AgentRunner) domain.AgentInstance {
	t.Helper()
	agent := domain.NewAgentInstance("delivery preparation")
	agent.Definition = domain.DefinitionRef{ID: "test.delivery-preparer", Version: "1"}
	agent.State = map[string]any{"nested": []any{map[string]any{"value": "original"}}}
	if err := runtime.Register(&agent, runner); err != nil {
		t.Fatal(err)
	}
	return agent
}

func TestDeliveryPreparerRunsAfterGateBeforeClaim(t *testing.T) {
	store := &deliveryPreparerClaimStore{StateStore: NewMemoryStore()}
	runtime, err := NewRuntimeWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	blocked := true
	prepared := 0
	var order []string
	store.before = func(context.Context, domain.DeliveryKey) { order = append(order, "claim") }
	agent := deliveryPreparerTestAgent(t, runtime, gatedDeliveryPreparerTestRunner{
		deliveryPreparerTestRunner: deliveryPreparerTestRunner{
			prepare: func(context.Context, AgentSnapshot, domain.Event) error {
				prepared++
				order = append(order, "prepare")
				return nil
			},
			run: func(ExecutionContext) (ExecutionResult, error) {
				order = append(order, "run")
				return ExecutionResult{}, nil
			},
		},
		gate: func(AgentSnapshot, domain.Event, []domain.Event) []BlockReason {
			order = append(order, "gate")
			if blocked {
				return []BlockReason{{Code: BlockAgentWaiting, Message: "waiting"}}
			}
			return nil
		},
	})
	event := domain.NewEvent("input", nil)
	if _, err := runtime.Process(agent.ID, event); !errors.Is(err, ErrDeliveryNotReady) {
		t.Fatalf("blocked delivery error: %v", err)
	}
	if prepared != 0 || store.claims.Load() != 0 {
		t.Fatalf("blocked delivery prepared or claimed: prepared=%d claims=%d", prepared, store.claims.Load())
	}
	queryAgent(t, runtime, agent.ID)
	if prepared != 0 {
		t.Fatal("status query prepared an execution dependency")
	}
	blocked = false
	order = nil
	if _, err := runtime.Process(agent.ID, event); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(order, []string{"gate", "prepare", "claim", "run"}) || prepared != 1 || store.claims.Load() != 1 {
		t.Fatalf("preparation occurred at the wrong boundary: order=%v prepared=%d claims=%d", order, prepared, store.claims.Load())
	}
	if _, err := runtime.Process(agent.ID, event); err != nil {
		t.Fatal(err)
	}
	if prepared != 1 || store.claims.Load() != 1 {
		t.Fatal("cached completed delivery prepared or claimed again")
	}
}

func TestDeliveryPreparerFailureKeepsUnclaimedDelivery(t *testing.T) {
	for _, scenario := range []string{"error", "panic", "cancel-error", "cancel-success"} {
		t.Run(scenario, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			store := &deliveryPreparerClaimStore{StateStore: NewMemoryStore()}
			runtime, err := NewRuntimeWithStore(store)
			if err != nil {
				t.Fatal(err)
			}
			fault := errors.New("missing model configuration")
			var runs atomic.Int32
			agent := deliveryPreparerTestAgent(t, runtime, deliveryPreparerTestRunner{
				prepare: func(passed context.Context, _ AgentSnapshot, _ domain.Event) error {
					if passed != ctx {
						t.Error("preparer did not receive the caller context")
					}
					switch scenario {
					case "panic":
						panic("dependency setup panicked")
					case "cancel-error":
						cancel()
						return passed.Err()
					case "cancel-success":
						cancel()
						return nil
					default:
						return fault
					}
				},
				run: func(ExecutionContext) (ExecutionResult, error) {
					runs.Add(1)
					return ExecutionResult{StateUpdate: map[string]any{"changed": true}}, nil
				},
			})
			event := domain.NewEvent("input", map[string]any{"message": "saved before preparation"})
			_, err = runtime.ProcessContext(ctx, agent.ID, event)
			switch scenario {
			case "panic":
				if err == nil || !strings.Contains(err.Error(), "dependency setup panicked") {
					t.Fatalf("panic was not surfaced as an error: %v", err)
				}
			case "cancel-error", "cancel-success":
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("preparation lost cancellation: %v", err)
				}
			default:
				if !errors.Is(err, fault) {
					t.Fatalf("preparation lost failure: %v", err)
				}
			}
			deliveryGateTestUnclaimed(t, runtime, agent.ID, event.ID)
			snapshot, err := runtime.Agent(agent.ID)
			if err != nil || snapshot.StateVersion != 0 || !reflect.DeepEqual(snapshot.State, agent.State) || store.claims.Load() != 0 || runs.Load() != 0 {
				t.Fatalf("failed preparation claimed or changed state: agent=%+v err=%v claims=%d runs=%d", snapshot, err, store.claims.Load(), runs.Load())
			}
		})
	}
}

func TestDeliveryPreparerSnapshotsRemainIsolated(t *testing.T) {
	store := NewMemoryStore()
	runtime, err := NewRuntimeWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	agent := deliveryPreparerTestAgent(t, runtime, deliveryPreparerTestRunner{
		prepare: func(_ context.Context, agent AgentSnapshot, event domain.Event) error {
			agent.State["nested"].([]any)[0].(map[string]any)["value"] = "preparer mutation"
			event.Payload["nested"].([]any)[0].(map[string]any)["value"] = "preparer mutation"
			return nil
		},
		run: func(input ExecutionContext) (ExecutionResult, error) {
			if input.Agent.State["nested"].([]any)[0].(map[string]any)["value"] != "original" ||
				input.Event.Payload["nested"].([]any)[0].(map[string]any)["value"] != "original" {
				t.Error("preparer changed the actual execution input")
			}
			return ExecutionResult{}, nil
		},
	})
	event := domain.NewEvent("input", map[string]any{"nested": []any{map[string]any{"value": "original"}}})
	if _, err := runtime.Process(agent.ID, event); err != nil {
		t.Fatal(err)
	}
	snapshot, err := runtime.Agent(agent.ID)
	if err != nil || !reflect.DeepEqual(snapshot.State, agent.State) {
		t.Fatalf("preparer changed persisted state: %+v, %v", snapshot, err)
	}
	saved, err := store.LoadEvent(context.Background(), event.ID)
	if err != nil || !reflect.DeepEqual(saved.Payload, event.Payload) {
		t.Fatalf("preparer changed persisted event: %+v, %v", saved, err)
	}
}

func TestDeliveryPreparerBlockedAllowsOtherAgentToExecute(t *testing.T) {
	runtime := NewRuntime()
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	first := deliveryPreparerTestAgent(t, runtime, gatedDeliveryPreparerTestRunner{
		deliveryPreparerTestRunner: deliveryPreparerTestRunner{
			prepare: func(ctx context.Context, _ AgentSnapshot, _ domain.Event) error {
				entered <- struct{}{}
				select {
				case <-release:
					return nil
				case <-ctx.Done():
					return ctx.Err()
				}
			},
		},
		gate: func(AgentSnapshot, domain.Event, []domain.Event) []BlockReason { return nil },
	})
	other := domain.NewAgentInstance("other agent")
	if err := runtime.Register(&other, functionRunner(func(ExecutionContext) (ExecutionResult, error) {
		return ExecutionResult{StateUpdate: map[string]any{"processed": true}}, nil
	})); err != nil {
		t.Fatal(err)
	}
	firstDone := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_, err := runtime.ProcessContext(ctx, first.ID, domain.NewEvent("input", nil))
		firstDone <- err
	}()
	p3Await(t, entered)
	otherDone := make(chan error, 1)
	go func() {
		_, err := runtime.Process(other.ID, domain.NewEvent("input", nil))
		otherDone <- err
	}()
	if err := p3Await(t, otherDone); err != nil {
		t.Fatalf("another agent was blocked by preparation: %v", err)
	}
	snapshot, err := runtime.Agent(other.ID)
	if err != nil || snapshot.State["processed"] != true {
		t.Fatalf("other agent did not finish: %+v, %v", snapshot, err)
	}
	cancel()
	if err := p3Await(t, firstDone); !errors.Is(err, context.Canceled) {
		t.Fatalf("blocked preparation did not stop on cancellation: %v", err)
	}
}

func TestDeliveryPreparerPlainRunnersRemainCompatible(t *testing.T) {
	runtime := NewRuntime()
	calls := 0
	agent := deliveryPreparerTestAgent(t, runtime, functionRunner(func(ExecutionContext) (ExecutionResult, error) {
		calls++
		return ExecutionResult{StateUpdate: map[string]any{"processed": true}}, nil
	}))
	event := domain.NewEvent("input", nil)
	for range 2 {
		if _, err := runtime.Process(agent.ID, event); err != nil {
			t.Fatal(err)
		}
	}
	snapshot, err := runtime.Agent(agent.ID)
	if err != nil || snapshot.State["processed"] != true || snapshot.StateVersion != 1 || calls != 1 {
		t.Fatalf("optional preparation changed plain runner behavior: %+v, calls=%d, err=%v", snapshot, calls, err)
	}
}
