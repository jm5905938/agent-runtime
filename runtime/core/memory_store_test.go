package core

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"agent-runtime/domain"
)

func memoryTestAgent(t *testing.T, store *MemoryStore) domain.AgentInstance {
	t.Helper()
	agent := domain.NewAgentInstance("test")
	agent.Definition = domain.DefinitionRef{ID: "test", Version: "1"}
	agent.Status = domain.AgentStatusActive
	agent.State = map[string]any{"count": 0, "nested": map[string]any{"value": 1}}
	if err := store.CreateAgent(context.Background(), agent); err != nil {
		t.Fatal(err)
	}
	return agent
}

func memoryTestClaim(t *testing.T, store *MemoryStore, agentID domain.ID) *ExecutionClaim {
	t.Helper()
	received, err := store.ReceiveEvent(context.Background(), agentID, domain.NewEvent("test", map[string]any{"value": 1}))
	if err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimExecution(context.Background(), received.Delivery.Key)
	if err != nil {
		t.Fatal(err)
	}
	return claim
}

func memoryTestAction(claim *ExecutionClaim) domain.ActionRecord {
	action := domain.NewAction("echo", map[string]any{"text": "hello"})
	action.BindExecution(claim.Token.ExecutionID)
	event := domain.NewEvent("action.result", nil)
	return domain.ActionRecord{Request: action, AgentID: claim.Agent.ID, HandlerVersion: "1",
		RecoveryPolicy: domain.RecoveryPolicySafeRetry, IdempotencyKey: string(action.ID), MaxAttempts: 3,
		Status: domain.ActionStatusPending, ResultEventID: event.ID}
}

func TestMemoryStoreReceiveDeduplicatesAtomically(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	agent := memoryTestAgent(t, store)
	event := domain.NewEvent("test", map[string]any{"count": 12, "null": []any(nil)})
	first, err := store.ReceiveEvent(ctx, agent.ID, event)
	if err != nil || first.Duplicate {
		t.Fatalf("first receive = %+v, %v", first, err)
	}
	if _, err := store.LoadExecution(ctx, first.Delivery.ExecutionID); !errors.Is(err, ErrStoreNotFound) {
		t.Fatalf("receive must not start an execution: %v", err)
	}
	duplicate := event
	duplicate.CreatedAt = event.CreatedAt.Add(time.Hour)
	duplicate.Payload = map[string]any{"null": nil, "count": json.Number("12.00")}
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			received, err := store.ReceiveEvent(ctx, agent.ID, duplicate)
			if err != nil || !received.Duplicate || received.Delivery != first.Delivery {
				t.Errorf("duplicate = %+v, %v", received, err)
			}
		}()
	}
	wg.Wait()
	saved, err := store.LoadEvent(ctx, event.ID)
	if err != nil || !saved.CreatedAt.Equal(event.CreatedAt) || saved.Payload["count"] != 12 {
		t.Fatalf("first event overwritten: %+v, %v", saved, err)
	}
	other := memoryTestAgent(t, store)
	conflict := duplicate
	conflict.Type = "different"
	if _, err := store.ReceiveEvent(ctx, other.ID, conflict); !errors.Is(err, ErrStoreConflict) {
		t.Fatalf("content conflict = %v", err)
	}
	if _, err := store.LoadDelivery(ctx, domain.DeliveryKey{AgentID: other.ID, EventID: event.ID}); !errors.Is(err, ErrStoreNotFound) {
		t.Fatalf("conflict left a Delivery: %v", err)
	}
	second, err := store.ReceiveEvent(ctx, other.ID, duplicate)
	if err != nil || second.Duplicate || second.Delivery.ExecutionID == first.Delivery.ExecutionID {
		t.Fatalf("second agent delivery = %+v, %v", second, err)
	}
	orphan := domain.NewEvent("orphan", nil)
	if _, err := store.ReceiveEvent(ctx, "missing", orphan); !errors.Is(err, ErrStoreNotFound) {
		t.Fatalf("missing agent = %v", err)
	}
	if _, err := store.LoadEvent(ctx, orphan.ID); !errors.Is(err, ErrStoreNotFound) {
		t.Fatalf("missing agent left an Event: %v", err)
	}
}

func TestMemoryStoreOnlyOneClaimPerAgent(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	agent := memoryTestAgent(t, store)
	keys := make([]domain.DeliveryKey, 24)
	for i := range keys {
		received, err := store.ReceiveEvent(ctx, agent.ID, domain.NewEvent("test", nil))
		if err != nil {
			t.Fatal(err)
		}
		keys[i] = received.Delivery.Key
	}
	var won atomic.Int32
	var wg sync.WaitGroup
	for _, key := range keys {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := store.ClaimExecution(ctx, key); err == nil {
				won.Add(1)
			} else if !errors.Is(err, ErrExecutionInProgress) {
				t.Errorf("claim = %v", err)
			}
		}()
	}
	wg.Wait()
	if won.Load() != 1 {
		t.Fatalf("%d concurrent claims won", won.Load())
	}
	other := memoryTestAgent(t, store)
	memoryTestClaim(t, store, other.ID)
}

func TestMemoryStoreCommitPublishesAllRecords(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	agent := memoryTestAgent(t, store)
	claim := memoryTestClaim(t, store, agent.ID)
	action := memoryTestAction(claim)
	update := map[string]any{"count": 1, "nested": map[string]any{"value": 2}}
	result, err := store.CommitExecution(ctx, ExecutionCommit{Token: claim.Token, StateUpdate: update, Actions: []domain.ActionRecord{action}})
	if err != nil {
		t.Fatal(err)
	}
	update["nested"].(map[string]any)["value"] = 99
	result.Actions[0].Payload["text"] = "changed"
	action.Request.Payload["text"] = "changed input"
	saved, _ := store.LoadAgent(ctx, agent.ID)
	execution, _ := store.LoadExecution(ctx, claim.Token.ExecutionID)
	delivery, _ := store.LoadDelivery(ctx, claim.Token.Delivery)
	actions, _ := store.ListActions(ctx, domain.ActionStatusPending)
	if saved.StateVersion != 1 || saved.State["count"] != 1 || saved.State["nested"].(map[string]any)["value"] != 2 ||
		execution.Execution.Status != domain.ExecutionStatusCompleted || execution.Execution.Result.Actions[0].Payload["text"] != "hello" ||
		len(execution.Attempts) != 1 || execution.Attempts[0].Status != domain.AttemptStatusSucceeded ||
		delivery.Status != domain.DeliveryStatusCompleted || len(actions) != 1 || actions[0].Request.Payload["text"] != "hello" {
		t.Fatalf("incomplete commit: agent=%+v execution=%+v delivery=%+v actions=%+v", saved, execution, delivery, actions)
	}
	if _, err := store.CommitExecution(ctx, ExecutionCommit{Token: claim.Token}); !errors.Is(err, ErrStoreStaleClaim) {
		t.Fatalf("completed claim reused: %v", err)
	}
	if _, err := store.ClaimExecution(ctx, claim.Token.Delivery); !errors.Is(err, ErrStoreConflict) {
		t.Fatalf("completed execution claimed: %v", err)
	}
}

func TestMemoryStoreCommitFailureHasNoPartialWrites(t *testing.T) {
	tests := []struct {
		name   string
		change func(*MemoryStore, *ExecutionCommit)
	}{
		{"duplicate action", func(_ *MemoryStore, commit *ExecutionCommit) {
			commit.Actions = append(commit.Actions, commit.Actions[0])
		}},
		{"invalid state", func(_ *MemoryStore, commit *ExecutionCommit) { commit.StateUpdate["bad"] = math.NaN() }},
		{"invalid second action", func(_ *MemoryStore, commit *ExecutionCommit) {
			action := memoryCloneActionRecord(commit.Actions[0])
			action.Request.ID = "second"
			action.ResultEventID = "second-result"
			action.Request.Payload["bad"] = func() {}
			commit.Actions = append(commit.Actions, action)
		}},
		{"foreign action execution", func(_ *MemoryStore, commit *ExecutionCommit) { commit.Actions[0].Request.BindExecution("foreign") }},
		{"stale attempt", func(_ *MemoryStore, commit *ExecutionCommit) { commit.Token.AttemptID = "old-attempt" }},
		{"stale version", func(_ *MemoryStore, commit *ExecutionCommit) { commit.Token.ExpectedStateVersion++ }},
		{"version changed after claim", func(store *MemoryStore, commit *ExecutionCommit) {
			agent := store.agents[commit.Token.Delivery.AgentID]
			agent.StateVersion++
			store.agents[agent.ID] = agent
		}},
		{"forged new version", func(store *MemoryStore, commit *ExecutionCommit) {
			agent := store.agents[commit.Token.Delivery.AgentID]
			agent.StateVersion++
			store.agents[agent.ID] = agent
			commit.Token.ExpectedStateVersion = agent.StateVersion
		}},
		{"version exhausted", func(store *MemoryStore, commit *ExecutionCommit) {
			agent := store.agents[commit.Token.Delivery.AgentID]
			agent.StateVersion = math.MaxUint64
			store.agents[agent.ID] = agent
			store.claimVersions[commit.Token.AttemptID] = math.MaxUint64
			commit.Token.ExpectedStateVersion = math.MaxUint64
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			store := NewMemoryStore()
			agent := memoryTestAgent(t, store)
			claim := memoryTestClaim(t, store, agent.ID)
			commit := ExecutionCommit{Token: claim.Token, StateUpdate: map[string]any{"count": 1}, Actions: []domain.ActionRecord{memoryTestAction(claim)}}
			test.change(store, &commit)
			before, _ := store.LoadAgent(ctx, agent.ID)
			if _, err := store.CommitExecution(ctx, commit); err == nil {
				t.Fatal("invalid commit accepted")
			}
			saved, _ := store.LoadAgent(ctx, agent.ID)
			execution, _ := store.LoadExecution(ctx, claim.Token.ExecutionID)
			delivery, _ := store.LoadDelivery(ctx, claim.Token.Delivery)
			actions, _ := store.ListActions(ctx)
			if saved.StateVersion != before.StateVersion || saved.State["count"] != 0 || len(actions) != 0 ||
				execution.Execution.Status != domain.ExecutionStatusRunning || execution.Execution.Result != nil ||
				execution.Attempts[0].Status != domain.AttemptStatusRunning || execution.Attempts[0].FinishedAt != nil ||
				delivery.Status != domain.DeliveryStatusRunning {
				t.Fatalf("partial commit: agent=%+v execution=%+v delivery=%+v actions=%+v", saved, execution, delivery, actions)
			}
		})
	}
}

func TestMemoryStoreFailedExecutionRequiresExplicitRetry(t *testing.T) {
	ctx := context.Background()
	for _, interrupted := range []bool{false, true} {
		store := NewMemoryStore()
		agent := memoryTestAgent(t, store)
		first := memoryTestClaim(t, store, agent.ID)
		if err := store.RequeueDelivery(ctx, first.Token.Delivery); !errors.Is(err, ErrStoreConflict) {
			t.Fatalf("requeue running = %v", err)
		}
		failure := domain.Failure{Kind: domain.ErrorKindBusiness, Message: "try again"}
		if interrupted {
			failure.Kind = domain.ErrorKindInterrupted
		}
		if err := store.FailExecution(ctx, ExecutionFailure{Token: first.Token, Failure: failure, Interrupted: interrupted}); err != nil {
			t.Fatal(err)
		}
		if _, err := store.ClaimExecution(ctx, first.Token.Delivery); !errors.Is(err, ErrDeliveryFailed) {
			t.Fatalf("failed delivery auto retried: %v", err)
		}
		if err := store.RequeueDelivery(ctx, first.Token.Delivery); err != nil {
			t.Fatal(err)
		}
		second, err := store.ClaimExecution(ctx, first.Token.Delivery)
		if err != nil || second.Token.ExecutionID != first.Token.ExecutionID || second.Token.AttemptID == first.Token.AttemptID || second.Attempt.Number != 2 {
			t.Fatalf("retry claim = %+v, %v", second, err)
		}
		if _, err := store.CommitExecution(ctx, ExecutionCommit{Token: first.Token}); !errors.Is(err, ErrStoreStaleClaim) {
			t.Fatalf("old attempt committed: %v", err)
		}
		if err := store.FailExecution(ctx, ExecutionFailure{Token: first.Token, Failure: failure}); !errors.Is(err, ErrStoreStaleClaim) {
			t.Fatalf("old attempt failed newer execution: %v", err)
		}
		if _, err := store.CommitExecution(ctx, ExecutionCommit{Token: second.Token, StateUpdate: map[string]any{"count": 1}}); err != nil {
			t.Fatal(err)
		}
		execution, _ := store.LoadExecution(ctx, first.Token.ExecutionID)
		want := domain.AttemptStatusFailed
		if interrupted {
			want = domain.AttemptStatusInterrupted
		}
		if len(execution.Attempts) != 2 || execution.Attempts[0].Status != want || execution.Attempts[1].Status != domain.AttemptStatusSucceeded {
			t.Fatalf("retry history = %+v", execution)
		}
		if err := store.RequeueDelivery(ctx, first.Token.Delivery); !errors.Is(err, ErrStoreConflict) {
			t.Fatalf("requeue completed = %v", err)
		}
	}
}

func memoryTestCompletion(claim *ActionClaim) ActionCompletion {
	output := map[string]any{"text": "hello"}
	return ActionCompletion{
		Token:  claim.Token,
		Result: domain.ActionResult{ActionID: claim.Record.Request.ID, EventID: claim.Record.ResultEventID, Status: domain.ActionStatusSucceeded, Output: output},
		Event: domain.Event{ID: claim.Record.ResultEventID, Type: "action.result", CreatedAt: time.Now().UTC(),
			Payload: map[string]any{"action_id": string(claim.Record.Request.ID), "action_type": claim.Record.Request.Type,
				"execution_id": string(*claim.Record.Request.ExecutionID), "status": "succeeded", "result": output}},
	}
}

func TestMemoryStoreActionResultAndDeliveryAreAtomic(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	agent := memoryTestAgent(t, store)
	execution := memoryTestClaim(t, store, agent.ID)
	action := memoryTestAction(execution)
	if _, err := store.CommitExecution(ctx, ExecutionCommit{Token: execution.Token, Actions: []domain.ActionRecord{action}}); err != nil {
		t.Fatal(err)
	}
	claim, err := store.ClaimAction(ctx, action.Request.ID)
	if err != nil {
		t.Fatal(err)
	}
	completion := memoryTestCompletion(claim)
	bad := completion
	bad.Event = cloneEvent(completion.Event)
	bad.Event.Payload["status"] = "failed"
	if _, err := store.CompleteAction(ctx, bad); !errors.Is(err, ErrStoreConflict) {
		t.Fatalf("bad result event accepted: %v", err)
	}
	saved, _ := store.LoadAction(ctx, action.Request.ID)
	if saved.Action.Status != domain.ActionStatusRunning || saved.Action.Result != nil || saved.Attempts[0].FinishedAt != nil {
		t.Fatalf("bad completion partially saved: %+v", saved)
	}
	if _, err := store.LoadEvent(ctx, action.ResultEventID); !errors.Is(err, ErrStoreNotFound) {
		t.Fatalf("bad completion left Event: %v", err)
	}
	if _, err := store.CompleteAction(ctx, completion); err != nil {
		t.Fatal(err)
	}
	replayed := completion
	replayed.Event.CreatedAt = completion.Event.CreatedAt.Add(time.Hour)
	if _, err := store.CompleteAction(ctx, replayed); err != nil {
		t.Fatalf("duplicate completion: %v", err)
	}
	saved, _ = store.LoadAction(ctx, action.Request.ID)
	event, _ := store.LoadEvent(ctx, action.ResultEventID)
	delivery, _ := store.LoadDelivery(ctx, domain.DeliveryKey{AgentID: agent.ID, EventID: action.ResultEventID})
	if saved.Action.Status != domain.ActionStatusSucceeded || saved.Action.Result == nil || len(saved.Attempts) != 1 ||
		saved.Attempts[0].Status != domain.ActionStatusSucceeded || !event.CreatedAt.Equal(completion.Event.CreatedAt) || delivery.Status != domain.DeliveryStatusPending {
		t.Fatalf("completion incomplete: action=%+v event=%+v delivery=%+v", saved, event, delivery)
	}
	if _, err := store.ClaimAction(ctx, action.Request.ID); !errors.Is(err, ErrStoreConflict) {
		t.Fatalf("completed action reclaimed: %v", err)
	}
}

func TestMemoryStoreUnknownActionPolicyAndStaleAttempt(t *testing.T) {
	ctx := context.Background()
	for _, policy := range []domain.RecoveryPolicy{domain.RecoveryPolicyManual, domain.RecoveryPolicySafeRetry} {
		store := NewMemoryStore()
		agent := memoryTestAgent(t, store)
		execution := memoryTestClaim(t, store, agent.ID)
		action := memoryTestAction(execution)
		action.RecoveryPolicy, action.MaxAttempts = policy, 2
		if _, err := store.CommitExecution(ctx, ExecutionCommit{Token: execution.Token, Actions: []domain.ActionRecord{action}}); err != nil {
			t.Fatal(err)
		}
		first, err := store.ClaimAction(ctx, action.Request.ID)
		if err != nil {
			t.Fatal(err)
		}
		failure := domain.Failure{Kind: domain.ErrorKindUnknown, Message: "no response"}
		if err := store.RecordActionUnknown(ctx, first.Token, failure); err != nil {
			t.Fatal(err)
		}
		saved, _ := store.LoadAction(ctx, action.Request.ID)
		if saved.Action.Status != domain.ActionStatusUnknown || saved.Action.Result != nil || saved.Action.LastError == nil {
			t.Fatalf("Unknown represented as a final result: %+v", saved)
		}
		second, err := store.ClaimAction(ctx, action.Request.ID)
		if policy == domain.RecoveryPolicyManual {
			if !errors.Is(err, ErrStoreConflict) {
				t.Fatalf("manual action retried: %v", err)
			}
			continue
		}
		if err != nil || second.Token.AttemptNumber != 2 || second.Record.ResultEventID != first.Record.ResultEventID {
			t.Fatalf("safe retry = %+v, %v", second, err)
		}
		if _, err := store.CompleteAction(ctx, memoryTestCompletion(first)); !errors.Is(err, ErrStoreStaleClaim) {
			t.Fatalf("old action attempt completed: %v", err)
		}
		if err := store.RecordActionUnknown(ctx, second.Token, failure); err != nil {
			t.Fatal(err)
		}
		if _, err := store.ClaimAction(ctx, action.Request.ID); !errors.Is(err, ErrStoreConflict) {
			t.Fatalf("action exceeded MaxAttempts: %v", err)
		}
	}
}

func TestMemoryStoreCopiesRecordsAndHonorsCanceledContext(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	agent := memoryTestAgent(t, store)
	agent.State["nested"].(map[string]any)["value"] = 8
	saved, _ := store.LoadAgent(ctx, agent.ID)
	if saved.State["nested"].(map[string]any)["value"] != 1 {
		t.Fatal("CreateAgent retained caller map")
	}
	saved.State["nested"].(map[string]any)["value"] = 9
	listed, _ := store.ListAgents(ctx)
	listed[0].State["nested"].(map[string]any)["value"] = 10
	again, _ := store.LoadAgent(ctx, agent.ID)
	if again.State["nested"].(map[string]any)["value"] != 1 {
		t.Fatal("query exposed stored state")
	}
	if err := store.CreateAgent(ctx, agent); !errors.Is(err, ErrStoreConflict) {
		t.Fatalf("duplicate agent = %v", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	event := domain.NewEvent("test", nil)
	if _, err := store.ReceiveEvent(canceled, agent.ID, event); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled receive = %v", err)
	}
	if _, err := store.LoadEvent(ctx, event.ID); !errors.Is(err, ErrStoreNotFound) {
		t.Fatalf("canceled receive saved event: %v", err)
	}
}

func TestMemoryStoreListsPreserveSubmissionOrder(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	agent := memoryTestAgent(t, store)
	for _, id := range []domain.ID{"z-event", "a-event", "z-event"} {
		event := domain.NewEvent("test", nil)
		event.ID = id
		if _, err := store.ReceiveEvent(ctx, agent.ID, event); err != nil {
			t.Fatal(err)
		}
	}
	deliveries, err := store.ListDeliveries(ctx, domain.DeliveryStatusPending)
	if err != nil || len(deliveries) != 2 || deliveries[0].Key.EventID != "z-event" || deliveries[1].Key.EventID != "a-event" {
		t.Fatalf("delivery order = %+v, %v", deliveries, err)
	}
	claim, err := store.ClaimExecution(ctx, deliveries[0].Key)
	if err != nil {
		t.Fatal(err)
	}
	first, second := memoryTestAction(claim), memoryTestAction(claim)
	first.Request.ID, second.Request.ID = "z-action", "a-action"
	if _, err := store.CommitExecution(ctx, ExecutionCommit{Token: claim.Token, Actions: []domain.ActionRecord{first, second}}); err != nil {
		t.Fatal(err)
	}
	actions, err := store.ListActions(ctx)
	if err != nil || len(actions) != 2 || actions[0].Request.ID != "z-action" || actions[1].Request.ID != "a-action" {
		t.Fatalf("action order = %+v, %v", actions, err)
	}
	if _, err := store.ClaimAction(ctx, first.Request.ID); err != nil {
		t.Fatal(err)
	}
	pending, err := store.ListActions(ctx, domain.ActionStatusPending)
	if err != nil || len(pending) != 1 || pending[0].Request.ID != second.Request.ID {
		t.Fatalf("pending action filter = %+v, %v", pending, err)
	}
	deliveries, err = store.ListDeliveries(ctx, domain.DeliveryStatusPending)
	if err != nil || len(deliveries) != 1 || deliveries[0].Key.EventID != "a-event" {
		t.Fatalf("pending delivery filter = %+v, %v", deliveries, err)
	}
}

func TestMemoryStoreReservesResultEventIdentity(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	agent := memoryTestAgent(t, store)
	claim := memoryTestClaim(t, store, agent.ID)
	action := memoryTestAction(claim)
	if _, err := store.CommitExecution(ctx, ExecutionCommit{Token: claim.Token, Actions: []domain.ActionRecord{action}}); err != nil {
		t.Fatal(err)
	}
	actionClaim, err := store.ClaimAction(ctx, action.Request.ID)
	if err != nil {
		t.Fatal(err)
	}
	completion := memoryTestCompletion(actionClaim)
	if _, err := store.ReceiveEvent(ctx, agent.ID, completion.Event); !errors.Is(err, ErrStoreConflict) {
		t.Fatalf("reserved result event accepted before completion: %v", err)
	}
	if _, err := store.LoadEvent(ctx, completion.Event.ID); !errors.Is(err, ErrStoreNotFound) {
		t.Fatalf("reserved event was written: %v", err)
	}
	if _, err := store.LoadDelivery(ctx, domain.DeliveryKey{AgentID: agent.ID, EventID: completion.Event.ID}); !errors.Is(err, ErrStoreNotFound) {
		t.Fatalf("reserved event delivery was written: %v", err)
	}
	if _, err := store.CompleteAction(ctx, completion); err != nil {
		t.Fatal(err)
	}
	received, err := store.ReceiveEvent(ctx, agent.ID, completion.Event)
	if err != nil || !received.Duplicate {
		t.Fatalf("saved result event cannot be redelivered: %+v, %v", received, err)
	}
}

func TestMemoryStoreActionConflictDoesNotChangeFinalResult(t *testing.T) {
	ctx := context.Background()
	store := NewMemoryStore()
	agent := memoryTestAgent(t, store)
	claim := memoryTestClaim(t, store, agent.ID)
	action := memoryTestAction(claim)
	if _, err := store.CommitExecution(ctx, ExecutionCommit{Token: claim.Token, Actions: []domain.ActionRecord{action}}); err != nil {
		t.Fatal(err)
	}
	actionClaim, err := store.ClaimAction(ctx, action.Request.ID)
	if err != nil {
		t.Fatal(err)
	}
	completion := memoryTestCompletion(actionClaim)
	if _, err := store.CompleteAction(ctx, completion); err != nil {
		t.Fatal(err)
	}
	completion.Result.Output["text"] = "conflicting"
	if _, err := store.CompleteAction(ctx, completion); !errors.Is(err, ErrStoreConflict) {
		t.Fatalf("conflicting repeated completion = %v", err)
	}
	saved, _ := store.LoadAction(ctx, action.Request.ID)
	if saved.Action.Result.Output["text"] != "hello" {
		t.Fatalf("final result changed: %+v", saved)
	}
	saved.Action.Result.Output["text"] = "changed query"
	saved.Action.Request.Payload["text"] = "changed request"
	*saved.Action.Request.ExecutionID = "changed execution"
	*saved.Attempts[0].FinishedAt = time.Time{}
	event, _ := store.LoadEvent(ctx, action.ResultEventID)
	event.Payload["result"].(map[string]any)["text"] = "changed event"
	again, _ := store.LoadAction(ctx, action.Request.ID)
	event, _ = store.LoadEvent(ctx, action.ResultEventID)
	if again.Action.Result.Output["text"] != "hello" || again.Action.Request.Payload["text"] != "hello" ||
		*again.Action.Request.ExecutionID != claim.Token.ExecutionID || again.Attempts[0].FinishedAt.IsZero() ||
		event.Payload["result"].(map[string]any)["text"] != "hello" {
		t.Fatalf("query leaked stored values: action=%+v event=%+v", again, event)
	}
}
