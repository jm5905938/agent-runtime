package core

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"

	"agent-runtime/domain"
)

func openMemoryRecoveryTest(t *testing.T, backend RecoveryStore) RecoverySession {
	t.Helper()
	session, err := backend.OpenSession(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return session
}

func TestMemoryRecoverySessionGates(t *testing.T) {
	ctx := context.Background()
	backend := NewMemoryRecoveryStore()
	if _, exposed := backend.(StateStore); exposed {
		t.Fatal("backend exposes writes without ownership")
	}
	session := openMemoryRecoveryTest(t, backend)
	writes := []struct {
		name string
		call func() error
	}{
		{"create", func() error { return session.CreateAgent(ctx, domain.AgentInstance{}) }},
		{"receive", func() error { _, err := session.ReceiveEvent(ctx, "", domain.Event{}); return err }},
		{"claim execution", func() error { _, err := session.ClaimExecution(ctx, domain.DeliveryKey{}); return err }},
		{"commit", func() error { _, err := session.CommitExecution(ctx, ExecutionCommit{}); return err }},
		{"fail", func() error { return session.FailExecution(ctx, ExecutionFailure{}) }},
		{"requeue", func() error { return session.RequeueDelivery(ctx, domain.DeliveryKey{}) }},
		{"claim action", func() error { _, err := session.ClaimAction(ctx, ""); return err }},
		{"complete", func() error { _, err := session.CompleteAction(ctx, ActionCompletion{}); return err }},
		{"unknown", func() error { return session.RecordActionUnknown(ctx, ActionToken{}, domain.Failure{}) }},
	}
	reads := []struct {
		name string
		call func() error
	}{
		{"load agent", func() error { _, err := session.LoadAgent(ctx, ""); return err }},
		{"agents", func() error { _, err := session.ListAgents(ctx); return err }},
		{"event", func() error { _, err := session.LoadEvent(ctx, ""); return err }},
		{"delivery", func() error { _, err := session.LoadDelivery(ctx, domain.DeliveryKey{}); return err }},
		{"deliveries", func() error { _, err := session.ListDeliveries(ctx); return err }},
		{"execution", func() error { _, err := session.LoadExecution(ctx, ""); return err }},
		{"action", func() error { _, err := session.LoadAction(ctx, ""); return err }},
		{"actions", func() error { _, err := session.ListActions(ctx); return err }},
	}
	for _, op := range writes {
		if err := op.call(); !errors.Is(err, ErrRecoveryRequired) {
			t.Errorf("startup %s = %v", op.name, err)
		}
	}
	for _, op := range reads {
		if err := op.call(); err != nil && !errors.Is(err, ErrStoreNotFound) {
			t.Errorf("startup %s = %v", op.name, err)
		}
	}
	if _, err := session.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := session.Close(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled close = %v", err)
	}
	if _, err := backend.OpenSession(ctx); !errors.Is(err, ErrStoreOwned) {
		t.Fatalf("canceled close released owner: %v", err)
	}
	if err := session.CreateAgent(canceled, domain.AgentInstance{}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled write = %v", err)
	}
	if err := session.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := session.Close(canceled); err != nil {
		t.Fatalf("repeat close = %v", err)
	}
	for _, op := range append(writes, reads...) {
		if err := op.call(); !errors.Is(err, ErrStoreClosed) {
			t.Errorf("closed %s = %v", op.name, err)
		}
	}
	if _, err := session.Recover(ctx); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("closed recovery = %v", err)
	}
	newSession := openMemoryRecoveryTest(t, backend)
	defer newSession.Close(ctx)
	if _, err := session.ListAgents(canceled); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("old session bypassed new owner: %v", err)
	}
}

func TestMemoryRecoveryConcurrentOwnership(t *testing.T) {
	ctx := context.Background()
	backend := NewMemoryRecoveryStore()
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := backend.OpenSession(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled open = %v", err)
	}
	winners := make(chan RecoverySession, 16)
	var wg sync.WaitGroup
	for i := 0; i < cap(winners); i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			session, err := backend.OpenSession(ctx)
			if err == nil {
				winners <- session
			} else if !errors.Is(err, ErrStoreOwned) {
				t.Errorf("open = %v", err)
			}
		}()
	}
	wg.Wait()
	if len(winners) != 1 {
		t.Fatalf("owners = %d", len(winners))
	}
	if err := (<-winners).Close(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestMemoryRecoveryRestoresAttemptsAndRejectsOldTokens(t *testing.T) {
	ctx := context.Background()
	backend := NewMemoryRecoveryStore().(*memoryRecoveryStore)
	agent := memoryTestAgent(t, backend.store)
	actionExecution := memoryTestClaim(t, backend.store, agent.ID)
	action := memoryTestAction(actionExecution)
	manual := memoryTestAction(actionExecution)
	manual.RecoveryPolicy = domain.RecoveryPolicyManual
	exhausted := memoryTestAction(actionExecution)
	exhausted.MaxAttempts = 1
	pending := memoryTestAction(actionExecution)
	completed := memoryTestAction(actionExecution)
	if _, err := backend.store.CommitExecution(ctx, ExecutionCommit{Token: actionExecution.Token, Actions: []domain.ActionRecord{action, manual, exhausted, pending, completed}}); err != nil {
		t.Fatal(err)
	}
	actionClaim, err := backend.store.ClaimAction(ctx, action.Request.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []domain.ID{manual.Request.ID, exhausted.Request.ID} {
		if _, err := backend.store.ClaimAction(ctx, id); err != nil {
			t.Fatal(err)
		}
	}
	completeClaim, err := backend.store.ClaimAction(ctx, completed.Request.ID)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := backend.store.CompleteAction(ctx, memoryTestCompletion(completeClaim)); err != nil {
		t.Fatal(err)
	}
	execution := memoryTestClaim(t, backend.store, agent.ID)
	beforeAgent, _ := backend.store.LoadAgent(ctx, agent.ID)
	beforeCompleted, _ := backend.store.LoadAction(ctx, completed.Request.ID)
	session := openMemoryRecoveryTest(t, backend)
	report, err := session.Recover(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := RecoveryReport{RequeuedDeliveries: []domain.DeliveryKey{execution.Token.Delivery}, UnknownActions: []domain.ID{action.Request.ID, manual.Request.ID, exhausted.Request.ID}, RetryableActions: []domain.ID{action.Request.ID}}
	if !reflect.DeepEqual(report, want) {
		t.Fatalf("report = %+v, want %+v", report, want)
	}
	afterAgent, _ := session.LoadAgent(ctx, agent.ID)
	afterCompleted, _ := session.LoadAction(ctx, completed.Request.ID)
	if !reflect.DeepEqual(beforeAgent, afterAgent) || !reflect.DeepEqual(beforeCompleted, afterCompleted) {
		t.Fatal("recovery changed business state or final result")
	}
	saved, _ := session.LoadExecution(ctx, execution.Token.ExecutionID)
	if saved.Execution.Status != domain.ExecutionStatusPending || saved.Execution.AttemptCount != 1 || saved.Execution.FinishedAt != nil || saved.Execution.StartedAt != nil || saved.Execution.Error != "" ||
		len(saved.Attempts) != 1 || saved.Attempts[0].Status != domain.AttemptStatusInterrupted || saved.Attempts[0].FinishedAt == nil || saved.Attempts[0].Error.Kind != domain.ErrorKindInterrupted {
		t.Fatalf("recovered execution = %+v", saved)
	}
	for _, beforeNewClaim := range []bool{true, false} {
		if !beforeNewClaim {
			newClaim, err := session.ClaimExecution(ctx, execution.Token.Delivery)
			if err != nil || newClaim.Token.ExecutionID != execution.Token.ExecutionID || newClaim.Token.AttemptID == execution.Token.AttemptID || newClaim.Attempt.Number != 2 {
				t.Fatalf("new execution = %+v, %v", newClaim, err)
			}
			newAction, err := session.ClaimAction(ctx, action.Request.ID)
			if err != nil || newAction.Token.AttemptNumber != 2 || newAction.Record.IdempotencyKey != action.IdempotencyKey || newAction.Record.ResultEventID != action.ResultEventID {
				t.Fatalf("new action = %+v, %v", newAction, err)
			}
		}
		if _, err := session.CommitExecution(ctx, ExecutionCommit{Token: execution.Token}); !errors.Is(err, ErrStoreStaleClaim) {
			t.Fatalf("old execution committed: %v", err)
		}
		if err := session.FailExecution(ctx, ExecutionFailure{Token: execution.Token}); !errors.Is(err, ErrStoreStaleClaim) {
			t.Fatalf("old execution failed: %v", err)
		}
		if _, err := session.CompleteAction(ctx, memoryTestCompletion(actionClaim)); !errors.Is(err, ErrStoreStaleClaim) {
			t.Fatalf("old action completed: %v", err)
		}
		if err := session.RecordActionUnknown(ctx, actionClaim.Token, domain.Failure{}); !errors.Is(err, ErrStoreStaleClaim) {
			t.Fatalf("old action changed unknown: %v", err)
		}
	}
	for _, id := range []domain.ID{manual.Request.ID, exhausted.Request.ID} {
		if _, err := session.ClaimAction(ctx, id); !errors.Is(err, ErrStoreConflict) {
			t.Fatalf("ineligible action retried: %v", err)
		}
	}
	if _, err := session.LoadEvent(ctx, action.ResultEventID); !errors.Is(err, ErrStoreNotFound) {
		t.Fatalf("unknown generated result: %v", err)
	}
	report.UnknownActions[0] = "changed"
	report.RequeuedDeliveries[0].EventID = "changed"
	report.RetryableActions[0] = "changed"
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cached, err := session.Recover(ctx)
			if err != nil || !reflect.DeepEqual(cached, want) {
				t.Errorf("cached report = %+v, %v", cached, err)
			}
			cached.UnknownActions[0] = "changed again"
		}()
	}
	wg.Wait()
	current, _ := session.LoadExecution(ctx, execution.Token.ExecutionID)
	if current.Execution.Status != domain.ExecutionStatusRunning || len(current.Attempts) != 2 {
		t.Fatal("repeat recovery reclaimed live execution")
	}
	if err := session.Close(ctx); err != nil {
		t.Fatal(err)
	}
	newSession := openMemoryRecoveryTest(t, backend)
	defer newSession.Close(ctx)
	if _, err := newSession.Recover(ctx); err != nil {
		t.Fatalf("new ownership recovery = %v", err)
	}
}

func TestMemoryRecoveryValidationRollsBackAndCanRetry(t *testing.T) {
	ctx := context.Background()
	for _, damage := range []string{"attempt", "claim version", "action source", "action attempt", "orphan execution", "result event"} {
		t.Run(damage, func(t *testing.T) {
			backend := NewMemoryRecoveryStore().(*memoryRecoveryStore)
			store := backend.store
			agent := memoryTestAgent(t, store)
			first := memoryTestClaim(t, store, agent.ID)
			action := memoryTestAction(first)
			if _, err := store.CommitExecution(ctx, ExecutionCommit{Token: first.Token, Actions: []domain.ActionRecord{action}}); err != nil {
				t.Fatal(err)
			}
			if _, err := store.ClaimAction(ctx, action.Request.ID); err != nil {
				t.Fatal(err)
			}
			claim := memoryTestClaim(t, store, agent.ID)
			var restore func()
			switch damage {
			case "attempt":
				previous := store.attempts[claim.Token.ExecutionID][0]
				store.attempts[claim.Token.ExecutionID][0].ExecutionID = "missing"
				restore = func() { store.attempts[claim.Token.ExecutionID][0] = previous }
			case "claim version":
				version := store.claimVersions[claim.Token.AttemptID]
				delete(store.claimVersions, claim.Token.AttemptID)
				restore = func() { store.claimVersions[claim.Token.AttemptID] = version }
			case "action source":
				previous := store.actions[action.Request.ID]
				changed := memoryCloneActionRecord(previous)
				changed.Request.BindExecution("missing")
				store.actions[action.Request.ID] = changed
				restore = func() { store.actions[action.Request.ID] = previous }
			case "action attempt":
				previous := store.actionAttempts[action.Request.ID][0]
				store.actionAttempts[action.Request.ID][0].Number++
				restore = func() { store.actionAttempts[action.Request.ID][0] = previous }
			case "orphan execution":
				store.executions["orphan"] = domain.Execution{ID: "orphan"}
				restore = func() { delete(store.executions, "orphan") }
			case "result event":
				store.events[action.ResultEventID] = domain.Event{ID: action.ResultEventID, Type: "action.result"}
				restore = func() { delete(store.events, action.ResultEventID) }
			}
			session := openMemoryRecoveryTest(t, backend)
			beforeExecution, _ := session.LoadExecution(ctx, claim.Token.ExecutionID)
			beforeAction, _ := session.LoadAction(ctx, action.Request.ID)
			if _, err := session.Recover(ctx); !errors.Is(err, ErrStoreConflict) {
				t.Fatalf("invalid recovery = %v", err)
			}
			afterExecution, _ := session.LoadExecution(ctx, claim.Token.ExecutionID)
			afterAction, _ := session.LoadAction(ctx, action.Request.ID)
			if !reflect.DeepEqual(beforeExecution, afterExecution) || !reflect.DeepEqual(beforeAction, afterAction) {
				t.Fatal("failed recovery left partial transitions")
			}
			if _, err := session.ClaimAction(ctx, action.Request.ID); !errors.Is(err, ErrRecoveryRequired) {
				t.Fatalf("failed recovery enabled writes: %v", err)
			}
			restore()
			if _, err := session.Recover(ctx); err != nil {
				t.Fatalf("recovery retry = %v", err)
			}
			if err := session.Close(ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
}
