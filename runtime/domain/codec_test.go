package domain_test

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"agent-runtime/codec"
	"agent-runtime/domain"
)

func TestRecoverySnapshotCodecRoundTrip(t *testing.T) {
	type snapshot struct {
		Agent          domain.AgentInstance   `json:"agent"`
		Event          domain.Event           `json:"event"`
		Delivery       domain.Delivery        `json:"delivery"`
		Execution      domain.Execution       `json:"execution"`
		Attempts       []domain.Attempt       `json:"attempts"`
		Action         domain.ActionRecord    `json:"action"`
		ActionAttempts []domain.ActionAttempt `json:"action_attempts"`
		ResultEvent    domain.Event           `json:"result_event"`
	}
	started := time.Date(2026, 9, 20, 9, 0, 0, 123456789, time.FixedZone("UTC+8", 8*60*60))
	finished := started.Add(time.Minute)
	data := map[string]any{
		"large": json.Number("18446744073709551615"),
		"nested": map[string]any{
			"items": []any{json.Number("9007199254740993"), nil, true, "消息"},
		},
		"empty": nil,
	}
	executionID := domain.ID("execution-1")
	action := domain.Action{ID: "action-1", ExecutionID: &executionID, Type: "echo", Payload: data}
	reason := &domain.Failure{Kind: domain.ErrorKindInterrupted, Message: "process stopped before saving result"}
	original := snapshot{
		Agent: domain.AgentInstance{
			ID: "agent-1", Name: "Echo", Definition: domain.DefinitionRef{ID: "echo", Version: "v1"},
			Status: domain.AgentStatusActive, State: data, StateVersion: 9,
		},
		Event: domain.Event{ID: "event-1", Type: "echo.request", Payload: data, CreatedAt: started},
		Delivery: domain.Delivery{
			Key:         domain.DeliveryKey{AgentID: "agent-1", EventID: "event-1"},
			ExecutionID: executionID, Status: domain.DeliveryStatusCompleted,
		},
		Execution: domain.Execution{
			ID: executionID, AgentID: "agent-1", EventID: "event-1", Status: domain.ExecutionStatusCompleted,
			CreatedAt: started, StartedAt: &started, FinishedAt: &finished, AttemptCount: 2,
			Result: &domain.ExecutionResult{StateUpdate: data, Actions: []domain.Action{action}},
		},
		Attempts: []domain.Attempt{
			{ID: "attempt-1", ExecutionID: executionID, Number: 1, Status: domain.AttemptStatusInterrupted, StartedAt: started, FinishedAt: &finished, Error: reason},
			{ID: "attempt-2", ExecutionID: executionID, Number: 2, Status: domain.AttemptStatusSucceeded, StartedAt: started, FinishedAt: &finished},
		},
		Action: domain.ActionRecord{
			Request: action, AgentID: "agent-1", HandlerVersion: "v1", RecoveryPolicy: domain.RecoveryPolicySafeRetry,
			IdempotencyKey: "echo-action-1", MaxAttempts: 3, Status: domain.ActionStatusSucceeded, AttemptCount: 2,
			ResultEventID: "result-event-1",
			Result:        &domain.ActionResult{ActionID: "action-1", EventID: "result-event-1", Status: domain.ActionStatusSucceeded, Output: data},
		},
		ActionAttempts: []domain.ActionAttempt{
			{ID: "action-attempt-1", ActionID: "action-1", Number: 1, Status: domain.ActionStatusUnknown, StartedAt: started, FinishedAt: &finished, Error: reason},
			{ID: "action-attempt-2", ActionID: "action-1", Number: 2, Status: domain.ActionStatusSucceeded, StartedAt: started, FinishedAt: &finished},
		},
		ResultEvent: domain.Event{ID: "result-event-1", Type: "echo.result", Payload: data, CreatedAt: finished},
	}
	encoded, err := codec.Encode(original)
	if err != nil {
		t.Fatal(err)
	}
	var restored snapshot
	if err := codec.Decode(encoded, &restored); err != nil {
		t.Fatal(err)
	}
	if restored.Agent.ID != original.Agent.ID || restored.Agent.Definition != original.Agent.Definition || restored.Agent.StateVersion != 9 {
		t.Fatalf("agent identity or definition lost: %#v", restored.Agent)
	}
	if restored.Delivery != original.Delivery || restored.Execution.ID != restored.Delivery.ExecutionID || restored.Execution.AgentID != restored.Delivery.Key.AgentID || restored.Execution.EventID != restored.Event.ID || restored.Delivery.Key.EventID != restored.Event.ID {
		t.Fatalf("event/delivery/execution associations lost: %#v", restored)
	}
	if restored.Execution.Status != domain.ExecutionStatusCompleted || restored.Execution.AttemptCount != 2 || len(restored.Attempts) != 2 || len(restored.ActionAttempts) != 2 {
		t.Fatalf("attempt history missing: %#v", restored)
	}
	for i, attempt := range restored.Attempts {
		if attempt.ID != original.Attempts[i].ID || attempt.ExecutionID != executionID || attempt.Number != uint64(i+1) || attempt.Status != original.Attempts[i].Status || !reflect.DeepEqual(attempt.Error, original.Attempts[i].Error) {
			t.Fatalf("execution attempt %d changed: %#v", i, attempt)
		}
		assertUTCTime(t, attempt.StartedAt, started)
		assertUTCTimePointer(t, attempt.FinishedAt, finished)
	}
	for i, attempt := range restored.ActionAttempts {
		if attempt.ID != original.ActionAttempts[i].ID || attempt.ActionID != action.ID || attempt.Number != uint64(i+1) || attempt.Status != original.ActionAttempts[i].Status || !reflect.DeepEqual(attempt.Error, original.ActionAttempts[i].Error) {
			t.Fatalf("action attempt %d changed: %#v", i, attempt)
		}
		assertUTCTime(t, attempt.StartedAt, started)
		assertUTCTimePointer(t, attempt.FinishedAt, finished)
	}
	if restored.Execution.Result == nil || len(restored.Execution.Result.Actions) != 1 || !reflect.DeepEqual(restored.Execution.Result.Actions[0], action) {
		t.Fatalf("committed execution result changed: %#v", restored.Execution.Result)
	}
	if !reflect.DeepEqual(restored.Action, original.Action) || restored.Action.Result == nil || restored.Action.Result.EventID != restored.ResultEvent.ID || restored.Action.Result.ActionID != restored.Action.Request.ID {
		t.Fatalf("action request, policy, final result or result event changed: %#v", restored.Action)
	}
	for name, decoded := range map[string]map[string]any{
		"state": restored.Agent.State, "event": restored.Event.Payload,
		"state update": restored.Execution.Result.StateUpdate, "action": restored.Action.Request.Payload,
		"action result": restored.Action.Result.Output, "result event": restored.ResultEvent.Payload,
	} {
		if !reflect.DeepEqual(decoded, data) {
			t.Fatalf("%s business numbers, nesting or null changed: %#v", name, decoded)
		}
		if number, ok := decoded["large"].(json.Number); !ok || number.String() != "18446744073709551615" {
			t.Fatalf("%s large number lost type or precision: %#v", name, decoded["large"])
		}
	}
	assertUTCTime(t, restored.Event.CreatedAt, started)
	assertUTCTime(t, restored.Execution.CreatedAt, started)
	assertUTCTimePointer(t, restored.Execution.StartedAt, started)
	assertUTCTimePointer(t, restored.Execution.FinishedAt, finished)
	assertUTCTime(t, restored.ResultEvent.CreatedAt, finished)
	if original.Execution.StartedAt.Location() == time.UTC || original.Attempts[0].FinishedAt.Location() == time.UTC {
		t.Fatal("encoding mutated the original timestamps")
	}
	again, err := codec.Encode(restored)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(encoded, again) {
		t.Fatalf("saved JSON changed after decode/re-encode:\nbefore: %s\nafter: %s", encoded, again)
	}
}

func assertUTCTime(t *testing.T, got, original time.Time) {
	t.Helper()
	if got.Location() != time.UTC || !got.Equal(original) {
		t.Fatalf("timestamp = %v (%v), want same instant as %v in UTC", got, got.Location(), original)
	}
}

func assertUTCTimePointer(t *testing.T, got *time.Time, original time.Time) {
	t.Helper()
	if got == nil {
		t.Fatal("timestamp was lost")
	}
	assertUTCTime(t, *got, original)
}
