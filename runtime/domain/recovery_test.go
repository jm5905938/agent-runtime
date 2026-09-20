package domain

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestDefinitionRefValidate(t *testing.T) {
	for _, tc := range []struct {
		name string
		ref  DefinitionRef
		want string
	}{
		{name: "missing id", ref: DefinitionRef{Version: "v1"}, want: "definition id"},
		{name: "blank id", ref: DefinitionRef{ID: " \t", Version: "v1"}, want: "definition id"},
		{name: "missing version", ref: DefinitionRef{ID: "echo"}, want: "definition version"},
		{name: "blank version", ref: DefinitionRef{ID: "echo", Version: "\n"}, want: "definition version"},
		{name: "exact binding", ref: DefinitionRef{ID: "echo", Version: "v1"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.ref.Validate()
			if tc.want == "" {
				if err != nil {
					t.Fatalf("Validate() = %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Validate() = %v, want error containing %q", err, tc.want)
			}
		})
	}
}

func TestRecoveryRecordsRetainIdentityAndAttempts(t *testing.T) {
	const saved = `{
		"agent": {"id":"agent-1","name":"echo","definition":{"id":"echo","version":"v1"},"status":"active","state":{"waiting":"action-1"},"state_version":9},
		"delivery": {"key":{"agent_id":"agent-1","event_id":"event-1"},"execution_id":"execution-1","status":"completed"},
		"execution": {"id":"execution-1","agent_id":"agent-1","event_id":"event-1","status":"completed","created_at":"2026-09-20T01:00:00Z","started_at":"2026-09-20T01:02:00Z","finished_at":"2026-09-20T01:03:00Z","attempt_count":2,"result":{"state_update":{"waiting":"action-1"},"actions":[{"id":"action-1","execution_id":"execution-1","type":"echo","payload":{"message":"hello"}}]}},
		"attempts": [
			{"id":"attempt-1","execution_id":"execution-1","number":1,"status":"interrupted","started_at":"2026-09-20T01:00:00Z","finished_at":"2026-09-20T01:01:00Z","error":{"kind":"interrupted","message":"process stopped before commit"}},
			{"id":"attempt-2","execution_id":"execution-1","number":2,"status":"succeeded","started_at":"2026-09-20T01:02:00Z","finished_at":"2026-09-20T01:03:00Z"}
		],
		"action": {"request":{"id":"action-1","execution_id":"execution-1","type":"echo","payload":{"message":"hello"}},"agent_id":"agent-1","handler_version":"v1","recovery_policy":"safe_retry","idempotency_key":"echo-action-1","max_attempts":3,"status":"succeeded","attempt_count":1,"result_event_id":"result-event-1","result":{"action_id":"action-1","event_id":"result-event-1","status":"succeeded","output":{"message":"hello","metadata":null}}}
	}`
	type snapshot struct {
		Agent     AgentInstance `json:"agent"`
		Delivery  Delivery      `json:"delivery"`
		Execution Execution     `json:"execution"`
		Attempts  []Attempt     `json:"attempts"`
		Action    ActionRecord  `json:"action"`
	}
	var original snapshot
	if err := json.Unmarshal([]byte(saved), &original); err != nil {
		t.Fatal(err)
	}
	if original.Agent.Definition != (DefinitionRef{ID: "echo", Version: "v1"}) || original.Agent.StateVersion != 9 {
		t.Fatalf("agent restoration metadata lost: %#v", original.Agent)
	}
	if original.Delivery.Key != (DeliveryKey{AgentID: original.Agent.ID, EventID: original.Execution.EventID}) || original.Delivery.ExecutionID != original.Execution.ID {
		t.Fatalf("delivery changed its logical identity: %#v", original.Delivery)
	}
	if original.Execution.AttemptCount != 2 || len(original.Attempts) != 2 {
		t.Fatalf("execution retry count lost: %#v", original.Execution)
	}
	for i, attempt := range original.Attempts {
		if attempt.ExecutionID != original.Execution.ID || attempt.Number != uint64(i+1) {
			t.Fatalf("attempt %d is detached from original execution: %#v", i, attempt)
		}
		if attempt.StartedAt.Location() != time.UTC || attempt.FinishedAt == nil || attempt.FinishedAt.Location() != time.UTC {
			t.Fatalf("attempt %d time lost UTC or completion: %#v", i, attempt)
		}
	}
	if original.Attempts[0].ID == original.Attempts[1].ID || original.Attempts[0].Status != AttemptStatusInterrupted || original.Attempts[1].Status != AttemptStatusSucceeded {
		t.Fatalf("attempt histories collapsed: %#v", original.Attempts)
	}
	if original.Attempts[0].Error == nil || original.Attempts[0].Error.Kind != ErrorKindInterrupted {
		t.Fatalf("interruption reason missing: %#v", original.Attempts[0])
	}
	if original.Execution.Result == nil || len(original.Execution.Result.Actions) != 1 {
		t.Fatalf("saved execution result missing: %#v", original.Execution)
	}
	action := original.Execution.Result.Actions[0]
	if action.ExecutionID == nil || *action.ExecutionID != original.Execution.ID || action.ID != original.Action.Request.ID {
		t.Fatalf("committed action lost producing execution: %#v", action)
	}
	if original.Action.Result == nil || original.Action.Result.ActionID != action.ID || original.Action.Result.EventID != original.Action.ResultEventID {
		t.Fatalf("action result lost stable associations: %#v", original.Action)
	}
	if original.Action.HandlerVersion != "v1" || original.Action.RecoveryPolicy != RecoveryPolicySafeRetry || original.Action.Status != ActionStatusSucceeded || original.Action.IdempotencyKey != "echo-action-1" || original.Action.MaxAttempts != 3 {
		t.Fatalf("action recovery metadata lost: %#v", original.Action)
	}
	encoded, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	var restored snapshot
	if err := json.Unmarshal(encoded, &restored); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(original, restored) {
		t.Fatalf("recovery records changed after JSON round trip:\nbefore: %#v\nafter: %#v", original, restored)
	}
}

func TestUnknownActionRetainsAttemptReasonWithoutFinalResult(t *testing.T) {
	started := time.Date(2026, 9, 20, 1, 0, 0, 0, time.UTC)
	finished := started.Add(time.Minute)
	reason := &Failure{Kind: ErrorKindInterrupted, Message: "handler may have completed before process stopped"}
	type actionSnapshot struct {
		Record  ActionRecord  `json:"record"`
		Attempt ActionAttempt `json:"attempt"`
	}
	original := actionSnapshot{
		Record: ActionRecord{
			Request:        Action{ID: "action-1", Type: "send", Payload: map[string]any{"text": "hello"}},
			AgentID:        "agent-1",
			HandlerVersion: "v1",
			RecoveryPolicy: RecoveryPolicyManual,
			IdempotencyKey: "send-action-1",
			MaxAttempts:    1,
			Status:         ActionStatusUnknown,
			AttemptCount:   1,
			ResultEventID:  "result-event-1",
			LastError:      reason,
		},
		Attempt: ActionAttempt{
			ID:         "action-attempt-1",
			ActionID:   "action-1",
			Number:     1,
			Status:     ActionStatusUnknown,
			StartedAt:  started,
			FinishedAt: &finished,
			Error:      reason,
		},
	}
	encoded, err := json.Marshal(original)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), `"result":`) {
		t.Fatalf("unknown action must not imply a final result: %s", encoded)
	}
	var restored actionSnapshot
	if err := json.Unmarshal(encoded, &restored); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(original, restored) {
		t.Fatalf("unknown reason or attempt associations lost: %s", encoded)
	}
	if restored.Record.LastError == nil || restored.Attempt.Error == nil || restored.Record.LastError.Kind != ErrorKindInterrupted || restored.Attempt.Error.Message != reason.Message {
		t.Fatalf("unknown action has no durable interruption reason: %#v", restored)
	}
}
