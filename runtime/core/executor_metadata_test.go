package core

import (
	"agent-runtime/domain"
	"errors"
	"strings"
	"testing"
)

func TestHandlerForRequiresSavedVersionAndReleasesLock(t *testing.T) {
	executor := NewExecutor()
	if err := executor.Register("echo", handlerFunc(func(domain.Action) (map[string]any, error) {
		if !executor.mu.TryLock() {
			t.Fatal("handler lookup left executor locked")
		}
		executor.mu.Unlock()
		return nil, nil
	})); err != nil {
		t.Fatal(err)
	}
	for _, record := range []domain.ActionRecord{
		{Request: domain.Action{Type: "missing"}, HandlerVersion: "1"},
		{Request: domain.Action{Type: "echo"}, HandlerVersion: "2"},
	} {
		if handler, err := executor.handlerFor(record); handler != nil || !errors.Is(err, ErrHandlerUnavailable) {
			t.Fatalf("unavailable binding: handler=%v, err=%v", handler, err)
		}
	}
	handler, err := executor.handlerFor(domain.ActionRecord{Request: domain.Action{Type: "echo"}, HandlerVersion: "1"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := handler.Execute(domain.Action{}); err != nil {
		t.Fatal(err)
	}
}

func TestPrepareActionsFreezesHandlerMetadataWithoutExecution(t *testing.T) {
	executor := NewExecutor()
	calls := 0
	options := HandlerOptions{Version: "echo-v2", RecoveryPolicy: domain.RecoveryPolicySafeRetry, MaxAttempts: 3}
	if err := executor.RegisterWithOptions("echo", handlerFunc(func(domain.Action) (map[string]any, error) {
		calls++
		return nil, nil
	}), options); err != nil {
		t.Fatal(err)
	}
	options.Version = "changed"
	action := domain.NewAction("echo", map[string]any{"nested": []any{map[string]any{"value": "original"}}})
	action.BindExecution("old-execution")
	records, err := executor.prepareActions("agent", "execution", []domain.Action{action})
	if err != nil || len(records) != 1 {
		t.Fatalf("prepareActions: records=%+v, err=%v", records, err)
	}
	record := records[0]
	if calls != 0 || record.AgentID != "agent" || *record.Request.ExecutionID != "execution" ||
		record.Request.ID != action.ID || record.HandlerVersion != "echo-v2" ||
		record.RecoveryPolicy != domain.RecoveryPolicySafeRetry || record.MaxAttempts != 3 ||
		record.IdempotencyKey != string(action.ID) || record.ResultEventID == "" ||
		record.Status != domain.ActionStatusPending || record.AttemptCount != 0 ||
		record.Result != nil || record.LastError != nil {
		t.Fatalf("prepared metadata: calls=%d, record=%+v", calls, record)
	}
	action.Payload["nested"].([]any)[0].(map[string]any)["value"] = "caller mutation"
	if got := record.Request.Payload["nested"].([]any)[0].(map[string]any)["value"]; got != "original" {
		t.Fatalf("prepared payload aliases caller: %v", got)
	}
	*record.Request.ExecutionID = "record mutation"
	if *action.ExecutionID != "old-execution" {
		t.Fatalf("prepared execution ID aliases caller: %s", *action.ExecutionID)
	}
}

func TestRegisterUsesConservativeDefaultsAndCannotReplaceBinding(t *testing.T) {
	executor := NewExecutor()
	if err := executor.Register("echo", EchoHandler{}); err != nil {
		t.Fatal(err)
	}
	if err := executor.RegisterWithOptions("echo", handlerFunc(func(domain.Action) (map[string]any, error) {
		t.Fatal("replacement handler was called")
		return nil, nil
	}), HandlerOptions{Version: "replacement", RecoveryPolicy: domain.RecoveryPolicySafeRetry, MaxAttempts: 3}); err == nil {
		t.Fatal("existing binding was replaced")
	}
	action := domain.NewAction("echo", map[string]any{"value": "original"})
	records, err := executor.prepareActions("agent", "execution", []domain.Action{action})
	if err != nil {
		t.Fatal(err)
	}
	if record := records[0]; record.HandlerVersion != "1" || record.RecoveryPolicy != domain.RecoveryPolicyManual || record.MaxAttempts != 1 {
		t.Fatalf("default metadata changed: %+v", record)
	}
	event, err := executor.Execute(action)
	if err != nil || event.Payload["result"].(map[string]any)["value"] != "original" {
		t.Fatalf("original binding changed: event=%+v, err=%v", event, err)
	}
}

func TestRegisterWithOptionsRejectsInvalidBindings(t *testing.T) {
	valid := HandlerOptions{Version: "1", RecoveryPolicy: domain.RecoveryPolicyManual, MaxAttempts: 1}
	var nilFunction handlerFunc
	var nilPointer *EchoHandler
	tests := []struct {
		name    string
		typeID  string
		handler ActionHandler
		options HandlerOptions
	}{
		{"empty type", "", EchoHandler{}, valid},
		{"blank type", "  ", EchoHandler{}, valid},
		{"nil handler", "echo", nil, valid},
		{"typed nil function", "echo", nilFunction, valid},
		{"typed nil pointer", "echo", nilPointer, valid},
		{"empty version", "echo", EchoHandler{}, HandlerOptions{RecoveryPolicy: domain.RecoveryPolicyManual, MaxAttempts: 1}},
		{"blank version", "echo", EchoHandler{}, HandlerOptions{Version: " ", RecoveryPolicy: domain.RecoveryPolicyManual, MaxAttempts: 1}},
		{"invalid version text", "echo", EchoHandler{}, HandlerOptions{Version: string([]byte{0xff}), RecoveryPolicy: domain.RecoveryPolicyManual, MaxAttempts: 1}},
		{"invalid type text", string([]byte{0xff}), EchoHandler{}, valid},
		{"invalid policy", "echo", EchoHandler{}, HandlerOptions{Version: "1", RecoveryPolicy: "unsafe", MaxAttempts: 1}},
		{"zero attempts", "echo", EchoHandler{}, HandlerOptions{Version: "1", RecoveryPolicy: domain.RecoveryPolicyManual}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			executor := NewExecutor()
			if err := executor.RegisterWithOptions(test.typeID, test.handler, test.options); err == nil {
				t.Fatal("invalid binding accepted")
			}
			if len(executor.handlers) != 0 || len(executor.options) != 0 {
				t.Fatal("failed registration left a binding")
			}
		})
	}
}

func TestPrepareActionsRejectsBatchWithoutCallingHandlers(t *testing.T) {
	executor := NewExecutor()
	calls := 0
	if err := executor.Register("echo", handlerFunc(func(domain.Action) (map[string]any, error) {
		calls++
		return nil, nil
	})); err != nil {
		t.Fatal(err)
	}
	action := domain.NewAction("echo", nil)
	tests := []struct {
		name    string
		actions []domain.Action
		message string
	}{
		{"duplicate ID", []domain.Action{action, action}, "重复action id"},
		{"missing handler", []domain.Action{action, domain.NewAction("missing", nil)}, "未注册action类型missing"},
		{"unsupported data", []domain.Action{action, domain.NewAction("echo", map[string]any{"invalid": func() {}})}, "输入"},
		{"empty action ID", []domain.Action{{Type: "echo"}}, "id/type"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			records, err := executor.prepareActions("agent", "execution", test.actions)
			if err == nil || !strings.Contains(err.Error(), test.message) || records != nil {
				t.Fatalf("records=%+v, err=%v", records, err)
			}
		})
	}
	if calls != 0 || len(executor.results) != 0 || len(executor.statuses) != 0 {
		t.Fatalf("preparation executed actions: calls=%d", calls)
	}
	if records, err := executor.prepareActions("agent", "execution", nil); err != nil || len(records) != 0 {
		t.Fatalf("empty actions rejected: records=%+v, err=%v", records, err)
	}
}
