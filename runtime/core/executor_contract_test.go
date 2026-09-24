package core

import (
	"agent-runtime/domain"
	"testing"
)

type handlerFunc func(domain.Action) (map[string]any, error)

func (f handlerFunc) Execute(action domain.Action) (map[string]any, error) { return f(action) }

func TestExecutorResultSnapshotsRemainIsolated(t *testing.T) {
	executor := NewExecutor()
	calls := 0
	output := map[string]any{"nested": []any{map[string]any{"value": "original"}}}
	if err := executor.Register("test", handlerFunc(func(action domain.Action) (map[string]any, error) {
		calls++
		action.Payload["input"].([]any)[0] = "handler mutation"
		return output, nil
	})); err != nil {
		t.Fatal(err)
	}
	action := domain.NewAction("test", map[string]any{"input": []any{"original"}})
	first, err := executor.Execute(action)
	if err != nil {
		t.Fatal(err)
	}
	output["nested"].([]any)[0].(map[string]any)["value"] = "caller mutation"
	first.Payload["result"].(map[string]any)["nested"].([]any)[0].(map[string]any)["value"] = "result mutation"
	second, err := executor.Execute(action)
	if err != nil {
		t.Fatal(err)
	}
	if calls != 1 || second.ID != first.ID || action.Payload["input"].([]any)[0] != "original" {
		t.Fatalf("calls=%d, request=%+v, events=%s/%s", calls, action, first.ID, second.ID)
	}
	if got := second.Payload["result"].(map[string]any)["nested"].([]any)[0].(map[string]any)["value"]; got != "original" {
		t.Fatalf("cached result was changed: %v", got)
	}
}

func TestExecutorRejectsUnsupportedDataWithoutUnsafeRetry(t *testing.T) {
	executor := NewExecutor()
	calls := 0
	if err := executor.Register("invalid", handlerFunc(func(domain.Action) (map[string]any, error) {
		calls++
		return map[string]any{"bad": func() {}}, nil
	})); err != nil {
		t.Fatal(err)
	}
	badInput := domain.NewAction("invalid", map[string]any{"bad": func() {}})
	if _, err := executor.Execute(badInput); err == nil || calls != 0 {
		t.Fatalf("invalid input executed: calls=%d, err=%v", calls, err)
	}
	action := domain.NewAction("invalid", nil)
	for range 2 {
		if _, err := executor.Execute(action); err == nil {
			t.Fatal("invalid output or unresolved action accepted")
		}
	}
	if calls != 1 || executor.statuses[action.ID] != domain.ActionStatusUnknown {
		t.Fatalf("unserializable result was retried: calls=%d status=%s", calls, executor.statuses[action.ID])
	}
}

func TestRunnerPanicRecordsFailureWithoutStateCommit(t *testing.T) {
	runtime := NewRuntime()
	agent := domain.NewAgentInstance("panic")
	agent.State["value"] = "original"
	if err := runtime.Register(&agent, functionRunner(func(ctx ExecutionContext) (ExecutionResult, error) {
		ctx.Agent.State["value"] = "changed"
		panic("failed runner")
	})); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Process(agent.ID, domain.NewEvent("test", nil)); err == nil {
		t.Fatal("panic was reported as success")
	}
	snapshot, err := runtime.Agent(agent.ID)
	if err != nil || snapshot.State["value"] != "original" || snapshot.StateVersion != 0 {
		t.Fatalf("failed state: %+v, %v", snapshot, err)
	}
	for _, attempt := range runtime.Attempts() {
		if attempt.Status != domain.AttemptStatusFailed || attempt.Error.Kind != domain.ErrorKindRuntime {
			t.Fatalf("panic attempt: %+v", attempt)
		}
	}
}
