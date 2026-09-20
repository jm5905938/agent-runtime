package core

import (
	"encoding/json"
	"errors"
	"reflect"
	"testing"
	"time"

	"agent-runtime/codec"
	"agent-runtime/domain"
)

func TestP1RestoreRoundTripPreservesLifecycleAndState(t *testing.T) {
	for _, status := range []domain.AgentStatus{
		domain.AgentStatusCreated, domain.AgentStatusActive, domain.AgentStatusPaused,
		domain.AgentStatusTerminating, domain.AgentStatusTerminated,
	} {
		t.Run(string(status), func(t *testing.T) {
			original := domain.NewAgentInstance("saved agent")
			original.Definition = domain.DefinitionRef{ID: "saved", Version: "2"}
			original.Status = status
			original.StateVersion = 17
			original.State = map[string]any{
				"large":  json.Number("9007199254740993"),
				"nested": map[string]any{"items": []any{"saved", nil}},
			}
			data, err := codec.Encode(original)
			if err != nil {
				t.Fatal(err)
			}
			var decoded domain.AgentInstance
			if err := codec.Decode(data, &decoded); err != nil {
				t.Fatal(err)
			}
			runtime := NewRuntime()
			if err := runtime.RestoreAgent(decoded); err != nil {
				t.Fatal(err)
			}
			snapshot := p1Agent(t, runtime, original.ID)
			if snapshot.ID != original.ID || snapshot.Name != original.Name ||
				snapshot.Definition != original.Definition || snapshot.Status != status ||
				snapshot.StateVersion != 17 || !reflect.DeepEqual(snapshot.State, original.State) {
				t.Fatalf("restored snapshot differs from original: %#v", snapshot)
			}
			decoded.State["nested"].(map[string]any)["items"].([]any)[0] = "caller change"
			if !reflect.DeepEqual(p1Agent(t, runtime, original.ID).State, original.State) {
				t.Fatal("restored state aliases the caller's decoded record")
			}
			if snapshot.BindingError == "" {
				t.Fatal("missing definition must remain visible on the restored record")
			}
		})
	}
}

func TestP1RestoreRequiresExactDefinitionBeforeExecution(t *testing.T) {
	for _, wrongVersion := range []bool{false, true} {
		name := "missing binding"
		if wrongVersion {
			name = "different version"
		}
		t.Run(name, func(t *testing.T) {
			runtime := NewRuntime()
			ref := domain.DefinitionRef{ID: "report", Version: "1"}
			wrongCalls, correctCalls := 0, 0
			if wrongVersion {
				if err := runtime.RegisterDefinition(domain.DefinitionRef{ID: ref.ID, Version: "2"},
					functionRunner(func(ExecutionContext) (ExecutionResult, error) {
						wrongCalls++
						return ExecutionResult{}, nil
					})); err != nil {
					t.Fatal(err)
				}
			}
			agent := domain.NewAgentInstance("restored")
			agent.Definition, agent.Status, agent.StateVersion = ref, domain.AgentStatusActive, 4
			agent.State["saved"] = "history"
			if err := runtime.RestoreAgent(agent); err != nil {
				t.Fatal(err)
			}
			if _, err := runtime.Process(agent.ID, domain.NewEvent("run", nil)); err == nil {
				t.Fatal("processing without the exact definition must fail")
			}
			before := p1Agent(t, runtime, agent.ID)
			if before.BindingError == "" || before.StateVersion != 4 || before.State["saved"] != "history" ||
				len(runtime.Executions()) != 0 || wrongCalls != 0 {
				t.Fatalf("unbound restore modified history or ran code: %#v", before)
			}
			if err := runtime.RegisterDefinition(ref, functionRunner(func(ctx ExecutionContext) (ExecutionResult, error) {
				correctCalls++
				if ctx.Agent.ID != agent.ID || ctx.Agent.StateVersion != 4 || ctx.Agent.State["saved"] != "history" {
					t.Fatalf("bound runner did not receive restored history: %#v", ctx.Agent)
				}
				return ExecutionResult{StateUpdate: map[string]any{"ran": true}}, nil
			})); err != nil {
				t.Fatal(err)
			}
			if _, err := runtime.Process(agent.ID, domain.NewEvent("run", nil)); err != nil {
				t.Fatal(err)
			}
			after := p1Agent(t, runtime, agent.ID)
			if after.BindingError != "" || after.StateVersion != 5 || after.State["ran"] != true ||
				correctCalls != 1 || wrongCalls != 0 {
				t.Fatalf("exact binding did not activate the existing record: %#v", after)
			}
		})
	}
}

func TestP1CreateAndDuplicateBindingsDoNotReplaceHistory(t *testing.T) {
	runtime := NewRuntime()
	ref := domain.DefinitionRef{ID: "create", Version: "1"}
	if _, err := runtime.CreateAgent("unbound", ref, nil); err == nil {
		t.Fatal("CreateAgent must require a registered definition")
	}
	if err := runtime.RegisterDefinition(ref, functionRunner(func(ExecutionContext) (ExecutionResult, error) {
		return ExecutionResult{StateUpdate: map[string]any{"runner": "original"}}, nil
	})); err != nil {
		t.Fatal(err)
	}
	created, err := runtime.CreateAgent("created", ref, map[string]any{"saved": "original"})
	if err != nil {
		t.Fatal(err)
	}
	if created.Definition != ref || created.Status != domain.AgentStatusActive || created.StateVersion != 0 {
		t.Fatalf("unexpected new agent: %#v", created)
	}
	if err := runtime.RegisterDefinition(ref, functionRunner(func(ExecutionContext) (ExecutionResult, error) {
		return ExecutionResult{StateUpdate: map[string]any{"runner": "replacement"}}, nil
	})); err == nil {
		t.Fatal("duplicate definition must not replace the original runner")
	}
	if err := runtime.RestoreAgent(domain.AgentInstance{
		ID: created.ID, Name: "replacement", Definition: ref, Status: domain.AgentStatusPaused,
		State: map[string]any{"saved": "replacement"}, StateVersion: 99,
	}); err == nil {
		t.Fatal("duplicate restore must not overwrite the existing instance")
	}
	unchanged := p1Agent(t, runtime, created.ID)
	if unchanged.Name != "created" || unchanged.Status != domain.AgentStatusActive ||
		unchanged.StateVersion != 0 || unchanged.State["saved"] != "original" {
		t.Fatalf("duplicate registration replaced history: %#v", unchanged)
	}
	if _, err := runtime.Process(created.ID, domain.NewEvent("run", nil)); err != nil {
		t.Fatal(err)
	}
	if p1Agent(t, runtime, created.ID).State["runner"] != "original" {
		t.Fatal("duplicate definition replaced the original runner")
	}
}

func TestP1NestedStateResultAndHistoryAreIsolated(t *testing.T) {
	runtime := NewRuntime()
	ref := domain.DefinitionRef{ID: "isolation", Version: "1"}
	initial := map[string]any{"input": p1Nested("initial"), "null_array": []any(nil)}
	output := map[string]any{"output": p1Nested("committed")}
	actionPayload := map[string]any{"effect": p1Nested("requested")}
	var context ExecutionContext
	if err := runtime.RegisterDefinition(ref, functionRunner(func(ctx ExecutionContext) (ExecutionResult, error) {
		context = ctx
		p1RequireNested(t, ctx.Agent.State["input"], "initial")
		p1MutateNested(ctx.Agent.State["input"], "runner mutation")
		p1MutateNested(ctx.Event.Payload["event"], "runner event mutation")
		return ExecutionResult{StateUpdate: output, Actions: []domain.Action{domain.NewAction("echo", actionPayload)}}, nil
	})); err != nil {
		t.Fatal(err)
	}
	agent, err := runtime.CreateAgent("isolation", ref, initial)
	if err != nil {
		t.Fatal(err)
	}
	p1MutateNested(initial["input"], "caller input mutation")
	p1MutateNested(agent.State["input"], "create result mutation")
	event := domain.NewEvent("run", map[string]any{"event": p1Nested("event input")})
	result, err := runtime.Process(agent.ID, event)
	if err != nil {
		t.Fatal(err)
	}
	p1RequireNested(t, event.Payload["event"], "event input")
	if context.ExecutionID == "" || context.AttemptID == "" || len(result.Actions) != 1 {
		t.Fatalf("execution context or result lacks identities: %#v / %#v", context, result)
	}
	executionID, actionID := context.ExecutionID, result.Actions[0].ID
	if result.Actions[0].ExecutionID == nil || *result.Actions[0].ExecutionID != executionID {
		t.Fatal("returned action must retain its originating execution")
	}
	p1MutateNested(output["output"], "retained runner result mutation")
	p1MutateNested(actionPayload["effect"], "retained action mutation")
	p1MutateNested(result.StateUpdate["output"], "returned result mutation")
	p1MutateNested(result.Actions[0].Payload["effect"], "returned action mutation")
	*result.Actions[0].ExecutionID = "changed"

	queried := p1Agent(t, runtime, agent.ID)
	p1MutateNested(queried.State["input"], "query mutation")
	p1MutateNested(queried.State["output"], "query mutation")
	saved := runtime.Executions()[executionID]
	if saved.Result == nil || saved.StartedAt == nil || saved.FinishedAt == nil {
		t.Fatalf("execution must save output and timestamps: %#v", saved)
	}
	p1MutateNested(saved.Result.StateUpdate["output"], "history mutation")
	p1MutateNested(saved.Result.Actions[0].Payload["effect"], "history action mutation")
	*saved.Result.Actions[0].ExecutionID = "changed"
	*saved.StartedAt, *saved.FinishedAt = time.Time{}, time.Time{}
	queriedAction := runtime.Actions()[actionID]
	p1MutateNested(queriedAction.Payload["effect"], "actions query mutation")
	*queriedAction.ExecutionID = "changed"
	queriedAttempt := runtime.Attempts()[context.AttemptID]
	if queriedAttempt.FinishedAt == nil {
		t.Fatal("successful attempt must finish")
	}
	*queriedAttempt.FinishedAt = time.Time{}

	actual := p1Agent(t, runtime, agent.ID)
	p1RequireNested(t, actual.State["input"], "initial")
	p1RequireNested(t, actual.State["output"], "committed")
	if actual.StateVersion != 1 || !reflect.DeepEqual(actual.State["null_array"], []any(nil)) {
		t.Fatalf("version or explicit null array changed: %#v", actual)
	}
	actualAction := runtime.Actions()[actionID]
	p1RequireNested(t, actualAction.Payload["effect"], "requested")
	if *actualAction.ExecutionID != executionID {
		t.Fatal("action query exposed its execution reference")
	}
	actualExecution := runtime.Executions()[executionID]
	p1RequireNested(t, actualExecution.Result.StateUpdate["output"], "committed")
	p1RequireNested(t, actualExecution.Result.Actions[0].Payload["effect"], "requested")
	if *actualExecution.Result.Actions[0].ExecutionID != executionID || actualExecution.StartedAt.IsZero() ||
		actualExecution.FinishedAt.IsZero() || actualExecution.AgentID != agent.ID || actualExecution.EventID != event.ID ||
		actualExecution.AttemptCount != 1 || actualExecution.Status != domain.ExecutionStatusCompleted {
		t.Fatalf("execution history lost identity or snapshot isolation: %#v", actualExecution)
	}
	attempt := runtime.Attempts()[context.AttemptID]
	if attempt.ID != context.AttemptID || attempt.ExecutionID != executionID || attempt.Number != 1 ||
		attempt.Status != domain.AttemptStatusSucceeded || attempt.FinishedAt.IsZero() {
		t.Fatalf("attempt is not linked to the execution: %#v", attempt)
	}
}

func TestP1StateUpdateUsesTopLevelReplacementAndExplicitNull(t *testing.T) {
	agent := domain.NewAgentInstance("merge")
	agent.StateVersion = 12
	agent.State = map[string]any{
		"keep": "old", "replace": "old", "null": "old",
		"nested": map[string]any{"keep_if_deep_merge": true, "value": "old"},
	}
	update := map[string]any{
		"replace": "new", "null": nil, "nested": map[string]any{"value": "new"},
	}
	if err := (StateManager{}).Apply(&agent, ExecutionResult{StateUpdate: update}); err != nil {
		t.Fatal(err)
	}
	expected := map[string]any{
		"keep": "old", "replace": "new", "null": nil, "nested": map[string]any{"value": "new"},
	}
	if !reflect.DeepEqual(agent.State, expected) || agent.StateVersion != 13 {
		t.Fatalf("wrong StateUpdate semantics: %#v at version %d", agent.State, agent.StateVersion)
	}
	update["nested"].(map[string]any)["value"] = "later mutation"
	if !reflect.DeepEqual(agent.State, expected) {
		t.Fatal("StateManager retained a mutable reference to its input")
	}
}

func TestP1InvalidOutputsCannotPartiallyCommit(t *testing.T) {
	for _, invalidKind := range []string{"function", "cycle"} {
		for _, location := range []string{"state", "action"} {
			t.Run(invalidKind+" in "+location, func(t *testing.T) {
				var invalid any = func() {}
				if invalidKind == "cycle" {
					cycle := map[string]any{}
					cycle["self"] = cycle
					invalid = cycle
				}
				runtime := NewRuntime()
				agent := domain.NewAgentInstance("invalid output")
				agent.StateVersion = 6
				agent.State = map[string]any{"original": p1Nested("saved")}
				if err := runtime.Register(&agent, functionRunner(func(ctx ExecutionContext) (ExecutionResult, error) {
					p1MutateNested(ctx.Agent.State["original"], "snapshot mutation")
					result := ExecutionResult{
						StateUpdate: map[string]any{"should_not_commit": true},
						Actions:     []domain.Action{domain.NewAction("echo", map[string]any{"valid": true})},
					}
					if location == "state" {
						result.StateUpdate["invalid"] = invalid
					} else {
						result.Actions[0].Payload["invalid"] = invalid
					}
					return result, nil
				})); err != nil {
					t.Fatal(err)
				}
				if _, err := runtime.Process(agent.ID, domain.NewEvent("run", nil)); err == nil {
					t.Fatal("invalid output must be rejected")
				}
				snapshot := p1Agent(t, runtime, agent.ID)
				if !reflect.DeepEqual(snapshot.State, agent.State) || snapshot.StateVersion != 6 ||
					len(runtime.Actions()) != 0 || len(runtime.pendingActions) != 0 {
					t.Fatalf("invalid output partially committed: %#v", snapshot)
				}
				if len(runtime.Executions()) != 1 || len(runtime.Attempts()) != 1 {
					t.Fatal("rejected output must retain the failed execution attempt")
				}
				for _, execution := range runtime.Executions() {
					if execution.Status != domain.ExecutionStatusFailed || execution.Result != nil {
						t.Fatalf("invalid output recorded as success: %#v", execution)
					}
				}
			})
		}
	}
}

func TestP1RunnerFailureKeepsNestedStateAndFailureHistoryIsolated(t *testing.T) {
	runtime := NewRuntime()
	agent := domain.NewAgentInstance("failed runner")
	agent.State = map[string]any{"original": p1Nested("saved")}
	agent.StateVersion = 3
	wantErr := errors.New("runner failed after changing its snapshot")
	if err := runtime.Register(&agent, functionRunner(func(ctx ExecutionContext) (ExecutionResult, error) {
		p1MutateNested(ctx.Agent.State["original"], "failed mutation")
		return ExecutionResult{StateUpdate: map[string]any{"should_not_commit": true}}, wantErr
	})); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Process(agent.ID, domain.NewEvent("run", nil)); !errors.Is(err, wantErr) {
		t.Fatalf("Process error = %v, want %v", err, wantErr)
	}
	snapshot := p1Agent(t, runtime, agent.ID)
	if !reflect.DeepEqual(snapshot.State, agent.State) || snapshot.StateVersion != 3 {
		t.Fatalf("failed runner changed saved state: %#v", snapshot)
	}
	for id, attempt := range runtime.Attempts() {
		if attempt.Status != domain.AttemptStatusFailed || attempt.Error == nil || attempt.Error.Message != wantErr.Error() {
			t.Fatalf("missing failure record: %#v", attempt)
		}
		attempt.Error.Message = "changed"
		if runtime.Attempts()[id].Error.Message != wantErr.Error() {
			t.Fatal("attempt query exposed its saved failure")
		}
		return
	}
	t.Fatal("failed runner did not retain an attempt")
}

func p1Agent(t *testing.T, runtime *Runtime, id domain.ID) AgentSnapshot {
	t.Helper()
	snapshot, err := runtime.Agent(id)
	if err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func p1Nested(value string) map[string]any {
	return map[string]any{"items": []any{map[string]any{"value": value}}}
}

func p1MutateNested(value any, replacement string) {
	value.(map[string]any)["items"].([]any)[0].(map[string]any)["value"] = replacement
}

func p1RequireNested(t *testing.T, value any, expected string) {
	t.Helper()
	if !reflect.DeepEqual(value, p1Nested(expected)) {
		t.Fatalf("nested value = %#v, want %#v", value, p1Nested(expected))
	}
}
