package core

import (
	"agent-runtime/codec"
	"agent-runtime/domain"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestUnencodableMetadataNeverCommits(t *testing.T) {
	runtime := NewRuntime()
	bad := string([]byte{0xff})
	if err := runtime.RegisterDefinition(domain.DefinitionRef{ID: bad, Version: "1"}, resultRunner{}); err == nil {
		t.Fatal("unencodable Definition accepted")
	}
	invalidAgent := domain.NewAgentInstance(bad)
	if err := runtime.Register(&invalidAgent, resultRunner{}); err == nil {
		t.Fatal("unencodable Agent accepted")
	}
	agent := domain.NewAgentInstance("valid")
	if err := runtime.Register(&agent, functionRunner(func(ExecutionContext) (ExecutionResult, error) {
		return ExecutionResult{StateUpdate: map[string]any{"changed": true}, Actions: []domain.Action{domain.NewAction(bad, nil)}}, nil
	})); err != nil {
		t.Fatal(err)
	}
	event := domain.NewEvent("test", nil)
	event.CreatedAt = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := runtime.Submit(agent.ID, event); err == nil {
		t.Fatal("unencodable Event queued")
	}
	if _, err := runtime.Process(agent.ID, event); err == nil || len(runtime.Executions()) != 0 {
		t.Fatal("unencodable Event processed")
	}
	if _, err := runtime.Process(agent.ID, domain.NewEvent("test", nil)); err == nil {
		t.Fatal("unencodable Action committed")
	}
	snapshot, err := runtime.Agent(agent.ID)
	if err != nil || snapshot.StateVersion != 0 || len(snapshot.State) != 0 || len(runtime.Actions()) != 0 {
		t.Fatalf("partial state committed: %+v, %v", snapshot, err)
	}
}

func TestFailuresAlwaysRemainEncodable(t *testing.T) {
	runtime := NewRuntime()
	agent := domain.NewAgentInstance("failure")
	badError := errors.New(string([]byte{0xff}))
	if err := runtime.Register(&agent, functionRunner(func(ExecutionContext) (ExecutionResult, error) {
		return ExecutionResult{}, badError
	})); err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.Process(agent.ID, domain.NewEvent("test", nil)); !errors.Is(err, badError) {
		t.Fatalf("original error not returned: %v", err)
	}
	for _, execution := range runtime.Executions() {
		if _, err := codec.Encode(execution); err != nil || !strings.Contains(execution.Error, `\xff`) {
			t.Fatalf("unencodable error record: %+v, %v", execution, err)
		}
	}
	for _, attempt := range runtime.Attempts() {
		if _, err := codec.Encode(attempt); err != nil {
			t.Fatal(err)
		}
	}
	executor := NewExecutor()
	if err := executor.Register("fail", handlerFunc(func(domain.Action) (map[string]any, error) {
		return nil, badError
	})); err != nil {
		t.Fatal(err)
	}
	event, err := executor.Execute(domain.NewAction("fail", nil))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := codec.Encode(event); err != nil {
		t.Fatalf("unencodable Handler failure: %v", err)
	}
}
