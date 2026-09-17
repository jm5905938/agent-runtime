package domain

import (
	"regexp"
	"testing"
	"time"
)

func TestConstructorsCreateDomainEntities(t *testing.T) {
	agent := NewAgentInstance("counter")
	event := NewEvent("counter.increment", map[string]any{"by": 1})
	execution := NewExecution(agent.ID, event.ID)
	action := NewAction("message.publish", map[string]any{"topic": "counts"})

	if agent.Status != AgentStatusCreated || agent.State == nil {
		t.Fatalf("new agent = %#v, want created agent with writable state", agent)
	}
	if execution.Status != ExecutionStatusPending {
		t.Fatalf("execution status = %q, want %q", execution.Status, ExecutionStatusPending)
	}
	if event.CreatedAt.Location() != time.UTC || execution.CreatedAt.Location() != time.UTC {
		t.Fatal("timestamps must be normalized to UTC")
	}
	if action.ExecutionID != nil {
		t.Fatal("new action must not be bound before its execution commits")
	}
}

func TestIDHasUUIDv4ShapeAndIsUnique(t *testing.T) {
	first, err := NewID()
	if err != nil {
		t.Fatalf("NewID() error = %v", err)
	}
	second, err := NewID()
	if err != nil {
		t.Fatalf("NewID() error = %v", err)
	}
	if first == second {
		t.Fatal("NewID() returned duplicate values")
	}
	if !regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`).MatchString(string(first)) {
		t.Fatalf("NewID() = %q, want UUIDv4", first)
	}
}

func TestBindExecutionCopiesIDValue(t *testing.T) {
	action := NewAction("email.send", nil)
	executionID := ID("execution-1")
	action.BindExecution(executionID)
	executionID = "changed"

	if action.ExecutionID == nil || *action.ExecutionID != "execution-1" {
		t.Fatalf("bound execution ID = %v, want execution-1", action.ExecutionID)
	}
}
