package sqlite

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"agent-runtime/domain"

	_ "modernc.org/sqlite"
)

func newTestSession(t *testing.T) (*Session, func()) {
	t.Helper()

	ctx := context.Background()
	dir, err := os.MkdirTemp("", "sqlite-test-*")
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "test.db")

	backend, err := Open(path)
	if err != nil {
		os.RemoveAll(dir)
		t.Fatal(err)
	}

	session, err := backend.OpenSession(ctx)
	if err != nil {
		_ = backend.Close()
		os.RemoveAll(dir)
		t.Fatal(err)
	}

	if _, err := session.Recover(ctx); err != nil {
		backend.Close()
		os.RemoveAll(dir)
		t.Fatal(err)
	}

	cleanup := func() {
		_ = session.Close(ctx)
		_ = backend.Close()
		_ = os.RemoveAll(dir)
	}

	t.Cleanup(cleanup)

	return session, cleanup
}

func TestReceiveEventNew(t *testing.T) {
	ctx := context.Background()
	session, cleanup := newTestSession(t)
	defer cleanup()

	agent := domain.AgentInstance{
		ID:     domain.ID("agent-1"),
		Name:   "test",
		Status: domain.AgentStatusActive,
		Definition: domain.DefinitionRef{
			ID:      "def-1",
			Version: "v1",
		},
		State: map[string]any{},
	}
	if err := session.CreateAgent(ctx, agent); err != nil {
		t.Fatal(err)
	}

	event := domain.Event{
		ID:      domain.ID("event-1"),
		Type:    "test.event",
		Payload: map[string]any{"a": 1},
	}

	got, err := session.ReceiveEvent(ctx, agent.ID, event)
	if err != nil {
		t.Fatal(err)
	}

	if got.Duplicate {
		t.Fatalf("expected Duplicate=false")
	}
	if got.Delivery.Key.AgentID != agent.ID {
		t.Fatalf("agent id mismatch")
	}
	if got.Delivery.Key.EventID != event.ID {
		t.Fatalf("event id mismatch")
	}
	if got.Delivery.Status != domain.DeliveryStatusPending {
		t.Fatalf("status mismatch: %s", got.Delivery.Status)
	}
	if got.Delivery.ExecutionID == "" {
		t.Fatalf("execution id is empty")
	}
}

func TestReceiveEventDuplicate(t *testing.T) {
	ctx := context.Background()
	session, _ := newTestSession(t)

	agent := domain.AgentInstance{
		ID:     domain.ID("agent-1"),
		Name:   "test",
		Status: domain.AgentStatusActive,
		Definition: domain.DefinitionRef{
			ID:      "def-1",
			Version: "v1",
		},
		State: map[string]any{},
	}
	if err := session.CreateAgent(ctx, agent); err != nil {
		t.Fatal(err)
	}

	event := domain.Event{
		ID:      domain.ID("event-1"),
		Type:    "test.event",
		Payload: map[string]any{"a": 1},
	}

	first, err := session.ReceiveEvent(ctx, agent.ID, event)
	if err != nil {
		t.Fatal(err)
	}
	if first.Duplicate {
		t.Fatalf("first receive should not be duplicate")
	}
	firstExecutionID := first.Delivery.ExecutionID

	// 再次投递同一 event
	second, err := session.ReceiveEvent(ctx, agent.ID, event)
	if err != nil {
		t.Fatal(err)
	}
	if !second.Duplicate {
		t.Fatalf("second receive should be duplicate")
	}
	if second.Delivery.ExecutionID != firstExecutionID {
		t.Fatalf("execution id changed: got %s want %s",
			second.Delivery.ExecutionID, firstExecutionID)
	}
	if second.Delivery.Key != first.Delivery.Key {
		t.Fatalf("delivery key changed")
	}
	if second.Delivery.Status != first.Delivery.Status {
		t.Fatalf("delivery status changed: got %s want %s",
			second.Delivery.Status, first.Delivery.Status)
	}
}
