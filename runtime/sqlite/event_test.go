package sqlite

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"

	"agent-runtime/domain"

	_ "modernc.org/sqlite"
)

func newTestSession(t *testing.T) (*Session, func()) {
	t.Helper()

	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "test.db")

	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}

	if err := RunMigrations(db); err != nil {
		db.Close()
		t.Fatal(err)
	}

	backend := NewBackend(db, path)
	session, err := backend.OpenSession(ctx)
	if err != nil {
		db.Close()
		t.Fatal(err)
	}

	if _, err := session.Recover(ctx); err != nil {
		db.Close()
		t.Fatal(err)
	}

	cleanup := func() {
		_ = session.Close(ctx)
		_ = backend.Close()
	}

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
