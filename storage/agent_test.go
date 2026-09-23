package storage

import (
	"encoding/json"
	"path/filepath"
	"testing"

	"agent-runtime/runtime"
	"agent-runtime/runtime/domain"
)

func TestAgentStoreCreateAgent(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	db, err := runtime.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer db.Close()

	if err := RunMigrations(db); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	store := NewAgentStore(db)

	agent := domain.NewAgentInstance("counter")
	agent.State["count"] = 3

	if err := store.CreateAgent(agent); err != nil {
		t.Fatalf("create agent: %v", err)
	}

	var (
		name      string
		status    string
		stateJSON string
	)

	err = db.QueryRow(`
		SELECT name, status, state_json
		FROM agents
		WHERE id = ?
	`, string(agent.ID)).Scan(
		&name,
		&status,
		&stateJSON,
	)
	if err != nil {
		t.Fatalf("query agent: %v", err)
	}

	if name != "counter" {
		t.Fatalf("name = %q, want counter", name)
	}

	if status != string(domain.AgentStatusCreated) {
		t.Fatalf(
			"status = %q, want %q",
			status,
			domain.AgentStatusCreated,
		)
	}

	var state map[string]any
	if err := json.Unmarshal([]byte(stateJSON), &state); err != nil {
		t.Fatalf("decode state json: %v", err)
	}

	if state["count"] != float64(3) {
		t.Fatalf("state = %#v, want count=3", state)
	}
}

func TestAgentStoreGetAgent(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "test.db")

	db, err := runtime.OpenSQLite(dbPath)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer db.Close()

	if err := RunMigrations(db); err != nil {
		t.Fatalf("run migrations: %v", err)
	}

	store := NewAgentStore(db)

	original := domain.NewAgentInstance("counter")
	original.State["count"] = 3
	original.State["enabled"] = true

	if err := store.CreateAgent(original); err != nil {
		t.Fatalf("create agent: %v", err)
	}

	loaded, err := store.GetAgent(original.ID)
	if err != nil {
		t.Fatalf("get agent: %v", err)
	}

	if loaded.ID != original.ID {
		t.Fatalf("id = %q, want %q", loaded.ID, original.ID)
	}

	if loaded.Name != "counter" {
		t.Fatalf("name = %q, want counter", loaded.Name)
	}

	if loaded.Status != domain.AgentStatusCreated {
		t.Fatalf(
			"status = %q, want %q",
			loaded.Status,
			domain.AgentStatusCreated,
		)
	}

	if loaded.State["count"] != float64(3) {
		t.Fatalf("count = %#v, want 3", loaded.State["count"])
	}

	if loaded.State["enabled"] != true {
		t.Fatalf("enabled = %#v, want true", loaded.State["enabled"])
	}
}
