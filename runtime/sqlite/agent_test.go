package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	"agent-runtime/core"
	"agent-runtime/domain"
)

func TestSessionCreateAndLoadAgent(t *testing.T) {
	backend, err := Open(
		filepath.Join(t.TempDir(), "test.db"),
	)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer backend.Close()

	session, err := backend.OpenSession(
		context.Background(),
	)
	if err != nil {
		t.Fatalf("open session: %v", err)
	}
	defer session.Close(context.Background())

	if _, err := session.Recover(
		context.Background(),
	); err != nil {
		t.Fatalf("recover: %v", err)
	}

	agent := domain.NewAgentInstance("counter")
	agent.Definition = domain.DefinitionRef{
		ID:      "counter",
		Version: "1",
	}
	agent.State["count"] = json.Number("9007199254740993")
	agent.State["enabled"] = true

	if err := session.CreateAgent(
		context.Background(),
		agent,
	); err != nil {
		t.Fatalf("create agent: %v", err)
	}

	loaded, err := session.LoadAgent(
		context.Background(),
		agent.ID,
	)
	if err != nil {
		t.Fatalf("load agent: %v", err)
	}

	if loaded.ID != agent.ID {
		t.Fatalf("id = %q, want %q", loaded.ID, agent.ID)
	}

	if loaded.Name != "counter" {
		t.Fatalf("name = %q, want counter", loaded.Name)
	}

	if loaded.Definition != agent.Definition {
		t.Fatalf(
			"definition = %#v, want %#v",
			loaded.Definition,
			agent.Definition,
		)
	}

	if loaded.StateVersion != 0 {
		t.Fatalf(
			"state version = %d, want 0",
			loaded.StateVersion,
		)
	}

	number, ok := loaded.State["count"].(json.Number)
	if !ok {
		t.Fatalf(
			"count type = %T, want json.Number",
			loaded.State["count"],
		)
	}

	if string(number) != "9007199254740993" {
		t.Fatalf(
			"count = %q, want 9007199254740993",
			number,
		)
	}

	if loaded.State["enabled"] != true {
		t.Fatalf(
			"enabled = %#v, want true",
			loaded.State["enabled"],
		)
	}
}

func TestSessionCreateAgentRejectsDuplicateID(t *testing.T) {
	backend, err := Open(
		filepath.Join(t.TempDir(), "test.db"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer backend.Close()

	session, err := backend.OpenSession(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close(context.Background())

	if _, err := session.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}

	agent := domain.NewAgentInstance("counter")
	agent.Definition = domain.DefinitionRef{
		ID:      "counter",
		Version: "1",
	}

	if err := session.CreateAgent(context.Background(), agent); err != nil {
		t.Fatal(err)
	}

	err = session.CreateAgent(context.Background(), agent)
	if !errors.Is(err, core.ErrStoreConflict) {
		t.Fatalf(
			"duplicate error = %v, want ErrStoreConflict",
			err,
		)
	}
}

func TestAgentSurvivesRestart(t *testing.T) {
	dbPath := filepath.Join(
		t.TempDir(),
		"runtime.db",
	)

	firstBackend, err := Open(dbPath)
	if err != nil {
		t.Fatalf("open first backend: %v", err)
	}

	firstSession, err := firstBackend.OpenSession(
		context.Background(),
	)
	if err != nil {
		firstBackend.Close()
		t.Fatalf("open first session: %v", err)
	}

	if _, err := firstSession.Recover(
		context.Background(),
	); err != nil {
		firstSession.Close(context.Background())
		firstBackend.Close()
		t.Fatalf("recover first session: %v", err)
	}

	agent := domain.NewAgentInstance("counter")
	agent.Definition = domain.DefinitionRef{
		ID:      "counter",
		Version: "1",
	}
	agent.State["count"] = json.Number(
		"9007199254740993",
	)

	if err := firstSession.CreateAgent(
		context.Background(),
		agent,
	); err != nil {
		t.Fatalf("create agent: %v", err)
	}

	if err := firstSession.Close(
		context.Background(),
	); err != nil {
		t.Fatalf("close first session: %v", err)
	}

	if err := firstBackend.Close(); err != nil {
		t.Fatalf("close first backend: %v", err)
	}

	secondBackend, err := Open(dbPath)
	if err != nil {
		t.Fatalf("open second backend: %v", err)
	}
	defer secondBackend.Close()

	secondSession, err := secondBackend.OpenSession(
		context.Background(),
	)
	if err != nil {
		t.Fatalf("open second session: %v", err)
	}
	defer secondSession.Close(context.Background())

	if _, err := secondSession.Recover(
		context.Background(),
	); err != nil {
		t.Fatalf("recover second session: %v", err)
	}

	loaded, err := secondSession.LoadAgent(
		context.Background(),
		agent.ID,
	)
	if err != nil {
		t.Fatalf("load recovered agent: %v", err)
	}

	if loaded.ID != agent.ID {
		t.Fatalf(
			"id = %q, want %q",
			loaded.ID,
			agent.ID,
		)
	}

	if loaded.Definition != agent.Definition {
		t.Fatalf(
			"definition = %#v, want %#v",
			loaded.Definition,
			agent.Definition,
		)
	}

	value, ok := loaded.State["count"].(json.Number)
	if !ok {
		t.Fatalf(
			"count type = %T, want json.Number",
			loaded.State["count"],
		)
	}

	if string(value) != "9007199254740993" {
		t.Fatalf(
			"count = %q, want 9007199254740993",
			value,
		)
	}
}
