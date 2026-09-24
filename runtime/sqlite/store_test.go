package sqlite

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"agent-runtime/core"
)

func TestOpen(t *testing.T) {
	backend, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer backend.Close()

	var foreignKeys int
	if err := backend.db.QueryRow(
		"PRAGMA foreign_keys",
	).Scan(&foreignKeys); err != nil {
		t.Fatalf("read foreign_keys: %v", err)
	}

	if foreignKeys != 1 {
		t.Fatalf("foreign_keys = %d, want 1", foreignKeys)
	}

	var busyTimeout int
	if err := backend.db.QueryRow(
		"PRAGMA busy_timeout",
	).Scan(&busyTimeout); err != nil {
		t.Fatalf("read busy_timeout: %v", err)
	}

	if busyTimeout != 5000 {
		t.Fatalf("busy_timeout = %d, want 5000", busyTimeout)
	}
}

func TestOpenRunsMigrations(t *testing.T) {
	backend, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer backend.Close()

	var count int
	if err := backend.db.QueryRow(`
	SELECT COUNT(*) 
	FROM schema_migrations
	WHERE version = 1
	`).Scan(&count); err != nil {
		t.Fatalf("read schema_migrations: %v", err)
	}

	if count != 1 {
		t.Fatalf("schema_migrations count = %d, want 1", count)
	}

	var tableName string
	if err := backend.db.QueryRow(`
	SELECT name 
	FROM sqlite_master
	WHERE type = 'table' AND name = 'agents'
	`).Scan(&tableName); err != nil {
		t.Fatalf("query agents table: %v", err)
	}

	if tableName != "agents" {
		t.Fatalf("table = %q, want agents", tableName)
	}
}

func TestSessionOwnership(t *testing.T) {
	backend, err := Open(
		filepath.Join(t.TempDir(), "test.db"),
	)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer backend.Close()

	first, err := backend.OpenSession(
		context.Background(),
	)
	if err != nil {
		t.Fatalf("open first session: %v", err)
	}

	if _, err := backend.OpenSession(
		context.Background(),
	); !errors.Is(err, core.ErrStoreOwned) {
		t.Fatalf(
			"second session error = %v, want ErrStoreOwned",
			err,
		)
	}

	if _, err := first.Recover(
		context.Background(),
	); err != nil {
		t.Fatalf("recover: %v", err)
	}

	if err := first.Close(
		context.Background(),
	); err != nil {
		t.Fatalf("close: %v", err)
	}

	second, err := backend.OpenSession(
		context.Background(),
	)
	if err != nil {
		t.Fatalf("open second session: %v", err)
	}
	defer second.Close(context.Background())
}
