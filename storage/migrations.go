package storage

import (
	"database/sql"
	"embed"
	"errors"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

func ensureMigrationsTable(db *sql.DB) error {
	_, err := db.Exec(`
	CREATE TABLE IF NOT EXISTS schema_migrations (
		version INTEGER PRIMARY KEY,
		applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
	)`)

	return err
}

func RunMigrations(db *sql.DB) error {
	if err := ensureMigrationsTable(db); err != nil {
		return err
	}

	var version int
	err := db.QueryRow(`
	SELECT version
	FROM schema_migrations
	WHERE version = 1
	`).Scan(&version)

	if err == nil {
		return nil
	}

	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}

	sqlBytes, err := migrationFiles.ReadFile(
		"migrations/001_initial.sql",
	)
	if err != nil {
		return err
	}

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.Exec(string(sqlBytes)); err != nil {
		return err
	}

	if _, err := tx.Exec(`
	INSERT INTO schema_migrations (version)
	VALUES (1)
	`); err != nil {
		return err
	}

	return tx.Commit()
}
