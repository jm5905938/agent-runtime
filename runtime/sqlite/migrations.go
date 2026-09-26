package sqlite

import (
	"database/sql"
	"embed"

	"fmt"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

func RunMigrations(db *sql.DB) error {
	if _, err := db.Exec(`
          CREATE TABLE IF NOT EXISTS schema_migrations (
              version    INTEGER PRIMARY KEY,
              applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
          )
      `); err != nil {
		return fmt.Errorf("create schema migrations: %w", err)
	}

	var version int
	err := db.QueryRow(`
          SELECT COALESCE(MAX(version), 0)
          FROM schema_migrations
      `).Scan(&version)
	if err != nil {
		return fmt.Errorf("read schema version: %w", err)
	}

	if version < 1 {
		content, err := migrationFiles.ReadFile(
			"migrations/001_initial.sql",
		)
		if err != nil {
			return fmt.Errorf("read migration 1: %w", err)
		}

		tx, err := db.Begin()
		if err != nil {
			return fmt.Errorf("begin migration: %w", err)
		}
		defer tx.Rollback()

		if _, err := tx.Exec(string(content)); err != nil {
			return fmt.Errorf("apply migration 1: %w", err)
		}

		if _, err := tx.Exec(`
              INSERT INTO schema_migrations(version)
              VALUES (1)
          `); err != nil {
			return fmt.Errorf("record migration 1: %w", err)
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit migration 1: %w", err)
		}
	}

	if version < 2 {
		content, err := migrationFiles.ReadFile(
			"migrations/002_batch_b.sql",
		)
		if err != nil {
			return fmt.Errorf("read migration 2: %w", err)
		}

		tx, err := db.Begin()
		if err != nil {
			return fmt.Errorf("begin migration 2: %w", err)
		}
		defer tx.Rollback()

		if _, err := tx.Exec(string(content)); err != nil {
			return fmt.Errorf("apply migration 2: %w", err)
		}

		if _, err := tx.Exec(`
				INSERT INTO schema_migrations(version)
				VALUES (2)
			`); err != nil {
			return fmt.Errorf("record migration 2: %w", err)
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit migration 2: %w", err)
		}
	}

	return nil
}
