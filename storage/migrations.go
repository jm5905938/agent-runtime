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

	migrations := []struct {
		version  int
		filename string
	}{
		{
			version:  1,
			filename: "migrations/001_initial.sql",
		},
		{
			version:  2,
			filename: "migrations/002_agents_state_json.sql",
		},
	}

	for _, migration := range migrations {
		var exists int

		err := db.QueryRow(`
			SELECT 1
			FROM schema_migrations
			WHERE version = ?
		`, migration.version).Scan(&exists)

		if err == nil {
			continue
		}

		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}

		sqlBytes, err := migrationFiles.ReadFile(
			migration.filename,
		)
		if err != nil {
			return err
		}

		tx, err := db.Begin()
		if err != nil {
			return err
		}

		if _, err := tx.Exec(string(sqlBytes)); err != nil {
			tx.Rollback()
			return err
		}

		if _, err := tx.Exec(`
			INSERT INTO schema_migrations (version)
			VALUES (?)
		`, migration.version); err != nil {
			tx.Rollback()
			return err
		}

		if err := tx.Commit(); err != nil {
			return err
		}
	}

	return nil
}
