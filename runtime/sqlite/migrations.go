package sqlite

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

func runMigrations(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)`); err != nil {
		return err
	}
	rows, err := tx.QueryContext(ctx, `SELECT version FROM schema_migrations ORDER BY version`)
	if err != nil {
		return err
	}
	var versions []int
	for rows.Next() {
		var version int
		if err := rows.Scan(&version); err != nil {
			rows.Close()
			return err
		}
		versions = append(versions, version)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	if len(versions) != 0 && (len(versions) != 1 || versions[0] != 1) {
		return fmt.Errorf("不支持的sqlite迁移版本: %v", versions)
	}
	if len(versions) == 0 {
		content, err := migrationFiles.ReadFile("migrations/001_initial.sql")
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, string(content)); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations(version) VALUES (1)`); err != nil {
			return err
		}
	}
	//旧分支也使用版本1，必须同时确认所需列存在
	for _, query := range []string{
		`SELECT id, name, definition_id, definition_version, status, state_json, state_version FROM agents LIMIT 0`,
		`SELECT id, type, payload_json, created_at FROM events LIMIT 0`,
		`SELECT id, agent_id, event_id, status, created_at, started_at, finished_at, error, attempt_count, result_json FROM executions LIMIT 0`,
	} {
		rows, err := tx.QueryContext(ctx, query)
		if err != nil {
			return fmt.Errorf("sqlite表结构不兼容: %w", err)
		}
		if err := rows.Close(); err != nil {
			return err
		}
	}
	return tx.Commit()
}
