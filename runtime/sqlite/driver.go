package sqlite

import (
	"database/sql"
	"fmt"
	"net/url"
	"path/filepath"

	_ "modernc.org/sqlite"
)

const Name = "sqlite"

func Open(path string) (*Backend, error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("get absolute path: %w", err)
	}

	dsn := "file:" + url.PathEscape(absPath) +
		"?_pragma=foreign_keys(1)" +
		"&_pragma=busy_timeout(5000)" +
		"&_pragma=synchronous(NORMAL)" +
		"&_txlock=immediate"

	db, err := sql.Open(Name, dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite database: %w", err)
	}

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("ping sqlite database: %w", err)
	}
	if err := RunMigrations(db); err != nil {
		return nil, fmt.Errorf("run migrations: %w", err)
	}

	return NewBackend(db, absPath), nil
}
