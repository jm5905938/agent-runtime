package sqlite

import (
	"database/sql"
	"fmt"
	"sync"
)

type Backend struct {
	db    *sql.DB
	path  string
	mu    sync.Mutex
	owner bool
}

func NewBackend(db *sql.DB, path string) *Backend {
	return &Backend{
		db:   db,
		path: path,
	}
}

func (b *Backend) DB() *sql.DB {
	return b.db
}

func (b *Backend) Path() string {
	return b.path
}

func (b *Backend) Close() error {
	if b.db == nil {
		return nil
	}

	if err := b.db.Close(); err != nil {
		return fmt.Errorf("close sqlite backend: %w", err)
	}

	return nil
}
