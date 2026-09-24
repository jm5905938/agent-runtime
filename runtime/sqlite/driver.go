package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	_ "modernc.org/sqlite"
)

const Name = "sqlite"

func databasePath(path string) (string, error) {
	if strings.TrimSpace(path) == "" || path == ":memory:" {
		return "", fmt.Errorf("sqlite需要文件路径")
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	resolved, err := filepath.EvalSymlinks(absolute)
	if err == nil {
		return resolved, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	if info, statErr := os.Lstat(absolute); statErr == nil && info.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("sqlite数据库路径不能是失效符号链接")
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(absolute))
	if err != nil {
		return "", err
	}
	return filepath.Join(parent, filepath.Base(absolute)), nil
}

func Open(path string) (backend *Backend, err error) {
	absolute, err := databasePath(path)
	if err != nil {
		return nil, fmt.Errorf("解析sqlite路径: %w", err)
	}
	ownership, err := acquireOwnership(absolute)
	if err != nil {
		return nil, err
	}
	var db *sql.DB
	defer func() {
		if err != nil && db != nil {
			err = errors.Join(err, db.Close())
			db = nil
		}
		err = errors.Join(err, ownership.Close())
		if err != nil {
			if db != nil {
				err = errors.Join(err, db.Close())
			}
			backend = nil
		}
	}()

	query := url.Values{}
	query.Add("_pragma", "foreign_keys(1)")
	query.Add("_pragma", "busy_timeout(5000)")
	query.Add("_pragma", "synchronous(NORMAL)")
	query.Set("_txlock", "immediate")
	dsn := (&url.URL{Scheme: "file", Path: absolute, RawQuery: query.Encode()}).String()
	db, err = sql.Open(Name, dsn)
	if err != nil {
		return nil, fmt.Errorf("打开sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("连接sqlite: %w", err)
	}
	if err := runMigrations(context.Background(), db); err != nil {
		return nil, fmt.Errorf("迁移sqlite: %w", err)
	}
	backend = &Backend{db: db, path: absolute, gate: make(chan struct{}, 1)}
	backend.unlock()
	return backend, nil
}
