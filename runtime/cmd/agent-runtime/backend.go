package main

import (
	"agent-runtime/cli"
	"agent-runtime/core"
	"agent-runtime/sqlite"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type backendHandle struct {
	Store core.RecoveryStore
	Close func() error
}

type backendOpener func(context.Context, string) (backendHandle, error)

type sqliteCommandStore struct {
	backend *sqlite.Backend
}

func (s sqliteCommandStore) OpenSession(ctx context.Context) (core.RecoverySession, error) {
	session, err := s.backend.OpenSession(ctx)
	if err != nil {
		return nil, err
	}
	return session, nil
}

func openCommandBackend(ctx context.Context, dataDir string) (backendHandle, error) {
	if err := ctx.Err(); err != nil {
		return backendHandle{}, err
	}
	if strings.TrimSpace(dataDir) == "" {
		return backendHandle{}, &cli.UsageError{Message: "请用--data-dir指定数据目录"}
	}
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		return backendHandle{}, fmt.Errorf("创建数据目录: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return backendHandle{}, err
	}
	backend, err := sqlite.Open(filepath.Join(dataDir, "store.db"))
	if err != nil {
		return backendHandle{}, fmt.Errorf("打开持久化后端: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return backendHandle{}, errors.Join(err, backend.Close())
	}
	return backendHandle{Store: sqliteCommandStore{backend: backend}, Close: backend.Close}, nil
}
