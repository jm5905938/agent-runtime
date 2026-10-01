package main

import (
	"agent-runtime/cli"
	"agent-runtime/core"
	"context"
)

type backendHandle struct {
	Store core.RecoveryStore
	Close func() error
}

type backendOpener func(context.Context, string) (backendHandle, error)

func openCommandBackend(ctx context.Context, dataDir string) (backendHandle, error) {
	if err := ctx.Err(); err != nil {
		return backendHandle{}, err
	}
	return backendHandle{}, cli.ErrBackendUnavailable
}
