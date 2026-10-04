package main

import (
	"agent-runtime/cli"
	"agent-runtime/core"
	"agent-runtime/domain"
	"agent-runtime/sqlite"
	"context"
	"errors"
	"fmt"
	"path/filepath"
)

type inputStore interface {
	SubmitMessage(context.Context, domain.ID, domain.ID, string) (core.ReceivedEvent, error)
}

type inputHandle struct {
	Store inputStore
	Close func() error
}

type inputOpener func(context.Context, string) (inputHandle, error)

func openCommandIngress(ctx context.Context, directory string) (inputHandle, error) {
	if err := ctx.Err(); err != nil {
		return inputHandle{}, err
	}
	store, err := sqlite.OpenIngress(filepath.Join(directory, "store.db"))
	if err != nil {
		return inputHandle{}, fmt.Errorf("打开输入通道: %w", err)
	}
	return inputHandle{Store: store, Close: store.Close}, nil
}

func submitCommand(ctx context.Context, options commandOptions, open inputOpener) (result cli.Result, err error) {
	input, err := open(ctx, options.dataDir)
	defer func() {
		if input.Close != nil {
			err = errors.Join(err, input.Close())
		}
	}()
	if err != nil {
		return cli.Result{}, err
	}
	if input.Store == nil {
		return cli.Result{}, cli.ErrBackendUnavailable
	}
	received, err := input.Store.SubmitMessage(ctx, options.request.AgentID, options.request.EventID, options.request.Message)
	if err != nil {
		return cli.Result{}, err
	}
	return cli.Result{
		Command: "submit", Agents: []core.AgentSnapshot{},
		Submission: &cli.Submission{Delivery: received.Delivery, Duplicate: received.Duplicate},
	}, nil
}
