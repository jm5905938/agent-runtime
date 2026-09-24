package sqlite

import (
	"context"

	"agent-runtime/core"
)

type Session struct {
	backend *Backend
	ready   bool
	closed  bool
}

func (b *Backend) OpenSession(
	ctx context.Context,
) (*Session, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.owner {
		return nil, core.ErrStoreOwned
	}

	b.owner = true

	return &Session{
		backend: b,
	}, nil
}

func (s *Session) Recover(
	ctx context.Context,
) (core.RecoveryReport, error) {
	if err := ctx.Err(); err != nil {
		return core.RecoveryReport{}, err
	}

	if s.closed {
		return core.RecoveryReport{}, core.ErrStoreClosed
	}

	s.ready = true

	return core.RecoveryReport{}, nil
}

func (s *Session) Close(
	ctx context.Context,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if s.closed {
		return nil
	}

	s.closed = true

	s.backend.mu.Lock()
	s.backend.owner = false
	s.backend.mu.Unlock()

	return nil
}

func (s *Session) guard(
	ctx context.Context,
	write bool,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if s.closed {
		return core.ErrStoreClosed
	}

	if write && !s.ready {
		return core.ErrRecoveryRequired
	}

	return nil
}
