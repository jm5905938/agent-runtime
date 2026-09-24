package sqlite

import (
	"context"
	"database/sql"
	"fmt"

	"agent-runtime/core"
)

//Backend目前只提供A批次的agent持久化，不实现完整RecoveryStore
type Backend struct {
	db     *sql.DB
	path   string
	gate   chan struct{}
	owner  *Session
	closed bool
}

func (b *Backend) Path() string { return b.path }

func (b *Backend) lock(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-b.gate:
	}
	if err := ctx.Err(); err != nil {
		b.unlock()
		return err
	}
	return nil
}

func (b *Backend) unlock() { b.gate <- struct{}{} }

func (b *Backend) Close() error {
	if err := b.lock(context.Background()); err != nil {
		return err
	}
	defer b.unlock()
	if b.closed {
		return nil
	}
	if b.owner != nil {
		return core.ErrStoreOwned
	}
	if err := b.db.Close(); err != nil {
		return fmt.Errorf("关闭sqlite后端: %w", err)
	}
	b.closed = true
	return nil
}
