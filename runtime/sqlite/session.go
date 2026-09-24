package sqlite

import (
	"context"
	"errors"
	"os"

	"agent-runtime/core"
)

var ErrRecoveryUnsupported = errors.New("sqlite A批次尚不支持event或execution恢复")

type Session struct {
	backend   *Backend
	ownership *os.File
	ready     bool
	closed    bool
	done      chan struct{}
}

// 仅返回A批次会话，完整StateStore交付前不接入core.OpenRuntime
func (b *Backend) OpenSession(ctx context.Context) (*Session, error) {
	if err := b.lock(ctx); err != nil {
		return nil, err
	}
	defer b.unlock()
	if b.closed {
		return nil, core.ErrStoreClosed
	}
	if b.owner != nil {
		return nil, core.ErrStoreOwned
	}
	ownership, err := acquireOwnership(b.path)
	if err != nil {
		return nil, err
	}
	//空闲期间其他进程可能升级数据库，领取后重新检查
	if err := runMigrations(ctx, b.db); err != nil {
		return nil, errors.Join(err, ownership.Close())
	}
	if err := ctx.Err(); err != nil {
		return nil, errors.Join(err, ownership.Close())
	}
	session := &Session{backend: b, ownership: ownership, done: make(chan struct{})}
	b.owner = session
	return session, nil
}

// A批次只校验已有agent，存在执行数据时拒绝冒充恢复成功
func (s *Session) Recover(ctx context.Context) (core.RecoveryReport, error) {
	if err := s.lock(ctx, false); err != nil {
		return core.RecoveryReport{}, err
	}
	defer s.backend.unlock()
	if s.ready {
		return core.RecoveryReport{}, nil
	}
	var count int
	if err := s.backend.db.QueryRowContext(ctx, `SELECT (SELECT COUNT(*) FROM events) + (SELECT COUNT(*) FROM executions)`).Scan(&count); err != nil {
		return core.RecoveryReport{}, err
	}
	if count != 0 {
		return core.RecoveryReport{}, ErrRecoveryUnsupported
	}
	if _, err := s.listAgents(ctx); err != nil {
		return core.RecoveryReport{}, err
	}
	if err := ctx.Err(); err != nil {
		return core.RecoveryReport{}, err
	}
	s.ready = true
	return core.RecoveryReport{}, nil
}

func (s *Session) Close(ctx context.Context) error {
	select {
	case <-s.done:
		return nil
	default:
	}
	if err := s.backend.lock(ctx); err != nil {
		select {
		case <-s.done:
			return nil
		default:
			return err
		}
	}
	defer s.backend.unlock()
	if s.closed {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	err := s.ownership.Close()
	s.closed = true
	close(s.done)
	s.backend.owner = nil
	return err
}

// 门禁覆盖整个数据库操作，Close不能在事务结束前释放文件锁
func (s *Session) lock(ctx context.Context, write bool) error {
	select {
	case <-s.done:
		return core.ErrStoreClosed
	default:
	}
	if err := s.backend.lock(ctx); err != nil {
		select {
		case <-s.done:
			return core.ErrStoreClosed
		default:
			return err
		}
	}
	var err error
	switch {
	case s.closed || s.backend.closed:
		err = core.ErrStoreClosed
	case write && !s.ready:
		err = core.ErrRecoveryRequired
	}
	if err != nil {
		s.backend.unlock()
	}
	return err
}
