package core

import (
	"context"
	"errors"
	"fmt"
	"time"
)

//启动失败且清理未完成，可重试close
type RuntimeOpenError struct {
	cause   error
	session RecoverySession
}

func (e *RuntimeOpenError) Error() string                   { return e.cause.Error() }
func (e *RuntimeOpenError) Unwrap() error                   { return e.cause }
func (e *RuntimeOpenError) Close(ctx context.Context) error { return e.session.Close(ctx) }

//取得会话，恢复后再允许执行
func OpenRuntime(ctx context.Context, backend RecoveryStore) (*Runtime, error) {
	if backend == nil || isNilValue(backend) {
		return nil, fmt.Errorf("创建runtime: recovery store不能为空")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	session, err := backend.OpenSession(ctx)
	if err != nil {
		return nil, err
	}
	if session == nil || isNilValue(session) {
		return nil, fmt.Errorf("创建runtime: store返回空会话")
	}
	report, err := session.Recover(ctx)
	if err == nil {
		err = ctx.Err()
	}
	if err != nil {
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if closeErr := session.Close(cleanup); closeErr != nil {
			return nil, &RuntimeOpenError{
				cause: errors.Join(err, fmt.Errorf("关闭启动失败的会话: %w", closeErr)), session: session,
			}
		}
		return nil, err
	}
	runtime := newRuntime(session)
	runtime.session = session
	runtime.recovery = cloneRecoveryReport(report)
	return runtime, nil
}

func (r *Runtime) RecoveryReport() RecoveryReport {
	return cloneRecoveryReport(r.recovery)
}

func (r *Runtime) enter() (func(), error) {
	r.lifeMu.Lock()
	defer r.lifeMu.Unlock()
	if r.stopping {
		return nil, ErrStoreClosed
	}
	r.inflight++
	return r.leave, nil
}

func (r *Runtime) leave() {
	r.lifeMu.Lock()
	defer r.lifeMu.Unlock()
	r.inflight--
	if r.stopping && r.inflight == 0 {
		close(r.drained)
	}
}

func (r *Runtime) isStopping() bool {
	select {
	case <-r.stop:
		return true
	default:
		return false
	}
}

//先停止接收，再等在途调用退出。超时不释放所有权
func (r *Runtime) Close(ctx context.Context) error {
	r.lifeMu.Lock()
	if r.closed {
		r.lifeMu.Unlock()
		return nil
	}
	if !r.stopping {
		r.stopping = true
		close(r.stop)
		if r.inflight == 0 {
			close(r.drained)
		}
	}
	r.lifeMu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.drained:
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.closeGate:
	}
	defer func() { r.closeGate <- struct{}{} }()
	r.lifeMu.Lock()
	closed := r.closed
	r.lifeMu.Unlock()
	if closed {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if r.session != nil {
		if err := r.session.Close(ctx); err != nil {
			return err
		}
	}
	r.lifeMu.Lock()
	r.closed = true
	r.lifeMu.Unlock()
	return nil
}
