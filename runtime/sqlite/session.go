package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"time"

	"agent-runtime/codec"
	"agent-runtime/core"
	"agent-runtime/domain"
)

var ErrRecoveryUnsupported = errors.New("sqlite A批次尚不支持event或execution恢复")

type Session struct {
	backend   *Backend
	ownership *os.File
	ready     bool
	closed    bool
	done      chan struct{}
	report    core.RecoveryReport
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
		return cloneSQLiteRecoveryReport(s.report), nil
	}
	// var count int
	// if err := s.backend.db.QueryRowContext(ctx, `SELECT (SELECT COUNT(*) FROM events) + (SELECT COUNT(*) FROM executions)`).Scan(&count); err != nil {
	// 	return core.RecoveryReport{}, err
	// }
	// if count != 0 {
	// 	return core.RecoveryReport{}, ErrRecoveryUnsupported
	// }
	tx, err := s.backend.db.BeginTx(ctx, nil)
	if err != nil {
		return core.RecoveryReport{}, err
	}
	defer tx.Rollback()

	records, err := loadRecoveryRecords(ctx, tx)
	if err != nil {
		return core.RecoveryReport{}, err
	}
	if err := core.ValidateRecoveryRecords(records); err != nil {
		return core.RecoveryReport{}, err
	}
	var orphanEvents int
	err = tx.QueryRowContext(
		ctx,
		`SELECT COUNT(*)
		 FROM events e
		 LEFT JOIN deliveries d ON d.event_id = e.id
		 WHERE d.event_id IS NULL`,
	).Scan(&orphanEvents)
	if err != nil {
		return core.RecoveryReport{}, err
	}
	if orphanEvents != 0 {
		// 存在没有Delivery的孤立Event，Recovery不支持这种数据
		return core.RecoveryReport{}, ErrRecoveryUnsupported
	}
	if err := ctx.Err(); err != nil {
		return core.RecoveryReport{}, err
	}

	report := core.RecoveryReport{} // 本次回复结果
	nowText := time.Now().UTC().Format(time.RFC3339Nano)
	executionFailure := domain.Failure{
		Kind:    domain.ErrorKindInterrupted,
		Message: "execution在恢复前仍处于运行状态",
	}
	executionFailureJSON, err := codec.Encode(executionFailure)
	if err != nil {
		return core.RecoveryReport{}, err
	}
	attempts := make(map[domain.ID]domain.Attempt)
	for _, attempt := range records.Attempts {
		attempts[attempt.ExecutionID] = attempt
	}
	for _, delivery := range records.Deliveries {
		if delivery.Status != domain.DeliveryStatusRunning {
			continue
		}
		attempt := attempts[delivery.ExecutionID]
		if err := recoveryUpdate(ctx, tx,
			`UPDATE execution_attempts SET status = ?, finished_at = ?, failure_json = ? WHERE id = ? AND status = ?`,
			string(domain.AttemptStatusInterrupted), nowText, string(executionFailureJSON), string(attempt.ID), string(domain.AttemptStatusRunning)); err != nil {
			return core.RecoveryReport{}, err
		}
		if err := recoveryUpdate(ctx, tx,
			`UPDATE executions SET status = ?, started_at = NULL, finished_at = NULL, error = NULL, result_json = NULL WHERE id = ? AND status = ?`,
			string(domain.ExecutionStatusPending), string(delivery.ExecutionID), string(domain.ExecutionStatusRunning)); err != nil {
			return core.RecoveryReport{}, err
		}
		if err := recoveryUpdate(ctx, tx,
			`UPDATE deliveries SET status = ? WHERE agent_id = ? AND event_id = ? AND status = ?`,
			string(domain.DeliveryStatusPending), string(delivery.Key.AgentID), string(delivery.Key.EventID), string(domain.DeliveryStatusRunning)); err != nil {
			return core.RecoveryReport{}, err
		}
		report.RequeuedDeliveries = append(report.RequeuedDeliveries, delivery.Key)
	}

	failure := domain.Failure{
		Kind:    domain.ErrorKindInterrupted,
		Message: "action在恢复前仍处于运行状态",
	}
	failureJSON, err := codec.Encode(failure)
	if err != nil {
		return core.RecoveryReport{}, err
	}
	actionAttempts := make(map[domain.ID]domain.ActionAttempt)
	for _, attempt := range records.ActionAttempts {
		actionAttempts[attempt.ActionID] = attempt
	}
	for _, action := range records.Actions {
		if action.Status == domain.ActionStatusRunning {
			if err := recoveryUpdate(ctx, tx,
				`UPDATE actions SET status = ?, last_error_json = ? WHERE id = ? AND status = ?`,
				string(domain.ActionStatusUnknown), string(failureJSON), string(action.Request.ID), string(domain.ActionStatusRunning)); err != nil {
				return core.RecoveryReport{}, err
			}
			attempt := actionAttempts[action.Request.ID]
			if err := recoveryUpdate(ctx, tx,
				`UPDATE action_attempts SET status = ?, finished_at = ?, failure_json = ? WHERE id = ? AND status = ?`,
				string(domain.ActionStatusUnknown), nowText, string(failureJSON), string(attempt.ID), string(domain.ActionStatusRunning)); err != nil {
				return core.RecoveryReport{}, err
			}
			action.Status = domain.ActionStatusUnknown
		}
		if action.Status == domain.ActionStatusUnknown {
			report.UnknownActions = append(report.UnknownActions, action.Request.ID)
			if action.RecoveryPolicy == domain.RecoveryPolicySafeRetry && action.AttemptCount < action.MaxAttempts {
				report.RetryableActions = append(report.RetryableActions, action.Request.ID)
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return core.RecoveryReport{}, err
	}

	s.report = report
	s.ready = true

	return cloneSQLiteRecoveryReport(report), nil
}

func recoveryUpdate(ctx context.Context, tx *sql.Tx, query string, args ...any) error {
	result, err := tx.ExecContext(ctx, query, args...)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count != 1 {
		return core.ErrStoreConflict
	}
	return nil
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

func cloneSQLiteRecoveryReport(
	report core.RecoveryReport,
) core.RecoveryReport {
	// 复制DeliveryKey
	deliveries := append(
		[]domain.DeliveryKey(nil),
		report.RequeuedDeliveries...,
	)

	// 复制Unknown Action ID
	unknown := append(
		[]domain.ID(nil),
		report.UnknownActions...,
	)

	// 复制Action ID
	retryable := append(
		[]domain.ID(nil),
		report.RetryableActions...,
	)

	return core.RecoveryReport{
		RequeuedDeliveries: deliveries,
		UnknownActions:     unknown,
		RetryableActions:   retryable,
	}
}
