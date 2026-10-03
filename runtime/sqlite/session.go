package sqlite

import (
	"context"
	"errors"
	"os"
	"strconv"
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
	if _, err := s.listAgents(ctx); err != nil {
		return core.RecoveryReport{}, err
	}
	var orphanEvents int

	err := s.backend.db.QueryRowContext(
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
		// 存在没有 Delivery 的孤立 Event，Recovery 不支持这种数据
		return core.RecoveryReport{}, ErrRecoveryUnsupported
	}
	if err := ctx.Err(); err != nil {
		return core.RecoveryReport{}, err
	}

	report := core.RecoveryReport{} // 本次回复结果

	now := time.Now().UTC()
	nowText := now.Format(time.RFC3339Nano)

	tx, err := s.backend.db.BeginTx(ctx, nil)
	if err != nil {
		return core.RecoveryReport{}, err
	}
	defer tx.Rollback()

	executionRows, err := tx.QueryContext(
		ctx,
		`SELECT d.agent_id, d.event_id, d.execution_id, a.id
		 FROM deliveries d
		 JOIN executions e ON e.id = d.execution_id
		 JOIN execution_attempts a
		   ON a.execution_id = e.id
		  AND a.number = e.attempt_count
		 WHERE d.status = ? AND e.status = ?
		 ORDER BY d.receive_seq`,
		string(domain.DeliveryStatusRunning),
		string(domain.ExecutionStatusRunning),
	)
	if err != nil {
		return core.RecoveryReport{}, err
	}
	defer executionRows.Close()

	executionFailure := domain.Failure{
		Kind:    domain.ErrorKindInterrupted,
		Message: "execution在恢复前仍处于运行状态",
	}
	executionFailureJSON, err := codec.Encode(executionFailure)
	if err != nil {
		return core.RecoveryReport{}, err
	}

	for executionRows.Next() {
		var (
			agentID     string
			eventID     string
			executionID string
			attemptID   string
		)

		if err := executionRows.Scan(
			&agentID,
			&eventID,
			&executionID,
			&attemptID,
		); err != nil {
			return core.RecoveryReport{}, err
		}

		_, err = tx.ExecContext(
			ctx,
			`UPDATE execution_attempts
			 SET status = ?, finished_at = ?, failure_json = ?
			 WHERE id = ? AND status = ?`,
			string(domain.AttemptStatusInterrupted),
			nowText,
			string(executionFailureJSON),
			attemptID,
			string(domain.AttemptStatusRunning),
		)
		if err != nil {
			return core.RecoveryReport{}, err
		}

		_, err = tx.ExecContext(
			ctx,
			`UPDATE executions
			 SET status = ?, started_at = NULL, finished_at = NULL, error = NULL, result_json = NULL
			 WHERE id = ? AND status = ?`,
			string(domain.ExecutionStatusPending),
			executionID,
			string(domain.ExecutionStatusRunning),
		)
		if err != nil {
			return core.RecoveryReport{}, err
		}

		_, err = tx.ExecContext(
			ctx,
			`UPDATE deliveries
			 SET status = ?
			 WHERE agent_id = ? AND event_id = ? AND status = ?`,
			string(domain.DeliveryStatusPending),
			agentID,
			eventID,
			string(domain.DeliveryStatusRunning),
		)
		if err != nil {
			return core.RecoveryReport{}, err
		}

		report.RequeuedDeliveries = append(
			report.RequeuedDeliveries,
			domain.DeliveryKey{
				AgentID: domain.ID(agentID),
				EventID: domain.ID(eventID),
			},
		)
	}

	if err := executionRows.Err(); err != nil {
		return core.RecoveryReport{}, err
	}

	rows, err := tx.QueryContext(
		ctx,
		`SELECT id, recovery_policy, max_attempts, attempt_count
		 FROM actions
		 WHERE status = ?
		 ORDER BY sequence`,
		string(domain.ActionStatusRunning),
	)
	if err != nil {
		return core.RecoveryReport{}, err
	}
	defer rows.Close()

	failure := domain.Failure{
		Kind:    domain.ErrorKindInterrupted,
		Message: "action在恢复前仍处于运行状态",
	}
	failureJSON, err := codec.Encode(failure)
	if err != nil {
		return core.RecoveryReport{}, err
	}

	for rows.Next() {
		var (
			actionID         string
			recoveryPolicy   string
			maxAttemptsText  string
			attemptCountText string
		)

		if err := rows.Scan(
			&actionID,
			&recoveryPolicy,
			&maxAttemptsText,
			&attemptCountText,
		); err != nil {
			return core.RecoveryReport{}, err
		}

		maxAttempts, err := strconv.ParseUint(maxAttemptsText, 10, 64)
		if err != nil {
			return core.RecoveryReport{}, err
		}

		attemptCount, err := strconv.ParseUint(attemptCountText, 10, 64)
		if err != nil {
			return core.RecoveryReport{}, err
		}

		_, err = tx.ExecContext(
			ctx,
			`UPDATE actions
			 SET status = ?, last_error_json = ?
			 WHERE id = ? AND status = ?`,
			string(domain.ActionStatusUnknown),
			string(failureJSON),
			actionID,
			string(domain.ActionStatusRunning),
		)
		if err != nil {
			return core.RecoveryReport{}, err
		}

		_, err = tx.ExecContext(
			ctx,
			`UPDATE action_attempts
			 SET status = ?, finished_at = ?, failure_json = ?
			 WHERE action_id = ? AND number = ? AND status = ?`,
			string(domain.ActionStatusUnknown),
			nowText,
			string(failureJSON),
			actionID,
			strconv.FormatUint(attemptCount, 10),
			string(domain.ActionStatusRunning),
		)
		if err != nil {
			return core.RecoveryReport{}, err
		}

		id := domain.ID(actionID)
		report.UnknownActions = append(report.UnknownActions, id)

		if domain.RecoveryPolicy(recoveryPolicy) == domain.RecoveryPolicySafeRetry &&
			attemptCount < maxAttempts {
			report.RetryableActions = append(report.RetryableActions, id)
		}
	}

	if err := rows.Err(); err != nil {
		return core.RecoveryReport{}, err
	}

	if err := tx.Commit(); err != nil {
		return core.RecoveryReport{}, err
	}

	s.report = report
	s.ready = true

	return cloneSQLiteRecoveryReport(report), nil
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
