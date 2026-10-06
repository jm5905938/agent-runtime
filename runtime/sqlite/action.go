package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"sort"
	"strconv"
	"time"

	"agent-runtime/codec"
	"agent-runtime/core"
	"agent-runtime/domain"
)

func (s *Session) ClaimAction(
	ctx context.Context,
	actionID domain.ID,
) (*core.ActionClaim, error) {
	return s.claimAction(ctx, actionID, false)
}

func (s *Session) ClaimSubagentCancelAction(ctx context.Context, actionID domain.ID) (*core.ActionClaim, error) {
	return s.claimAction(ctx, actionID, true)
}

func (s *Session) claimAction(ctx context.Context, actionID domain.ID, cancel bool) (*core.ActionClaim, error) {
	if err := s.lock(ctx, true); err != nil {
		return nil, err
	}
	defer s.backend.unlock()

	tx, err := s.backend.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	var (
		requestJSON      string
		agentID          string
		handlerVersion   string
		recoveryPolicy   string
		maxAttemptsText  string
		status           string
		attemptCountText string
		resultEventID    string
	)

	err = tx.QueryRowContext(
		ctx,
		`SELECT request_json, agent_id, handler_version, recovery_policy, max_attempts, status, attempt_count, result_event_id
		 FROM actions
		 WHERE id = ?`,
		string(actionID),
	).Scan(
		&requestJSON,
		&agentID,
		&handlerVersion,
		&recoveryPolicy,
		&maxAttemptsText,
		&status,
		&attemptCountText,
		&resultEventID,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, core.ErrStoreNotFound
		}
		return nil, err
	}

	maxAttempts, err := strconv.ParseUint(maxAttemptsText, 10, 64)
	if err != nil {
		return nil, err
	}

	attemptCount, err := strconv.ParseUint(attemptCountText, 10, 64)
	if err != nil {
		return nil, err
	}

	task, err := loadChildSubagentTask(ctx, tx, domain.ID(agentID))
	if err != nil && !errors.Is(err, core.ErrStoreNotFound) {
		return nil, err
	}
	if cancel {
		if task == nil || !task.CancelRequested || task.Result != nil {
			return nil, core.ErrStoreConflict
		}
	} else if task != nil {
		if task.Result != nil {
			return nil, core.ErrAgentUnavailable
		}
		if task.CancelRequested {
			return nil, core.ErrActionNotReady
		}
	}

	currentStatus := domain.ActionStatus(status)
	policy := domain.RecoveryPolicy(recoveryPolicy)

	if currentStatus != domain.ActionStatusPending &&
		!(currentStatus == domain.ActionStatusUnknown &&
			policy == domain.RecoveryPolicySafeRetry) {
		return nil, core.ErrStoreConflict
	}

	if attemptCount >= maxAttempts {
		return nil, core.ErrStoreConflict
	}

	var request domain.Action
	if err := codec.Decode([]byte(requestJSON), &request); err != nil {
		return nil, err
	}

	attemptID, err := domain.NewID()
	if err != nil {
		return nil, err
	}

	attemptNumber := attemptCount + 1
	now := time.Now().UTC()
	nowText := now.Format(time.RFC3339Nano)

	_, err = tx.ExecContext(
		ctx,
		`UPDATE actions
		 SET status = ?, attempt_count = ?
		 WHERE id = ?`,
		string(domain.ActionStatusRunning),
		strconv.FormatUint(attemptNumber, 10),
		string(actionID),
	)
	if err != nil {
		return nil, err
	}

	_, err = tx.ExecContext(
		ctx,
		`INSERT INTO action_attempts (
			id, action_id, number, status, started_at
		) VALUES (?, ?, ?, ?, ?)`,
		string(attemptID),
		string(actionID),
		strconv.FormatUint(attemptNumber, 10),
		string(domain.ActionStatusRunning),
		nowText,
	)
	if err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	record := domain.ActionRecord{
		Request:        request,
		AgentID:        domain.ID(agentID),
		HandlerVersion: handlerVersion,
		RecoveryPolicy: policy,
		MaxAttempts:    maxAttempts,
		Status:         domain.ActionStatusRunning,
		AttemptCount:   attemptNumber,
		ResultEventID:  domain.ID(resultEventID),
	}

	return &core.ActionClaim{
		Token: core.ActionToken{
			ActionID:      actionID,
			AttemptNumber: attemptNumber,
		},
		Record: record,
	}, nil
}

func (s *Session) CompleteAction(
	ctx context.Context,
	completion core.ActionCompletion,
) (domain.ActionResult, error) {
	if err := s.lock(ctx, true); err != nil {
		return domain.ActionResult{}, err
	}
	defer s.backend.unlock()

	tx, err := s.backend.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.ActionResult{}, err
	}
	defer tx.Rollback()

	var (
		requestJSON      string
		agentID          string
		handlerVersion   string
		recoveryPolicy   string
		maxAttemptsText  string
		status           string
		attemptCountText string
		resultEventID    string
		resultJSON       sql.NullString
		lastErrorJSON    sql.NullString
	)

	err = tx.QueryRowContext(
		ctx,
		`SELECT request_json, agent_id, handler_version, recovery_policy, max_attempts, status, attempt_count, result_event_id, result_json, last_error_json
		 FROM actions
		 WHERE id = ?`,
		string(completion.Token.ActionID),
	).Scan(
		&requestJSON,
		&agentID,
		&handlerVersion,
		&recoveryPolicy,
		&maxAttemptsText,
		&status,
		&attemptCountText,
		&resultEventID,
		&resultJSON,
		&lastErrorJSON,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.ActionResult{}, core.ErrStoreNotFound
		}
		return domain.ActionResult{}, err
	}

	maxAttempts, err := strconv.ParseUint(maxAttemptsText, 10, 64)
	if err != nil {
		return domain.ActionResult{}, err
	}

	attemptCount, err := strconv.ParseUint(attemptCountText, 10, 64)
	if err != nil {
		return domain.ActionResult{}, err
	}

	var request domain.Action
	if err := codec.Decode([]byte(requestJSON), &request); err != nil {
		return domain.ActionResult{}, err
	}

	action := domain.ActionRecord{
		Request:        request,
		AgentID:        domain.ID(agentID),
		HandlerVersion: handlerVersion,
		RecoveryPolicy: domain.RecoveryPolicy(recoveryPolicy),
		MaxAttempts:    maxAttempts,
		Status:         domain.ActionStatus(status),
		AttemptCount:   attemptCount,
		ResultEventID:  domain.ID(resultEventID),
	}

	if resultJSON.Valid {
		var result domain.ActionResult
		if err := codec.Decode([]byte(resultJSON.String), &result); err != nil {
			return domain.ActionResult{}, err
		}
		action.Result = &result
	}

	if lastErrorJSON.Valid {
		var failure domain.Failure
		if err := codec.Decode([]byte(lastErrorJSON.String), &failure); err != nil {
			return domain.ActionResult{}, err
		}
		action.LastError = &failure
	}

	if err := core.ValidateActionCompletion(action, completion); err != nil {
		return domain.ActionResult{}, err
	}
	if action.AttemptCount != completion.Token.AttemptNumber {
		return domain.ActionResult{}, core.ErrStoreStaleClaim
	}

	if action.Result != nil {
		sameResult, err := core.SameJSONValue(*action.Result, completion.Result)
		if err != nil {
			return domain.ActionResult{}, err
		}
		if !sameResult {
			return domain.ActionResult{}, core.ErrStoreConflict
		}

		storedEvent, err := s.loadEventTx(ctx, tx, action.ResultEventID)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return domain.ActionResult{}, core.ErrStoreConflict
			}
			return domain.ActionResult{}, err
		}

		sameEvent, err := core.SameEventContent(storedEvent, completion.Event)
		if err != nil {
			return domain.ActionResult{}, err
		}
		if !sameEvent {
			return domain.ActionResult{}, core.ErrStoreConflict
		}

		return cloneSQLiteActionResult(*action.Result)
	}

	if domain.ActionStatus(status) != domain.ActionStatusRunning {
		return domain.ActionResult{}, core.ErrStoreStaleClaim
	}

	resultJSONBytes, err := codec.Encode(completion.Result)
	if err != nil {
		return domain.ActionResult{}, err
	}

	var failureJSON any
	if completion.Result.Error != nil {
		encodedFailure, err := codec.Encode(*completion.Result.Error)
		if err != nil {
			return domain.ActionResult{}, err
		}
		failureJSON = string(encodedFailure)
	}

	finishedAt := time.Now().UTC().Format(time.RFC3339Nano)

	updateResult, err := tx.ExecContext(
		ctx,
		`UPDATE actions
		 SET status = ?, result_json = ?, last_error_json = ?
		 WHERE id = ? AND status = ? AND attempt_count = ?`,
		string(completion.Result.Status),
		string(resultJSONBytes),
		failureJSON,
		string(completion.Token.ActionID),
		string(domain.ActionStatusRunning),
		strconv.FormatUint(completion.Token.AttemptNumber, 10),
	)
	if err != nil {
		return domain.ActionResult{}, err
	}

	affected, err := updateResult.RowsAffected()
	if err != nil {
		return domain.ActionResult{}, err
	}
	if affected != 1 {
		return domain.ActionResult{}, core.ErrStoreStaleClaim
	}

	attemptResult, err := tx.ExecContext(
		ctx,
		`UPDATE action_attempts
		 SET status = ?, finished_at = ?, failure_json = ?
		 WHERE action_id = ? AND number = ? AND status = ?`,
		string(completion.Result.Status),
		finishedAt,
		failureJSON,
		string(completion.Token.ActionID),
		strconv.FormatUint(completion.Token.AttemptNumber, 10),
		string(domain.ActionStatusRunning),
	)
	if err != nil {
		return domain.ActionResult{}, err
	}

	attemptAffected, err := attemptResult.RowsAffected()
	if err != nil {
		return domain.ActionResult{}, err
	}
	if attemptAffected != 1 {
		return domain.ActionResult{}, core.ErrStoreStaleClaim
	}

	// 失败回滚
	eventPayload, err := codec.Encode(completion.Event.Payload)
	if err != nil {
		return domain.ActionResult{}, err
	}

	var existingEventID string
	err = tx.QueryRowContext(
		ctx,
		`SELECT id FROM events WHERE id = ?`,
		string(completion.Event.ID),
	).Scan(&existingEventID)

	if err == nil {
		return domain.ActionResult{}, core.ErrStoreConflict
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return domain.ActionResult{}, err
	}

	_, err = tx.ExecContext(
		ctx,
		`INSERT INTO events (id, type, payload_json, created_at)
		 VALUES (?, ?, ?, ?)`,
		string(completion.Event.ID),
		completion.Event.Type,
		string(eventPayload),
		completion.Event.CreatedAt.UTC().Format(time.RFC3339Nano),
	)
	if err != nil {
		return domain.ActionResult{}, err
	}

	resultExecutionID, err := domain.NewID()
	if err != nil {
		return domain.ActionResult{}, err
	}

	var maxReceiveSeq sql.NullInt64
	err = tx.QueryRowContext(
		ctx,
		`SELECT MAX(receive_seq) FROM deliveries`,
	).Scan(&maxReceiveSeq)
	if err != nil {
		return domain.ActionResult{}, err
	}

	receiveSeq := int64(1)
	if maxReceiveSeq.Valid {
		receiveSeq = maxReceiveSeq.Int64 + 1
	}

	_, err = tx.ExecContext(
		ctx,
		`INSERT INTO deliveries (
			agent_id, event_id, execution_id, status, receive_seq
		) VALUES (?, ?, ?, ?, ?)`,
		agentID,
		string(completion.Event.ID),
		string(resultExecutionID),
		string(domain.DeliveryStatusPending),
		receiveSeq,
	)
	if err != nil {
		return domain.ActionResult{}, err
	}

	if err := tx.Commit(); err != nil {
		return domain.ActionResult{}, err
	}

	return cloneSQLiteActionResult(completion.Result)
}

func (s *Session) RecordActionUnknown(
	ctx context.Context,
	token core.ActionToken,
	failure domain.Failure,
) error {
	if err := s.lock(ctx, true); err != nil {
		return err
	}
	defer s.backend.unlock()

	tx, err := s.backend.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var (
		status       string
		attemptCount string
	)

	err = tx.QueryRowContext(
		ctx,
		`SELECT status, attempt_count
		 FROM actions
		 WHERE id = ?`,
		string(token.ActionID),
	).Scan(&status, &attemptCount)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return core.ErrStoreStaleClaim
		}
		return err
	}

	currentAttempt, err := strconv.ParseUint(attemptCount, 10, 64)
	if err != nil {
		return err
	}

	if domain.ActionStatus(status) != domain.ActionStatusRunning ||
		currentAttempt != token.AttemptNumber {
		return core.ErrStoreStaleClaim
	}

	var attemptStatus string
	err = tx.QueryRowContext(
		ctx,
		`SELECT status
		 FROM action_attempts
		 WHERE action_id = ? AND number = ?`,
		string(token.ActionID),
		strconv.FormatUint(token.AttemptNumber, 10),
	).Scan(&attemptStatus)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return core.ErrStoreStaleClaim
		}
		return err
	}

	if domain.ActionStatus(attemptStatus) != domain.ActionStatusRunning {
		return core.ErrStoreStaleClaim
	}

	if err := core.ValidateFailure(failure); err != nil {
		return core.ErrStoreConflict
	}

	failureJSON, err := codec.Encode(failure)
	if err != nil {
		return err
	}

	finishedAt := time.Now().UTC().Format(time.RFC3339Nano)

	result, err := tx.ExecContext(
		ctx,
		`UPDATE actions
		 SET status = ?, last_error_json = ?
		 WHERE id = ? AND status = ? AND attempt_count = ?`,
		string(domain.ActionStatusUnknown),
		string(failureJSON),
		string(token.ActionID),
		string(domain.ActionStatusRunning),
		strconv.FormatUint(token.AttemptNumber, 10),
	)
	if err != nil {
		return err
	}

	affected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return core.ErrStoreStaleClaim
	}

	result, err = tx.ExecContext(
		ctx,
		`UPDATE action_attempts
		 SET status = ?, finished_at = ?, failure_json = ?
		 WHERE action_id = ? AND number = ? AND status = ?`,
		string(domain.ActionStatusUnknown),
		finishedAt,
		string(failureJSON),
		string(token.ActionID),
		strconv.FormatUint(token.AttemptNumber, 10),
		string(domain.ActionStatusRunning),
	)
	if err != nil {
		return err
	}

	affected, err = result.RowsAffected()
	if err != nil {
		return err
	}
	if affected != 1 {
		return core.ErrStoreStaleClaim
	}

	return tx.Commit()
}

func (s *Session) LoadAction(
	ctx context.Context,
	actionID domain.ID,
) (*core.StoredAction, error) {
	if err := s.lock(ctx, false); err != nil {
		return nil, err
	}
	defer s.backend.unlock()

	var (
		requestJSON      string
		agentID          string
		handlerVersion   string
		recoveryPolicy   string
		maxAttemptsText  string
		status           string
		attemptCountText string
		resultEventID    string
		resultJSON       sql.NullString
		lastErrorJSON    sql.NullString
	)

	err := s.backend.db.QueryRowContext(
		ctx,
		`SELECT request_json, agent_id, handler_version, recovery_policy, max_attempts, status, attempt_count, result_event_id, result_json, last_error_json
		 FROM actions
		 WHERE id = ?`,
		string(actionID),
	).Scan(
		&requestJSON,
		&agentID,
		&handlerVersion,
		&recoveryPolicy,
		&maxAttemptsText,
		&status,
		&attemptCountText,
		&resultEventID,
		&resultJSON,
		&lastErrorJSON,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, core.ErrStoreNotFound
		}
		return nil, err
	}

	maxAttempts, err := strconv.ParseUint(maxAttemptsText, 10, 64)
	if err != nil {
		return nil, err
	}
	attemptCount, err := strconv.ParseUint(attemptCountText, 10, 64)
	if err != nil {
		return nil, err
	}

	var request domain.Action
	if err := codec.Decode([]byte(requestJSON), &request); err != nil {
		return nil, err
	}

	action := domain.ActionRecord{
		Request:        request,
		AgentID:        domain.ID(agentID),
		HandlerVersion: handlerVersion,
		RecoveryPolicy: domain.RecoveryPolicy(recoveryPolicy),
		MaxAttempts:    maxAttempts,
		Status:         domain.ActionStatus(status),
		AttemptCount:   attemptCount,
		ResultEventID:  domain.ID(resultEventID),
	}

	if resultJSON.Valid {
		var result domain.ActionResult
		if err := codec.Decode([]byte(resultJSON.String), &result); err != nil {
			return nil, err
		}
		action.Result = &result
	}

	if lastErrorJSON.Valid {
		var failure domain.Failure
		if err := codec.Decode([]byte(lastErrorJSON.String), &failure); err != nil {
			return nil, err
		}
		action.LastError = &failure
	}

	rows, err := s.backend.db.QueryContext(
		ctx,
		`SELECT id, number, status, started_at, finished_at, failure_json
		 FROM action_attempts
		 WHERE action_id = ?`,
		string(actionID),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	attempts := make([]domain.ActionAttempt, 0)
	for rows.Next() {
		var (
			attemptID      string
			numberText     string
			attemptStatus  string
			startedAtText  string
			finishedAtText sql.NullString
			failureJSON    sql.NullString
		)

		if err := rows.Scan(
			&attemptID,
			&numberText,
			&attemptStatus,
			&startedAtText,
			&finishedAtText,
			&failureJSON,
		); err != nil {
			return nil, err
		}

		number, err := strconv.ParseUint(numberText, 10, 64)
		if err != nil {
			return nil, err
		}

		startedAt, err := time.Parse(time.RFC3339Nano, startedAtText)
		if err != nil {
			return nil, err
		}

		var finishedAt *time.Time
		if finishedAtText.Valid {
			parsed, err := time.Parse(time.RFC3339Nano, finishedAtText.String)
			if err != nil {
				return nil, err
			}
			finishedAt = &parsed
		}

		var failure *domain.Failure
		if failureJSON.Valid {
			var decoded domain.Failure
			if err := codec.Decode([]byte(failureJSON.String), &decoded); err != nil {
				return nil, err
			}
			failure = &decoded
		}

		attempts = append(attempts, domain.ActionAttempt{
			ID:         domain.ID(attemptID),
			ActionID:   actionID,
			Number:     number,
			Status:     domain.ActionStatus(attemptStatus),
			StartedAt:  startedAt,
			FinishedAt: finishedAt,
			Error:      failure,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Slice(attempts, func(i, j int) bool { return attempts[i].Number < attempts[j].Number })

	return &core.StoredAction{
		Action:   action,
		Attempts: attempts,
	}, nil

}

func (s *Session) ListActions(
	ctx context.Context,
	statuses ...domain.ActionStatus,
) ([]domain.ActionRecord, error) {
	if err := s.lock(ctx, false); err != nil {
		return nil, err
	}
	defer s.backend.unlock()

	rows, err := s.backend.db.QueryContext(
		ctx,
		`SELECT request_json, agent_id, handler_version, recovery_policy, max_attempts, status, attempt_count, result_event_id, result_json, last_error_json
		 FROM actions
		 ORDER BY sequence`,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	filter := make(map[domain.ActionStatus]bool, len(statuses))
	for _, status := range statuses {
		filter[status] = true
	}

	actions := make([]domain.ActionRecord, 0)
	for rows.Next() {
		var (
			requestJSON      string
			agentID          string
			handlerVersion   string
			recoveryPolicy   string
			maxAttemptsText  string
			status           string
			attemptCountText string
			resultEventID    string
			resultJSON       sql.NullString
			lastErrorJSON    sql.NullString
		)

		if err := rows.Scan(
			&requestJSON,
			&agentID,
			&handlerVersion,
			&recoveryPolicy,
			&maxAttemptsText,
			&status,
			&attemptCountText,
			&resultEventID,
			&resultJSON,
			&lastErrorJSON,
		); err != nil {
			return nil, err
		}

		maxAttempts, err := strconv.ParseUint(maxAttemptsText, 10, 64)
		if err != nil {
			return nil, err
		}
		attemptCount, err := strconv.ParseUint(attemptCountText, 10, 64)
		if err != nil {
			return nil, err
		}

		var request domain.Action
		if err := codec.Decode([]byte(requestJSON), &request); err != nil {
			return nil, err
		}

		action := domain.ActionRecord{
			Request:        request,
			AgentID:        domain.ID(agentID),
			HandlerVersion: handlerVersion,
			RecoveryPolicy: domain.RecoveryPolicy(recoveryPolicy),
			MaxAttempts:    maxAttempts,
			Status:         domain.ActionStatus(status),
			AttemptCount:   attemptCount,
			ResultEventID:  domain.ID(resultEventID),
		}

		if resultJSON.Valid {
			var result domain.ActionResult
			if err := codec.Decode([]byte(resultJSON.String), &result); err != nil {
				return nil, err
			}
			action.Result = &result
		}

		if lastErrorJSON.Valid {
			var failure domain.Failure
			if err := codec.Decode([]byte(lastErrorJSON.String), &failure); err != nil {
				return nil, err
			}
			action.LastError = &failure
		}

		if len(filter) == 0 || filter[action.Status] {
			actions = append(actions, action)
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}
	return actions, nil
}

func cloneSQLiteActionResult(
	result domain.ActionResult,
) (domain.ActionResult, error) {
	encoded, err := codec.Encode(result)
	if err != nil {
		return domain.ActionResult{}, err
	}

	var cloned domain.ActionResult
	if err := codec.Decode(encoded, &cloned); err != nil {
		return domain.ActionResult{}, err
	}

	return cloned, nil
}
