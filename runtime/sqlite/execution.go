package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"time"

	"agent-runtime/codec"
	"agent-runtime/core"
	"agent-runtime/domain"
)

func (s *Session) LoadExecution(
	ctx context.Context,
	executionID domain.ID,
) (*core.StoredExecution, error) {
	if err := s.lock(ctx, false); err != nil {
		return nil, err
	}
	defer s.backend.unlock()

	var (
		agentID        string
		eventID        string
		status         string
		createdAtText  string
		startedAtText  sql.NullString
		finishedAtText sql.NullString
		errorText      sql.NullString
		attemptCount   string
		resultJSON     sql.NullString
	)
	err := s.backend.db.QueryRowContext(
		ctx,
		`SELECT agent_id, event_id, status, created_at, started_at, finished_at, error, attempt_count, result_json
		 FROM executions WHERE id = ?`,
		string(executionID),
	).Scan(
		&agentID, &eventID, &status, &createdAtText,
		&startedAtText, &finishedAtText, &errorText,
		&attemptCount, &resultJSON,
	)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, core.ErrStoreNotFound
		}
		return nil, err
	}

	createdAt, err := time.Parse(time.RFC3339Nano, createdAtText)
	if err != nil {
		return nil, err
	}

	var startedAt *time.Time
	if startedAtText.Valid {
		t, err := time.Parse(time.RFC3339Nano, startedAtText.String)
		if err != nil {
			return nil, err
		}
		startedAt = &t
	}

	var finishedAt *time.Time
	if finishedAtText.Valid {
		t, err := time.Parse(time.RFC3339Nano, finishedAtText.String)
		if err != nil {
			return nil, err
		}
		finishedAt = &t
	}

	count, err := strconv.ParseUint(attemptCount, 10, 64)
	if err != nil {
		return nil, err
	}

	var result *domain.ExecutionResult
	if resultJSON.Valid {
		var r domain.ExecutionResult
		if err := codec.Decode([]byte(resultJSON.String), &r); err != nil {
			return nil, err
		}
		result = &r
	}

	execution := domain.Execution{
		ID:           executionID,
		AgentID:      domain.ID(agentID),
		EventID:      domain.ID(eventID),
		Status:       domain.ExecutionStatus(status),
		CreatedAt:    createdAt,
		StartedAt:    startedAt,
		FinishedAt:   finishedAt,
		Error:        errorText.String,
		AttemptCount: count,
		Result:       result,
	}

	attempts, err := s.loadAttempts(ctx, executionID)
	if err != nil {
		return nil, err
	}

	return &core.StoredExecution{
		Execution: execution,
		Attempts:  attempts,
	}, nil
}

func (s *Session) loadAttempts(
	ctx context.Context,
	executionID domain.ID,
) ([]domain.Attempt, error) {
	rows, err := s.backend.db.QueryContext(
		ctx,
		`SELECT id, number, status, started_at, finished_at, failure_json
		 FROM execution_attempts WHERE execution_id = ?`,
		string(executionID),
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []domain.Attempt
	for rows.Next() {
		var (
			id             string
			numberText     string
			status         string
			startedAtText  string
			finishedAtText sql.NullString
			failureJSON    sql.NullString
		)
		if err := rows.Scan(&id, &numberText, &status, &startedAtText, &finishedAtText, &failureJSON); err != nil {
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
			t, err := time.Parse(time.RFC3339Nano, finishedAtText.String)
			if err != nil {
				return nil, err
			}
			finishedAt = &t
		}

		var failure *domain.Failure
		if failureJSON.Valid {
			var f domain.Failure
			if err := codec.Decode([]byte(failureJSON.String), &f); err != nil {
				return nil, err
			}
			failure = &f
		}

		result = append(result, domain.Attempt{
			ID:          domain.ID(id),
			ExecutionID: executionID,
			Number:      number,
			Status:      domain.AttemptStatus(status),
			StartedAt:   startedAt,
			FinishedAt:  finishedAt,
			Error:       failure,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Slice(result, func(i, j int) bool { return result[i].Number < result[j].Number })

	return result, nil
}

func (s *Session) ClaimExecution(
	ctx context.Context,
	key domain.DeliveryKey,
) (*core.ExecutionClaim, error) {
	if err := s.lock(ctx, true); err != nil {
		return nil, err
	}
	defer s.backend.unlock()

	tx, err := s.backend.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	// 查delivery
	var (
		deliveryExecutionID string
		deliveryStatus      string
	)
	err = tx.QueryRowContext(
		ctx,
		`SELECT execution_id, status FROM deliveries WHERE agent_id = ? AND event_id = ?`,
		string(key.AgentID),
		string(key.EventID),
	).Scan(&deliveryExecutionID, &deliveryStatus)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, core.ErrStoreNotFound
		}
		return nil, err
	}

	if deliveryStatus == string(domain.DeliveryStatusFailed) {
		// delivery失败，显式调用RequeueDelivery
		return nil, core.ErrDeliveryFailed
	}

	if deliveryStatus != string(domain.DeliveryStatusPending) {
		return nil, core.ErrStoreConflict
	}

	// 查agent
	var (
		agentName         string
		agentDefID        string
		agentDefVersion   string
		agentStatus       string
		agentStateJSON    string
		agentStateVersion string
	)
	err = tx.QueryRowContext(
		ctx,
		`SELECT name, definition_id, definition_version, status, state_json, state_version
		 FROM agents WHERE id = ?`,
		string(key.AgentID),
	).Scan(
		&agentName, &agentDefID, &agentDefVersion,
		&agentStatus, &agentStateJSON, &agentStateVersion,
	)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, core.ErrStoreNotFound
		}
		return nil, err
	}

	if agentStatus != string(domain.AgentStatusActive) {
		return nil, core.ErrAgentUnavailable
	}

	task, err := loadChildSubagentTask(ctx, tx, key.AgentID)
	if err != nil && !errors.Is(err, core.ErrStoreNotFound) {
		return nil, err
	}
	if task != nil && task.CancelRequested && task.Result == nil {
		return nil, core.ErrDeliveryNotReady
	}

	// 检查running delivery
	var runningCount int
	err = tx.QueryRowContext(
		ctx,
		`SELECT COUNT(*) FROM deliveries
		 WHERE agent_id = ? AND status = ?`,
		string(key.AgentID),
		string(domain.DeliveryStatusRunning),
	).Scan(&runningCount)
	if err != nil {
		return nil, err
	}

	if runningCount > 0 {
		return nil, core.ErrExecutionInProgress
	}

	// 检查execution
	var existingExecutionID string
	err = tx.QueryRowContext(
		ctx,
		`SELECT id FROM executions WHERE id = ?`,
		deliveryExecutionID,
	).Scan(&existingExecutionID)

	var executionID domain.ID

	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return nil, err
		}

		// execution不存在
		executionID = domain.ID(deliveryExecutionID)

		now := time.Now().UTC().Format(time.RFC3339Nano)

		_, err = tx.ExecContext(
			ctx,
			`INSERT INTO executions (id, agent_id, event_id, status, created_at, attempt_count)
			 VALUES (?, ?, ?, ?, ?, ?)`,
			string(executionID),
			string(key.AgentID),
			string(key.EventID),
			string(domain.ExecutionStatusRunning),
			now,
			"0",
		)
		if err != nil {
			return nil, err
		}
	} else {
		// execution存在
		executionID = domain.ID(existingExecutionID)

		_, err = tx.ExecContext(
			ctx,
			`UPDATE executions SET status = ?, started_at = NULL, finished_at = NULL, error = NULL, result_json = NULL WHERE id = ?`,
			string(domain.ExecutionStatusRunning),
			string(executionID),
		)
		if err != nil {
			return nil, err
		}
	}

	// 读attempt_count
	var attemptCountText string
	err = tx.QueryRowContext(
		ctx,
		`SELECT attempt_count FROM executions WHERE id = ?`,
		string(executionID),
	).Scan(&attemptCountText)
	if err != nil {
		return nil, err
	}

	attemptCount, err := strconv.ParseUint(attemptCountText, 10, 64)
	if err != nil {
		return nil, err
	}
	if attemptCount == math.MaxUint64 {
		return nil, fmt.Errorf("execution %s尝试次数已耗尽: %w", executionID, core.ErrStoreConflict)
	}

	newCount := attemptCount + 1
	newCountText := strconv.FormatUint(newCount, 10)

	// 新建attempt
	attemptID, err := domain.NewID()
	if err != nil {
		return nil, err
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)

	_, err = tx.ExecContext(
		ctx,
		`INSERT INTO execution_attempts
		 (id, execution_id, number, status, started_at, expected_state_version)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		string(attemptID),
		string(executionID),
		newCountText,
		string(domain.AttemptStatusRunning),
		now,
		agentStateVersion,
	)
	if err != nil {
		return nil, err
	}

	// 更新
	_, err = tx.ExecContext(
		ctx,
		`UPDATE executions SET attempt_count = ?, started_at = ? WHERE id = ?`,
		newCountText,
		now,
		string(executionID),
	)
	if err != nil {
		return nil, err
	}

	// delivery转running
	_, err = tx.ExecContext(
		ctx,
		`UPDATE deliveries SET status = ? WHERE agent_id = ? AND event_id = ?`,
		string(domain.DeliveryStatusRunning),
		string(key.AgentID),
		string(key.EventID),
	)
	if err != nil {
		return nil, err
	}

	// 读agent_state
	var agentState map[string]any
	if err := codec.Decode([]byte(agentStateJSON), &agentState); err != nil {
		return nil, err
	}

	stateVersion, err := strconv.ParseUint(agentStateVersion, 10, 64)
	if err != nil {
		return nil, err
	}

	agent := domain.AgentInstance{
		ID:   key.AgentID,
		Name: agentName,
		Definition: domain.DefinitionRef{
			ID:      agentDefID,
			Version: agentDefVersion,
		},
		Status:       domain.AgentStatus(agentStatus),
		State:        agentState,
		StateVersion: stateVersion,
	}

	// 读event
	event, err := s.loadEventTx(ctx, tx, key.EventID)
	if err != nil {
		return nil, err
	}

	startedAt, err := time.Parse(time.RFC3339Nano, now)
	if err != nil {
		return nil, err
	}

	attempt := domain.Attempt{
		ID:          attemptID,
		ExecutionID: executionID,
		Number:      newCount,
		Status:      domain.AttemptStatusRunning,
		StartedAt:   startedAt,
	}

	token := core.ExecutionToken{
		Delivery:             key,
		ExecutionID:          executionID,
		AttemptID:            attemptID,
		ExpectedStateVersion: stateVersion,
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return &core.ExecutionClaim{
		Token:   token,
		Agent:   agent,
		Event:   event,
		Attempt: attempt,
	}, nil
}

// 读event
func (s *Session) loadEventTx(
	ctx context.Context,
	tx *sql.Tx,
	eventID domain.ID,
) (domain.Event, error) {
	var (
		eventType     string
		payloadJSON   string
		createdAtText string
	)
	err := tx.QueryRowContext(
		ctx,
		`SELECT type, payload_json, created_at FROM events WHERE id = ?`,
		string(eventID),
	).Scan(&eventType, &payloadJSON, &createdAtText)
	if err != nil {
		return domain.Event{}, err
	}

	var payload map[string]any
	if err := codec.Decode([]byte(payloadJSON), &payload); err != nil {
		return domain.Event{}, err
	}

	createdAt, err := time.Parse(time.RFC3339Nano, createdAtText)
	if err != nil {
		return domain.Event{}, err
	}

	return domain.Event{
		ID:        eventID,
		Type:      eventType,
		Payload:   payload,
		CreatedAt: createdAt,
	}, nil
}

// 校验，保存
func (s *Session) CommitExecution(
	ctx context.Context,
	commit core.ExecutionCommit,
) (domain.ExecutionResult, error) {
	if err := s.lock(ctx, true); err != nil {
		return domain.ExecutionResult{}, err
	}
	defer s.backend.unlock()

	tx, err := s.backend.db.BeginTx(ctx, nil)
	if err != nil {
		return domain.ExecutionResult{}, err
	}
	defer tx.Rollback()

	result, err := commitExecutionTx(ctx, tx, commit)
	if err != nil {
		return domain.ExecutionResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return domain.ExecutionResult{}, err
	}
	return result, nil
}

func commitExecutionTx(ctx context.Context, tx *sql.Tx, commit core.ExecutionCommit) (domain.ExecutionResult, error) {
	token := commit.Token

	// 校验delivery
	var (
		deliveryExecutionID string
		deliveryStatus      string
	)
	err := tx.QueryRowContext(
		ctx,
		`SELECT execution_id, status
		 FROM deliveries
		 WHERE agent_id = ? AND event_id = ?`,
		string(token.Delivery.AgentID),
		string(token.Delivery.EventID),
	).Scan(&deliveryExecutionID, &deliveryStatus)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.ExecutionResult{}, core.ErrStoreStaleClaim
		}
		return domain.ExecutionResult{}, err
	}

	if domain.ID(deliveryExecutionID) != token.ExecutionID {
		return domain.ExecutionResult{}, core.ErrStoreStaleClaim
	}

	if deliveryStatus != string(domain.DeliveryStatusRunning) {
		return domain.ExecutionResult{}, core.ErrStoreStaleClaim
	}

	// 校验execution,attempt
	var (
		executionStatus   string
		attemptStatus     string
		attemptNumber     string
		executionAttempts string
		expectedVersion   string
	)

	err = tx.QueryRowContext(
		ctx,
		`SELECT e.status, a.status, a.number, e.attempt_count, a.expected_state_version
		 FROM executions e
		 JOIN execution_attempts a ON a.execution_id = e.id
		 WHERE e.id = ?
		   AND a.id = ?
		   AND e.agent_id = ?
		   AND e.event_id = ?`,
		string(token.ExecutionID),
		string(token.AttemptID),
		string(token.Delivery.AgentID),
		string(token.Delivery.EventID),
	).Scan(
		&executionStatus,
		&attemptStatus,
		&attemptNumber,
		&executionAttempts,
		&expectedVersion,
	)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.ExecutionResult{}, core.ErrStoreStaleClaim
		}
		return domain.ExecutionResult{}, err
	}

	if executionStatus != string(domain.ExecutionStatusRunning) {
		return domain.ExecutionResult{}, core.ErrStoreStaleClaim
	}
	if attemptStatus != string(domain.AttemptStatusRunning) {
		return domain.ExecutionResult{}, core.ErrStoreStaleClaim
	}
	if attemptNumber != executionAttempts {
		return domain.ExecutionResult{}, core.ErrStoreStaleClaim
	}

	// 校验版本
	expectedVer, err := strconv.ParseUint(expectedVersion, 10, 64)
	if err != nil {
		return domain.ExecutionResult{}, err
	}
	if expectedVer != token.ExpectedStateVersion {
		return domain.ExecutionResult{}, core.ErrStoreStaleClaim
	}

	// 读agent_state
	var (
		agentStateJSON    string
		agentStateVersion string
	)
	err = tx.QueryRowContext(
		ctx,
		`SELECT state_json, state_version FROM agents WHERE id = ?`,
		string(token.Delivery.AgentID),
	).Scan(&agentStateJSON, &agentStateVersion)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return domain.ExecutionResult{}, core.ErrStoreNotFound
		}
		return domain.ExecutionResult{}, err
	}

	currentVersion, err := strconv.ParseUint(agentStateVersion, 10, 64)
	if err != nil {
		return domain.ExecutionResult{}, err
	}

	if currentVersion != token.ExpectedStateVersion {
		return domain.ExecutionResult{}, core.ErrStoreStaleClaim
	}
	if currentVersion == math.MaxUint64 {
		return domain.ExecutionResult{}, fmt.Errorf("提交状态: 状态版本已耗尽")
	}
	if _, err := codec.Encode(commit); err != nil {
		return domain.ExecutionResult{}, fmt.Errorf("提交execution记录: %w", err)
	}
	if err := validateNewActionsTx(ctx, tx, commit.Actions, domain.Execution{
		ID: token.ExecutionID, AgentID: token.Delivery.AgentID,
	}); err != nil {
		return domain.ExecutionResult{}, err
	}
	var task *domain.SubagentTask
	if commit.TaskResult != nil {
		if err := core.ValidateSubagentResult(commit.TaskResult); err != nil {
			return domain.ExecutionResult{}, err
		}
		if len(commit.Actions) != 0 {
			return domain.ExecutionResult{}, fmt.Errorf("subagent完成时不能新增action: %w", core.ErrStoreConflict)
		}
		task, err = loadChildSubagentTask(ctx, tx, token.Delivery.AgentID)
		if err != nil {
			if errors.Is(err, core.ErrStoreNotFound) {
				return domain.ExecutionResult{}, fmt.Errorf("subagent完成关联无效: %w", core.ErrStoreConflict)
			}
			return domain.ExecutionResult{}, err
		}
		if task.Result != nil {
			return domain.ExecutionResult{}, core.ErrStoreConflict
		}
		if err := subagentCanFinish(ctx, tx, task.ChildAgentID, token.ExecutionID); err != nil {
			return domain.ExecutionResult{}, err
		}
	}

	// 解码state
	var currentState map[string]any
	if err := codec.Decode([]byte(agentStateJSON), &currentState); err != nil {
		return domain.ExecutionResult{}, err
	}

	// 顶层键覆盖
	if currentState == nil {
		currentState = map[string]any{}
	}
	for k, v := range commit.StateUpdate {
		currentState[k] = v
	}

	// 新state
	newStateJSON, err := codec.Encode(currentState)
	if err != nil {
		return domain.ExecutionResult{}, err
	}

	newVersion := currentVersion + 1
	newVersionText := strconv.FormatUint(newVersion, 10)

	// 条件更新
	res, err := tx.ExecContext(
		ctx,
		`UPDATE agents SET state_json = ?, state_version = ?
		 WHERE id = ? AND state_version = ?`,
		string(newStateJSON),
		newVersionText,
		string(token.Delivery.AgentID),
		agentStateVersion,
	)
	if err != nil {
		return domain.ExecutionResult{}, err
	}

	affected, err := res.RowsAffected()
	if err != nil {
		return domain.ExecutionResult{}, err
	}
	if affected != 1 {
		return domain.ExecutionResult{}, core.ErrStoreStaleClaim
	}

	// 构造result
	var actionRequests []domain.Action
	if commit.Actions != nil {
		actionRequests = make([]domain.Action, 0, len(commit.Actions))
		for _, action := range commit.Actions {
			actionRequests = append(actionRequests, action.Request)
		}
	}
	result := domain.ExecutionResult{
		StateUpdate: commit.StateUpdate,
		Actions:     actionRequests,
		TaskResult:  commit.TaskResult,
	}

	resultJSON, err := codec.Encode(result)
	if err != nil {
		return domain.ExecutionResult{}, err
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)

	for _, action := range commit.Actions {
		requestJSON, err := codec.Encode(action.Request)
		if err != nil {
			return domain.ExecutionResult{}, err
		}

		_, err = tx.ExecContext(
			ctx,
			`INSERT INTO actions (id, execution_id, agent_id, request_json, handler_version, recovery_policy, idempotency_key, max_attempts, status, attempt_count, result_event_id)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			string(action.Request.ID),
			string(*action.Request.ExecutionID),
			string(action.AgentID),
			string(requestJSON),
			action.HandlerVersion,
			string(action.RecoveryPolicy),
			string(action.Request.ID),
			strconv.FormatUint(action.MaxAttempts, 10),
			string(domain.ActionStatusPending),
			"0",
			string(action.ResultEventID),
		)

		if err != nil {
			return domain.ExecutionResult{}, err
		}
	}

	// 更新execution
	_, err = tx.ExecContext(
		ctx,
		`UPDATE executions
		 SET status = ?, finished_at = ?, result_json = ?
		 WHERE id = ?`,
		string(domain.ExecutionStatusCompleted),
		now,
		string(resultJSON),
		string(token.ExecutionID),
	)
	if err != nil {
		return domain.ExecutionResult{}, err
	}

	// 更新attempt
	_, err = tx.ExecContext(
		ctx,
		`UPDATE execution_attempts
		 SET status = ?, finished_at = ?
		 WHERE id = ?`,
		string(domain.AttemptStatusSucceeded),
		now,
		string(token.AttemptID),
	)
	if err != nil {
		return domain.ExecutionResult{}, err
	}

	// 更新delivery
	_, err = tx.ExecContext(
		ctx,
		`UPDATE deliveries
		 SET status = ?
		 WHERE agent_id = ? AND event_id = ?`,
		string(domain.DeliveryStatusCompleted),
		string(token.Delivery.AgentID),
		string(token.Delivery.EventID),
	)
	if err != nil {
		return domain.ExecutionResult{}, err
	}

	if task != nil {
		if err := finishSubagentTaskTx(ctx, tx, task, token.ExecutionID, *commit.TaskResult); err != nil {
			return domain.ExecutionResult{}, err
		}
	}

	var storedResult domain.ExecutionResult
	if err := codec.Decode([]byte(resultJSON), &storedResult); err != nil {
		return domain.ExecutionResult{}, err
	}

	return storedResult, nil
}

func validateNewActionsTx(ctx context.Context, tx *sql.Tx, actions []domain.ActionRecord, execution domain.Execution) error {
	ids, eventIDs := make(map[domain.ID]bool), make(map[domain.ID]bool)
	for _, action := range actions {
		if err := core.ValidateNewActionRecord(action, execution); err != nil {
			return err
		}
		if ids[action.Request.ID] || eventIDs[action.ResultEventID] {
			return fmt.Errorf("action或结果event id重复: %w", core.ErrStoreConflict)
		}
		ids[action.Request.ID], eventIDs[action.ResultEventID] = true, true
		var actionExists, eventExists, eventReserved bool
		if err := tx.QueryRowContext(ctx,
			`SELECT EXISTS (SELECT 1 FROM actions WHERE id = ?),
			 EXISTS (SELECT 1 FROM events WHERE id = ?),
			 EXISTS (SELECT 1 FROM actions WHERE result_event_id = ?)`,
			string(action.Request.ID), string(action.ResultEventID), string(action.ResultEventID),
		).Scan(&actionExists, &eventExists, &eventReserved); err != nil {
			return err
		}
		if actionExists {
			return fmt.Errorf("action %s已存在: %w", action.Request.ID, core.ErrStoreConflict)
		}
		if eventExists {
			return fmt.Errorf("action结果event %s已存在: %w", action.ResultEventID, core.ErrStoreConflict)
		}
		if eventReserved {
			return fmt.Errorf("action结果event %s已保留: %w", action.ResultEventID, core.ErrStoreConflict)
		}
	}
	return nil
}

func (s *Session) FailExecution(
	ctx context.Context,
	failure core.ExecutionFailure,
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

	if err := failExecutionTx(ctx, tx, failure); err != nil {
		return err
	}
	return tx.Commit()
}

func failExecutionTx(ctx context.Context, tx *sql.Tx, failure core.ExecutionFailure) error {
	token := failure.Token

	// 校验delivery
	var (
		deliveryExecutionID string
		deliveryStatus      string
	)

	err := tx.QueryRowContext(
		ctx,
		`SELECT execution_id, status
		 FROM deliveries
		 WHERE agent_id = ? AND event_id = ?`,
		string(token.Delivery.AgentID),
		string(token.Delivery.EventID),
	).Scan(&deliveryExecutionID, &deliveryStatus)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return core.ErrStoreStaleClaim
		}
		return err
	}

	if deliveryStatus != string(domain.DeliveryStatusRunning) {
		return core.ErrStoreStaleClaim
	}
	if domain.ID(deliveryExecutionID) != token.ExecutionID {
		return core.ErrStoreStaleClaim
	}

	// 校验execution, attempt
	var (
		executionStatus   string
		attemptStatus     string
		attemptNumber     string
		executionAttempts string
		expectedVersion   string
	)

	err = tx.QueryRowContext(
		ctx,
		`SELECT e.status, a.status, a.number, e.attempt_count, a.expected_state_version
		 FROM executions e
		 JOIN execution_attempts a ON a.execution_id = e.id
		 WHERE e.id = ?
		   AND a.id = ?
		   AND e.agent_id = ?
		   AND e.event_id = ?`,
		string(token.ExecutionID),
		string(token.AttemptID),
		string(token.Delivery.AgentID),
		string(token.Delivery.EventID),
	).Scan(
		&executionStatus,
		&attemptStatus,
		&attemptNumber,
		&executionAttempts,
		&expectedVersion,
	)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return core.ErrStoreStaleClaim
		}
		return err
	}

	if executionStatus != string(domain.ExecutionStatusRunning) {
		return core.ErrStoreStaleClaim
	}
	if attemptStatus != string(domain.AttemptStatusRunning) {
		return core.ErrStoreStaleClaim
	}
	if attemptNumber != executionAttempts {
		return core.ErrStoreStaleClaim
	}

	expectedVer, err := strconv.ParseUint(expectedVersion, 10, 64)
	if err != nil {
		return err
	}
	if expectedVer != token.ExpectedStateVersion {
		return core.ErrStoreStaleClaim
	}
	var currentVersionText string
	if err := tx.QueryRowContext(ctx, `SELECT state_version FROM agents WHERE id = ?`,
		string(token.Delivery.AgentID)).Scan(&currentVersionText); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return core.ErrStoreStaleClaim
		}
		return err
	}
	currentVersion, err := strconv.ParseUint(currentVersionText, 10, 64)
	if err != nil {
		return err
	}
	if currentVersion != token.ExpectedStateVersion {
		return core.ErrStoreStaleClaim
	}
	if err := core.ValidateFailure(failure.Failure); err != nil {
		return err
	}
	var task *domain.SubagentTask
	if !failure.Interrupted {
		task, err = loadChildSubagentTask(ctx, tx, token.Delivery.AgentID)
		if err != nil && !errors.Is(err, core.ErrStoreNotFound) {
			return err
		}
		if task != nil {
			if err := subagentCanFinish(ctx, tx, task.ChildAgentID, token.ExecutionID); err != nil {
				return err
			}
		}
	}

	now := time.Now().UTC().Format(time.RFC3339Nano)

	attemptStatusFinal := string(domain.AttemptStatusFailed)
	if failure.Interrupted {
		attemptStatusFinal = string(domain.AttemptStatusInterrupted)
	}

	failureJSON, err := codec.Encode(failure.Failure)
	if err != nil {
		return err
	}

	// 更新attempt
	_, err = tx.ExecContext(
		ctx,
		`UPDATE execution_attempts
		 SET status = ?, finished_at = ?, failure_json = ?
		 WHERE id = ?`,
		attemptStatusFinal,
		now,
		string(failureJSON),
		string(token.AttemptID),
	)
	if err != nil {
		return err
	}

	// 更新execution
	_, err = tx.ExecContext(
		ctx,
		`UPDATE executions
		 SET status = ?, finished_at = ?, error = ?
		 WHERE id = ?`,
		string(domain.ExecutionStatusFailed),
		now,
		failure.Failure.Message,
		string(token.ExecutionID),
	)
	if err != nil {
		return err
	}

	// 更新delivery
	_, err = tx.ExecContext(
		ctx,
		`UPDATE deliveries
		 SET status = ?
		 WHERE agent_id = ? AND event_id = ?`,
		string(domain.DeliveryStatusFailed),
		string(token.Delivery.AgentID),
		string(token.Delivery.EventID),
	)
	if err != nil {
		return err
	}

	if task != nil {
		result := domain.SubagentResult{Status: domain.SubagentStatusFailed, Output: map[string]any{}, Error: &failure.Failure}
		if err := finishSubagentTaskTx(ctx, tx, task, token.ExecutionID, result); err != nil {
			return err
		}
	}

	return nil
}

func (s *Session) RequeueDelivery(
	ctx context.Context,
	key domain.DeliveryKey,
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

	var executionID string
	var status string
	err = tx.QueryRowContext(
		ctx,
		`SELECT execution_id, status FROM deliveries WHERE agent_id = ? AND event_id = ?`,
		string(key.AgentID),
		string(key.EventID),
	).Scan(&executionID, &status)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return core.ErrStoreNotFound
		}
		return err
	}

	if status != string(domain.DeliveryStatusFailed) {
		return core.ErrStoreConflict
	}

	task, err := loadChildSubagentTask(ctx, tx, key.AgentID)
	if err != nil && !errors.Is(err, core.ErrStoreNotFound) {
		return err
	}
	if task != nil && task.Result != nil {
		return core.ErrStoreConflict
	}

	executionResult, err := tx.ExecContext(
		ctx,
		`UPDATE executions
		 SET status = ?, started_at = NULL, finished_at = NULL, error = NULL, result_json = NULL
		 WHERE id = ? AND status = ?`,
		string(domain.ExecutionStatusPending),
		executionID,
		string(domain.ExecutionStatusFailed),
	)
	if err != nil {
		return err
	}

	executionAffected, err := executionResult.RowsAffected()
	if err != nil {
		return err
	}
	if executionAffected != 1 {
		return core.ErrStoreConflict
	}

	deliveryResult, err := tx.ExecContext(
		ctx,
		`UPDATE deliveries SET status = ? WHERE agent_id = ? AND event_id = ? AND status = ?`,
		string(domain.DeliveryStatusPending),
		string(key.AgentID),
		string(key.EventID),
		string(domain.DeliveryStatusFailed),
	)
	if err != nil {
		return err
	}

	deliveryAffected, err := deliveryResult.RowsAffected()
	if err != nil {
		return err
	}
	if deliveryAffected != 1 {
		return core.ErrStoreConflict
	}

	return tx.Commit()
}
