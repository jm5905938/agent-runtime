package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"time"

	"agent-runtime/codec"
	"agent-runtime/core"
	"agent-runtime/domain"
)

// 在同一事务中读完整表，缺失或孤立记录不能通过关联查询被过滤
func loadRecoveryRecords(ctx context.Context, tx *sql.Tx) (core.RecoveryRecords, error) {
	records := core.RecoveryRecords{ClaimVersions: make(map[domain.ID]uint64)}
	queries := []struct {
		query string
		read  func(*sql.Rows) error
	}{
		{`SELECT id, name, definition_id, definition_version, status, state_json, state_version FROM agents ORDER BY id`, func(rows *sql.Rows) error {
			var agent domain.AgentInstance
			var state, version string
			if err := rows.Scan(&agent.ID, &agent.Name, &agent.Definition.ID, &agent.Definition.Version, &agent.Status, &state, &version); err != nil {
				return err
			}
			if err := codec.Decode([]byte(state), &agent.State); err != nil {
				return err
			}
			var err error
			if agent.StateVersion, err = recoveryUint(version); err != nil {
				return err
			}
			if err := validateAgent(agent); err != nil {
				return err
			}
			records.Agents = append(records.Agents, agent)
			return nil
		}},
		{`SELECT id, type, payload_json, created_at FROM events ORDER BY id`, func(rows *sql.Rows) error {
			var event domain.Event
			var payload, created string
			if err := rows.Scan(&event.ID, &event.Type, &payload, &created); err != nil {
				return err
			}
			if err := codec.Decode([]byte(payload), &event.Payload); err != nil {
				return err
			}
			var err error
			if event.CreatedAt, err = time.Parse(time.RFC3339Nano, created); err != nil {
				return err
			}
			records.Events = append(records.Events, event)
			return nil
		}},
		{`SELECT agent_id, event_id, execution_id, status FROM deliveries ORDER BY receive_seq`, func(rows *sql.Rows) error {
			var delivery domain.Delivery
			if err := rows.Scan(&delivery.Key.AgentID, &delivery.Key.EventID, &delivery.ExecutionID, &delivery.Status); err != nil {
				return err
			}
			records.Deliveries = append(records.Deliveries, delivery)
			return nil
		}},
		{`SELECT id, agent_id, event_id, status, created_at, started_at, finished_at, error, attempt_count, result_json FROM executions ORDER BY id`, func(rows *sql.Rows) error {
			var execution domain.Execution
			var created, count string
			var started, finished, message, result sql.NullString
			if err := rows.Scan(&execution.ID, &execution.AgentID, &execution.EventID, &execution.Status, &created, &started, &finished, &message, &count, &result); err != nil {
				return err
			}
			var err error
			if execution.CreatedAt, err = time.Parse(time.RFC3339Nano, created); err != nil {
				return err
			}
			if execution.StartedAt, err = recoveryTime(started); err != nil {
				return err
			}
			if execution.FinishedAt, err = recoveryTime(finished); err != nil {
				return err
			}
			if execution.AttemptCount, err = recoveryUint(count); err != nil {
				return err
			}
			execution.Error = message.String
			if result.Valid {
				if err := codec.Decode([]byte(result.String), &execution.Result); err != nil {
					return err
				}
			}
			records.Executions = append(records.Executions, execution)
			return nil
		}},
		{`SELECT id, execution_id, number, status, started_at, finished_at, failure_json, expected_state_version
		 FROM execution_attempts ORDER BY execution_id, length(number), number`, func(rows *sql.Rows) error {
			var attempt domain.Attempt
			var number, started, version string
			var finished, failure sql.NullString
			if err := rows.Scan(&attempt.ID, &attempt.ExecutionID, &number, &attempt.Status, &started, &finished, &failure, &version); err != nil {
				return err
			}
			var err error
			if attempt.Number, err = recoveryUint(number); err != nil {
				return err
			}
			if attempt.StartedAt, err = time.Parse(time.RFC3339Nano, started); err != nil {
				return err
			}
			if attempt.FinishedAt, err = recoveryTime(finished); err != nil {
				return err
			}
			if attempt.Error, err = recoveryFailure(failure); err != nil {
				return err
			}
			expected, err := recoveryUint(version)
			if err != nil {
				return err
			}
			if attempt.Status == domain.AttemptStatusRunning {
				records.ClaimVersions[attempt.ID] = expected
			}
			records.Attempts = append(records.Attempts, attempt)
			return nil
		}},
		{`SELECT id, execution_id, agent_id, request_json, handler_version, recovery_policy, max_attempts, status, attempt_count, result_event_id, result_json, last_error_json
		 FROM actions ORDER BY sequence`, func(rows *sql.Rows) error {
			var action domain.ActionRecord
			var id, executionID domain.ID
			var request, max, count string
			var result, failure sql.NullString
			if err := rows.Scan(&id, &executionID, &action.AgentID, &request, &action.HandlerVersion, &action.RecoveryPolicy, &max, &action.Status, &count, &action.ResultEventID, &result, &failure); err != nil {
				return err
			}
			if err := codec.Decode([]byte(request), &action.Request); err != nil {
				return err
			}
			if action.Request.ID != id || action.Request.ExecutionID == nil || *action.Request.ExecutionID != executionID {
				return fmt.Errorf("action %s存储身份不一致: %w", id, core.ErrStoreConflict)
			}
			var err error
			if action.MaxAttempts, err = recoveryUint(max); err != nil {
				return err
			}
			if action.AttemptCount, err = recoveryUint(count); err != nil {
				return err
			}
			if action.LastError, err = recoveryFailure(failure); err != nil {
				return err
			}
			if result.Valid {
				if err := codec.Decode([]byte(result.String), &action.Result); err != nil {
					return err
				}
			}
			records.Actions = append(records.Actions, action)
			return nil
		}},
		{`SELECT id, action_id, number, status, started_at, finished_at, failure_json
		 FROM action_attempts ORDER BY action_id, length(number), number`, func(rows *sql.Rows) error {
			var attempt domain.ActionAttempt
			var number, started string
			var finished, failure sql.NullString
			if err := rows.Scan(&attempt.ID, &attempt.ActionID, &number, &attempt.Status, &started, &finished, &failure); err != nil {
				return err
			}
			var err error
			if attempt.Number, err = recoveryUint(number); err != nil {
				return err
			}
			if attempt.StartedAt, err = time.Parse(time.RFC3339Nano, started); err != nil {
				return err
			}
			if attempt.FinishedAt, err = recoveryTime(finished); err != nil {
				return err
			}
			if attempt.Error, err = recoveryFailure(failure); err != nil {
				return err
			}
			records.ActionAttempts = append(records.ActionAttempts, attempt)
			return nil
		}},
	}
	for _, query := range queries {
		rows, err := tx.QueryContext(ctx, query.query)
		if err != nil {
			return core.RecoveryRecords{}, err
		}
		for rows.Next() {
			if err := query.read(rows); err != nil {
				rows.Close()
				return core.RecoveryRecords{}, err
			}
		}
		err = rows.Err()
		closeErr := rows.Close()
		if err != nil {
			return core.RecoveryRecords{}, err
		}
		if closeErr != nil {
			return core.RecoveryRecords{}, closeErr
		}
	}
	tasks, err := listSubagentTasks(ctx, tx)
	if err != nil {
		return core.RecoveryRecords{}, err
	}
	records.Tasks = tasks
	return records, nil
}

func recoveryUint(text string) (uint64, error) {
	number, err := strconv.ParseUint(text, 10, 64)
	if err != nil {
		return 0, err
	}
	if strconv.FormatUint(number, 10) != text {
		return 0, fmt.Errorf("恢复时整数格式无效: %w", core.ErrStoreConflict)
	}
	return number, nil
}

func recoveryTime(text sql.NullString) (*time.Time, error) {
	if !text.Valid {
		return nil, nil
	}
	parsed, err := time.Parse(time.RFC3339Nano, text.String)
	if err != nil {
		return nil, err
	}
	return &parsed, nil
}

func recoveryFailure(text sql.NullString) (*domain.Failure, error) {
	if !text.Valid {
		return nil, nil
	}
	var failure domain.Failure
	if err := codec.Decode([]byte(text.String), &failure); err != nil {
		return nil, err
	}
	return &failure, nil
}
