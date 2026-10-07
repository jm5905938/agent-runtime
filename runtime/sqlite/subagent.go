package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"

	"agent-runtime/codec"
	"agent-runtime/core"
	"agent-runtime/domain"
)

var _ core.SubagentStore = (*Session)(nil)

func (s *Session) SpawnSubagent(ctx context.Context, spawn core.SubagentSpawn) (*domain.SubagentTask, error) {
	if err := s.lock(ctx, true); err != nil {
		return nil, err
	}
	defer s.backend.unlock()
	tx, err := s.backend.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	var requestJSON, status, count string
	var parentID domain.ID
	if err := tx.QueryRowContext(ctx,
		`SELECT request_json, agent_id, status, attempt_count FROM actions WHERE id = ?`,
		string(spawn.Token.ActionID)).Scan(&requestJSON, &parentID, &status, &count); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, core.ErrStoreNotFound
		}
		return nil, err
	}
	var request domain.Action
	if err := codec.Decode([]byte(requestJSON), &request); err != nil {
		return nil, err
	}
	if request.ID != spawn.Token.ActionID || request.Type != core.SubagentSpawnActionType {
		return nil, fmt.Errorf("subagent来源action无效: %w", core.ErrStoreConflict)
	}
	if status != string(domain.ActionStatusRunning) || count != strconv.FormatUint(spawn.Token.AttemptNumber, 10) {
		return nil, core.ErrStoreStaleClaim
	}
	var claimed bool
	if err := tx.QueryRowContext(ctx,
		`SELECT EXISTS (SELECT 1 FROM action_attempts WHERE action_id = ? AND number = ? AND status = ?)`,
		string(spawn.Token.ActionID), count, string(domain.ActionStatusRunning)).Scan(&claimed); err != nil {
		return nil, err
	}
	if !claimed {
		return nil, core.ErrStoreStaleClaim
	}
	if err := core.ValidateSubagentSpawn(domain.ActionRecord{Request: request, AgentID: parentID}, spawn); err != nil {
		return nil, err
	}
	if _, err := loadChildSubagentTask(ctx, tx, parentID); err == nil {
		return nil, fmt.Errorf("subagent不能继续创建subagent: %w", core.ErrStoreConflict)
	} else if !errors.Is(err, core.ErrStoreNotFound) {
		return nil, err
	}
	if task, err := loadSubagentTask(ctx, tx, request.ID); err == nil {
		if task.ParentAgentID != parentID || task.ChildAgentID != spawn.Child.ID || task.InitialEventID != spawn.Event.ID {
			return nil, core.ErrStoreConflict
		}
		event, err := s.loadEventTx(ctx, tx, task.InitialEventID)
		if err != nil {
			return nil, err
		}
		same, err := core.SameEventContent(event, spawn.Event)
		if err != nil {
			return nil, err
		}
		if !same {
			return nil, core.ErrStoreConflict
		}
		return task, nil
	} else if !errors.Is(err, core.ErrStoreNotFound) {
		return nil, err
	}
	var eventExists bool
	if err := tx.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM events WHERE id = ?)`, string(spawn.Event.ID)).Scan(&eventExists); err != nil {
		return nil, err
	}
	if eventExists {
		return nil, core.ErrStoreConflict
	}
	if err := createAgentTx(ctx, tx, spawn.Child); err != nil {
		return nil, err
	}
	if _, err := receiveEvent(ctx, tx, spawn.Child.ID, spawn.Event); err != nil {
		return nil, err
	}
	task := &domain.SubagentTask{ID: request.ID, ParentAgentID: parentID, ChildAgentID: spawn.Child.ID, InitialEventID: spawn.Event.ID}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO subagent_tasks(id, parent_agent_id, child_agent_id, initial_event_id) VALUES (?, ?, ?, ?)`,
		string(task.ID), string(task.ParentAgentID), string(task.ChildAgentID), string(task.InitialEventID)); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	return task, nil
}

func (s *Session) LoadSubagentTask(ctx context.Context, taskID domain.ID) (*domain.SubagentTask, error) {
	if err := s.lock(ctx, false); err != nil {
		return nil, err
	}
	defer s.backend.unlock()
	return loadSubagentTask(ctx, s.backend.db, taskID)
}

const subagentTaskColumns = `id, parent_agent_id, child_agent_id, initial_event_id, completion_execution_id, cancel_requested, result_json`

func loadSubagentTask(ctx context.Context, reader agentReader, taskID domain.ID) (*domain.SubagentTask, error) {
	return scanSubagentTask(reader.QueryRowContext(ctx, `SELECT `+subagentTaskColumns+` FROM subagent_tasks WHERE id = ?`, string(taskID)))
}

func loadChildSubagentTask(ctx context.Context, reader agentReader, childID domain.ID) (*domain.SubagentTask, error) {
	return scanSubagentTask(reader.QueryRowContext(ctx, `SELECT `+subagentTaskColumns+` FROM subagent_tasks WHERE child_agent_id = ?`, string(childID)))
}

type subagentTaskScanner interface {
	Scan(...any) error
}

func scanSubagentTask(row subagentTaskScanner) (*domain.SubagentTask, error) {
	var task domain.SubagentTask
	var completion, result sql.NullString
	var cancel int
	if err := row.Scan(&task.ID, &task.ParentAgentID, &task.ChildAgentID, &task.InitialEventID, &completion, &cancel, &result); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, core.ErrStoreNotFound
		}
		return nil, err
	}
	if task.ID == "" || task.ParentAgentID == "" || task.ChildAgentID == "" || task.InitialEventID == "" ||
		task.ParentAgentID == task.ChildAgentID || (cancel != 0 && cancel != 1) {
		return nil, fmt.Errorf("subagent task记录无效: %w", core.ErrStoreConflict)
	}
	task.CompletionExecutionID = domain.ID(completion.String)
	task.CancelRequested = cancel == 1
	if result.Valid {
		if err := codec.Decode([]byte(result.String), &task.Result); err != nil {
			return nil, err
		}
		if task.Result == nil {
			return nil, fmt.Errorf("subagent结果不能为空: %w", core.ErrStoreConflict)
		}
		if err := core.ValidateSubagentResult(task.Result); err != nil {
			return nil, err
		}
	}
	return &task, nil
}

func (s *Session) ListSubagentTasks(ctx context.Context) ([]domain.SubagentTask, error) {
	if err := s.lock(ctx, false); err != nil {
		return nil, err
	}
	defer s.backend.unlock()
	return listSubagentTasks(ctx, s.backend.db)
}

type subagentTaskQuery interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func listSubagentTasks(ctx context.Context, reader subagentTaskQuery) ([]domain.SubagentTask, error) {
	rows, err := reader.QueryContext(ctx, `SELECT `+subagentTaskColumns+` FROM subagent_tasks ORDER BY id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	tasks := make([]domain.SubagentTask, 0)
	for rows.Next() {
		task, err := scanSubagentTask(rows)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, *task)
	}
	return tasks, rows.Err()
}

func (s *Session) RequestSubagentCancel(ctx context.Context, parentID, taskID domain.ID) (*domain.SubagentTask, error) {
	if err := s.lock(ctx, true); err != nil {
		return nil, err
	}
	defer s.backend.unlock()
	tx, err := s.backend.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	task, err := loadSubagentTask(ctx, tx, taskID)
	if err != nil {
		return nil, err
	}
	if task.ParentAgentID != parentID {
		return nil, fmt.Errorf("subagent task不属于parent: %w", core.ErrStoreConflict)
	}
	if task.Result != nil || task.CancelRequested {
		return task, nil
	}
	if _, err := tx.ExecContext(ctx, `UPDATE subagent_tasks SET cancel_requested = 1 WHERE id = ?`, string(task.ID)); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	task.CancelRequested = true
	return task, nil
}

func (s *Session) FinishSubagentCancel(ctx context.Context, taskID domain.ID) error {
	if err := s.lock(ctx, true); err != nil {
		return err
	}
	defer s.backend.unlock()
	tx, err := s.backend.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	task, err := loadSubagentTask(ctx, tx, taskID)
	if err != nil {
		return err
	}
	if task.Result != nil {
		return nil
	}
	if !task.CancelRequested {
		return fmt.Errorf("subagent未请求取消: %w", core.ErrStoreConflict)
	}
	if err := subagentCanFinish(ctx, tx, task.ChildAgentID, ""); err != nil {
		return err
	}
	result := domain.SubagentResult{Status: domain.SubagentStatusCancelled, Output: map[string]any{},
		Error: &domain.Failure{Kind: domain.ErrorKindBusiness, Message: "任务已取消"}}
	if err := finishSubagentTaskTx(ctx, tx, task, "", result); err != nil {
		return err
	}
	return tx.Commit()
}

func subagentCanFinish(ctx context.Context, tx *sql.Tx, childID, currentExecutionID domain.ID) error {
	var running int
	if err := tx.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM deliveries WHERE agent_id = ? AND status = ? AND execution_id <> ?`,
		string(childID), string(domain.DeliveryStatusRunning), string(currentExecutionID)).Scan(&running); err != nil {
		return err
	}
	if running != 0 {
		return core.ErrSubagentBusy
	}
	records, err := loadRecoveryRecords(ctx, tx)
	if err != nil {
		return err
	}
	for _, action := range records.Actions {
		if action.AgentID == childID && core.SubagentActionUnresolved(action, records.Events) {
			return core.ErrSubagentBusy
		}
	}
	return nil
}

func finishSubagentTaskTx(ctx context.Context, tx *sql.Tx, task *domain.SubagentTask, executionID domain.ID, result domain.SubagentResult) error {
	if err := core.ValidateSubagentResult(&result); err != nil {
		return err
	}
	if task.Result != nil {
		return core.ErrStoreConflict
	}
	agent, err := loadAgent(ctx, tx, task.ChildAgentID)
	if err != nil {
		return err
	}
	if err := core.TerminateSubagentAgent(agent); err != nil {
		return err
	}
	encoded, err := codec.Encode(result)
	if err != nil {
		return err
	}
	var completion any
	if executionID != "" {
		completion = string(executionID)
	}
	if err := recoveryUpdate(ctx, tx,
		`UPDATE subagent_tasks SET result_json = ?, completion_execution_id = ? WHERE id = ? AND result_json IS NULL`,
		string(encoded), completion, string(task.ID)); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `UPDATE agents SET status = ? WHERE id = ?`, string(agent.Status), string(agent.ID))
	return err
}
