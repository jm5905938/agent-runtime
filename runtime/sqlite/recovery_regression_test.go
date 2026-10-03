package sqlite

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"agent-runtime/core"
	"agent-runtime/domain"
)

func recoveryFixture(t *testing.T) (*Backend, *Session, *core.ExecutionClaim, domain.ActionRecord) {
	t.Helper()
	ctx := context.Background()
	b := openTestBackend(t)
	s := openTestSession(t, b)
	if _, err := s.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	agent := testAgent()
	agent.Status = domain.AgentStatusActive
	if err := s.CreateAgent(ctx, agent); err != nil {
		t.Fatal(err)
	}
	claim := func(agentID domain.ID) *core.ExecutionClaim {
		event, err := s.ReceiveEvent(ctx, agentID, domain.NewEvent("test", nil))
		if err != nil {
			t.Fatal(err)
		}
		execution, err := s.ClaimExecution(ctx, event.Delivery.Key)
		if err != nil {
			t.Fatal(err)
		}
		return execution
	}
	source := claim(agent.ID)
	request := domain.NewAction("echo", nil)
	request.BindExecution(source.Token.ExecutionID)
	action := domain.ActionRecord{Request: request, AgentID: agent.ID, HandlerVersion: "1", RecoveryPolicy: domain.RecoveryPolicySafeRetry,
		IdempotencyKey: string(request.ID), MaxAttempts: 2, Status: domain.ActionStatusPending, ResultEventID: domain.NewEvent("action.result", nil).ID}
	if _, err := s.CommitExecution(ctx, core.ExecutionCommit{Token: source.Token, Actions: []domain.ActionRecord{action}}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ClaimAction(ctx, request.ID); err != nil {
		t.Fatal(err)
	}
	running := claim(agent.ID)
	if err := s.Close(ctx); err != nil {
		t.Fatal(err)
	}
	return b, openTestSession(t, b), running, action
}

func recoveryDatabaseSnapshot(t *testing.T, b *Backend) map[string][][]any {
	t.Helper()
	result := make(map[string][][]any)
	for _, table := range []string{"agents", "events", "deliveries", "executions", "execution_attempts", "actions", "action_attempts"} {
		rows, err := b.db.Query("SELECT * FROM " + table + " ORDER BY rowid")
		if err != nil {
			t.Fatal(err)
		}
		columns, err := rows.Columns()
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			values := make([]any, len(columns))
			pointers := make([]any, len(columns))
			for i := range values {
				pointers[i] = &values[i]
			}
			if err := rows.Scan(pointers...); err != nil {
				t.Fatal(err)
			}
			result[table] = append(result[table], values)
		}
		err = rows.Err()
		rows.Close()
		if err != nil {
			t.Fatal(err)
		}
	}
	return result
}

func TestRecoveryRejectsIncompleteRecordsAtomically(t *testing.T) {
	for _, damage := range []string{"missing_execution", "missing_execution_attempt", "delivery_status", "attempt_status", "attempt_count", "claim_version", "missing_action_attempt", "action_source_column", "action_attempt_number", "bad_event_time", "bad_event_text", "bad_event_json", "execution_wrong_agent", "execution_wrong_event", "duplicate_attempt_id", "missing_finished_at", "invalid_failure_kind", "orphan_execution", "blank_result_event_id"} {
		t.Run(damage, func(t *testing.T) {
			ctx := context.Background()
			b, s, claim, action := recoveryFixture(t)
			var query string
			var args []any
			switch damage {
			case "missing_execution":
				if _, err := b.db.Exec("DELETE FROM execution_attempts WHERE execution_id = ?", string(claim.Token.ExecutionID)); err != nil {
					t.Fatal(err)
				}
				query, args = "DELETE FROM executions WHERE id = ?", []any{string(claim.Token.ExecutionID)}
			case "missing_execution_attempt":
				query, args = "DELETE FROM execution_attempts WHERE id = ?", []any{string(claim.Token.AttemptID)}
			case "delivery_status":
				query, args = "UPDATE deliveries SET status = 'pending' WHERE execution_id = ?", []any{string(claim.Token.ExecutionID)}
			case "attempt_status":
				query, args = "UPDATE execution_attempts SET status = 'succeeded' WHERE id = ?", []any{string(claim.Token.AttemptID)}
			case "attempt_count":
				query, args = "UPDATE executions SET attempt_count = '2' WHERE id = ?", []any{string(claim.Token.ExecutionID)}
			case "claim_version":
				query, args = "UPDATE execution_attempts SET expected_state_version = '2' WHERE id = ?", []any{string(claim.Token.AttemptID)}
			case "missing_action_attempt":
				query, args = "DELETE FROM action_attempts WHERE action_id = ?", []any{string(action.Request.ID)}
			case "action_source_column":
				query, args = "UPDATE actions SET execution_id = ? WHERE id = ?", []any{string(claim.Token.ExecutionID), string(action.Request.ID)}
			case "action_attempt_number":
				query, args = "UPDATE action_attempts SET number = '01' WHERE action_id = ?", []any{string(action.Request.ID)}
			case "bad_event_time":
				query, args = "UPDATE events SET created_at = '10000-01-01T00:00:00Z' WHERE id = ?", []any{string(claim.Token.Delivery.EventID)}
			case "bad_event_text":
				query, args = "UPDATE events SET type = '' WHERE id = ?", []any{string(claim.Token.Delivery.EventID)}
			case "bad_event_json":
				query, args = "UPDATE events SET payload_json = '{' WHERE id = ?", []any{string(claim.Token.Delivery.EventID)}
			case "execution_wrong_agent":
				if _, err := b.db.Exec("INSERT INTO agents SELECT 'other-agent', name, definition_id, definition_version, status, state_json, state_version FROM agents WHERE id = ?", string(claim.Token.Delivery.AgentID)); err != nil {
					t.Fatal(err)
				}
				query, args = "UPDATE executions SET agent_id = 'other-agent' WHERE id = ?", []any{string(claim.Token.ExecutionID)}
			case "execution_wrong_event":
				query, args = "UPDATE executions SET event_id = (SELECT event_id FROM executions WHERE id = ?) WHERE id = ?", []any{string(*action.Request.ExecutionID), string(claim.Token.ExecutionID)}
			case "duplicate_attempt_id":
				query, args = "UPDATE action_attempts SET id = ? WHERE action_id = ?", []any{string(claim.Token.AttemptID), string(action.Request.ID)}
			case "missing_finished_at":
				query, args = `UPDATE execution_attempts SET status = 'failed', failure_json = '{"kind":"runtime","message":"失败历史缺少结束时间"}' WHERE id = ?`, []any{string(claim.Token.AttemptID)}
			case "invalid_failure_kind":
				query, args = `UPDATE execution_attempts SET failure_json = '{"kind":"invalid","message":"损坏错误种类"}' WHERE id = ?`, []any{string(claim.Token.AttemptID)}
			case "blank_result_event_id":
				query, args = "UPDATE actions SET result_event_id = ? WHERE id = ?", []any{" \t\u3000\n", string(action.Request.ID)}
			case "orphan_execution":
				query, args = "INSERT INTO executions SELECT 'orphan-execution', agent_id, event_id, status, created_at, started_at, finished_at, error, attempt_count, result_json FROM executions WHERE id = ?", []any{string(claim.Token.ExecutionID)}
			}
			if _, err := b.db.Exec(query, args...); err != nil {
				t.Fatal(err)
			}
			before := recoveryDatabaseSnapshot(t, b)
			if report, err := s.Recover(ctx); err == nil {
				t.Fatalf("损坏记录被恢复成功: %+v", report)
			}
			if after := recoveryDatabaseSnapshot(t, b); !reflect.DeepEqual(after, before) {
				t.Fatal("恢复校验失败留下部分转换")
			}
			if err := s.CreateAgent(ctx, testAgent()); !errors.Is(err, core.ErrRecoveryRequired) {
				t.Fatalf("恢复失败打开写入门禁: %v", err)
			}
		})
	}
}

func TestRecoveryRollsBackAfterSQLWritesAndCanRetry(t *testing.T) {
	ctx := context.Background()
	b, s, _, _ := recoveryFixture(t)
	//触发器仅在execution和delivery的转换已实际发生后拒绝action更新
	_, err := b.db.Exec(`CREATE TRIGGER fail_recovery_after_execution BEFORE UPDATE ON actions
		WHEN NEW.status = 'unknown' AND EXISTS(SELECT 1 FROM execution_attempts WHERE status = 'interrupted')
		AND EXISTS(SELECT 1 FROM executions WHERE status = 'pending')
		AND EXISTS(SELECT 1 FROM deliveries WHERE status = 'pending')
		BEGIN SELECT RAISE(ABORT, '执行转换已发生'); END`)
	if err != nil {
		t.Fatal(err)
	}
	before := recoveryDatabaseSnapshot(t, b)
	if _, err := s.Recover(ctx); err == nil || !strings.Contains(err.Error(), "执行转换已发生") {
		t.Fatalf("未在部分SQL写入后注入失败: %v", err)
	}
	if after := recoveryDatabaseSnapshot(t, b); !reflect.DeepEqual(after, before) {
		t.Fatal("SQL中途失败留下部分恢复转换")
	}
	if err := s.CreateAgent(ctx, testAgent()); !errors.Is(err, core.ErrRecoveryRequired) {
		t.Fatalf("事务失败打开写入门禁: %v", err)
	}
	if _, err := b.db.Exec("DROP TRIGGER fail_recovery_after_execution"); err != nil {
		t.Fatal(err)
	}
	if report, err := s.Recover(ctx); err != nil || len(report.RequeuedDeliveries) != 1 || len(report.UnknownActions) != 1 {
		t.Fatalf("移除故障后不能重试恢复: %+v, %v", report, err)
	}
}
