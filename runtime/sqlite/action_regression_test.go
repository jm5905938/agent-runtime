package sqlite

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"agent-runtime/core"
	"agent-runtime/domain"
)

func newActionRegressionFixture(t *testing.T) (*Session, *core.ActionClaim) {
	t.Helper()
	ctx := context.Background()
	session, _ := newTestSession(t)
	agent := domain.AgentInstance{ID: "agent-regression", Definition: domain.DefinitionRef{ID: "test", Version: "1"}, Status: domain.AgentStatusActive}
	if err := session.CreateAgent(ctx, agent); err != nil {
		t.Fatal(err)
	}
	received, err := session.ReceiveEvent(ctx, agent.ID, domain.NewEvent("test", nil))
	if err != nil {
		t.Fatal(err)
	}
	execution, err := session.ClaimExecution(ctx, received.Delivery.Key)
	if err != nil {
		t.Fatal(err)
	}
	request := domain.NewAction("echo", nil)
	request.ID = "action-regression"
	request.BindExecution(execution.Token.ExecutionID)
	action := domain.ActionRecord{Request: request, AgentID: agent.ID, HandlerVersion: "1", RecoveryPolicy: domain.RecoveryPolicySafeRetry,
		MaxAttempts: 2, Status: domain.ActionStatusPending, ResultEventID: "result-regression"}
	if _, err := session.CommitExecution(ctx, core.ExecutionCommit{Token: execution.Token, Actions: []domain.ActionRecord{action}}); err != nil {
		t.Fatal(err)
	}
	claim, err := session.ClaimAction(ctx, request.ID)
	if err != nil {
		t.Fatal(err)
	}
	return session, claim
}

func actionRegressionCompletion(claim *core.ActionClaim) core.ActionCompletion {
	output := map[string]any{"text": "hello"}
	return core.ActionCompletion{Token: claim.Token,
		Result: domain.ActionResult{ActionID: claim.Record.Request.ID, EventID: claim.Record.ResultEventID, Status: domain.ActionStatusSucceeded, Output: output},
		Event: domain.Event{ID: claim.Record.ResultEventID, Type: "action.result", CreatedAt: time.Now().UTC(),
			Payload: map[string]any{"action_id": string(claim.Record.Request.ID), "action_type": claim.Record.Request.Type,
				"execution_id": string(*claim.Record.Request.ExecutionID), "status": "succeeded", "result": output}}}
}

func TestLegacyIdempotencyColumnPreservesActionIdentityOnRecovery(t *testing.T) {
	ctx := context.Background()
	session, claim := newActionRegressionFixture(t)
	backend := session.backend
	var key string
	if err := backend.db.QueryRowContext(ctx, `SELECT idempotency_key FROM actions WHERE id = ?`, claim.Record.Request.ID).Scan(&key); err != nil {
		t.Fatal(err)
	}
	if key != string(claim.Record.Request.ID) {
		t.Fatalf("兼容列未使用action id: %q", key)
	}
	if _, err := backend.db.ExecContext(ctx, `UPDATE actions SET idempotency_key = ? WHERE id = ?`, "legacy-provider-key", claim.Record.Request.ID); err != nil {
		t.Fatal(err)
	}
	if err := session.Close(ctx); err != nil {
		t.Fatal(err)
	}
	reopened := openTestSession(t, backend)
	if _, err := reopened.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	retried, err := reopened.ClaimAction(ctx, claim.Record.Request.ID)
	if err != nil {
		t.Fatal(err)
	}
	if retried.Token.AttemptNumber != 2 || !reflect.DeepEqual(retried.Record.Request, claim.Record.Request) || retried.Record.ResultEventID != claim.Record.ResultEventID {
		t.Fatalf("旧数据库恢复改变了action身份: %+v", retried)
	}
	if _, err := reopened.CompleteAction(ctx, actionRegressionCompletion(retried)); err != nil {
		t.Fatal(err)
	}
	if err := backend.db.QueryRowContext(ctx, `SELECT idempotency_key FROM actions WHERE id = ?`, claim.Record.Request.ID).Scan(&key); err != nil {
		t.Fatal(err)
	}
	if key != "legacy-provider-key" {
		t.Fatalf("恢复覆盖了旧数据库兼容列: %q", key)
	}
}

func TestActionAttemptOrderFullUint64(t *testing.T) {
	ctx := context.Background()
	session, claim := newActionRegressionFixture(t)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if err := session.RecordActionUnknown(ctx, claim.Token, domain.Failure{Kind: domain.ErrorKindUnknown, Message: "故障注入前结束attempt"}); err != nil {
		t.Fatal(err)
	}
	numbers := []uint64{1, 2, 10, uint64(math.MaxInt64) + 1, math.MaxUint64}
	for _, number := range numbers[1:] {
		if _, err := session.backend.db.ExecContext(ctx,
			`INSERT INTO action_attempts(id, action_id, number, status, started_at, finished_at, failure_json) VALUES (?, ?, ?, ?, ?, ?, ?)`,
			fmt.Sprintf("injected-%d", number), string(claim.Record.Request.ID), strconv.FormatUint(number, 10), string(domain.ActionStatusUnknown), now, now,
			`{"kind":"unknown","message":"数值排序故障注入"}`); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := session.backend.db.ExecContext(ctx, `UPDATE actions SET attempt_count = ?, max_attempts = ? WHERE id = ?`,
		strconv.FormatUint(math.MaxUint64, 10), strconv.FormatUint(math.MaxUint64, 10), string(claim.Record.Request.ID)); err != nil {
		t.Fatal(err)
	}
	saved, err := session.LoadAction(ctx, claim.Record.Request.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(saved.Attempts) != len(numbers) {
		t.Fatalf("故障注入记录数变化: %d", len(saved.Attempts))
	}
	for i, number := range numbers {
		if saved.Attempts[i].Number != number {
			t.Fatalf("action uint64排序错误，第%d项为%d，预期%d", i, saved.Attempts[i].Number, number)
		}
	}
}

func TestCompleteActionRollbackAfterPartialWrites(t *testing.T) {
	ctx := context.Background()
	session, claim := newActionRegressionFixture(t)
	before, err := session.LoadAction(ctx, claim.Record.Request.ID)
	if err != nil {
		t.Fatal(err)
	}
	deliveriesBefore, err := session.ListDeliveries(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// 只有action、attempt更新和event插入均已发生时，才触发预期失败
	if _, err := session.backend.db.ExecContext(ctx, `CREATE TRIGGER fail_result_delivery BEFORE INSERT ON deliveries
		WHEN NEW.event_id = 'result-regression'
		BEGIN
			SELECT CASE WHEN
				(SELECT status FROM actions WHERE id = 'action-regression') = 'succeeded' AND
				(SELECT status FROM action_attempts WHERE action_id = 'action-regression' AND number = '1') = 'succeeded' AND
				(SELECT COUNT(*) FROM events WHERE id = 'result-regression') = 1
			THEN RAISE(ABORT, '已发生部分SQL写入') ELSE RAISE(ABORT, '注入点前写入未完成') END;
		END`); err != nil {
		t.Fatal(err)
	}
	if _, err := session.CompleteAction(ctx, actionRegressionCompletion(claim)); err == nil || !strings.Contains(err.Error(), "已发生部分SQL写入") {
		t.Fatalf("未在确认部分写入后失败: %v", err)
	}
	after, err := session.LoadAction(ctx, claim.Record.Request.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("SQL失败留下action或attempt更新: 前=%+v，后=%+v", before, after)
	}
	var eventCount int
	if err := session.backend.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM events WHERE id = ?`, string(claim.Record.ResultEventID)).Scan(&eventCount); err != nil {
		t.Fatal(err)
	}
	if eventCount != 0 {
		t.Fatalf("SQL失败留下结果event: %d", eventCount)
	}
	deliveriesAfter, err := session.ListDeliveries(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(deliveriesBefore, deliveriesAfter) {
		t.Fatalf("SQL失败改变delivery: 前=%+v，后=%+v", deliveriesBefore, deliveriesAfter)
	}
	if _, err := session.backend.db.ExecContext(ctx, `DROP TRIGGER fail_result_delivery`); err != nil {
		t.Fatal(err)
	}
	if _, err := session.CompleteAction(ctx, actionRegressionCompletion(claim)); err != nil {
		t.Fatalf("回滚后相同token无法完成: %v", err)
	}
}
