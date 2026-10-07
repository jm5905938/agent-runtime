package sqlite

import (
	"context"
	"errors"
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

func executionRegressionClaim(t *testing.T) (*Session, *core.ExecutionClaim) {
	t.Helper()
	s, _ := newTestSession(t)
	agent := domain.AgentInstance{
		ID: "agent", Definition: domain.DefinitionRef{ID: "regression", Version: "1"},
		Status: domain.AgentStatusActive, State: map[string]any{"keep": "保留"},
	}
	ctx := context.Background()
	if err := s.CreateAgent(ctx, agent); err != nil {
		t.Fatal(err)
	}
	received, err := s.ReceiveEvent(ctx, agent.ID, domain.NewEvent("regression", nil))
	if err != nil {
		t.Fatal(err)
	}
	claim, err := s.ClaimExecution(ctx, received.Delivery.Key)
	if err != nil {
		t.Fatal(err)
	}
	return s, claim
}

type executionRegressionSnapshot struct {
	agent     *domain.AgentInstance
	execution *core.StoredExecution
	delivery  *domain.Delivery
	actions   []domain.ActionRecord
}

func snapshotExecutionRegression(t *testing.T, s *Session, token core.ExecutionToken) executionRegressionSnapshot {
	t.Helper()
	ctx := context.Background()
	agent, err := s.LoadAgent(ctx, token.Delivery.AgentID)
	if err != nil {
		t.Fatal(err)
	}
	execution, err := s.LoadExecution(ctx, token.ExecutionID)
	if err != nil {
		t.Fatal(err)
	}
	delivery, err := s.LoadDelivery(ctx, token.Delivery)
	if err != nil {
		t.Fatal(err)
	}
	actions, err := s.ListActions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return executionRegressionSnapshot{agent: agent, execution: execution, delivery: delivery, actions: actions}
}

func assertExecutionRegressionUnchanged(t *testing.T, s *Session, token core.ExecutionToken, before executionRegressionSnapshot) {
	t.Helper()
	after := snapshotExecutionRegression(t, s, token)
	if !reflect.DeepEqual(after, before) {
		t.Fatalf("失败写入改变已存记录\n实际: %+v\n预期: %+v", after, before)
	}
}

func TestFailExecutionRejectsInjectedStateVersionDrift(t *testing.T) {
	ctx := context.Background()
	s, claim := executionRegressionClaim(t)
	//故障注入绕过session修改版本，正常串行store流程不会自行产生这种漂移
	if _, err := s.backend.db.ExecContext(ctx, `UPDATE agents SET state_version = '1' WHERE id = ?`, string(claim.Agent.ID)); err != nil {
		t.Fatal(err)
	}
	before := snapshotExecutionRegression(t, s, claim.Token)
	if _, err := s.CommitExecution(ctx, core.ExecutionCommit{Token: claim.Token}); !errors.Is(err, core.ErrStoreStaleClaim) {
		t.Fatalf("commit未拒绝版本漂移token: %v", err)
	}
	if err := s.FailExecution(ctx, core.ExecutionFailure{Token: claim.Token, Failure: domain.Failure{
		Kind: domain.ErrorKindBusiness, Message: "旧token失败",
	}}); !errors.Is(err, core.ErrStoreStaleClaim) {
		t.Fatalf("fail未拒绝版本漂移token: %v", err)
	}
	assertExecutionRegressionUnchanged(t, s, claim.Token, before)
}

func TestClaimExecutionRejectsAttemptCountExhaustion(t *testing.T) {
	ctx := context.Background()
	s, claim := executionRegressionClaim(t)
	if err := s.FailExecution(ctx, core.ExecutionFailure{Token: claim.Token, Failure: domain.Failure{
		Kind: domain.ErrorKindBusiness, Message: "先结束attempt",
	}}); err != nil {
		t.Fatal(err)
	}
	if err := s.RequeueDelivery(ctx, claim.Token.Delivery); err != nil {
		t.Fatal(err)
	}
	//公开接口无法实际执行MaxUint64次，通过临时数据库注入计数边界
	if _, err := s.backend.db.ExecContext(ctx, `UPDATE executions SET attempt_count = ? WHERE id = ?`,
		strconv.FormatUint(math.MaxUint64, 10), string(claim.Token.ExecutionID)); err != nil {
		t.Fatal(err)
	}
	before := snapshotExecutionRegression(t, s, claim.Token)
	if _, err := s.ClaimExecution(ctx, claim.Token.Delivery); !errors.Is(err, core.ErrStoreConflict) {
		t.Fatalf("attempt计数耗尽未拒绝claim: %v", err)
	}
	assertExecutionRegressionUnchanged(t, s, claim.Token, before)
}

func TestLoadExecutionAttemptsPreservesUint64Order(t *testing.T) {
	ctx := context.Background()
	s, claim := executionRegressionClaim(t)
	for _, number := range []uint64{2, 10, uint64(1) << 63, math.MaxUint64} {
		if _, err := s.backend.db.ExecContext(ctx,
			`INSERT INTO execution_attempts (id, execution_id, number, status, started_at, expected_state_version)
			 VALUES (?, ?, ?, ?, ?, '0')`,
			fmt.Sprintf("attempt-%d", number), string(claim.Token.ExecutionID), strconv.FormatUint(number, 10),
			string(domain.AttemptStatusRunning), time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			t.Fatal(err)
		}
	}
	stored, err := s.LoadExecution(ctx, claim.Token.ExecutionID)
	if err != nil {
		t.Fatal(err)
	}
	want := []uint64{1, 2, 10, uint64(1) << 63, math.MaxUint64}
	if len(stored.Attempts) != len(want) {
		t.Fatalf("attempt数量不正确: %+v", stored.Attempts)
	}
	for i, attempt := range stored.Attempts {
		if attempt.Number != want[i] {
			t.Fatalf("完整uint64范围未按数值排序: %+v", stored.Attempts)
		}
	}
}

func TestCommitExecutionRollsBackAfterSQLWrites(t *testing.T) {
	ctx := context.Background()
	s, claim := executionRegressionClaim(t)
	request := domain.NewAction("echo", map[string]any{"text": "测试"})
	request.BindExecution(claim.Token.ExecutionID)
	action := domain.ActionRecord{
		Request: request, AgentID: claim.Agent.ID, HandlerVersion: "1", RecoveryPolicy: domain.RecoveryPolicySafeRetry,
		MaxAttempts: 2, Status: domain.ActionStatusPending, ResultEventID: "result-event",
	}
	//trigger内部确认前序agent和action写入已发生，再令后续execution写入失败
	if _, err := s.backend.db.ExecContext(ctx, `CREATE TEMP TRIGGER fail_execution_commit
		BEFORE UPDATE OF status ON executions WHEN NEW.status = 'completed'
		BEGIN
			SELECT CASE WHEN (SELECT state_version FROM agents WHERE id = NEW.agent_id) = '1'
				AND EXISTS (SELECT 1 FROM actions WHERE execution_id = NEW.id)
				THEN RAISE(ABORT, '回归注入: 前序写入已发生')
				ELSE RAISE(ABORT, '回归注入: 缺少前序写入') END;
		END`); err != nil {
		t.Fatal(err)
	}
	before := snapshotExecutionRegression(t, s, claim.Token)
	commit := core.ExecutionCommit{Token: claim.Token, StateUpdate: map[string]any{"changed": true}, Actions: []domain.ActionRecord{action}}
	if _, err := s.CommitExecution(ctx, commit); err == nil || !strings.Contains(err.Error(), "前序写入已发生") {
		t.Fatalf("未命中部分SQL写入后的故障: %v", err)
	}
	assertExecutionRegressionUnchanged(t, s, claim.Token, before)
	if _, err := s.LoadAction(ctx, action.Request.ID); !errors.Is(err, core.ErrStoreNotFound) {
		t.Fatalf("SQL回滚后留下action: %v", err)
	}
	if _, err := s.backend.db.ExecContext(ctx, `DROP TRIGGER fail_execution_commit`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.CommitExecution(ctx, commit); err != nil {
		t.Fatalf("回滚消耗了有效token: %v", err)
	}
}

func TestFailExecutionRollsBackAfterSQLWrites(t *testing.T) {
	ctx := context.Background()
	s, claim := executionRegressionClaim(t)
	//先确认当前attempt在事务中已写成failed，再阻止execution更新
	if _, err := s.backend.db.ExecContext(ctx, `CREATE TEMP TRIGGER fail_execution_failure
		BEFORE UPDATE OF status ON executions WHEN NEW.status = 'failed'
		BEGIN
			SELECT CASE WHEN EXISTS (SELECT 1 FROM execution_attempts WHERE execution_id = NEW.id AND status = 'failed')
				THEN RAISE(ABORT, '回归注入: attempt写入已发生')
				ELSE RAISE(ABORT, '回归注入: 缺少attempt写入') END;
		END`); err != nil {
		t.Fatal(err)
	}
	before := snapshotExecutionRegression(t, s, claim.Token)
	failure := core.ExecutionFailure{Token: claim.Token, Failure: domain.Failure{Kind: domain.ErrorKindBusiness, Message: "测试失败"}}
	if err := s.FailExecution(ctx, failure); err == nil || !strings.Contains(err.Error(), "attempt写入已发生") {
		t.Fatalf("未命中attempt SQL写入后的故障: %v", err)
	}
	assertExecutionRegressionUnchanged(t, s, claim.Token, before)
	if _, err := s.backend.db.ExecContext(ctx, `DROP TRIGGER fail_execution_failure`); err != nil {
		t.Fatal(err)
	}
	if err := s.FailExecution(ctx, failure); err != nil {
		t.Fatalf("回滚消耗了有效token: %v", err)
	}
}
