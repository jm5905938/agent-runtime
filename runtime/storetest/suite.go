// storetest提供可由不同后端共用的公开存储契约测试
package storetest

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"agent-runtime/core"
	"agent-runtime/domain"
)

// Run为每个用例创建独立后端，工厂负责底层资源清理，套件负责关闭session
func Run(t *testing.T, newBackend func(*testing.T) core.RecoveryStore) {
	t.Helper()
	for _, test := range []struct {
		name string
		run  func(*testing.T, core.RecoveryStore)
	}{
		{"agent_snapshots", testAgentSnapshots},
		{"event_receipt", testEventReceipt},
		{"execution_claims", testExecutionClaims},
		{"execution_commit", testExecutionCommit},
		{"execution_failure_retry", func(t *testing.T, b core.RecoveryStore) { testExecutionRetry(t, b, false) }},
		{"execution_interrupted_retry", func(t *testing.T, b core.RecoveryStore) { testExecutionRetry(t, b, true) }},
		{"action_completion", testActionCompletion},
		{"action_manual_unknown", func(t *testing.T, b core.RecoveryStore) { testActionUnknown(t, b, domain.RecoveryPolicyManual) }},
		{"action_safe_retry", func(t *testing.T, b core.RecoveryStore) { testActionUnknown(t, b, domain.RecoveryPolicySafeRetry) }},
		{"session_gates", testSessionGates},
		{"session_recovery", testSessionRecovery},
	} {
		t.Run(test.name, func(t *testing.T) {
			backend := newBackend(t)
			if backend == nil {
				t.Fatal("后端工厂返回nil")
			}
			if _, exposed := backend.(core.StateStore); exposed {
				t.Fatal("恢复后端暴露了可绕过session的store")
			}
			test.run(t, backend)
		})
	}
}

func openSession(t *testing.T, backend core.RecoveryStore) core.RecoverySession {
	t.Helper()
	session, err := backend.OpenSession(context.Background())
	must(t, err)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := session.Close(ctx); err != nil {
			t.Errorf("清理session: %v", err)
		}
	})
	return session
}

func readySession(t *testing.T, backend core.RecoveryStore) core.RecoverySession {
	t.Helper()
	session := openSession(t, backend)
	_, err := session.Recover(context.Background())
	must(t, err)
	return session
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func createAgent(t *testing.T, store core.StateStore, id domain.ID) domain.AgentInstance {
	t.Helper()
	agent := domain.AgentInstance{
		ID: id, Name: "契约测试", Definition: domain.DefinitionRef{ID: "storetest", Version: "1"},
		Status: domain.AgentStatusActive,
		State:  map[string]any{"keep": "保留", "nested": map[string]any{"value": "初始", "old": true}},
	}
	must(t, store.CreateAgent(context.Background(), agent))
	return agent
}

func receive(t *testing.T, store core.StateStore, agentID domain.ID, event domain.Event) core.ReceivedEvent {
	t.Helper()
	received, err := store.ReceiveEvent(context.Background(), agentID, event)
	must(t, err)
	return received
}

func claimExecution(t *testing.T, store core.StateStore, key domain.DeliveryKey) *core.ExecutionClaim {
	t.Helper()
	claim, err := store.ClaimExecution(context.Background(), key)
	must(t, err)
	return claim
}

func newExecution(t *testing.T, store core.StateStore, agentID domain.ID) *core.ExecutionClaim {
	t.Helper()
	received := receive(t, store, agentID, domain.NewEvent("test", map[string]any{"value": "输入"}))
	return claimExecution(t, store, received.Delivery.Key)
}

func newAction(claim *core.ExecutionClaim) domain.ActionRecord {
	action := domain.NewAction("echo", map[string]any{"text": "hello"})
	action.BindExecution(claim.Token.ExecutionID)
	return domain.ActionRecord{
		Request: action, AgentID: claim.Agent.ID, HandlerVersion: "1",
		RecoveryPolicy: domain.RecoveryPolicySafeRetry, IdempotencyKey: string(action.ID),
		MaxAttempts: 2, Status: domain.ActionStatusPending, ResultEventID: domain.NewEvent("action.result", nil).ID,
	}
}

func claimAction(t *testing.T, store core.StateStore, id domain.ID) *core.ActionClaim {
	t.Helper()
	claim, err := store.ClaimAction(context.Background(), id)
	must(t, err)
	return claim
}

func completion(claim *core.ActionClaim, text string) core.ActionCompletion {
	output := map[string]any{"text": text}
	return core.ActionCompletion{
		Token: claim.Token,
		Result: domain.ActionResult{
			ActionID: claim.Record.Request.ID, EventID: claim.Record.ResultEventID,
			Status: domain.ActionStatusSucceeded, Output: output,
		},
		Event: domain.Event{
			ID: claim.Record.ResultEventID, Type: "action.result", CreatedAt: time.Now().UTC(),
			Payload: map[string]any{
				"action_id": string(claim.Record.Request.ID), "action_type": claim.Record.Request.Type,
				"execution_id": string(*claim.Record.Request.ExecutionID), "status": "succeeded", "result": output,
			},
		},
	}
}

// 比较已保存的同一内容，允许后端将整数解码为json.Number
func sameJSON(t *testing.T, got, want any) {
	t.Helper()
	a, err := json.Marshal(got)
	must(t, err)
	b, err := json.Marshal(want)
	must(t, err)
	if string(a) != string(b) {
		t.Fatalf("内容不一致\n实际: %s\n预期: %s", a, b)
	}
}
