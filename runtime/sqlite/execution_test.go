package sqlite

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"

	"agent-runtime/core"
	"agent-runtime/domain"
)

func TestClaimExecution(t *testing.T) {
	ctx := context.Background()
	session, _ := newTestSession(t)

	agent := domain.AgentInstance{
		ID:     domain.ID("agent-1"),
		Name:   "test",
		Status: domain.AgentStatusActive,
		Definition: domain.DefinitionRef{
			ID:      "def-1",
			Version: "v1",
		},
		State: map[string]any{},
	}
	if err := session.CreateAgent(ctx, agent); err != nil {
		t.Fatal(err)
	}

	event := domain.Event{
		ID:      domain.ID("event-1"),
		Type:    "test.event",
		Payload: map[string]any{"a": 1},
	}
	if _, err := session.ReceiveEvent(ctx, agent.ID, event); err != nil {
		t.Fatal(err)
	}

	key := domain.DeliveryKey{AgentID: agent.ID, EventID: event.ID}

	claim, err := session.ClaimExecution(ctx, key)
	if err != nil {
		t.Fatal(err)
	}

	if claim.Token.Delivery != key {
		t.Fatalf("delivery key mismatch")
	}
	if claim.Token.ExecutionID == "" {
		t.Fatalf("execution id empty")
	}
	if claim.Token.AttemptID == "" {
		t.Fatalf("attempt id empty")
	}
	if claim.Token.ExpectedStateVersion != agent.StateVersion {
		t.Fatalf("expected state version mismatch: got %d want %d",
			claim.Token.ExpectedStateVersion, agent.StateVersion)
	}
	if claim.Agent.ID != agent.ID {
		t.Fatalf("agent id mismatch")
	}
	if claim.Event.ID != event.ID {
		t.Fatalf("event id mismatch")
	}
	if claim.Attempt.Number != 1 {
		t.Fatalf("attempt number mismatch: got %d want 1", claim.Attempt.Number)
	}
	if claim.Attempt.Status != domain.AttemptStatusRunning {
		t.Fatalf("attempt status mismatch: %s", claim.Attempt.Status)
	}

	delivery, err := session.LoadDelivery(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if delivery.Status != domain.DeliveryStatusRunning {
		t.Fatalf("delivery status mismatch: %s", delivery.Status)
	}

	if _, err := session.ClaimExecution(ctx, key); err == nil {
		t.Fatalf("expected error on second claim")
	}
}

func TestCommitExecution(t *testing.T) {
	ctx := context.Background()
	session, _ := newTestSession(t)

	agent := domain.AgentInstance{
		ID:     domain.ID("agent-1"),
		Name:   "test",
		Status: domain.AgentStatusActive,
		Definition: domain.DefinitionRef{
			ID:      "def-1",
			Version: "v1",
		},
		State: map[string]any{"count": 0},
	}
	if err := session.CreateAgent(ctx, agent); err != nil {
		t.Fatal(err)
	}

	event := domain.Event{
		ID:      domain.ID("event-1"),
		Type:    "test.event",
		Payload: map[string]any{},
	}
	if _, err := session.ReceiveEvent(ctx, agent.ID, event); err != nil {
		t.Fatal(err)
	}

	key := domain.DeliveryKey{AgentID: agent.ID, EventID: event.ID}
	claim, err := session.ClaimExecution(ctx, key)
	if err != nil {
		t.Fatal(err)
	}

	commit := core.ExecutionCommit{
		Token:       claim.Token,
		StateUpdate: map[string]any{"count": 1},
	}

	result, err := session.CommitExecution(ctx, commit)
	if err != nil {
		t.Fatal(err)
	}
	if got, ok := result.StateUpdate["count"].(json.Number); !ok || got.String() != "1" {
		t.Fatalf("result count mismatch: %v", result.StateUpdate["count"])
	}

	// 读 agent，确认 state 和版本
	loaded, err := session.LoadAgent(ctx, agent.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.StateVersion != agent.StateVersion+1 {
		t.Fatalf("state version mismatch: got %d want %d",
			loaded.StateVersion, agent.StateVersion+1)
	}
	if fmt.Sprint(loaded.State["count"]) != "1" {
		t.Fatalf("state count mismatch: %v", loaded.State["count"])
	}

	// 读 delivery，确认 completed
	delivery, err := session.LoadDelivery(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if delivery.Status != domain.DeliveryStatusCompleted {
		t.Fatalf("delivery status mismatch: %s", delivery.Status)
	}

	// 读 execution，确认 completed、有结果
	stored, err := session.LoadExecution(ctx, claim.Token.ExecutionID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Execution.Status != domain.ExecutionStatusCompleted {
		t.Fatalf("execution status mismatch: %s", stored.Execution.Status)
	}
	if stored.Execution.Result == nil {
		t.Fatalf("execution result is nil")
	}
	if len(stored.Attempts) != 1 {
		t.Fatalf("attempt count mismatch: %d", len(stored.Attempts))
	}
	if stored.Attempts[0].Status != domain.AttemptStatusSucceeded {
		t.Fatalf("attempt status mismatch: %s", stored.Attempts[0].Status)
	}
}

func TestFailExecution(t *testing.T) {
	ctx := context.Background()
	session, _ := newTestSession(t)

	agent := domain.AgentInstance{
		ID:     domain.ID("agent-1"),
		Name:   "test",
		Status: domain.AgentStatusActive,
		Definition: domain.DefinitionRef{
			ID:      "def-1",
			Version: "v1",
		},
		State: map[string]any{"count": 0},
	}
	if err := session.CreateAgent(ctx, agent); err != nil {
		t.Fatal(err)
	}

	event := domain.Event{
		ID:      domain.ID("event-1"),
		Type:    "test.event",
		Payload: map[string]any{},
	}
	if _, err := session.ReceiveEvent(ctx, agent.ID, event); err != nil {
		t.Fatal(err)
	}

	key := domain.DeliveryKey{AgentID: agent.ID, EventID: event.ID}
	claim, err := session.ClaimExecution(ctx, key)
	if err != nil {
		t.Fatal(err)
	}

	failure := core.ExecutionFailure{
		Token: claim.Token,
		Failure: domain.Failure{
			Kind:    domain.ErrorKind("test"),
			Message: "boom",
		},
		Interrupted: false,
	}

	if err := session.FailExecution(ctx, failure); err != nil {
		t.Fatal(err)
	}

	// delivery 应该是 failed
	delivery, err := session.LoadDelivery(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if delivery.Status != domain.DeliveryStatusFailed {
		t.Fatalf("delivery status mismatch: %s", delivery.Status)
	}

	// execution 应该是 failed
	stored, err := session.LoadExecution(ctx, claim.Token.ExecutionID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Execution.Status != domain.ExecutionStatusFailed {
		t.Fatalf("execution status mismatch: %s", stored.Execution.Status)
	}
	if stored.Execution.Error != "boom" {
		t.Fatalf("execution error mismatch: %s", stored.Execution.Error)
	}

	// attempt 应该是 failed，有 failure
	if len(stored.Attempts) != 1 {
		t.Fatalf("attempt count mismatch: %d", len(stored.Attempts))
	}
	if stored.Attempts[0].Status != domain.AttemptStatusFailed {
		t.Fatalf("attempt status mismatch: %s", stored.Attempts[0].Status)
	}
	if stored.Attempts[0].Error == nil || stored.Attempts[0].Error.Message != "boom" {
		t.Fatalf("attempt error mismatch: %v", stored.Attempts[0].Error)
	}
}

func TestRequeueDelivery(t *testing.T) {
	ctx := context.Background()
	session, _ := newTestSession(t)

	agent := domain.AgentInstance{
		ID:     domain.ID("agent-1"),
		Name:   "test",
		Status: domain.AgentStatusActive,
		Definition: domain.DefinitionRef{
			ID:      "def-1",
			Version: "v1",
		},
		State: map[string]any{"count": 0},
	}
	if err := session.CreateAgent(ctx, agent); err != nil {
		t.Fatal(err)
	}

	event := domain.Event{
		ID:      domain.ID("event-1"),
		Type:    "test.event",
		Payload: map[string]any{},
	}
	if _, err := session.ReceiveEvent(ctx, agent.ID, event); err != nil {
		t.Fatal(err)
	}

	key := domain.DeliveryKey{AgentID: agent.ID, EventID: event.ID}
	claim, err := session.ClaimExecution(ctx, key)
	if err != nil {
		t.Fatal(err)
	}

	// 先 fail
	failure := core.ExecutionFailure{
		Token: claim.Token,
		Failure: domain.Failure{
			Kind:    domain.ErrorKind("test"),
			Message: "boom",
		},
	}
	if err := session.FailExecution(ctx, failure); err != nil {
		t.Fatal(err)
	}

	// requeue
	if err := session.RequeueDelivery(ctx, key); err != nil {
		t.Fatal(err)
	}

	// delivery 应该是 pending，execution id 不变
	delivery, err := session.LoadDelivery(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if delivery.Status != domain.DeliveryStatusPending {
		t.Fatalf("delivery status mismatch: %s", delivery.Status)
	}
	if delivery.ExecutionID != claim.Token.ExecutionID {
		t.Fatalf("execution id changed: got %s want %s",
			delivery.ExecutionID, claim.Token.ExecutionID)
	}

	// attempt 历史应该还在
	stored, err := session.LoadExecution(ctx, claim.Token.ExecutionID)
	if err != nil {
		t.Fatal(err)
	}
	if len(stored.Attempts) != 1 {
		t.Fatalf("attempt count mismatch: %d", len(stored.Attempts))
	}

	// 再领取，应该新增 attempt 2，沿用 execution id
	claim2, err := session.ClaimExecution(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if claim2.Token.ExecutionID != claim.Token.ExecutionID {
		t.Fatalf("execution id changed on second claim")
	}
	if claim2.Attempt.Number != 2 {
		t.Fatalf("attempt number mismatch: got %d want 2", claim2.Attempt.Number)
	}

	stored2, err := session.LoadExecution(ctx, claim2.Token.ExecutionID)
	if err != nil {
		t.Fatal(err)
	}
	if len(stored2.Attempts) != 2 {
		t.Fatalf("attempt count mismatch after second claim: %d", len(stored2.Attempts))
	}
}

func TestCommitExecutionStaleToken(t *testing.T) {
	ctx := context.Background()
	session, _ := newTestSession(t)

	agent := domain.AgentInstance{
		ID:     domain.ID("agent-1"),
		Name:   "test",
		Status: domain.AgentStatusActive,
		Definition: domain.DefinitionRef{
			ID:      "def-1",
			Version: "v1",
		},
		State: map[string]any{"count": 0},
	}
	if err := session.CreateAgent(ctx, agent); err != nil {
		t.Fatal(err)
	}

	event := domain.Event{
		ID:      domain.ID("event-1"),
		Type:    "test.event",
		Payload: map[string]any{},
	}
	if _, err := session.ReceiveEvent(ctx, agent.ID, event); err != nil {
		t.Fatal(err)
	}

	key := domain.DeliveryKey{AgentID: agent.ID, EventID: event.ID}
	claim, err := session.ClaimExecution(ctx, key)
	if err != nil {
		t.Fatal(err)
	}

	// 1. attempt id 被改
	badToken := claim.Token
	badToken.AttemptID = domain.ID("fake-attempt")
	_, err = session.CommitExecution(ctx, core.ExecutionCommit{
		Token:       badToken,
		StateUpdate: map[string]any{"count": 1},
	})
	if err == nil {
		t.Fatalf("expected error for fake attempt id")
	}

	// 2. expected state version 被改
	badToken = claim.Token
	badToken.ExpectedStateVersion = claim.Token.ExpectedStateVersion + 100
	_, err = session.CommitExecution(ctx, core.ExecutionCommit{
		Token:       badToken,
		StateUpdate: map[string]any{"count": 1},
	})
	if err == nil {
		t.Fatalf("expected error for wrong version")
	}

	// 确认 agent state 没变
	loaded, err := session.LoadAgent(ctx, agent.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.StateVersion != agent.StateVersion {
		t.Fatalf("state version changed: got %d want %d",
			loaded.StateVersion, agent.StateVersion)
	}

	// 3. 正常 commit
	if _, err := session.CommitExecution(ctx, core.ExecutionCommit{
		Token:       claim.Token,
		StateUpdate: map[string]any{"count": 1},
	}); err != nil {
		t.Fatal(err)
	}

	// 4. 再用同一个 token，应该失败
	_, err = session.CommitExecution(ctx, core.ExecutionCommit{
		Token:       claim.Token,
		StateUpdate: map[string]any{"count": 2},
	})
	if err == nil {
		t.Fatalf("expected error on reuse of token")
	}

	// 确认 state 还是 count=1，版本只加了一次
	loaded, err = session.LoadAgent(ctx, agent.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.StateVersion != agent.StateVersion+1 {
		t.Fatalf("state version mismatch: got %d want %d",
			loaded.StateVersion, agent.StateVersion+1)
	}
	if got, ok := loaded.State["count"].(json.Number); !ok || got.String() != "1" {
		t.Fatalf("state count mismatch: %v", loaded.State["count"])
	}
}

func TestClaimExecutionConcurrent(t *testing.T) {
	ctx := context.Background()
	session, _ := newTestSession(t)

	agent := domain.AgentInstance{
		ID:     domain.ID("agent-1"),
		Name:   "test",
		Status: domain.AgentStatusActive,
		Definition: domain.DefinitionRef{
			ID:      "def-1",
			Version: "v1",
		},
		State: map[string]any{},
	}
	if err := session.CreateAgent(ctx, agent); err != nil {
		t.Fatal(err)
	}

	// 两条不同 event，同 agent
	for i := 0; i < 2; i++ {
		event := domain.Event{
			ID:      domain.ID(fmt.Sprintf("event-%d", i)),
			Type:    "test.event",
			Payload: map[string]any{},
		}
		if _, err := session.ReceiveEvent(ctx, agent.ID, event); err != nil {
			t.Fatal(err)
		}
	}

	keys := []domain.DeliveryKey{
		{AgentID: agent.ID, EventID: "event-0"},
		{AgentID: agent.ID, EventID: "event-1"},
	}

	var wg sync.WaitGroup
	results := make([]error, len(keys))
	for i, key := range keys {
		wg.Add(1)
		go func(i int, key domain.DeliveryKey) {
			defer wg.Done()
			_, err := session.ClaimExecution(ctx, key)
			results[i] = err
		}(i, key)
	}
	wg.Wait()

	success := 0
	for i, err := range results {
		if err == nil {
			success++
		} else {
			t.Logf("claim %d failed: %v", i, err)
		}
	}
	if success != 1 {
		t.Fatalf("expected exactly 1 success, got %d", success)
	}
}

func TestExecutionRetryCommit(t *testing.T) {
	ctx := context.Background()
	session, _ := newTestSession(t)

	agent := domain.AgentInstance{
		ID:     domain.ID("agent-1"),
		Name:   "test",
		Status: domain.AgentStatusActive,
		Definition: domain.DefinitionRef{
			ID:      "def-1",
			Version: "v1",
		},
		State: map[string]any{"count": 0},
	}
	if err := session.CreateAgent(ctx, agent); err != nil {
		t.Fatal(err)
	}

	event := domain.Event{
		ID:      domain.ID("event-1"),
		Type:    "test.event",
		Payload: map[string]any{},
	}
	if _, err := session.ReceiveEvent(ctx, agent.ID, event); err != nil {
		t.Fatal(err)
	}

	key := domain.DeliveryKey{AgentID: agent.ID, EventID: event.ID}

	// claim 1
	claim1, err := session.ClaimExecution(ctx, key)
	if err != nil {
		t.Fatal(err)
	}

	// fail
	failure := core.ExecutionFailure{
		Token: claim1.Token,
		Failure: domain.Failure{
			Kind:    domain.ErrorKind("test"),
			Message: "boom",
		},
	}
	if err := session.FailExecution(ctx, failure); err != nil {
		t.Fatal(err)
	}

	// requeue
	if err := session.RequeueDelivery(ctx, key); err != nil {
		t.Fatal(err)
	}

	// claim 2
	claim2, err := session.ClaimExecution(ctx, key)
	if err != nil {
		t.Fatal(err)
	}
	if claim2.Token.ExecutionID != claim1.Token.ExecutionID {
		t.Fatalf("execution id changed on retry")
	}
	if claim2.Attempt.Number != 2 {
		t.Fatalf("attempt number mismatch: got %d want 2", claim2.Attempt.Number)
	}

	// commit 2
	result, err := session.CommitExecution(ctx, core.ExecutionCommit{
		Token:       claim2.Token,
		StateUpdate: map[string]any{"count": 1},
	})
	if err != nil {
		t.Fatalf("commit after retry failed: %v", err)
	}
	if got, ok := result.StateUpdate["count"].(json.Number); !ok || got.String() != "1" {
		t.Fatalf("result count mismatch: %v", result.StateUpdate["count"])
	}

	// 确认 agent state 更新
	loaded, err := session.LoadAgent(ctx, agent.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.StateVersion != agent.StateVersion+1 {
		t.Fatalf("state version mismatch: got %d want %d",
			loaded.StateVersion, agent.StateVersion+1)
	}

	// 确认 attempt 历史有 2 条
	stored, err := session.LoadExecution(ctx, claim1.Token.ExecutionID)
	if err != nil {
		t.Fatal(err)
	}
	if len(stored.Attempts) != 2 {
		t.Fatalf("attempt count mismatch: %d", len(stored.Attempts))
	}
	if stored.Attempts[0].Status != domain.AttemptStatusFailed {
		t.Fatalf("attempt 1 status mismatch: %s", stored.Attempts[0].Status)
	}
	if stored.Attempts[1].Status != domain.AttemptStatusSucceeded {
		t.Fatalf("attempt 2 status mismatch: %s", stored.Attempts[1].Status)
	}
}
