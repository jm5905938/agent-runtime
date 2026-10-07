package core

import (
	"agent-runtime/domain"
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

type resolutionTestRunner struct {
	actionType string
	payload    map[string]any
	entered    chan struct{}
	release    <-chan struct{}
}

func (r *resolutionTestRunner) Run(input ExecutionContext) (ExecutionResult, error) {
	switch input.Event.Type {
	case "main.request":
		action := domain.NewAction(r.actionType, r.payload)
		return ExecutionResult{StateUpdate: map[string]any{
			"request_status": "waiting", "waiting_action_id": string(action.ID),
			"waiting_action_type": r.actionType, "waiting_execution_id": string(input.ExecutionID),
			"request_execution_id": string(input.ExecutionID),
		}, Actions: []domain.Action{action}}, nil
	case "fixture.state":
		return ExecutionResult{StateUpdate: input.Event.Payload}, nil
	case "fixture.hold":
		close(r.entered)
		<-r.release
	case ActionResolutionEventType:
		// 实际MainAgent的重试与放弃由Python和跨进程测试覆盖；这里只推进等待指针。
		return ExecutionResult{StateUpdate: map[string]any{
			"request_status": "finished", "waiting_action_id": "", "waiting_execution_id": "",
		}}, nil
	}
	return ExecutionResult{}, nil
}

func (*resolutionTestRunner) DeliveryBlockedBy(AgentSnapshot, domain.Event, []domain.Event) []BlockReason {
	return nil
}

type resolutionTestOptions struct {
	definition domain.DefinitionRef
	actionType string
	policy     domain.RecoveryPolicy
	status     domain.ActionStatus
}

type resolutionTestFixture struct {
	runtime *Runtime
	store   *MemoryStore
	agent   domain.AgentInstance
	action  domain.ActionRecord
	runner  *resolutionTestRunner
	calls   atomic.Int32
}

func newResolutionTestFixture(t *testing.T, options resolutionTestOptions) *resolutionTestFixture {
	t.Helper()
	if options.definition == (domain.DefinitionRef{}) {
		options.definition = domain.DefinitionRef{ID: "main", Version: "1"}
	}
	if options.actionType == "" {
		options.actionType = "model.generate"
	}
	if options.policy == "" {
		options.policy = domain.RecoveryPolicyManual
	}
	if options.status == "" {
		options.status = domain.ActionStatusUnknown
	}
	fixture := &resolutionTestFixture{store: NewMemoryStore()}
	runtime, err := NewRuntimeWithStore(fixture.store)
	if err != nil {
		t.Fatal(err)
	}
	fixture.runtime = runtime
	fixture.runner = &resolutionTestRunner{actionType: options.actionType, payload: map[string]any{
		"messages": []any{map[string]any{"role": "system", "content": "frozen system"},
			map[string]any{"role": "user", "content": "本轮输入"}},
		"tools": []any{map[string]any{"type": "function", "function": map[string]any{"name": "clock"}}},
	}}
	fixture.agent = domain.NewAgentInstance("resolution fixture")
	fixture.agent.Definition = options.definition
	if err := runtime.Register(&fixture.agent, fixture.runner); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Executor().RegisterWithOptions(options.actionType, handlerFunc(func(domain.Action) (map[string]any, error) {
		fixture.calls.Add(1)
		return map[string]any{"text": "unexpected execution"}, nil
	}), HandlerOptions{Version: "1", RecoveryPolicy: options.policy, MaxAttempts: 2}); err != nil {
		t.Fatal(err)
	}
	result, err := runtime.Process(fixture.agent.ID, domain.NewEvent("main.request", map[string]any{"message": "本轮输入"}))
	if err != nil || len(result.Actions) != 1 {
		t.Fatalf("source execution result=%+v error=%v", result, err)
	}
	actionID := result.Actions[0].ID
	if options.status != domain.ActionStatusPending {
		claim, err := fixture.store.ClaimAction(context.Background(), actionID)
		if err != nil {
			t.Fatal(err)
		}
		switch options.status {
		case domain.ActionStatusUnknown:
			if err := fixture.store.RecordActionUnknown(context.Background(), claim.Token,
				domain.Failure{Kind: domain.ErrorKindInterrupted, Message: "response was not persisted"}); err != nil {
				t.Fatal(err)
			}
		case domain.ActionStatusSucceeded, domain.ActionStatusFailed:
			completion := memoryTestCompletion(claim)
			if options.status == domain.ActionStatusFailed {
				completion.Result.Status = domain.ActionStatusFailed
				completion.Result.Output = nil
				completion.Result.Error = &domain.Failure{Kind: domain.ErrorKindBusiness, Message: "known failure"}
				completion.Event.Payload["status"] = "failed"
				completion.Event.Payload["error"] = "known failure"
				delete(completion.Event.Payload, "result")
			}
			if _, err := fixture.store.CompleteAction(context.Background(), completion); err != nil {
				t.Fatal(err)
			}
		case domain.ActionStatusRunning:
		default:
			t.Fatalf("unsupported fixture action status %q", options.status)
		}
	}
	saved, err := fixture.store.LoadAction(context.Background(), actionID)
	if err != nil {
		t.Fatal(err)
	}
	fixture.action = saved.Action
	t.Cleanup(func() { p4Close(t, runtime) })
	return fixture
}

func resolutionTestUnchanged(t *testing.T, fixture *resolutionTestFixture, before AgentQuery) {
	t.Helper()
	after := queryAgent(t, fixture.runtime, fixture.agent.ID)
	if !reflect.DeepEqual(before, after) || fixture.calls.Load() != 0 {
		t.Fatalf("rejected resolution changed records: before=%+v after=%+v handler calls=%d", before, after, fixture.calls.Load())
	}
}

func TestActionResolutionRejectsInvalidInputWithoutMutation(t *testing.T) {
	for _, scenario := range []struct {
		name     string
		actionID domain.ID
		decision ResolutionDecision
		reason   string
	}{
		{"empty action", "", ResolutionRetry, "重试"},
		{"blank action", " \t", ResolutionRetry, "重试"},
		{"invalid action UTF-8", domain.ID("bad\xff"), ResolutionRetry, "重试"},
		{"unsupported decision", "current", "complete", "重试"},
		{"empty reason", "current", ResolutionAbandon, ""},
		{"blank reason", "current", ResolutionAbandon, " \n"},
		{"invalid reason UTF-8", "current", ResolutionRetry, "bad\xff"},
		{"reason exceeds limit", "current", ResolutionRetry, strings.Repeat("字", 1025)},
		{"unknown action", "missing", ResolutionRetry, "重试"},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			fixture := newResolutionTestFixture(t, resolutionTestOptions{})
			actionID := scenario.actionID
			if actionID == "current" {
				actionID = fixture.action.Request.ID
			}
			before := queryAgent(t, fixture.runtime, fixture.agent.ID)
			if _, err := fixture.runtime.ResolveAction(context.Background(), actionID, scenario.decision, scenario.reason); err == nil {
				t.Fatal("invalid resolution accepted")
			}
			resolutionTestUnchanged(t, fixture, before)
		})
	}
}

func TestActionResolutionRequiresCurrentUnknownManualMainModel(t *testing.T) {
	for _, scenario := range []struct {
		name    string
		options resolutionTestOptions
		state   map[string]any
		paused  bool
	}{
		{name: "pending", options: resolutionTestOptions{status: domain.ActionStatusPending}},
		{name: "running", options: resolutionTestOptions{status: domain.ActionStatusRunning}},
		{name: "succeeded", options: resolutionTestOptions{status: domain.ActionStatusSucceeded}},
		{name: "failed", options: resolutionTestOptions{status: domain.ActionStatusFailed}},
		{name: "safe retry policy", options: resolutionTestOptions{policy: domain.RecoveryPolicySafeRetry}},
		{name: "other definition", options: resolutionTestOptions{definition: domain.DefinitionRef{ID: "echo", Version: "1"}}},
		{name: "other definition version", options: resolutionTestOptions{definition: domain.DefinitionRef{ID: "main", Version: "2"}}},
		{name: "not waiting", state: map[string]any{"request_status": "finished"}},
		{name: "other waiting action", state: map[string]any{"waiting_action_id": "other"}},
		{name: "other waiting type", state: map[string]any{"waiting_action_type": "tool.clock"}},
		{name: "other source execution", state: map[string]any{"waiting_execution_id": "other"}},
		{name: "paused agent", paused: true},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			fixture := newResolutionTestFixture(t, scenario.options)
			if scenario.state != nil {
				if _, err := fixture.runtime.Process(fixture.agent.ID, domain.NewEvent("fixture.state", scenario.state)); err != nil {
					t.Fatal(err)
				}
			}
			if scenario.paused {
				fixture.store.mu.Lock()
				agent := fixture.store.agents[fixture.agent.ID]
				agent.Status = domain.AgentStatusPaused
				fixture.store.agents[agent.ID] = agent
				fixture.store.mu.Unlock()
			}
			before := queryAgent(t, fixture.runtime, fixture.agent.ID)
			_, err := fixture.runtime.ResolveAction(context.Background(), fixture.action.Request.ID, ResolutionAbandon, "放弃")
			if err == nil {
				t.Fatal("unsupported action accepted")
			}
			resolutionTestUnchanged(t, fixture, before)
		})
	}
}

func TestActionResolutionAcceptsLegacyWaitingStateAndUnicodeReason(t *testing.T) {
	fixture := newResolutionTestFixture(t, resolutionTestOptions{})
	if _, err := fixture.runtime.Process(fixture.agent.ID, domain.NewEvent("fixture.state", map[string]any{
		"waiting_execution_id": nil, "waiting_action_type": nil,
	})); err != nil {
		t.Fatal(err)
	}
	reason := strings.Repeat("字", 1024)
	receipt, err := fixture.runtime.ResolveAction(context.Background(), fixture.action.Request.ID, ResolutionRetry, reason)
	if err != nil || receipt.Duplicate || receipt.Resolution.Reason != reason || receipt.Delivery.Status != domain.DeliveryStatusPending {
		t.Fatalf("legacy waiting state rejected: receipt=%+v error=%v", receipt, err)
	}
}

func TestActionResolutionAcceptsUnknownManualToolAction(t *testing.T) {
	fixture := newResolutionTestFixture(t, resolutionTestOptions{actionType: "tool.clock"})
	before, err := fixture.store.LoadAction(context.Background(), fixture.action.Request.ID)
	if err != nil {
		t.Fatal(err)
	}

	receipt, err := fixture.runtime.ResolveAction(
		context.Background(), fixture.action.Request.ID, ResolutionRetry, "重试工具调用")
	if err != nil || receipt.Duplicate || receipt.Resolution.ActionID != fixture.action.Request.ID ||
		receipt.Resolution.Decision != ResolutionRetry || receipt.Delivery.Status != domain.DeliveryStatusPending {
		t.Fatalf("unknown manual tool action rejected: receipt=%+v error=%v", receipt, err)
	}

	event, err := fixture.store.LoadEvent(context.Background(), receipt.Resolution.EventID)
	if err != nil || event.Payload["action_type"] != "tool.clock" ||
		event.Payload["decision"] != string(ResolutionRetry) {
		t.Fatalf("resolution event not generic: payload=%+v error=%v", event.Payload, err)
	}

	saved, err := fixture.store.LoadAction(context.Background(), fixture.action.Request.ID)
	if err != nil || !reflect.DeepEqual(saved, before) || fixture.calls.Load() != 0 {
		t.Fatalf("resolution executed or changed the tool action: saved=%+v calls=%d error=%v",
			saved, fixture.calls.Load(), err)
	}

	query := queryAgent(t, fixture.runtime, fixture.agent.ID)
	if len(query.Actions) != 1 || query.Actions[0].Resolution == nil ||
		query.Actions[0].Resolution.Decision != ResolutionRetry {
		t.Fatalf("tool resolution not exposed in query: %+v", query.Actions)
	}
}

func TestActionResolutionIdempotencyAndConflictingDecisions(t *testing.T) {
	for _, decision := range []ResolutionDecision{ResolutionRetry, ResolutionAbandon} {
		t.Run(string(decision), func(t *testing.T) {
			fixture := newResolutionTestFixture(t, resolutionTestOptions{})
			beforeAction, _ := fixture.store.LoadAction(context.Background(), fixture.action.Request.ID)
			first, err := fixture.runtime.ResolveAction(context.Background(), fixture.action.Request.ID, decision, "人工决定")
			if err != nil || first.Duplicate || first.Resolution.ActionID != fixture.action.Request.ID || first.Delivery.Key.AgentID != fixture.agent.ID {
				t.Fatalf("first resolution=%+v error=%v", first, err)
			}
			firstEvent, _ := fixture.store.LoadEvent(context.Background(), first.Resolution.EventID)
			for _, processed := range []bool{false, true} {
				if processed {
					if err := fixture.runtime.RunUntilIdle(); err != nil {
						t.Fatal(err)
					}
				}
				duplicate, err := fixture.runtime.ResolveAction(context.Background(), fixture.action.Request.ID, decision, "人工决定")
				wantStatus := domain.DeliveryStatusPending
				if processed {
					wantStatus = domain.DeliveryStatusCompleted
				}
				if err != nil || !duplicate.Duplicate || duplicate.Resolution != first.Resolution || duplicate.Delivery.Status != wantStatus {
					t.Fatalf("processed=%t duplicate=%+v error=%v", processed, duplicate, err)
				}
				before := queryAgent(t, fixture.runtime, fixture.agent.ID)
				opposite := ResolutionRetry
				if decision == ResolutionRetry {
					opposite = ResolutionAbandon
				}
				for _, changed := range []struct {
					decision ResolutionDecision
					reason   string
				}{{opposite, "人工决定"}, {decision, "另一个原因"}} {
					if _, err := fixture.runtime.ResolveAction(context.Background(), fixture.action.Request.ID, changed.decision, changed.reason); !errors.Is(err, ErrStoreConflict) {
						t.Fatalf("conflicting decision accepted: %v", err)
					}
					resolutionTestUnchanged(t, fixture, before)
				}
			}
			afterAction, _ := fixture.store.LoadAction(context.Background(), fixture.action.Request.ID)
			afterEvent, _ := fixture.store.LoadEvent(context.Background(), first.Resolution.EventID)
			if !reflect.DeepEqual(beforeAction, afterAction) || !reflect.DeepEqual(firstEvent, afterEvent) || fixture.calls.Load() != 0 {
				t.Fatal("resolution changed the original action, attempts, event or executed the old action")
			}
			if _, err := fixture.store.LoadEvent(context.Background(), fixture.action.ResultEventID); !errors.Is(err, ErrStoreNotFound) {
				t.Fatalf("unknown action acquired an invented final result: %v", err)
			}
		})
	}
}

func TestActionResolutionFreezesPayloadAndQuerySnapshots(t *testing.T) {
	fixture := newResolutionTestFixture(t, resolutionTestOptions{})
	before := queryAgent(t, fixture.runtime, fixture.agent.ID)
	if before.Actions[0].Resolution != nil || !queryHasBlock(before.Actions[0].BlockedBy, BlockManualUnknown) {
		t.Fatalf("unresolved action query=%+v", before.Actions[0])
	}
	receipt, err := fixture.runtime.ResolveAction(context.Background(), fixture.action.Request.ID, ResolutionRetry, "重试")
	if err != nil {
		t.Fatal(err)
	}
	event, err := fixture.store.LoadEvent(context.Background(), receipt.Resolution.EventID)
	if err != nil {
		t.Fatal(err)
	}
	frozen, ok := event.Payload["retry_payload"].(map[string]any)
	if !ok || !reflect.DeepEqual(frozen, fixture.action.Request.Payload) || event.Type != ActionResolutionEventType ||
		event.Payload["execution_id"] != string(*fixture.action.Request.ExecutionID) {
		t.Fatalf("retry did not freeze source payload: %+v", event)
	}
	fixture.runner.payload["messages"].([]any)[0].(map[string]any)["content"] = "changed environment"
	frozen["messages"].([]any)[0].(map[string]any)["content"] = "changed read snapshot"
	query := queryAgent(t, fixture.runtime, fixture.agent.ID)
	if query.Actions[0].Ready || query.Actions[0].Resolution == nil || *query.Actions[0].Resolution != receipt.Resolution ||
		!queryHasBlock(query.Actions[0].BlockedBy, BlockActionResolved) || queryHasBlock(query.Actions[0].BlockedBy, BlockManualUnknown) {
		t.Fatalf("resolved old action query=%+v", query.Actions[0])
	}
	query.Actions[0].Resolution.Reason = "changed query resolution"
	query.Actions[0].Action.Request.Payload["messages"].([]any)[0].(map[string]any)["content"] = "changed action snapshot"
	query.Actions[0].Attempts[0].Error.Message = "changed attempt snapshot"
	for i := range query.Deliveries {
		if query.Deliveries[i].Event.ID == receipt.Resolution.EventID {
			query.Deliveries[i].Event.Payload["retry_payload"].(map[string]any)["tools"].([]any)[0].(map[string]any)["type"] = "changed tool snapshot"
		}
	}
	fresh := queryAgent(t, fixture.runtime, fixture.agent.ID)
	freshEvent, _ := fixture.store.LoadEvent(context.Background(), receipt.Resolution.EventID)
	if fresh.Actions[0].Resolution.Reason != "重试" || !reflect.DeepEqual(fresh.Actions[0].Action.Request.Payload, fixture.action.Request.Payload) ||
		!reflect.DeepEqual(freshEvent.Payload["retry_payload"], fixture.action.Request.Payload) || fresh.Actions[0].Attempts[0].Error.Message != "response was not persisted" {
		t.Fatalf("mutating caller snapshots changed stored audit: %+v", fresh)
	}
}

func TestActionResolutionBusyGateAndDuplicateFastPath(t *testing.T) {
	for _, decided := range []bool{false, true} {
		t.Run(map[bool]string{false: "first decision", true: "existing decision"}[decided], func(t *testing.T) {
			fixture := newResolutionTestFixture(t, resolutionTestOptions{})
			if decided {
				if _, err := fixture.runtime.ResolveAction(context.Background(), fixture.action.Request.ID, ResolutionAbandon, "放弃"); err != nil {
					t.Fatal(err)
				}
				if err := fixture.runtime.RunUntilIdle(); err != nil {
					t.Fatal(err)
				}
			}
			release := make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			t.Cleanup(unblock)
			fixture.runner.entered, fixture.runner.release = make(chan struct{}), release
			done := make(chan error, 1)
			go func() {
				_, err := fixture.runtime.Process(fixture.agent.ID, domain.NewEvent("fixture.hold", nil))
				done <- err
			}()
			p3Await(t, fixture.runner.entered)
			before := queryAgent(t, fixture.runtime, fixture.agent.ID)
			receipt, err := fixture.runtime.ResolveAction(context.Background(), fixture.action.Request.ID, ResolutionAbandon, "放弃")
			if decided {
				if err != nil || !receipt.Duplicate || receipt.Delivery.Status != domain.DeliveryStatusCompleted {
					t.Fatalf("existing receipt hidden by later execution: %+v error=%v", receipt, err)
				}
			} else if !errors.Is(err, ErrExecutionInProgress) {
				t.Fatalf("resolution raced with active execution: %v", err)
			}
			resolutionTestUnchanged(t, fixture, before)
			unblock()
			if err := p3Await(t, done); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestActionResolutionRejectsStoreRunningDeliveryWithoutGateOwnership(t *testing.T) {
	fixture := newResolutionTestFixture(t, resolutionTestOptions{})
	received, err := fixture.runtime.SubmitContext(context.Background(), fixture.agent.ID, domain.NewEvent("fixture.external", nil))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.store.ClaimExecution(context.Background(), received.Delivery.Key); err != nil {
		t.Fatal(err)
	}
	before := queryAgent(t, fixture.runtime, fixture.agent.ID)
	if _, err := fixture.runtime.ResolveAction(context.Background(), fixture.action.Request.ID, ResolutionRetry, "重试"); !errors.Is(err, ErrExecutionInProgress) {
		t.Fatalf("running store delivery accepted resolution: %v", err)
	}
	resolutionTestUnchanged(t, fixture, before)
}

func TestActionResolutionCancellationAndClosedRuntime(t *testing.T) {
	fixture := newResolutionTestFixture(t, resolutionTestOptions{})
	before := queryAgent(t, fixture.runtime, fixture.agent.ID)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := fixture.runtime.ResolveAction(ctx, fixture.action.Request.ID, ResolutionRetry, "重试"); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled resolution error=%v", err)
	}
	resolutionTestUnchanged(t, fixture, before)
	if err := fixture.runtime.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.runtime.ResolveAction(context.Background(), fixture.action.Request.ID, ResolutionRetry, "重试"); !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("closed runtime accepted resolution: %v", err)
	}
	if _, err := fixture.store.LoadEvent(context.Background(), actionResolutionEventID(fixture.action.Request.ID)); !errors.Is(err, ErrStoreNotFound) {
		t.Fatalf("cancelled or closed call saved a decision: %v", err)
	}
}

func TestActionResolutionDecisionSurvivesRecovery(t *testing.T) {
	fixture := newResolutionTestFixture(t, resolutionTestOptions{})
	first, err := fixture.runtime.ResolveAction(context.Background(), fixture.action.Request.ID, ResolutionAbandon, "放弃")
	if err != nil {
		t.Fatal(err)
	}
	if err := fixture.runtime.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	backend := &memoryRecoveryStore{store: fixture.store}
	for _, process := range []bool{true, false} {
		recovered := p4Open(t, backend)
		if err := recovered.RegisterDefinition(fixture.agent.Definition, fixture.runner); err != nil {
			t.Fatal(err)
		}
		if err := recovered.Executor().Register("model.generate", EchoHandler{}); err != nil {
			t.Fatal(err)
		}
		duplicate, err := recovered.ResolveAction(context.Background(), fixture.action.Request.ID, ResolutionAbandon, "放弃")
		if err != nil || !duplicate.Duplicate || duplicate.Resolution != first.Resolution {
			t.Fatalf("recovery lost decision: %+v error=%v", duplicate, err)
		}
		if process {
			if err := recovered.RunUntilIdle(); err != nil {
				t.Fatal(err)
			}
		}
		query := queryAgent(t, recovered, fixture.agent.ID)
		if query.Actions[0].Action.Status != domain.ActionStatusUnknown || query.Actions[0].Resolution == nil ||
			!queryHasBlock(query.Actions[0].BlockedBy, BlockActionResolved) || query.Actions[0].Action.AttemptCount != 1 {
			t.Fatalf("recovery changed old unknown audit: %+v", query.Actions[0])
		}
		p4Close(t, recovered)
	}
}
