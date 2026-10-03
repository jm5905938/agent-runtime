package core

import (
	"agent-runtime/domain"
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
)

type queryReadFaultStore struct {
	StateStore
	operation string
	fault     error
}

func (s *queryReadFaultStore) ListAgents(ctx context.Context) ([]domain.AgentInstance, error) {
	if s.operation == "ListAgents" {
		return nil, s.fault
	}
	return s.StateStore.ListAgents(ctx)
}

func (s *queryReadFaultStore) LoadAgent(ctx context.Context, id domain.ID) (*domain.AgentInstance, error) {
	if s.operation == "LoadAgent" {
		return nil, s.fault
	}
	return s.StateStore.LoadAgent(ctx, id)
}

func (s *queryReadFaultStore) ListDeliveries(ctx context.Context, statuses ...domain.DeliveryStatus) ([]domain.Delivery, error) {
	if s.operation == "ListDeliveries" {
		return nil, s.fault
	}
	return s.StateStore.ListDeliveries(ctx, statuses...)
}

func (s *queryReadFaultStore) LoadEvent(ctx context.Context, id domain.ID) (*domain.Event, error) {
	if s.operation == "LoadEvent" {
		return nil, s.fault
	}
	return s.StateStore.LoadEvent(ctx, id)
}

func (s *queryReadFaultStore) LoadExecution(ctx context.Context, id domain.ID) (*StoredExecution, error) {
	if s.operation == "LoadExecution" {
		return nil, s.fault
	}
	return s.StateStore.LoadExecution(ctx, id)
}

func (s *queryReadFaultStore) ListActions(ctx context.Context, statuses ...domain.ActionStatus) ([]domain.ActionRecord, error) {
	if s.operation == "ListActions" {
		return nil, s.fault
	}
	return s.StateStore.ListActions(ctx, statuses...)
}

func (s *queryReadFaultStore) LoadAction(ctx context.Context, id domain.ID) (*StoredAction, error) {
	if s.operation == "LoadAction" {
		return nil, s.fault
	}
	return s.StateStore.LoadAction(ctx, id)
}

func TestQueryPropagatesEveryStoreReadFailure(t *testing.T) {
	for _, operation := range []string{"ListAgents", "LoadAgent", "ListDeliveries", "LoadEvent", "LoadExecution", "ListActions", "LoadAction"} {
		t.Run(operation, func(t *testing.T) {
			fault := errors.New("查询读取失败")
			store := &queryReadFaultStore{StateStore: NewMemoryStore()}
			runtime, agent := p3Runtime(t, store, resultRunner{})
			if _, err := runtime.Process(agent.ID, domain.NewEvent("start", nil)); err != nil {
				t.Fatal(err)
			}
			store.operation, store.fault = operation, fault
			var err error
			if operation == "ListAgents" {
				_, err = runtime.Agents()
			} else {
				_, err = runtime.QueryAgent(agent.ID)
			}
			if !errors.Is(err, fault) {
				t.Fatalf("%s读取错误被吞掉或替换: %v", operation, err)
			}
		})
	}
}

func TestQueryRejectsMissingRequiredRecords(t *testing.T) {
	for _, operation := range []string{"LoadAgent", "LoadEvent", "LoadAction"} {
		t.Run(operation, func(t *testing.T) {
			store := &queryReadFaultStore{StateStore: NewMemoryStore()}
			runtime, agent := p3Runtime(t, store, resultRunner{})
			if _, err := runtime.Process(agent.ID, domain.NewEvent("start", nil)); err != nil {
				t.Fatal(err)
			}
			store.operation, store.fault = operation, ErrStoreNotFound
			if _, err := runtime.QueryAgent(agent.ID); !errors.Is(err, ErrStoreNotFound) {
				t.Fatalf("%s缺失被作为成功查询返回: %v", operation, err)
			}
		})
	}
	runtime := NewRuntime()
	if _, err := runtime.QueryAgent("missing"); !errors.Is(err, ErrStoreNotFound) {
		t.Fatalf("不存在的agent被作为成功查询返回: %v", err)
	}
}

func TestQueryOnlyAllowsMissingExecutionForPendingDelivery(t *testing.T) {
	for _, status := range []domain.DeliveryStatus{
		domain.DeliveryStatusPending, domain.DeliveryStatusRunning, domain.DeliveryStatusFailed, domain.DeliveryStatusCompleted,
	} {
		t.Run(string(status), func(t *testing.T) {
			ctx := context.Background()
			store := &queryReadFaultStore{StateStore: NewMemoryStore()}
			runtime, agent := p3Runtime(t, store, resultRunner{})
			received, err := store.ReceiveEvent(ctx, agent.ID, domain.NewEvent("start", nil))
			if err != nil {
				t.Fatal(err)
			}
			if status != domain.DeliveryStatusPending {
				claim, err := store.ClaimExecution(ctx, received.Delivery.Key)
				if err != nil {
					t.Fatal(err)
				}
				switch status {
				case domain.DeliveryStatusFailed:
					err = store.FailExecution(ctx, ExecutionFailure{Token: claim.Token, Failure: domain.Failure{
						Kind: domain.ErrorKindBusiness, Message: "执行失败",
					}})
				case domain.DeliveryStatusCompleted:
					_, err = store.CommitExecution(ctx, ExecutionCommit{Token: claim.Token})
				}
				if err != nil {
					t.Fatal(err)
				}
				store.operation, store.fault = "LoadExecution", ErrStoreNotFound
			}
			query, err := runtime.QueryAgent(agent.ID)
			if status != domain.DeliveryStatusPending {
				if !errors.Is(err, ErrStoreNotFound) {
					t.Fatalf("%s的execution缺失被作为成功返回: %v", status, err)
				}
				return
			}
			if err != nil || len(query.Deliveries) != 1 || query.Deliveries[0].Execution != nil || len(query.Deliveries[0].Attempts) != 0 {
				t.Fatalf("尚未领取的delivery查询失败: %+v, %v", query, err)
			}
			if _, err := store.LoadExecution(ctx, received.Delivery.ExecutionID); !errors.Is(err, ErrStoreNotFound) {
				t.Fatalf("查询创建了execution: %v", err)
			}
			store.operation, store.fault = "LoadExecution", errors.New("pending读取失败")
			if _, err := runtime.QueryAgent(agent.ID); !errors.Is(err, store.fault) {
				t.Fatalf("pending掩盖了其它execution读取错误: %v", err)
			}
		})
	}
}

func TestQueriesRespectCanceledContext(t *testing.T) {
	runtime, agent := p3Runtime(t, NewMemoryStore(), resultRunner{})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := runtime.AgentsContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("agent列表忽略context取消: %v", err)
	}
	if _, err := runtime.QueryAgentContext(ctx, agent.ID); !errors.Is(err, context.Canceled) {
		t.Fatalf("完整查询忽略context取消: %v", err)
	}
	if _, err := runtime.QueryAgent(agent.ID); err != nil {
		t.Fatalf("取消一次查询影响后续查询: %v", err)
	}
}

func queryAssertClosed(t *testing.T, runtime *Runtime, agentID domain.ID) {
	t.Helper()
	queries := []struct {
		name string
		call func() error
	}{
		{"Agents", func() error { _, err := runtime.Agents(); return err }},
		{"AgentsContext", func() error { _, err := runtime.AgentsContext(context.Background()); return err }},
		{"QueryAgent", func() error { _, err := runtime.QueryAgent(agentID); return err }},
		{"QueryAgentContext", func() error { _, err := runtime.QueryAgentContext(context.Background(), agentID); return err }},
	}
	for _, query := range queries {
		if err := query.call(); !errors.Is(err, ErrStoreClosed) {
			t.Errorf("%s在关闭后仍接受查询: %v", query.name, err)
		}
	}
}

func TestQueriesRejectClosedAndClosingRuntime(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	runtime, agent := p3Runtime(t, NewMemoryStore(), functionRunner(func(ExecutionContext) (ExecutionResult, error) {
		close(entered)
		<-release
		return ExecutionResult{}, nil
	}))
	done := p3Process(runtime, agent.ID, domain.NewEvent("start", nil))
	p3Await(t, entered)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := runtime.Close(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("阻塞runner期间取消关闭: %v", err)
	}
	queryAssertClosed(t, runtime, agent.ID)
	unblock()
	if outcome := p3Await(t, done); outcome.err != nil {
		t.Fatalf("已接受的执行未完成: %v", outcome.err)
	}
	if err := runtime.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	queryAssertClosed(t, runtime, agent.ID)
}

func TestQueriesRemainAvailableDuringCallbacks(t *testing.T) {
	for _, callback := range []string{"runner", "handler"} {
		t.Run(callback, func(t *testing.T) {
			runtime := NewRuntime()
			var agentID domain.ID
			inside, release := make(chan error, 1), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			t.Cleanup(unblock)
			checkAndWait := func() {
				_, listErr := runtime.Agents()
				query, err := runtime.QueryAgent(agentID)
				if err == nil && query.Agent.ID != agentID {
					err = fmt.Errorf("callback查询返回了其它agent: %s", query.Agent.ID)
				}
				inside <- errors.Join(listErr, err)
				<-release
			}
			runner := functionRunner(func(ctx ExecutionContext) (ExecutionResult, error) {
				if ctx.Event.Type == "action.result" {
					return ExecutionResult{}, nil
				}
				if callback == "runner" {
					checkAndWait()
				}
				return ExecutionResult{Actions: []domain.Action{domain.NewAction("echo", nil)}}, nil
			})
			handler := handlerFunc(func(domain.Action) (map[string]any, error) {
				checkAndWait()
				return nil, nil
			})
			agent := p4Agent(t, runtime, runner, handler, domain.RecoveryPolicyManual)
			agentID = agent.ID
			done := make(chan error, 1)
			if callback == "runner" {
				go func() { _, err := runtime.Process(agentID, domain.NewEvent("start", nil)); done <- err }()
			} else {
				if _, err := runtime.Process(agentID, domain.NewEvent("start", nil)); err != nil {
					t.Fatal(err)
				}
				go func() { done <- runtime.RunUntilIdle() }()
			}
			if err := p3Await(t, inside); err != nil {
				t.Fatalf("%s内重入查询失败: %v", callback, err)
			}
			outside := make(chan error, 1)
			go func() {
				query, err := runtime.QueryAgent(agentID)
				if err == nil {
					if callback == "runner" && (len(query.Deliveries) != 1 || query.Deliveries[0].Execution == nil || query.Deliveries[0].Execution.Status != domain.ExecutionStatusRunning) {
						err = fmt.Errorf("未显示阻塞中的runner: %+v", query.Deliveries)
					}
					if callback == "handler" && (len(query.Actions) != 1 || query.Actions[0].Action.Status != domain.ActionStatusRunning) {
						err = fmt.Errorf("未显示阻塞中的handler: %+v", query.Actions)
					}
				}
				outside <- err
			}()
			if err := p3Await(t, outside); err != nil {
				t.Fatalf("%s阻塞期间查询失败: %v", callback, err)
			}
			unblock()
			if err := p3Await(t, done); err != nil {
				t.Fatalf("%s执行失败: %v", callback, err)
			}
		})
	}
}

func TestQueryStartupRecoveryRemainsIndependentOfCurrentState(t *testing.T) {
	ctx := context.Background()
	backend := NewMemoryRecoveryStore()
	session := openMemoryRecoveryTest(t, backend)
	if _, err := session.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	agent := domain.NewAgentInstance("恢复查询")
	agent.Definition, agent.Status = domain.DefinitionRef{ID: "query-recovery", Version: "1"}, domain.AgentStatusActive
	if err := session.CreateAgent(ctx, agent); err != nil {
		t.Fatal(err)
	}
	claimEvent := func() *ExecutionClaim {
		t.Helper()
		received, err := session.ReceiveEvent(ctx, agent.ID, domain.NewEvent("start", nil))
		if err != nil {
			t.Fatal(err)
		}
		claim, err := session.ClaimExecution(ctx, received.Delivery.Key)
		if err != nil {
			t.Fatal(err)
		}
		return claim
	}
	actionExecution := claimEvent()
	action := memoryTestAction(actionExecution)
	if _, err := session.CommitExecution(ctx, ExecutionCommit{Token: actionExecution.Token, Actions: []domain.ActionRecord{action}}); err != nil {
		t.Fatal(err)
	}
	if _, err := session.ClaimAction(ctx, action.Request.ID); err != nil {
		t.Fatal(err)
	}
	execution := claimEvent()
	if err := session.Close(ctx); err != nil {
		t.Fatal(err)
	}
	runtime := p4Open(t, backend)
	p4Bind(t, runtime, agent.Definition, functionRunner(func(ExecutionContext) (ExecutionResult, error) {
		return ExecutionResult{}, nil
	}), EchoHandler{}, domain.RecoveryPolicySafeRetry)
	want := RecoveryReport{
		RequeuedDeliveries: []domain.DeliveryKey{execution.Token.Delivery},
		UnknownActions:     []domain.ID{action.Request.ID},
		RetryableActions:   []domain.ID{action.Request.ID},
	}
	before, err := runtime.QueryAgent(agent.ID)
	if err != nil || !reflect.DeepEqual(before.StartupRecovery, want) {
		t.Fatalf("启动恢复报告不完整: %+v, %v", before.StartupRecovery, err)
	}
	before.StartupRecovery.RequeuedDeliveries[0].EventID = "changed"
	before.StartupRecovery.UnknownActions[0] = "changed"
	before.StartupRecovery.RetryableActions[0] = "changed"
	if report := runtime.RecoveryReport(); !reflect.DeepEqual(report, want) {
		t.Fatalf("查询结果修改了启动恢复报告: %+v", report)
	}
	if err := runtime.RunUntilIdle(); err != nil {
		t.Fatal(err)
	}
	after, err := runtime.QueryAgent(agent.ID)
	if err != nil || !reflect.DeepEqual(after.StartupRecovery, want) {
		t.Fatalf("运行改变了启动恢复快照: %+v, %v", after.StartupRecovery, err)
	}
	if len(after.Actions) != 1 || after.Actions[0].Action.Status != domain.ActionStatusSucceeded {
		t.Fatalf("恢复action没有完成: %+v", after.Actions)
	}
	for _, delivery := range after.Deliveries {
		if delivery.Delivery.Status != domain.DeliveryStatusCompleted {
			t.Fatalf("当前状态仍未完成: %+v", delivery)
		}
	}
}
