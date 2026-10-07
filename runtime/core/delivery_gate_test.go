package core

import (
	"agent-runtime/domain"
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type deliveryGateTestRunner struct {
	run  func(ExecutionContext) (ExecutionResult, error)
	gate func(AgentSnapshot, domain.Event, []domain.Event) []BlockReason
}

func (r deliveryGateTestRunner) Run(input ExecutionContext) (ExecutionResult, error) {
	if r.run == nil {
		return ExecutionResult{}, nil
	}
	return r.run(input)
}

func (r deliveryGateTestRunner) DeliveryBlockedBy(agent AgentSnapshot, event domain.Event, earlier []domain.Event) []BlockReason {
	if r.gate == nil {
		return nil
	}
	return r.gate(agent, event, earlier)
}

func deliveryGateTestPolicy(agent AgentSnapshot, event domain.Event, earlier []domain.Event) []BlockReason {
	if event.Type != "test.input" {
		return nil
	}
	var reasons []BlockReason
	if agent.State["waiting"] == true {
		reasons = append(reasons, BlockReason{BlockAgentWaiting, "等待当前请求的结果"})
	}
	for _, prior := range earlier {
		if prior.Type == "test.input" {
			reasons = append(reasons, BlockReason{BlockEarlierInput, "较早输入尚未执行"})
			break
		}
	}
	return reasons
}

func deliveryGateTestAgent(t *testing.T, runtime *Runtime, runner AgentRunner, state map[string]any) AgentSnapshot {
	t.Helper()
	ref := domain.DefinitionRef{ID: "test.delivery-gate", Version: "1"}
	if err := runtime.RegisterDefinition(ref, runner); err != nil {
		t.Fatal(err)
	}
	agent, err := runtime.CreateAgent("gated", ref, state)
	if err != nil {
		t.Fatal(err)
	}
	return agent
}

func deliveryGateTestUnclaimed(t *testing.T, runtime *Runtime, agentID, eventID domain.ID) DeliveryQuery {
	t.Helper()
	for _, query := range queryAgent(t, runtime, agentID).Deliveries {
		if query.Event.ID != eventID {
			continue
		}
		if query.Delivery.Status != domain.DeliveryStatusPending || query.Execution != nil || len(query.Attempts) != 0 {
			t.Fatalf("受阻输入产生了execution或attempt: %+v", query)
		}
		return query
	}
	t.Fatalf("受阻输入未留在队列: %s", eventID)
	return DeliveryQuery{}
}

func TestDeliveryGateBlockedProcessKeepsPending(t *testing.T) {
	runtime := NewRuntime()
	calls := 0
	agent := deliveryGateTestAgent(t, runtime, deliveryGateTestRunner{
		gate: deliveryGateTestPolicy,
		run: func(ExecutionContext) (ExecutionResult, error) {
			calls++
			return ExecutionResult{}, nil
		},
	}, map[string]any{"waiting": true})
	event := domain.NewEvent("test.input", map[string]any{"message": "queued"})
	if _, err := runtime.Process(agent.ID, event); !errors.Is(err, ErrDeliveryNotReady) {
		t.Fatalf("Process错误=%v，期待ErrDeliveryNotReady", err)
	}
	query := deliveryGateTestUnclaimed(t, runtime, agent.ID, event.ID)
	if query.Ready || !queryHasBlock(query.BlockedBy, BlockAgentWaiting) || calls != 0 {
		t.Fatalf("查询与执行的门控不一致: %+v, runner调用=%d", query, calls)
	}
	if _, err := runtime.Process(agent.ID, event); !errors.Is(err, ErrDeliveryNotReady) {
		t.Fatalf("重复Process错误=%v", err)
	}
	if len(queryAgent(t, runtime, agent.ID).Deliveries) != 1 || len(runtime.Executions()) != 0 || len(runtime.Attempts()) != 0 {
		t.Fatal("重复受阻输入创建了额外投递或执行记录")
	}
}

func TestDeliveryGateBusyDrainStopsAndOtherAgentContinues(t *testing.T) {
	runtime := NewRuntime()
	gateCalls := 0
	agent := deliveryGateTestAgent(t, runtime, deliveryGateTestRunner{
		gate: func(agent AgentSnapshot, event domain.Event, earlier []domain.Event) []BlockReason {
			gateCalls++
			return deliveryGateTestPolicy(agent, event, earlier)
		},
		run: func(ExecutionContext) (ExecutionResult, error) {
			return ExecutionResult{}, errors.New("受阻输入不应执行")
		},
	}, map[string]any{"waiting": true})
	inputs := []domain.Event{domain.NewEvent("test.input", nil), domain.NewEvent("test.input", nil)}
	for _, event := range inputs {
		if err := runtime.Submit(agent.ID, event); err != nil {
			t.Fatal(err)
		}
	}
	other := domain.NewAgentInstance("other")
	if err := runtime.Register(&other, functionRunner(func(ExecutionContext) (ExecutionResult, error) {
		return ExecutionResult{StateUpdate: map[string]any{"processed": true}}, nil
	})); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Submit(other.ID, domain.NewEvent("plain.input", nil)); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := runtime.RunUntilIdleContext(ctx); err != nil {
		t.Fatalf("受阻队列应正常停止: %v", err)
	}
	if gateCalls > 10 {
		t.Fatalf("受阻投递被反复调度: gate调用=%d", gateCalls)
	}
	for _, input := range inputs {
		deliveryGateTestUnclaimed(t, runtime, agent.ID, input.ID)
	}
	completed, err := runtime.Agent(other.ID)
	if err != nil || completed.State["processed"] != true || len(runtime.Executions()) != 1 {
		t.Fatalf("一个agent的等待阻止了其他agent: %+v, %v", completed, err)
	}
}

func TestDeliveryGateResultsBypassQueuedInputsAndResumeFIFO(t *testing.T) {
	runtime := NewRuntime()
	var seen []string
	agent := deliveryGateTestAgent(t, runtime, deliveryGateTestRunner{
		gate: deliveryGateTestPolicy,
		run: func(input ExecutionContext) (ExecutionResult, error) {
			if input.Event.Type == "action.result" {
				request := input.Event.Payload["result"].(map[string]any)["request"].(string)
				seen = append(seen, "result:"+request)
				return ExecutionResult{StateUpdate: map[string]any{"waiting": false}}, nil
			}
			if input.Agent.State["waiting"] == true {
				return ExecutionResult{}, errors.New("在上一轮完成前执行了新输入")
			}
			seen = append(seen, string(input.Event.ID))
			return ExecutionResult{
				StateUpdate: map[string]any{"waiting": true},
				Actions:     []domain.Action{domain.NewAction("test.reply", map[string]any{"request": string(input.Event.ID)})},
			}, nil
		},
	}, nil)
	if err := runtime.Executor().Register("test.reply", handlerFunc(func(action domain.Action) (map[string]any, error) {
		return map[string]any{"request": action.Payload["request"]}, nil
	})); err != nil {
		t.Fatal(err)
	}
	var inputs []domain.Event
	for i, id := range []domain.ID{"z-first", "a-second", "m-third"} {
		event := domain.NewEvent("test.input", nil)
		event.ID = id
		//Receive order决定顺序，事件ID和创建时间均不决定顺序。
		event.CreatedAt = time.Unix(int64(30-i), 0).UTC()
		inputs = append(inputs, event)
		if err := runtime.Submit(agent.ID, event); err != nil {
			t.Fatal(err)
		}
	}
	before := queryAgent(t, runtime, agent.ID)
	if !before.Deliveries[0].Ready || before.Deliveries[1].Ready || before.Deliveries[2].Ready ||
		!queryHasBlock(before.Deliveries[1].BlockedBy, BlockEarlierInput) {
		t.Fatalf("较早输入没有优先级: %+v", before.Deliveries)
	}
	if _, err := runtime.Process(agent.ID, inputs[2]); !errors.Is(err, ErrDeliveryNotReady) {
		t.Fatalf("直接执行后续输入越过了FIFO: %v", err)
	}
	deliveryGateTestUnclaimed(t, runtime, agent.ID, inputs[2].ID)
	if err := runtime.RunUntilIdle(); err != nil {
		t.Fatal(err)
	}
	want := []string{"z-first", "result:z-first", "a-second", "result:a-second", "m-third", "result:m-third"}
	if !reflect.DeepEqual(seen, want) {
		t.Fatalf("输入或结果执行顺序=%v，期待%v", seen, want)
	}
	after := queryAgent(t, runtime, agent.ID)
	if after.Agent.State["waiting"] != false || len(after.Deliveries) != 6 || len(after.Actions) != 3 {
		t.Fatalf("队列没有全部完成: %+v", after)
	}
	for _, delivery := range after.Deliveries {
		if delivery.Delivery.Status != domain.DeliveryStatusCompleted || delivery.Execution == nil ||
			delivery.Execution.Status != domain.ExecutionStatusCompleted || len(delivery.Attempts) != 1 {
			t.Fatalf("正常排队产生了失败或重试记录: %+v", delivery)
		}
	}
}

func TestDeliveryGateAbsentPreservesDirectProcessBehavior(t *testing.T) {
	runtime := NewRuntime()
	agent := deliveryGateTestAgent(t, runtime, functionRunner(func(input ExecutionContext) (ExecutionResult, error) {
		return ExecutionResult{StateUpdate: map[string]any{"last_event": string(input.Event.ID)}}, nil
	}), map[string]any{"waiting": true})
	first, second := domain.NewEvent("test.input", nil), domain.NewEvent("test.input", nil)
	if err := runtime.Submit(agent.ID, first); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Submit(agent.ID, second); err != nil {
		t.Fatal(err)
	}
	before := queryAgent(t, runtime, agent.ID)
	if !before.Deliveries[0].Ready || !before.Deliveries[1].Ready {
		t.Fatalf("没有DeliveryGate的定义被新增策略阻塞: %+v", before.Deliveries)
	}
	if _, err := runtime.Process(agent.ID, second); err != nil {
		t.Fatalf("没有DeliveryGate的直接执行语义变化: %v", err)
	}
	deliveryGateTestUnclaimed(t, runtime, agent.ID, first.ID)
	if err := runtime.RunUntilIdle(); err != nil {
		t.Fatal(err)
	}
	if len(runtime.Executions()) != 2 {
		t.Fatal("没有DeliveryGate的待办未正常完成")
	}
}

func TestDeliveryGateHookInputsAreIsolatedSnapshots(t *testing.T) {
	runtime := NewRuntime()
	observed := make(map[domain.ID][]domain.ID)
	agent := deliveryGateTestAgent(t, runtime, deliveryGateTestRunner{
		gate: func(agent AgentSnapshot, event domain.Event, earlier []domain.Event) []BlockReason {
			if agent.State["nested"].([]any)[0] != "state" || event.Payload["nested"].([]any)[0] != string(event.ID) {
				t.Errorf("hook再次收到已被修改的数据: agent=%+v, event=%+v", agent, event)
			}
			var ids []domain.ID
			for _, prior := range earlier {
				ids = append(ids, prior.ID)
				if prior.Payload["nested"].([]any)[0] != string(prior.ID) {
					t.Errorf("earlier包含已修改的数据: %+v", prior)
				}
			}
			observed[event.ID] = ids
			agent.State["nested"].([]any)[0] = "changed"
			event.Payload["nested"].([]any)[0] = "changed"
			for i := range earlier {
				earlier[i].ID = "changed"
				earlier[i].Payload["nested"].([]any)[0] = "changed"
			}
			return []BlockReason{{BlockAgentWaiting, "隔离测试保持队列待处理"}}
		},
	}, map[string]any{"nested": []any{"state"}})
	other := domain.NewAgentInstance("other")
	if err := runtime.Register(&other, functionRunner(func(ExecutionContext) (ExecutionResult, error) { return ExecutionResult{}, nil })); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Submit(other.ID, domain.NewEvent("other.input", nil)); err != nil {
		t.Fatal(err)
	}
	var inputs []domain.Event
	for _, id := range []domain.ID{"z-first", "a-second"} {
		event := domain.NewEvent("test.input", map[string]any{"nested": []any{string(id)}})
		event.ID = id
		inputs = append(inputs, event)
		if err := runtime.Submit(agent.ID, event); err != nil {
			t.Fatal(err)
		}
	}
	before := queryAgent(t, runtime, agent.ID)
	if before.Agent.State["nested"].([]any)[0] != "state" {
		t.Fatalf("hook修改污染了Query快照: %+v", before.Agent)
	}
	for i, query := range before.Deliveries {
		if query.Event.ID != inputs[i].ID || query.Event.Payload["nested"].([]any)[0] != string(inputs[i].ID) {
			t.Fatalf("hook修改污染了Query事件: %+v", query.Event)
		}
	}
	if len(observed[inputs[0].ID]) != 0 || !reflect.DeepEqual(observed[inputs[1].ID], []domain.ID{inputs[0].ID}) {
		t.Fatalf("earlier不是同一agent的先前待办: %v", observed)
	}
	if _, err := runtime.Process(agent.ID, inputs[1]); !errors.Is(err, ErrDeliveryNotReady) {
		t.Fatalf("Process门控错误: %v", err)
	}
	after := queryAgent(t, runtime, agent.ID)
	if !reflect.DeepEqual(before, after) || inputs[1].Payload["nested"].([]any)[0] != string(inputs[1].ID) {
		t.Fatalf("hook修改污染了存储或调用方: %+v", after)
	}
}

type deliveryGateCommitPauseStore struct {
	StateStore
	pause func()
}

func (s deliveryGateCommitPauseStore) CommitExecution(ctx context.Context, commit ExecutionCommit) (ExecutionResult, error) {
	result, err := s.StateStore.CommitExecution(ctx, commit)
	if err == nil {
		s.pause()
	}
	return result, err
}

func TestDeliveryGateConcurrentProcessCannotClaimFromStaleState(t *testing.T) {
	for _, stage := range []string{"before_claim", "runner", "after_store_commit"} {
		t.Run(stage, func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			t.Cleanup(unblock)
			pause := func() { close(entered); <-release }
			var store StateStore = NewMemoryStore()
			if stage == "after_store_commit" {
				store = deliveryGateCommitPauseStore{StateStore: store, pause: pause}
			}
			runtime, err := NewRuntimeWithStore(store)
			if err != nil {
				t.Fatal(err)
			}
			first, second := domain.NewEvent("test.input", nil), domain.NewEvent("test.input", nil)
			agent := deliveryGateTestAgent(t, runtime, deliveryGateTestRunner{
				gate: func(agent AgentSnapshot, event domain.Event, earlier []domain.Event) []BlockReason {
					if stage == "before_claim" && event.ID == first.ID {
						pause()
					}
					return deliveryGateTestPolicy(agent, event, earlier)
				},
				run: func(input ExecutionContext) (ExecutionResult, error) {
					if stage == "runner" && input.Event.ID == first.ID {
						pause()
					}
					return ExecutionResult{StateUpdate: map[string]any{"waiting": true}}, nil
				},
			}, nil)
			other := domain.NewAgentInstance("other")
			if err := runtime.Register(&other, functionRunner(func(ExecutionContext) (ExecutionResult, error) {
				return ExecutionResult{}, nil
			})); err != nil {
				t.Fatal(err)
			}
			firstDone := make(chan error, 1)
			go func() { _, err := runtime.Process(agent.ID, first); firstDone <- err }()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("首个Process未到达暂停点")
			}
			secondDone := make(chan error, 1)
			go func() { _, err := runtime.Process(agent.ID, second); secondDone <- err }()
			select {
			case err := <-secondDone:
				if !errors.Is(err, ErrExecutionInProgress) {
					t.Fatalf("并发Process错误=%v，期待ErrExecutionInProgress", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("并发Process等待了runner或commit，而没有返回进行中")
			}
			//直接读store，避免测试用的gate再次进入首个请求的暂停点。
			delivery, err := store.LoadDelivery(context.Background(), domain.DeliveryKey{AgentID: agent.ID, EventID: second.ID})
			if err != nil || delivery.Status != domain.DeliveryStatusPending {
				t.Fatalf("并发输入被领取: %+v, %v", delivery, err)
			}
			if _, err := store.LoadExecution(context.Background(), delivery.ExecutionID); !errors.Is(err, ErrStoreNotFound) {
				t.Fatalf("并发输入产生了execution: %v", err)
			}
			if stage == "runner" {
				pending := deliveryGateTestUnclaimed(t, runtime, agent.ID, second.ID)
				if !queryHasBlock(pending.BlockedBy, BlockExecutionRunning) || !queryHasBlock(pending.BlockedBy, BlockEarlierInput) {
					t.Fatalf("Query未将更早的running输入传给gate: %+v", pending)
				}
			}
			if stage != "after_store_commit" {
				//一个agent的门控和runner锁不能阻止其他agent执行。
				otherDone := make(chan error, 1)
				go func() { _, err := runtime.Process(other.ID, domain.NewEvent("plain.input", nil)); otherDone <- err }()
				select {
				case err := <-otherDone:
					if err != nil {
						t.Fatalf("其他agent被阻塞: %v", err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("其他agent等待了不相关的execution")
				}
			}
			if stage == "after_store_commit" {
				//已完成的同一事件仍可读取已保存结果，不必等待首个调用返回。
				duplicateDone := make(chan error, 1)
				go func() { _, err := runtime.Process(agent.ID, first); duplicateDone <- err }()
				select {
				case err := <-duplicateDone:
					if err != nil {
						t.Fatalf("已完成事件的重复读取被锁阻塞: %v", err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("已完成事件的重复读取等待了commit返回")
				}
			}
			unblock()
			select {
			case err := <-firstDone:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("首个Process未完成")
			}
			if _, err := runtime.Process(agent.ID, second); !errors.Is(err, ErrDeliveryNotReady) {
				t.Fatalf("首轮提交后没有基于最新waiting状态阻塞第二轮: %v", err)
			}
			deliveryGateTestUnclaimed(t, runtime, agent.ID, second.ID)
			wantExecutions := 2
			if stage == "after_store_commit" {
				wantExecutions = 1
			}
			if len(runtime.Executions()) != wantExecutions || len(runtime.Attempts()) != wantExecutions {
				t.Fatalf("并发输入创建额外记录: executions=%d attempts=%d", len(runtime.Executions()), len(runtime.Attempts()))
			}
		})
	}
}

type deliveryGateLoadPauseStore struct {
	StateStore
	beforeReturn func(*domain.Delivery)
}

func (s deliveryGateLoadPauseStore) LoadDelivery(ctx context.Context, key domain.DeliveryKey) (*domain.Delivery, error) {
	delivery, err := s.StateStore.LoadDelivery(ctx, key)
	if err == nil {
		s.beforeReturn(delivery)
	}
	return delivery, err
}

func TestDeliveryGateCompletedDuringPendingReadReturnsSavedResult(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var paused atomic.Bool
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	store := deliveryGateLoadPauseStore{
		StateStore: NewMemoryStore(),
		beforeReturn: func(delivery *domain.Delivery) {
			if delivery.Status == domain.DeliveryStatusPending && paused.CompareAndSwap(false, true) {
				close(entered)
				<-release
			}
		},
	}
	runtime, err := NewRuntimeWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	agent := deliveryGateTestAgent(t, runtime, deliveryGateTestRunner{
		gate: deliveryGateTestPolicy,
		run: func(ExecutionContext) (ExecutionResult, error) {
			return ExecutionResult{StateUpdate: map[string]any{"waiting": true}}, nil
		},
	}, nil)
	event := domain.NewEvent("test.input", nil)
	type outcome struct {
		result ExecutionResult
		err    error
	}
	firstDone := make(chan outcome, 1)
	go func() {
		result, err := runtime.Process(agent.ID, event)
		firstDone <- outcome{result, err}
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("Process未暂停在读取pending之后")
	}
	//另一个调用完成同一事件，原调用所读的pending已经过期。
	saved, err := runtime.Process(agent.ID, event)
	if err != nil {
		t.Fatal(err)
	}
	unblock()
	select {
	case original := <-firstDone:
		if original.err != nil || !reflect.DeepEqual(original.result, saved) {
			t.Fatalf("过期pending读取未复用已完成结果: %+v, %v", original.result, original.err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("原Process未完成")
	}
	if len(runtime.Executions()) != 1 || len(runtime.Attempts()) != 1 {
		t.Fatal("相同事件的并发Process产生了重复执行")
	}
}
