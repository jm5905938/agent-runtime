package core

import (
	"agent-runtime/codec"
	"agent-runtime/domain"
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"
)

var (
	ErrExecutionInProgress = errors.New("execution进行中")
	ErrDeliveryFailed      = errors.New("delivery失败，需要显式重试")
	ErrAgentUnavailable    = errors.New("agent不可用")
	ErrExecutionFailed     = errors.New("execution失败")
)

//事件、action、结果事件的执行闭环
type Runtime struct {
	store       StateStore
	lifecycle   LifecycleManager
	executor    *Executor
	mu          sync.Mutex
	definitions map[domain.DefinitionRef]AgentRunner
	drain       chan struct{}
	session     RecoverySession
	recovery    RecoveryReport
	lifeMu      sync.Mutex
	stopping    bool
	closed      bool
	inflight    int
	stop        chan struct{}
	drained     chan struct{}
	closeGate   chan struct{}
}

func NewRuntime() *Runtime {
	return newRuntime(NewMemoryStore())
}

func NewRuntimeWithStore(store StateStore) (*Runtime, error) {
	if store == nil || isNilValue(store) {
		return nil, fmt.Errorf("创建runtime: store不能为空")
	}
	if _, session := store.(RecoverySession); session {
		return nil, fmt.Errorf("恢复会话请通过OpenRuntime使用")
	}
	return newRuntime(store), nil
}

func newRuntime(store StateStore) *Runtime {
	runtime := &Runtime{store: store, executor: NewExecutor(), definitions: make(map[domain.DefinitionRef]AgentRunner),
		drain: make(chan struct{}, 1), stop: make(chan struct{}), drained: make(chan struct{}), closeGate: make(chan struct{}, 1)}
	runtime.drain <- struct{}{}
	runtime.closeGate <- struct{}{}
	runtime.executor.runtime = runtime
	return runtime
}

func isNilValue(value any) bool {
	v := reflect.ValueOf(value)
	switch v.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return v.IsNil()
	}
	return false
}

//获取执行器以注册能力
func (r *Runtime) Executor() *Executor { return r.executor }

//保存agent与实现，新实例转为active
func (r *Runtime) Register(agent *domain.AgentInstance, runner AgentRunner) error {
	done, err := r.enter()
	if err != nil {
		return err
	}
	defer done()
	if agent == nil || nilRunner(runner) {
		return fmt.Errorf("注册agent: agent和runner不能为空")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	owned := *agent
	if owned.Definition == (domain.DefinitionRef{}) {
		owned.Definition = domain.DefinitionRef{ID: "inline/" + string(agent.ID), Version: "1"}
	}
	if err := owned.Definition.Validate(); err != nil {
		return err
	}
	if _, exists := r.definitions[owned.Definition]; exists {
		return fmt.Errorf("definition已注册，请使用CreateAgent或RestoreAgent")
	}
	if err := validateAgentStatus(owned.Status); err != nil {
		return err
	}
	if owned.Status == domain.AgentStatusCreated {
		if err := r.lifecycle.Transition(&owned, domain.AgentStatusActive); err != nil {
			return err
		}
	}
	if err := r.store.CreateAgent(context.Background(), owned); err != nil {
		return err
	}
	r.definitions[owned.Definition] = runner
	return nil
}

//agent只读快照
func (r *Runtime) Agent(agentID domain.ID) (AgentSnapshot, error) {
	return r.AgentContext(context.Background(), agentID)
}

func (r *Runtime) AgentContext(ctx context.Context, agentID domain.ID) (AgentSnapshot, error) {
	done, err := r.enter()
	if err != nil {
		return AgentSnapshot{}, err
	}
	defer done()
	agent, err := r.store.LoadAgent(ctx, agentID)
	if err != nil {
		return AgentSnapshot{}, err
	}
	snapshot := snapshotAgent(*agent)
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.bindingError(snapshot.Definition); err != nil {
		snapshot.BindingError = err.Error()
	}
	return snapshot, nil
}

//立即处理事件，同一agent不并行执行
func (r *Runtime) Process(agentID domain.ID, event domain.Event) (ExecutionResult, error) {
	return r.ProcessContext(context.Background(), agentID, event)
}

func (r *Runtime) ProcessContext(ctx context.Context, agentID domain.ID, event domain.Event) (ExecutionResult, error) {
	done, err := r.enter()
	if err != nil {
		return ExecutionResult{}, err
	}
	defer done()
	received, err := r.receiveEvent(ctx, agentID, event)
	if err != nil {
		return ExecutionResult{}, err
	}
	return r.processDelivery(ctx, received.Delivery.Key)
}

func (r *Runtime) processDelivery(ctx context.Context, key domain.DeliveryKey) (ExecutionResult, error) {
	delivery, err := r.store.LoadDelivery(ctx, key)
	if err != nil {
		return ExecutionResult{}, &storeFailureError{cause: err}
	}
	switch delivery.Status {
	case domain.DeliveryStatusCompleted:
		saved, err := r.store.LoadExecution(ctx, delivery.ExecutionID)
		if err != nil {
			return ExecutionResult{}, &storeFailureError{cause: err}
		}
		if saved.Execution.Result == nil {
			return ExecutionResult{}, fmt.Errorf("已完成execution %s缺少结果", delivery.ExecutionID)
		}
		return cloneResult(*saved.Execution.Result), nil
	case domain.DeliveryStatusRunning:
		return ExecutionResult{}, ErrExecutionInProgress
	case domain.DeliveryStatusFailed:
		return ExecutionResult{}, ErrDeliveryFailed
	case domain.DeliveryStatusPending:
	default:
		return ExecutionResult{}, fmt.Errorf("投递状态无效%q", delivery.Status)
	}
	agent, err := r.store.LoadAgent(ctx, key.AgentID)
	if err != nil {
		return ExecutionResult{}, &storeFailureError{cause: err}
	}
	if agent.Status != domain.AgentStatusActive {
		return ExecutionResult{}, fmt.Errorf("%w: agent %s当前状态%s", ErrAgentUnavailable, agent.ID, agent.Status)
	}
	r.mu.Lock()
	runner := r.definitions[agent.Definition]
	err = r.bindingError(agent.Definition)
	r.mu.Unlock()
	if err != nil {
		return ExecutionResult{}, err
	}
	claim, err := r.store.ClaimExecution(ctx, key)
	if err != nil {
		return r.resolveClaimError(ctx, key, err)
	}
	if err := ctx.Err(); err != nil {
		return ExecutionResult{}, r.recordFailure(ctx, claim.Token, domain.ErrorKindInterrupted, err, true)
	}
	result, kind, runErr := runAgent(ctx, runner, ExecutionContext{
		Agent: snapshotAgent(claim.Agent), Event: cloneEvent(claim.Event),
		ExecutionID: claim.Token.ExecutionID, AttemptID: claim.Token.AttemptID,
	})
	if runErr != nil {
		return ExecutionResult{}, r.recordFailure(ctx, claim.Token, kind, runErr, kind == domain.ErrorKindInterrupted)
	}
	if err := ctx.Err(); err != nil {
		return ExecutionResult{}, r.recordFailure(ctx, claim.Token, domain.ErrorKindInterrupted, err, true)
	}
	if err := validateResult(result); err != nil {
		return ExecutionResult{}, r.failExecution(ctx, claim.Token, domain.ErrorKindRuntime, err)
	}
	actions, err := r.executor.prepareActions(agent.ID, claim.Token.ExecutionID, result.Actions)
	if err != nil {
		return ExecutionResult{}, r.failExecution(ctx, claim.Token, domain.ErrorKindRuntime, err)
	}
	committed, err := r.store.CommitExecution(ctx, ExecutionCommit{
		Token: claim.Token, StateUpdate: cloneMap(result.StateUpdate), Actions: actions,
	})
	if err != nil {
		//提交报错也可能已生效，不能改记失败
		return ExecutionResult{}, &storeFailureError{cause: fmt.Errorf("提交execution %s: %w", claim.Token.ExecutionID, err)}
	}
	return cloneResult(committed), nil
}

func (r *Runtime) resolveClaimError(ctx context.Context, key domain.DeliveryKey, cause error) (ExecutionResult, error) {
	if !errors.Is(cause, ErrStoreConflict) && !errors.Is(cause, ErrStoreStaleClaim) &&
		!errors.Is(cause, ErrExecutionInProgress) && !errors.Is(cause, ErrDeliveryFailed) && !errors.Is(cause, ErrAgentUnavailable) {
		return ExecutionResult{}, &storeFailureError{cause: cause}
	}
	latest, err := r.store.LoadDelivery(ctx, key)
	if err != nil {
		return ExecutionResult{}, &storeFailureError{cause: err}
	}
	if latest.Status != domain.DeliveryStatusPending {
		return r.processDelivery(ctx, key)
	}
	if errors.Is(cause, ErrExecutionInProgress) {
		running, err := r.store.ListDeliveries(ctx, domain.DeliveryStatusRunning)
		if err != nil {
			return ExecutionResult{}, &storeFailureError{cause: err}
		}
		for _, delivery := range running {
			if delivery.Key.AgentID == key.AgentID {
				return ExecutionResult{}, ErrExecutionInProgress
			}
		}
		//其他投递可能刚完成，本次仍待处理
		return ExecutionResult{}, ErrExecutionInProgress
	}
	if errors.Is(cause, ErrAgentUnavailable) {
		agent, err := r.store.LoadAgent(ctx, key.AgentID)
		if err != nil {
			return ExecutionResult{}, &storeFailureError{cause: err}
		}
		if agent.Status != domain.AgentStatusActive {
			return ExecutionResult{}, ErrAgentUnavailable
		}
	}
	return ExecutionResult{}, &storeFailureError{cause: cause}
}

func runAgent(ctx context.Context, runner AgentRunner, input ExecutionContext) (result ExecutionResult, failureKind domain.ErrorKind, err error) {
	failureKind = domain.ErrorKindBusiness
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("runner异常: %v", recovered)
			result = ExecutionResult{}
			failureKind = domain.ErrorKindRuntime
		}
	}()
	if contextual, ok := runner.(ContextAgentRunner); ok {
		result, err = contextual.RunContext(ctx, input)
	} else {
		result, err = runner.Run(input)
	}
	if err != nil {
		var failure RunnerFailure
		switch {
		case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
			failureKind = domain.ErrorKindInterrupted
		case errors.As(err, &failure):
			switch kind := failure.FailureKind(); kind {
			case domain.ErrorKindBusiness, domain.ErrorKindRuntime, domain.ErrorKindInterrupted:
				failureKind = kind
			default:
				failureKind = domain.ErrorKindRuntime
			}
		}
	}
	return
}

func (r *Runtime) failExecution(ctx context.Context, token ExecutionToken, kind domain.ErrorKind, cause error) error {
	return r.recordFailure(ctx, token, kind, cause, false)
}

func (r *Runtime) recordFailure(ctx context.Context, token ExecutionToken, kind domain.ErrorKind, cause error, interrupted bool) error {
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	err := r.store.FailExecution(cleanup, ExecutionFailure{
		Token: token, Failure: domain.Failure{Kind: kind, Message: recordText(cause.Error())}, Interrupted: interrupted,
	})
	if err != nil {
		return &storeFailureError{cause: fmt.Errorf("保存execution失败记录: %w", errors.Join(cause, err))}
	}
	return &executionFailureError{cause: cause}
}

type executionFailureError struct{ cause error }

func (e *executionFailureError) Error() string        { return e.cause.Error() }
func (e *executionFailureError) Unwrap() error        { return e.cause }
func (e *executionFailureError) Is(target error) bool { return target == ErrExecutionFailed }

type storeFailureError struct{ cause error }

func (e *storeFailureError) Error() string { return e.cause.Error() }
func (e *storeFailureError) Unwrap() error { return e.cause }

//接收事件，事务提交后返回，等待调度
func (r *Runtime) Submit(agentID domain.ID, event domain.Event) error {
	_, err := r.SubmitContext(context.Background(), agentID, event)
	return err
}

func (r *Runtime) SubmitContext(ctx context.Context, agentID domain.ID, event domain.Event) (ReceivedEvent, error) {
	done, err := r.enter()
	if err != nil {
		return ReceivedEvent{}, err
	}
	defer done()
	return r.receiveEvent(ctx, agentID, event)
}

func (r *Runtime) receiveEvent(ctx context.Context, agentID domain.ID, event domain.Event) (ReceivedEvent, error) {
	if err := validateEvent(event); err != nil {
		return ReceivedEvent{}, err
	}
	return r.store.ReceiveEvent(ctx, agentID, cloneEvent(event))
}

func (r *Runtime) Retry(ctx context.Context, key domain.DeliveryKey) error {
	done, err := r.enter()
	if err != nil {
		return err
	}
	defer done()
	return r.store.RequeueDelivery(ctx, key)
}

//按序处理事件和action，直到没有可执行工作
func (r *Runtime) RunUntilIdle() error {
	return r.RunUntilIdleContext(context.Background())
}

func (r *Runtime) RunUntilIdleContext(ctx context.Context) error {
	done, err := r.enter()
	if err != nil {
		return err
	}
	defer done()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-r.stop:
		return nil
	case <-r.drain:
	}
	defer func() { r.drain <- struct{}{} }()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if r.isStopping() {
			return nil
		}
		deliveries, err := r.store.ListDeliveries(ctx, domain.DeliveryStatusPending)
		if err != nil {
			return err
		}
		progress := false
		for _, delivery := range deliveries {
			if r.isStopping() {
				return nil
			}
			_, err := r.processDelivery(ctx, delivery.Key)
			_, storageFailure := err.(*storeFailureError)
			_, executionFailure := err.(*executionFailureError)
			switch {
			case storageFailure:
				return err
			case err == nil, executionFailure:
				progress = true
			case errors.Is(err, ErrExecutionInProgress), errors.Is(err, ErrAgentUnavailable), errors.Is(err, ErrDeliveryFailed):
				continue
			default:
				return err
			}
		}
		statuses := []domain.ActionStatus{domain.ActionStatusPending}
		if r.session != nil {
			statuses = append(statuses, domain.ActionStatusUnknown)
		}
		actions, err := r.store.ListActions(ctx, statuses...)
		if err != nil {
			return err
		}
		for _, action := range actions {
			if r.isStopping() {
				return nil
			}
			if action.Status == domain.ActionStatusUnknown &&
				(action.RecoveryPolicy != domain.RecoveryPolicySafeRetry || action.AttemptCount >= action.MaxAttempts) {
				continue
			}
			if err := r.executeAction(ctx, action); err != nil {
				if _, storageFailure := err.(*storeFailureError); storageFailure {
					return err
				}
				if errors.Is(err, ErrAgentUnavailable) || errors.Is(err, ErrHandlerUnavailable) || errors.Is(err, ErrExecutionInProgress) {
					continue
				}
				return err
			}
			progress = true
		}
		if !progress {
			return nil
		}
	}
}

//执行记录副本
func (r *Runtime) Executions() map[domain.ID]domain.Execution {
	result, _ := r.ExecutionsContext(context.Background())
	return result
}

func (r *Runtime) ExecutionsContext(ctx context.Context) (map[domain.ID]domain.Execution, error) {
	done, err := r.enter()
	if err != nil {
		return nil, err
	}
	defer done()
	stored, err := r.storedExecutions(ctx)
	if err != nil {
		return nil, err
	}
	result := make(map[domain.ID]domain.Execution, len(stored))
	for _, record := range stored {
		result[record.Execution.ID] = cloneExecution(record.Execution)
	}
	return result, nil
}

func (r *Runtime) Attempts() map[domain.ID]domain.Attempt {
	result, _ := r.AttemptsContext(context.Background())
	return result
}

func (r *Runtime) AttemptsContext(ctx context.Context) (map[domain.ID]domain.Attempt, error) {
	done, err := r.enter()
	if err != nil {
		return nil, err
	}
	defer done()
	stored, err := r.storedExecutions(ctx)
	if err != nil {
		return nil, err
	}
	result := make(map[domain.ID]domain.Attempt)
	for _, record := range stored {
		for _, attempt := range record.Attempts {
			attempt.FinishedAt = cloneTime(attempt.FinishedAt)
			if attempt.Error != nil {
				failure := *attempt.Error
				attempt.Error = &failure
			}
			result[attempt.ID] = attempt
		}
	}
	return result, nil
}

func (r *Runtime) storedExecutions(ctx context.Context) ([]StoredExecution, error) {
	deliveries, err := r.store.ListDeliveries(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]StoredExecution, 0, len(deliveries))
	for _, delivery := range deliveries {
		record, err := r.store.LoadExecution(ctx, delivery.ExecutionID)
		if errors.Is(err, ErrStoreNotFound) && delivery.Status == domain.DeliveryStatusPending {
			continue
		}
		if err != nil {
			return nil, err
		}
		result = append(result, *record)
	}
	return result, nil
}

func validateEvent(event domain.Event) error {
	if event.ID == "" || event.Type == "" {
		return fmt.Errorf("event id/type不能为空")
	}
	if _, err := codec.Encode(event); err != nil {
		return fmt.Errorf("event记录: %w", err)
	}
	return nil
}

//action记录副本
func (r *Runtime) Actions() map[domain.ID]domain.Action {
	result, _ := r.ActionsContext(context.Background())
	return result
}

func (r *Runtime) ActionsContext(ctx context.Context) (map[domain.ID]domain.Action, error) {
	done, err := r.enter()
	if err != nil {
		return nil, err
	}
	defer done()
	records, err := r.store.ListActions(ctx)
	if err != nil {
		return nil, err
	}
	result := make(map[domain.ID]domain.Action, len(records))
	for _, record := range records {
		result[record.Request.ID] = cloneActions([]domain.Action{record.Request})[0]
	}
	return result, nil
}

//action状态、结果和尝试记录
func (r *Runtime) ActionContext(ctx context.Context, actionID domain.ID) (*StoredAction, error) {
	done, err := r.enter()
	if err != nil {
		return nil, err
	}
	defer done()
	return r.store.LoadAction(ctx, actionID)
}
