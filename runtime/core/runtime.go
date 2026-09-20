package core

import (
	"agent-runtime/codec"
	"agent-runtime/domain"
	"fmt"
	"sync"
	"time"
)

type pendingEvent struct {
	agentID domain.ID
	event   domain.Event
}
type pendingAction struct {
	agentID domain.ID
	action  domain.Action
}

// Runtime 组装运行时组件，并提供最小的事件—Action—结果事件闭环。
type Runtime struct {
	registry       *AgentRegistry
	lifecycle      LifecycleManager
	stateManager   StateManager
	executor       *Executor
	mu             sync.Mutex
	definitions    map[domain.DefinitionRef]AgentRunner
	executions     map[domain.ID]domain.Execution
	attempts       map[domain.ID]domain.Attempt
	actions        map[domain.ID]domain.Action
	pending        []pendingEvent
	pendingActions []pendingAction
	agentLocks     map[domain.ID]*sync.Mutex
	drainMu        sync.Mutex
}

func NewRuntime() *Runtime {
	return &Runtime{registry: NewAgentRegistry(), executor: NewExecutor(),
		definitions: make(map[domain.DefinitionRef]AgentRunner), executions: make(map[domain.ID]domain.Execution),
		attempts: make(map[domain.ID]domain.Attempt), actions: make(map[domain.ID]domain.Action),
		agentLocks: make(map[domain.ID]*sync.Mutex)}
}

// Executor 返回 Runtime 使用的 Action 执行器，以便调用方注册能力。
func (r *Runtime) Executor() *Executor { return r.executor }

// Register 保存 Agent 和它的业务实现。新 Agent 会自动从 created 进入 active。
func (r *Runtime) Register(
	agent *domain.AgentInstance,
	runner AgentRunner,
) error {
	if agent == nil || nilRunner(runner) {
		return fmt.Errorf("注册 Agent: Agent 和 runner 不能为空")
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
		return fmt.Errorf("Definition 已注册，请使用 CreateAgent 或 RestoreAgent")
	}
	if err := validateAgentStatus(owned.Status); err != nil {
		return err
	}
	if owned.Status == domain.AgentStatusCreated {
		if err := r.lifecycle.Transition(&owned, domain.AgentStatusActive); err != nil {
			return err
		}
	}
	if err := r.loadAgentLocked(owned); err != nil {
		return err
	}
	r.definitions[owned.Definition] = runner
	return nil
}

func (r *Runtime) loadAgentLocked(agent domain.AgentInstance) error {
	if err := r.registry.Register(&agent); err != nil {
		return err
	}
	r.agentLocks[agent.ID] = &sync.Mutex{}
	return nil
}

// Agent只读快照
func (r *Runtime) Agent(
	agentID domain.ID,
) (AgentSnapshot, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	snapshot, err := r.registry.Get(agentID)
	if err == nil {
		if bindingErr := r.bindingError(snapshot.Definition); bindingErr != nil {
			snapshot.BindingError = bindingErr.Error()
		}
	}
	return snapshot, err
}

// Process 立即处理一条事件。同一个 Agent 的 Process 调用会串行执行。
func (r *Runtime) Process(agentID domain.ID, event domain.Event) (ExecutionResult, error) {
	if err := validateEvent(event); err != nil {
		return ExecutionResult{}, err
	}
	r.mu.Lock()
	agent, err := r.registry.getMutable(agentID)
	if err != nil {
		r.mu.Unlock()
		return ExecutionResult{}, err
	}
	runner, lock := r.definitions[agent.Definition], r.agentLocks[agentID]
	bindingErr := r.bindingError(agent.Definition)
	r.mu.Unlock()
	if bindingErr != nil {
		return ExecutionResult{}, bindingErr
	}
	lock.Lock()
	defer lock.Unlock()
	return r.processLocked(agent, runner, event)
}

func (r *Runtime) processLocked(agent *domain.AgentInstance, runner AgentRunner, event domain.Event) (result ExecutionResult, err error) {
	if agent.Status != domain.AgentStatusActive {
		return ExecutionResult{}, fmt.Errorf("Agent %s 当前状态 %s 不允许执行", agent.ID, agent.Status)
	}
	execution := domain.NewExecution(agent.ID, event.ID)
	execution.AttemptCount = 1
	attemptID, err := domain.NewID()
	if err != nil {
		return ExecutionResult{}, err
	}
	started := time.Now().UTC()
	attempt := domain.Attempt{ID: attemptID, ExecutionID: execution.ID, Number: 1,
		Status: domain.AttemptStatusRunning, StartedAt: started}
	execution.Status = domain.ExecutionStatusRunning
	execution.StartedAt = &started
	r.mu.Lock()
	r.executions[execution.ID] = execution
	r.attempts[attempt.ID] = attempt
	r.mu.Unlock()
	failureKind := domain.ErrorKindRuntime
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("Runner panic: %v", recovered)
			result = ExecutionResult{}
			failureKind = domain.ErrorKindRuntime
		}
		finished := time.Now().UTC()
		execution.FinishedAt = &finished
		attempt.FinishedAt = &finished
		if err != nil {
			execution.Status = domain.ExecutionStatusFailed
			execution.Error = recordText(err.Error())
			attempt.Status = domain.AttemptStatusFailed
			attempt.Error = &domain.Failure{Kind: failureKind, Message: execution.Error}
		} else {
			execution.Status = domain.ExecutionStatusCompleted
			attempt.Status = domain.AttemptStatusSucceeded
			saved := cloneResult(result)
			execution.Result = &saved
		}
		r.mu.Lock()
		r.executions[execution.ID] = execution
		r.attempts[attempt.ID] = attempt
		r.mu.Unlock()
	}()
	result, err = runner.Run(ExecutionContext{Agent: snapshotAgent(*agent), Event: cloneEvent(event),
		ExecutionID: execution.ID, AttemptID: attempt.ID})

	if err != nil {
		failureKind = domain.ErrorKindBusiness
		return ExecutionResult{}, err
	}
	if err = validateResult(result); err != nil {
		return ExecutionResult{}, err
	}
	actions := cloneActions(result.Actions)
	if err = r.commit(agent, execution.ID, result, actions); err != nil {
		return ExecutionResult{}, err
	}
	result = ExecutionResult{StateUpdate: cloneMap(result.StateUpdate), Actions: cloneActions(actions)}
	return result, nil
}

func (r *Runtime) commit(agent *domain.AgentInstance, executionID domain.ID, result ExecutionResult, actions []domain.Action) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	seen := make(map[domain.ID]bool)
	for _, action := range actions {
		if seen[action.ID] {
			return fmt.Errorf("Action 标识重复: %s", action.ID)
		}
		if _, exists := r.actions[action.ID]; exists {
			return fmt.Errorf("Action 标识重复: %s", action.ID)
		}
		seen[action.ID] = true
	}
	if err := r.stateManager.Apply(agent, result); err != nil {
		return err
	}
	for i := range actions {
		actions[i].BindExecution(executionID)
		r.actions[actions[i].ID] = actions[i]
		r.pendingActions = append(r.pendingActions, pendingAction{agent.ID, actions[i]})
	}
	return nil
}

// Submit 将事件放入内存队列；调用 RunUntilIdle 后才处理它。
func (r *Runtime) Submit(agentID domain.ID, event domain.Event) error {
	if err := validateEvent(event); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, err := r.registry.Get(agentID); err != nil {
		return err
	}
	r.pending = append(r.pending, pendingEvent{agentID, cloneEvent(event)})
	return nil
}

// RunUntilIdle 按队列顺序处理事件和 Action，直到没有待处理工作。
func (r *Runtime) RunUntilIdle() error {
	r.drainMu.Lock()
	defer r.drainMu.Unlock()
	for {
		r.mu.Lock()
		if len(r.pending) > 0 {
			item := r.pending[0]
			r.mu.Unlock()
			if _, err := r.Process(item.agentID, item.event); err != nil {
				return err
			}
			r.mu.Lock()
			r.pending = r.pending[1:]
			r.mu.Unlock()
			continue
		}
		if len(r.pendingActions) == 0 {
			r.mu.Unlock()
			return nil
		}
		item := r.pendingActions[0]
		agent, err := r.registry.Get(item.agentID)
		r.mu.Unlock()
		if err != nil {
			return err
		}
		if agent.Status != domain.AgentStatusActive {
			return fmt.Errorf("Agent %s 当前状态 %s 不允许执行 Action", agent.ID, agent.Status)
		}
		event, err := r.executor.Execute(item.action)
		if err != nil {
			return err
		}
		if err := r.Submit(item.agentID, event); err != nil {
			return err
		}
		r.mu.Lock()
		r.pendingActions = r.pendingActions[1:]
		r.mu.Unlock()
	}
}

// Executions 返回执行记录的副本，避免调用方直接修改 Runtime 内部数据。
func (r *Runtime) Executions() map[domain.ID]domain.Execution {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make(map[domain.ID]domain.Execution, len(r.executions))
	for id, execution := range r.executions {
		result[id] = cloneExecution(execution)
	}
	return result
}

func (r *Runtime) Attempts() map[domain.ID]domain.Attempt {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make(map[domain.ID]domain.Attempt, len(r.attempts))
	for id, attempt := range r.attempts {
		attempt.FinishedAt = cloneTime(attempt.FinishedAt)
		if attempt.Error != nil {
			failure := *attempt.Error
			attempt.Error = &failure
		}
		result[id] = attempt
	}
	return result
}

func validateEvent(event domain.Event) error {
	if event.ID == "" || event.Type == "" {
		return fmt.Errorf("Event id/type 不能为空")
	}
	if _, err := codec.Encode(event); err != nil {
		return fmt.Errorf("Event 记录: %w", err)
	}
	return nil
}

// Actions 返回 Action 记录的副本。
func (r *Runtime) Actions() map[domain.ID]domain.Action {
	r.mu.Lock()
	defer r.mu.Unlock()
	result := make(map[domain.ID]domain.Action, len(r.actions))
	for id, action := range r.actions {
		result[id] = cloneActions([]domain.Action{action})[0]
	}
	return result
}
