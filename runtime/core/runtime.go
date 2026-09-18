package runtime

import (
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
	runners        map[domain.ID]AgentRunner
	executions     map[domain.ID]domain.Execution
	actions        map[domain.ID]domain.Action
	pending        []pendingEvent
	pendingActions []pendingAction
	agentLocks     map[domain.ID]*sync.Mutex
	drainMu        sync.Mutex
}

func NewRuntime() *Runtime {
	return &Runtime{registry: NewAgentRegistry(), executor: NewExecutor(), runners: make(map[domain.ID]AgentRunner), executions: make(map[domain.ID]domain.Execution), actions: make(map[domain.ID]domain.Action), agentLocks: make(map[domain.ID]*sync.Mutex)}
}

// Executor 返回 Runtime 使用的 Action 执行器，以便调用方注册能力。
func (r *Runtime) Executor() *Executor { return r.executor }

// Register 保存 Agent 和它的业务实现。新 Agent 会自动从 created 进入 active。
func (r *Runtime) Register(
	agent *domain.AgentInstance,
	runner AgentRunner,
) error {
	if runner == nil {
		return fmt.Errorf("注册 Agent: runner 不能为空")
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if err := r.registry.Register(agent); err != nil {
		return err
	}

	stored, err := r.registry.getMutable(agent.ID)
	if err != nil {
		return err
	}

	r.runners[agent.ID] = runner
	r.agentLocks[agent.ID] = &sync.Mutex{}

	if stored.Status == domain.AgentStatusCreated {
		if err := r.lifecycle.Transition(
			stored,
			domain.AgentStatusActive,
		); err != nil {
			return err
		}
	}

	return nil
}
// Agent只读快照
func (r *Runtime) Agent(
	agentID domain.ID,
) (AgentSnapshot, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.registry.Get(agentID)
}
// Process 立即处理一条事件。同一个 Agent 的 Process 调用会串行执行。
func (r *Runtime) Process(agentID domain.ID, event domain.Event) (ExecutionResult, error) {
	r.mu.Lock()
	agent, err := r.registry.getMutable(agentID)
	runner, lock := r.runners[agentID], r.agentLocks[agentID]
	r.mu.Unlock()
	if err != nil {
		return ExecutionResult{}, err
	}
	if runner == nil || lock == nil {
		return ExecutionResult{}, fmt.Errorf("Agent %s 没有注册运行器", agentID)
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
	started := time.Now().UTC()
	execution.Status = domain.ExecutionStatusRunning
	execution.StartedAt = &started
	r.mu.Lock()
	r.executions[execution.ID] = execution
	r.mu.Unlock()
	defer func() {
		finished := time.Now().UTC()
		execution.FinishedAt = &finished
		if err != nil {
			execution.Status = domain.ExecutionStatusFailed
			execution.Error = err.Error()
		} else {
			execution.Status = domain.ExecutionStatusCompleted
		}
		r.mu.Lock()
		r.executions[execution.ID] = execution
		r.mu.Unlock()
	}()
	result, err = runner.Run(ExecutionContext{Agent: AgentSnapshot{
		ID: agent.ID,
		Name: agent.Name,
		Status: agent.Status,
		State: cloneMap(agent.State)}, Event: cloneEvent(event)})

	if err != nil {
		return ExecutionResult{}, err
	}
	actions := cloneActions(result.Actions)
	if err = r.commit(agent, execution.ID, result, actions); err != nil {
		return ExecutionResult{}, err
	}
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
	r.stateManager.Apply(agent, result)
	for i := range actions {
		actions[i].BindExecution(executionID)
		r.actions[actions[i].ID] = actions[i]
		r.pendingActions = append(r.pendingActions, pendingAction{agent.ID, actions[i]})
	}
	return nil
}

// Submit 将事件放入内存队列；调用 RunUntilIdle 后才处理它。
func (r *Runtime) Submit(agentID domain.ID, event domain.Event) error {
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
		result[id] = execution
	}
	return result
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

