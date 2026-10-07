package core

import (
	"agent-runtime/domain"
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
)

type QueryBlockCode string

const (
	BlockAgentInactive         QueryBlockCode = "agent_inactive"
	BlockDefinitionUnavailable QueryBlockCode = "definition_unavailable"
	BlockHandlerUnavailable    QueryBlockCode = "handler_unavailable"
	BlockExecutionRunning      QueryBlockCode = "execution_running"
	BlockActionRunning         QueryBlockCode = "action_running"
	BlockRetryRequired         QueryBlockCode = "retry_required"
	BlockManualUnknown         QueryBlockCode = "manual_unknown"
	BlockAttemptsExhausted     QueryBlockCode = "attempts_exhausted"
	BlockRecoveryRequired      QueryBlockCode = "recovery_required"
	BlockAgentWaiting          QueryBlockCode = "agent_waiting_result"
	BlockEarlierInput          QueryBlockCode = "earlier_input_pending"
	BlockActionResolved        QueryBlockCode = "action_resolved"
	BlockSubagentWaiting       QueryBlockCode = "subagent_waiting"
	BlockSubagentCancelling    QueryBlockCode = "subagent_cancelling"
)

type BlockReason struct {
	Code    QueryBlockCode `json:"code"`
	Message string         `json:"message"`
}

type AgentQuery struct {
	Agent           AgentSnapshot         `json:"agent"`
	Deliveries      []DeliveryQuery       `json:"deliveries"`
	Actions         []ActionQuery         `json:"actions"`
	StartupRecovery RecoveryReport        `json:"startup_recovery"`
	Tasks           []domain.SubagentTask `json:"tasks,omitempty"`
}

type DeliveryQuery struct {
	Delivery  domain.Delivery   `json:"delivery"`
	Event     domain.Event      `json:"event"`
	Execution *domain.Execution `json:"execution,omitempty"`
	Attempts  []domain.Attempt  `json:"attempts"`
	Ready     bool              `json:"ready"`
	BlockedBy []BlockReason     `json:"blocked_by"`
}

type ActionQuery struct {
	Action     domain.ActionRecord    `json:"action"`
	Attempts   []domain.ActionAttempt `json:"attempts"`
	Ready      bool                   `json:"ready"`
	BlockedBy  []BlockReason          `json:"blocked_by"`
	Resolution *ActionResolution      `json:"resolution,omitempty"`
}

func (r *Runtime) Agents() ([]AgentSnapshot, error) {
	return r.AgentsContext(context.Background())
}

func (r *Runtime) AgentsContext(ctx context.Context) ([]AgentSnapshot, error) {
	done, err := r.enter()
	if err != nil {
		return nil, err
	}
	defer done()
	agents, err := r.store.ListAgents(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]AgentSnapshot, 0, len(agents))
	for _, agent := range agents {
		result = append(result, r.queryAgentSnapshot(agent))
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, nil
}

func (r *Runtime) QueryAgent(agentID domain.ID) (AgentQuery, error) {
	return r.QueryAgentContext(context.Background(), agentID)
}

func (r *Runtime) QueryAgentContext(ctx context.Context, agentID domain.ID) (AgentQuery, error) {
	done, err := r.enter()
	if err != nil {
		return AgentQuery{}, err
	}
	defer done()
	agent, err := r.store.LoadAgent(ctx, agentID)
	if err != nil {
		return AgentQuery{}, err
	}
	result := AgentQuery{
		Agent: r.queryAgentSnapshot(*agent), Deliveries: make([]DeliveryQuery, 0),
		Actions: make([]ActionQuery, 0), StartupRecovery: r.RecoveryReport(),
	}
	if store, ok := r.store.(SubagentStore); ok {
		tasks, err := store.ListSubagentTasks(ctx)
		if err != nil {
			return AgentQuery{}, err
		}
		for _, task := range tasks {
			if task.ParentAgentID == agentID || task.ChildAgentID == agentID {
				result.Tasks = append(result.Tasks, cloneSubagentTask(task))
			}
		}
	}
	cancelled, err := r.subagentCancelled(ctx, agentID)
	if err != nil {
		return AgentQuery{}, err
	}
	deliveries, err := r.store.ListDeliveries(ctx)
	if err != nil {
		return AgentQuery{}, err
	}
	running := false
	for _, delivery := range deliveries {
		if delivery.Key.AgentID != agentID {
			continue
		}
		query, err := r.queryDelivery(ctx, delivery)
		if err != nil {
			return AgentQuery{}, err
		}
		running = running || delivery.Status == domain.DeliveryStatusRunning
		result.Deliveries = append(result.Deliveries, query)
	}
	r.mu.Lock()
	runner := r.definitions[agent.Definition]
	r.mu.Unlock()
	var earlier []domain.Event
	for i := range result.Deliveries {
		query := &result.Deliveries[i]
		query.setReadiness(result.Agent, running)
		if query.Delivery.Status == domain.DeliveryStatusPending {
			if cancelled {
				query.BlockedBy = append(query.BlockedBy, BlockReason{Code: BlockSubagentCancelling, Message: "subagent取消中，已停止新工作"})
			}
			blocked, err := deliveryBlockedBy(runner, result.Agent, query.Event, earlier)
			if err != nil {
				return AgentQuery{}, err
			}
			query.BlockedBy = append(query.BlockedBy, blocked...)
			query.Ready = len(query.BlockedBy) == 0
		}
		if query.Delivery.Status == domain.DeliveryStatusPending || query.Delivery.Status == domain.DeliveryStatusRunning {
			earlier = append(earlier, query.Event)
		}
	}
	actions, err := r.store.ListActions(ctx)
	if err != nil {
		return AgentQuery{}, err
	}
	resolutionEvents := make(map[domain.ID]domain.Event)
	for _, delivery := range result.Deliveries {
		if delivery.Event.Type == ActionResolutionEventType {
			resolutionEvents[delivery.Event.ID] = delivery.Event
		}
	}
	for _, action := range actions {
		if action.AgentID != agentID {
			continue
		}
		saved, err := r.store.LoadAction(ctx, action.Request.ID)
		if err != nil {
			return AgentQuery{}, err
		}
		query := ActionQuery{
			Action:   memoryCloneActionRecord(saved.Action),
			Attempts: make([]domain.ActionAttempt, len(saved.Attempts)), BlockedBy: make([]BlockReason, 0),
		}
		for i, attempt := range saved.Attempts {
			attempt.FinishedAt, attempt.Error = cloneTime(attempt.FinishedAt), memoryCloneFailure(attempt.Error)
			query.Attempts[i] = attempt
		}
		sort.Slice(query.Attempts, func(i, j int) bool { return query.Attempts[i].Number < query.Attempts[j].Number })
		if event, exists := resolutionEvents[actionResolutionEventID(query.Action.Request.ID)]; exists {
			query.Resolution = resolutionFromEvent(query.Action, event)
		}
		r.setActionReadiness(&query, result.Agent)
		if query.Ready {
			if cancelled {
				query.BlockedBy = append(query.BlockedBy, BlockReason{Code: BlockSubagentCancelling, Message: "subagent取消中，已停止新工作"})
			}
			handler, err := r.executor.handlerFor(query.Action)
			if err != nil {
				return AgentQuery{}, err
			}
			if gate, ok := handler.(ActionGate); ok {
				blocked, err := gate.ActionBlockedBy(ctx, query.Action)
				if err != nil {
					return AgentQuery{}, err
				}
				query.BlockedBy = append(query.BlockedBy, blocked...)
			}
			query.Ready = len(query.BlockedBy) == 0
		}
		result.Actions = append(result.Actions, query)
	}
	return result, nil
}

func (r *Runtime) queryAgentSnapshot(agent domain.AgentInstance) AgentSnapshot {
	snapshot := snapshotAgent(agent)
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.bindingError(agent.Definition); err != nil {
		snapshot.BindingError = err.Error()
	}
	return snapshot
}

func (r *Runtime) queryDelivery(ctx context.Context, delivery domain.Delivery) (DeliveryQuery, error) {
	event, err := r.store.LoadEvent(ctx, delivery.Key.EventID)
	if err != nil {
		return DeliveryQuery{}, err
	}
	query := DeliveryQuery{
		Delivery: delivery, Event: cloneEvent(*event), Attempts: make([]domain.Attempt, 0), BlockedBy: make([]BlockReason, 0),
	}
	saved, err := r.store.LoadExecution(ctx, delivery.ExecutionID)
	if errors.Is(err, ErrStoreNotFound) && delivery.Status == domain.DeliveryStatusPending {
		return query, nil
	}
	if err != nil {
		return DeliveryQuery{}, err
	}
	execution := cloneExecution(saved.Execution)
	query.Execution = &execution
	for _, attempt := range saved.Attempts {
		query.Attempts = append(query.Attempts, memoryCloneAttempt(attempt))
	}
	sort.Slice(query.Attempts, func(i, j int) bool { return query.Attempts[i].Number < query.Attempts[j].Number })
	return query, nil
}

func (q *DeliveryQuery) setReadiness(agent AgentSnapshot, running bool) {
	switch q.Delivery.Status {
	case domain.DeliveryStatusCompleted:
		return
	case domain.DeliveryStatusFailed:
		q.BlockedBy = append(q.BlockedBy, BlockReason{BlockRetryRequired, "delivery失败，需要显式重试"})
		return
	case domain.DeliveryStatusRunning:
		q.BlockedBy = append(q.BlockedBy, BlockReason{BlockExecutionRunning, "execution进行中"})
		return
	case domain.DeliveryStatusPending:
		q.BlockedBy = append(q.BlockedBy, queryAgentBlockers(agent)...)
		if agent.BindingError != "" {
			q.BlockedBy = append(q.BlockedBy, BlockReason{BlockDefinitionUnavailable, agent.BindingError})
		}
		if running {
			q.BlockedBy = append(q.BlockedBy, BlockReason{BlockExecutionRunning, "当前agent已有execution执行中"})
		}
		if q.Execution != nil && q.Execution.AttemptCount == math.MaxUint64 {
			q.BlockedBy = append(q.BlockedBy, BlockReason{BlockAttemptsExhausted, "execution已达尝试上限"})
		}
		q.Ready = len(q.BlockedBy) == 0
	}
}

func (r *Runtime) setActionReadiness(q *ActionQuery, agent AgentSnapshot) {
	if q.Resolution != nil {
		q.BlockedBy = append(q.BlockedBy, BlockReason{BlockActionResolved, "已保存处理决定，原调用结果仍未知"})
		return
	}
	switch q.Action.Status {
	case domain.ActionStatusSucceeded, domain.ActionStatusFailed:
		return
	case domain.ActionStatusRunning:
		q.BlockedBy = append(q.BlockedBy, BlockReason{BlockActionRunning, "action进行中"})
		return
	case domain.ActionStatusPending, domain.ActionStatusUnknown:
		q.BlockedBy = append(q.BlockedBy, queryAgentBlockers(agent)...)
		if _, err := r.executor.handlerFor(q.Action); err != nil {
			q.BlockedBy = append(q.BlockedBy, BlockReason{BlockHandlerUnavailable, err.Error()})
		}
		if q.Action.Status == domain.ActionStatusUnknown {
			if q.Action.RecoveryPolicy != domain.RecoveryPolicySafeRetry {
				q.BlockedBy = append(q.BlockedBy, BlockReason{BlockManualUnknown, "action结果未知，需手动处理"})
			} else if r.session == nil {
				q.BlockedBy = append(q.BlockedBy, BlockReason{BlockRecoveryRequired, "无恢复会话，无法自动重试未知action"})
			}
		}
		if q.Action.AttemptCount >= q.Action.MaxAttempts {
			q.BlockedBy = append(q.BlockedBy, BlockReason{BlockAttemptsExhausted, "action已达尝试上限"})
		}
		q.Ready = len(q.BlockedBy) == 0
	}
}

func queryAgentBlockers(agent AgentSnapshot) []BlockReason {
	if agent.Status != domain.AgentStatusActive {
		return []BlockReason{{BlockAgentInactive, fmt.Sprintf("agent %s当前状态%s", agent.ID, agent.Status)}}
	}
	return nil
}
