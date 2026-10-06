package core

import "agent-runtime/domain"

// 恢复前的完整记录快照，delivery和action保持持久化顺序
// attempt按各自Number升序提供，ClaimVersions只包含运行中的execution attempt
// 校验不修改快照，也不执行恢复转换
type RecoveryRecords struct {
	Agents         []domain.AgentInstance
	Events         []domain.Event
	Deliveries     []domain.Delivery
	Executions     []domain.Execution
	Attempts       []domain.Attempt
	ClaimVersions  map[domain.ID]uint64
	Actions        []domain.ActionRecord
	ActionAttempts []domain.ActionAttempt
	Tasks          []domain.SubagentTask
}

func ValidateRecoveryRecords(records RecoveryRecords) error {
	store := NewMemoryStore()
	for _, agent := range records.Agents {
		if _, exists := store.agents[agent.ID]; exists {
			return recoveryConflict("重复agent", agent.ID)
		}
		store.agents[agent.ID] = agent
	}
	for _, event := range records.Events {
		if _, exists := store.events[event.ID]; exists {
			return recoveryConflict("重复event", event.ID)
		}
		store.events[event.ID] = event
	}
	for _, delivery := range records.Deliveries {
		if _, exists := store.deliveries[delivery.Key]; exists {
			return recoveryConflict("重复delivery", delivery.Key)
		}
		store.deliveries[delivery.Key] = delivery
		store.deliveryOrder = append(store.deliveryOrder, delivery.Key)
	}
	for _, execution := range records.Executions {
		if _, exists := store.executions[execution.ID]; exists {
			return recoveryConflict("重复execution", execution.ID)
		}
		store.executions[execution.ID] = execution
	}
	for _, attempt := range records.Attempts {
		store.attempts[attempt.ExecutionID] = append(store.attempts[attempt.ExecutionID], attempt)
	}
	store.claimVersions = records.ClaimVersions
	for _, action := range records.Actions {
		if _, exists := store.actions[action.Request.ID]; exists {
			return recoveryConflict("重复action", action.Request.ID)
		}
		store.actions[action.Request.ID] = action
		store.actionOrder = append(store.actionOrder, action.Request.ID)
	}
	for _, attempt := range records.ActionAttempts {
		store.actionAttempts[attempt.ActionID] = append(store.actionAttempts[attempt.ActionID], attempt)
	}
	for _, task := range records.Tasks {
		if _, exists := store.tasks[task.ID]; exists {
			return recoveryConflict("重复subagent任务", task.ID)
		}
		store.tasks[task.ID] = cloneSubagentTask(task)
	}
	return validateRecoveryRecords(store)
}
