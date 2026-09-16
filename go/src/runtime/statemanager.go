package runtime

import "agent-runtime-go/src/domain"

// StateManager 统一将 Agent 返回的状态增量写回 Agent。
// 当前是内存实现；以后可在这里替换为带事务的数据库提交。
type StateManager struct{}

func (StateManager) Apply(agent *domain.AgentInstance, result ExecutionResult) {
	if agent.State == nil {
		agent.State = make(map[string]any)
	}
	for key, value := range result.StateUpdate {
		agent.State[key] = value
	}
}
