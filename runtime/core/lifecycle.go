package core

import (
	"agent-runtime/domain"
	"fmt"
)

// LifecycleManager 检查并执行 Agent 的生命周期转换。
type LifecycleManager struct{}

var allowedTransitions = map[domain.AgentStatus]map[domain.AgentStatus]bool{
	domain.AgentStatusCreated:     {domain.AgentStatusActive: true},
	domain.AgentStatusActive:      {domain.AgentStatusPaused: true, domain.AgentStatusTerminating: true},
	domain.AgentStatusPaused:      {domain.AgentStatusActive: true, domain.AgentStatusTerminating: true},
	domain.AgentStatusTerminating: {domain.AgentStatusTerminated: true},
	domain.AgentStatusTerminated:  {},
}

// Transition 将 agent 切换到 target。非法转换不会修改原状态。
func (LifecycleManager) Transition(agent *domain.AgentInstance, target domain.AgentStatus) error {
	if agent == nil {
		return fmt.Errorf("切换 Agent 生命周期: Agent 不能为空")
	}
	if !allowedTransitions[agent.Status][target] {
		return fmt.Errorf("不能从 %s 切换到 %s", agent.Status, target)
	}
	agent.Status = target
	return nil
}
