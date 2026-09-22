package core

import (
	"agent-runtime/domain"
	"fmt"
)

//agent生命周期转换
type LifecycleManager struct{}

var allowedTransitions = map[domain.AgentStatus]map[domain.AgentStatus]bool{
	domain.AgentStatusCreated:     {domain.AgentStatusActive: true},
	domain.AgentStatusActive:      {domain.AgentStatusPaused: true, domain.AgentStatusTerminating: true},
	domain.AgentStatusPaused:      {domain.AgentStatusActive: true, domain.AgentStatusTerminating: true},
	domain.AgentStatusTerminating: {domain.AgentStatusTerminated: true},
	domain.AgentStatusTerminated:  {},
}

//切换状态，非法转换保持原状
func (LifecycleManager) Transition(agent *domain.AgentInstance, target domain.AgentStatus) error {
	if agent == nil {
		return fmt.Errorf("切换 agent 生命周期: agent 不能为空")
	}
	if !allowedTransitions[agent.Status][target] {
		return fmt.Errorf("不能从 %s 切换到 %s", agent.Status, target)
	}
	agent.Status = target
	return nil
}
