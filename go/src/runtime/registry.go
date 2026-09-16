package runtime

import (
	"agent-runtime-go/src/domain"
	"fmt"
)

// AgentRegistry 用 ID 保存已注册的 Agent。它由 Runtime 的锁保护。
type AgentRegistry struct {
	agents map[domain.ID]*domain.AgentInstance
}

func NewAgentRegistry() *AgentRegistry {
	return &AgentRegistry{agents: make(map[domain.ID]*domain.AgentInstance)}
}

func (r *AgentRegistry) Register(agent *domain.AgentInstance) error {
	if agent == nil {
		return fmt.Errorf("注册 Agent: Agent 不能为空")
	}
	if _, exists := r.agents[agent.ID]; exists {
		return fmt.Errorf("Agent %s 已存在", agent.ID)
	}
	r.agents[agent.ID] = agent
	return nil
}

func (r *AgentRegistry) Get(agentID domain.ID) (*domain.AgentInstance, error) {
	agent, exists := r.agents[agentID]
	if !exists {
		return nil, fmt.Errorf("找不到 Agent %s", agentID)
	}
	return agent, nil
}

func (r *AgentRegistry) Remove(agentID domain.ID) error {
	if _, exists := r.agents[agentID]; !exists {
		return fmt.Errorf("找不到 Agent %s", agentID)
	}
	delete(r.agents, agentID)
	return nil
}
