package runtime

import (
	"agent-runtime/domain"
	"fmt"
)

type AgentRegistry struct {
	agents map[domain.ID]*domain.AgentInstance
}

func NewAgentRegistry() *AgentRegistry {
	return &AgentRegistry{
		agents: make(map[domain.ID]*domain.AgentInstance),
	}
}

func (r *AgentRegistry) Register(agent *domain.AgentInstance) error {
	if agent == nil {
		return fmt.Errorf("注册 Agent: Agent 不能为空")
	}

	if _, exists := r.agents[agent.ID]; exists {
		return fmt.Errorf("Agent %s 已存在", agent.ID)
	}

	owned := *agent
	owned.State = cloneMap(agent.State)

	r.agents[owned.ID] = &owned

	return nil
}

//real
func (r *AgentRegistry) getMutable(
	agentID domain.ID,
) (*domain.AgentInstance, error) {
	agent, exists := r.agents[agentID]
	if !exists {
		return nil, fmt.Errorf("找不到 Agent %s", agentID)
	}

	return agent, nil
}

//快照
func (r *AgentRegistry) Get(
	agentID domain.ID,
) (AgentSnapshot, error) {
	agent, err := r.getMutable(agentID)
	if err != nil {
		return AgentSnapshot{}, err
	}

	return AgentSnapshot{
		ID:     agent.ID,
		Name:   agent.Name,
		Status: agent.Status,
		State:  cloneMap(agent.State),
	}, nil
}

func (r *AgentRegistry) Remove(agentID domain.ID) error {
	if _, exists := r.agents[agentID]; !exists {
		return fmt.Errorf("删除时找不到 Agent %s", agentID)
	}

	delete(r.agents, agentID)

	return nil
}
