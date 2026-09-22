package core

import (
	"agent-runtime/codec"
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
		return fmt.Errorf("注册 agent: agent 不能为空")
	}
	if agent.ID == "" {
		return fmt.Errorf("注册 agent: id 不能为空")
	}
	if _, err := codec.Encode(agent); err != nil {
		return fmt.Errorf("注册 agent 记录: %w", err)
	}

	if _, exists := r.agents[agent.ID]; exists {
		return fmt.Errorf("agent %s 已存在", agent.ID)
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
		return nil, fmt.Errorf("找不到 agent %s", agentID)
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

	return snapshotAgent(*agent), nil
}

func snapshotAgent(agent domain.AgentInstance) AgentSnapshot {
	return AgentSnapshot{ID: agent.ID, Name: agent.Name, Definition: agent.Definition,
		Status: agent.Status, State: cloneMap(agent.State), StateVersion: agent.StateVersion}
}

func (r *AgentRegistry) Remove(agentID domain.ID) error {
	if _, exists := r.agents[agentID]; !exists {
		return fmt.Errorf("删除时找不到 agent %s", agentID)
	}

	delete(r.agents, agentID)

	return nil
}
