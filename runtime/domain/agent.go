package domain

import (
	"fmt"
	"strings"
)

type DefinitionRef struct {
	ID      string `json:"id"`
	Version string `json:"version"`
}

func (d DefinitionRef) Validate() error {
	if strings.TrimSpace(d.ID) == "" {
		return fmt.Errorf("definition id must not be empty")
	}
	if strings.TrimSpace(d.Version) == "" {
		return fmt.Errorf("definition version must not be empty")
	}
	return nil
}

//agent生命周期状态

type AgentStatus string

const (
	AgentStatusCreated     AgentStatus = "created"
	AgentStatusActive      AgentStatus = "active"
	AgentStatusPaused      AgentStatus = "paused"
	AgentStatusTerminating AgentStatus = "terminating"
	AgentStatusTerminated  AgentStatus = "terminated"
)

//agent实例及其状态
type AgentInstance struct {
	ID           ID             `json:"id"`
	Name         string         `json:"name"`
	Definition   DefinitionRef  `json:"definition"`
	Status       AgentStatus    `json:"status"`
	State        map[string]any `json:"state"`
	StateVersion uint64         `json:"state_version"`
}

//创建agent，初始状态为created
func NewAgentInstance(name string) AgentInstance {
	return AgentInstance{
		ID:     mustNewID(),
		Name:   name,
		Status: AgentStatusCreated,
		State:  make(map[string]any),
	}
}
