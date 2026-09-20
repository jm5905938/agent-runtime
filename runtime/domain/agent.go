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

// AgentStatus描述Agent本身

type AgentStatus string

const (
	AgentStatusCreated     AgentStatus = "created"
	AgentStatusActive      AgentStatus = "active"
	AgentStatusPaused      AgentStatus = "paused"
	AgentStatusTerminating AgentStatus = "terminating"
	AgentStatusTerminated  AgentStatus = "terminated"
)

// AgentInstance 就是一个 Agent。
// 它有自己的编号、名字、当前状态和记忆（State）。
// 不同 Agent 可以在 State 中保存不同的数据。
type AgentInstance struct {
	ID           ID             `json:"id"`
	Name         string         `json:"name"`
	Definition   DefinitionRef  `json:"definition"`
	Status       AgentStatus    `json:"status"`
	State        map[string]any `json:"state"`
	StateVersion uint64         `json:"state_version"`
}

// NewAgentInstance 创建一个新的 Agent。
// 新 Agent 一开始是 Created，注册成功后通常会变成 Active。
func NewAgentInstance(name string) AgentInstance {
	return AgentInstance{
		ID:     mustNewID(),
		Name:   name,
		Status: AgentStatusCreated,
		State:  make(map[string]any),
	}
}
