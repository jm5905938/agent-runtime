package domain

// AgentStatus 表示 Agent 现在能不能接收新工作。
// 它描述的是 Agent 本身，不是某一次执行。
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
	ID     ID             `json:"id"`
	Name   string         `json:"name"`
	Status AgentStatus    `json:"status"`
	State  map[string]any `json:"state"`
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
