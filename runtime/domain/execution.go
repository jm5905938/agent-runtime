package domain

import "time"

// ExecutionStatus 表示一次工作进行到哪一步。
// 它只描述这一次工作，不表示 Agent 本身的状态。
type ExecutionStatus string

const (
	ExecutionStatusPending   ExecutionStatus = "pending"
	ExecutionStatusRunning   ExecutionStatus = "running"
	ExecutionStatusCompleted ExecutionStatus = "completed"
	ExecutionStatusFailed    ExecutionStatus = "failed"
)

// Execution 是“一次 Agent 处理一条消息”的记录。
// 它记录是谁处理了什么、何时开始、何时结束，以及有没有出错。
type Execution struct {
	ID           ID               `json:"id"`
	AgentID      ID               `json:"agent_id"`
	EventID      ID               `json:"event_id"`
	Status       ExecutionStatus  `json:"status"`
	CreatedAt    time.Time        `json:"created_at"`
	StartedAt    *time.Time       `json:"started_at,omitempty"`
	FinishedAt   *time.Time       `json:"finished_at,omitempty"`
	Error        string           `json:"error,omitempty"`
	AttemptCount uint64           `json:"attempt_count"`
	Result       *ExecutionResult `json:"result,omitempty"`
}

type ExecutionResult struct {
	StateUpdate map[string]any `json:"state_update"`
	Actions     []Action       `json:"actions"`
}

// NewExecution 创建一条等待开始的执行记录。
func NewExecution(agentID, eventID ID) Execution {
	return Execution{
		ID:        mustNewID(),
		AgentID:   agentID,
		EventID:   eventID,
		Status:    ExecutionStatusPending,
		CreatedAt: time.Now().UTC(),
	}
}
