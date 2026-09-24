package domain

import "time"

//本轮执行状态，与agent状态独立
type ExecutionStatus string

const (
	ExecutionStatusPending   ExecutionStatus = "pending"
	ExecutionStatusRunning   ExecutionStatus = "running"
	ExecutionStatusCompleted ExecutionStatus = "completed"
	ExecutionStatusFailed    ExecutionStatus = "failed"
)

//agent处理一次投递的记录
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

//创建待执行记录
func NewExecution(agentID, eventID ID) Execution {
	return Execution{
		ID:        mustNewID(),
		AgentID:   agentID,
		EventID:   eventID,
		Status:    ExecutionStatusPending,
		CreatedAt: time.Now().UTC(),
	}
}
