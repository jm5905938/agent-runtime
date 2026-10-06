package domain

type SubagentStatus string

const (
	SubagentStatusSucceeded SubagentStatus = "succeeded"
	SubagentStatusFailed    SubagentStatus = "failed"
	SubagentStatusCancelled SubagentStatus = "cancelled"
)

type SubagentResult struct {
	Status SubagentStatus `json:"status"`
	Output map[string]any `json:"output"`
	Error  *Failure       `json:"error,omitempty"`
}

type SubagentTask struct {
	ID                    ID              `json:"id"`
	ParentAgentID         ID              `json:"parent_agent_id"`
	ChildAgentID          ID              `json:"child_agent_id"`
	InitialEventID        ID              `json:"initial_event_id"`
	CompletionExecutionID ID              `json:"completion_execution_id,omitempty"`
	CancelRequested       bool            `json:"cancel_requested,omitempty"`
	Result                *SubagentResult `json:"result,omitempty"`
}
