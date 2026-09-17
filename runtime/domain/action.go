package domain

// Action 是 Agent 想让外部世界做的一件事。
// 例如发消息、写文件或调用接口。
// Agent 先提出 Action，之后由 runtime 去执行它。
type Action struct {
	ID          ID             `json:"id"`
	ExecutionID *ID            `json:"execution_id,omitempty"`
	Type        string         `json:"type"`
	Payload     map[string]any `json:"payload"`
}

// NewAction 创建一件新的外部操作。
func NewAction(actionType string, payload map[string]any) Action {
	return Action{
		ID:      mustNewID(),
		Type:    actionType,
		Payload: payload,
	}
}

// BindExecution 记录是哪一次执行产生了这个操作。
func (a *Action) BindExecution(executionID ID) {
	id := executionID
	a.ExecutionID = &id
}
