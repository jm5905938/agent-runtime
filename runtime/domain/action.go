package domain

//agent提出的外部操作，由runtime执行
type Action struct {
	ID          ID             `json:"id"`
	ExecutionID *ID            `json:"execution_id,omitempty"`
	Type        string         `json:"type"`
	Payload     map[string]any `json:"payload"`
}

//创建外部操作
func NewAction(actionType string, payload map[string]any) Action {
	return Action{
		ID:      mustNewID(),
		Type:    actionType,
		Payload: payload,
	}
}

//绑定来源执行
func (a *Action) BindExecution(executionID ID) {
	id := executionID
	a.ExecutionID = &id
}
