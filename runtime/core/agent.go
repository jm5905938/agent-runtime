//core负责运行流程，domain定义数据
package core

import "agent-runtime/domain"

//防止修改原agent
type AgentSnapshot struct {
	ID           domain.ID            `json:"id"`
	Name         string               `json:"name"`
	Definition   domain.DefinitionRef `json:"definition"`
	Status       domain.AgentStatus   `json:"status"`
	State        map[string]any       `json:"state"`
	StateVersion uint64               `json:"state_version"`
	BindingError string               `json:"binding_error,omitempty"`
}

//本轮执行的输入
type ExecutionContext struct {
	Agent       AgentSnapshot `json:"agent"`
	Event       domain.Event  `json:"event"`
	ExecutionID domain.ID     `json:"execution_id"`
	AttemptID   domain.ID     `json:"attempt_id"`
}

//本轮输出，状态提交后才执行action
type ExecutionResult = domain.ExecutionResult

//agent执行接口
type AgentRunner interface {
	Run(context ExecutionContext) (ExecutionResult, error)
}
