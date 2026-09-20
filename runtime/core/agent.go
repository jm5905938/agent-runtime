// Package core 实现 Agent 运行时的内存版本。
//
// 这个包负责“怎么运行”，domain 包负责“运行时有哪些数据”。
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

// ExecutionContext 是 Agent 处理一条事件时能看到的输入。
type ExecutionContext struct {
	Agent       AgentSnapshot `json:"agent"`
	Event       domain.Event  `json:"event"`
	ExecutionID domain.ID     `json:"execution_id"`
	AttemptID   domain.ID     `json:"attempt_id"`
}

// ExecutionResult 是一次 Agent 处理的输出。
// StateUpdate 会合并回 Agent.State；Actions 会在状态提交成功后交给 Executor。
type ExecutionResult = domain.ExecutionResult

// AgentRunner 是具体 Agent 必须实现的接口。
type AgentRunner interface {
	Run(context ExecutionContext) (ExecutionResult, error)
}
