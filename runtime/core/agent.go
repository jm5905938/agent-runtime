// Package runtime 实现 Agent 运行时的内存版本。
//
// 这个包负责“怎么运行”，domain 包负责“运行时有哪些数据”。
package runtime

import "agent-runtime/domain"

// ExecutionContext 是 Agent 处理一条事件时能看到的输入。
type ExecutionContext struct {
	Agent *domain.AgentInstance
	Event domain.Event
}

// ExecutionResult 是一次 Agent 处理的输出。
// StateUpdate 会合并回 Agent.State；Actions 会在状态提交成功后交给 Executor。
type ExecutionResult struct {
	StateUpdate map[string]any
	Actions     []domain.Action
}

// AgentRunner 是具体 Agent 必须实现的接口。
type AgentRunner interface {
	Run(context ExecutionContext) (ExecutionResult, error)
}
