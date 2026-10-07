// core负责运行流程，domain定义数据
package core

import (
	"agent-runtime/domain"
	"context"
)

// 防止修改原agent
type AgentSnapshot struct {
	ID           domain.ID            `json:"id"`
	Name         string               `json:"name"`
	Definition   domain.DefinitionRef `json:"definition"`
	Status       domain.AgentStatus   `json:"status"`
	State        map[string]any       `json:"state"`
	StateVersion uint64               `json:"state_version"`
	BindingError string               `json:"binding_error,omitempty"`
}

// 本轮执行的输入
type ExecutionContext struct {
	Agent       AgentSnapshot `json:"agent"`
	Event       domain.Event  `json:"event"`
	ExecutionID domain.ID     `json:"execution_id"`
	AttemptID   domain.ID     `json:"attempt_id"`
}

// 本轮输出，状态提交后才执行action
type ExecutionResult = domain.ExecutionResult

// agent执行接口
type AgentRunner interface {
	Run(context ExecutionContext) (ExecutionResult, error)
}

// 支持取消的runner，原有接口仍可使用
type ContextAgentRunner interface {
	AgentRunner
	RunContext(context.Context, ExecutionContext) (ExecutionResult, error)
}

// DeliveryGate在领取execution前判断事件是否需要等待；查询使用相同规则。
// earlier是同一agent按接收顺序排列在当前事件之前的pending或running事件。
// 实现只读取传入快照，不执行外部操作或修改runtime。
type DeliveryGate interface {
	DeliveryBlockedBy(agent AgentSnapshot, event domain.Event, earlier []domain.Event) []BlockReason
}

// DeliveryPreparer在投递就绪后、领取execution前准备依赖。
// 准备失败时保留pending，不创建执行或消耗尝试次数。
type DeliveryPreparer interface {
	PrepareDelivery(context.Context, AgentSnapshot, domain.Event) error
}

// 适配层区分业务错误、运行错误和中断
type RunnerFailure interface {
	error
	FailureKind() domain.ErrorKind
}

func snapshotAgent(agent domain.AgentInstance) AgentSnapshot {
	return AgentSnapshot{ID: agent.ID, Name: agent.Name, Definition: agent.Definition,
		Status: agent.Status, State: cloneMap(agent.State), StateVersion: agent.StateVersion}
}
