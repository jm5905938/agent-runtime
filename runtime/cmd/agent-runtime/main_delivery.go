package main

import (
	"agent-runtime/core"
	"agent-runtime/domain"
	"context"
)

type mainAgentRunner struct {
	core.AgentRunner
	prepare func(context.Context, core.AgentSnapshot, domain.Event) error
}

func (runner mainAgentRunner) PrepareDelivery(ctx context.Context, agent core.AgentSnapshot, event domain.Event) error {
	if runner.prepare != nil {
		return runner.prepare(ctx, agent, event)
	}
	return nil
}

func (runner mainAgentRunner) RunContext(ctx context.Context, input core.ExecutionContext) (core.ExecutionResult, error) {
	if contextual, ok := runner.AgentRunner.(core.ContextAgentRunner); ok {
		return contextual.RunContext(ctx, input)
	}
	return runner.Run(input)
}

func (runner mainAgentRunner) DeliveryBlockedBy(agent core.AgentSnapshot, event domain.Event, earlier []domain.Event) []core.BlockReason {
	if event.Type != "main.request" {
		return nil
	}
	if agent.State["request_status"] == "waiting" {
		return []core.BlockReason{{Code: core.BlockAgentWaiting, Message: "当前请求等待模型或工具结果，新输入保留在队列中"}}
	}
	for _, previous := range earlier {
		if previous.Type == "main.request" {
			return []core.BlockReason{{Code: core.BlockEarlierInput, Message: "较早的用户输入尚未处理，按接收顺序等待"}}
		}
	}
	return nil
}
