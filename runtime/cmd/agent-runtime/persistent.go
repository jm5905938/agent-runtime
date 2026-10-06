package main

import (
	"agent-runtime/core"
	"agent-runtime/domain"
	"context"
	"strings"
)

// Check before advancing deliveries so missing model configuration leaves the
// request queued. Tool work needs a model for the following turn; a final text
// result can still be delivered without model configuration unless it unlocks
// queued user input.
func modelWorkPending(ctx context.Context, runtime *core.Runtime) (bool, error) {
	agents, err := runtime.AgentsContext(ctx)
	if err != nil {
		return false, err
	}
	for _, agent := range agents {
		if agent.Status != domain.AgentStatusActive {
			continue
		}
		query, err := runtime.QueryAgentContext(ctx, agent.ID)
		if err != nil {
			return false, err
		}
		if queryNeedsModel(query) {
			return true, nil
		}
	}
	return false, nil
}

func modelWorkPendingForAgent(ctx context.Context, runtime *core.Runtime, agentID domain.ID) (bool, error) {
	query, err := runtime.QueryAgentContext(ctx, agentID)
	if err != nil {
		return false, err
	}
	return queryNeedsModel(query), nil
}

func modelWorkPendingForTree(ctx context.Context, runtime *core.Runtime, agentID domain.ID) (bool, error) {
	agents, err := runtime.AgentTreeContext(ctx, agentID)
	if err != nil {
		return false, err
	}
	for _, id := range agents {
		needed, err := modelWorkPendingForAgent(ctx, runtime, id)
		if err != nil || needed {
			return needed, err
		}
	}
	return false, nil
}

func modelAgentDefinition(definition domain.DefinitionRef) bool {
	return definition.Version == "1" && (definition.ID == "main" || definition.ID == "subagent")
}

func modelRequestEvent(agent core.AgentSnapshot, event domain.Event) bool {
	return modelAgentDefinition(agent.Definition) && event.Type == agent.Definition.ID+".request"
}

func queryNeedsModel(query core.AgentQuery) bool {
	agent := query.Agent
	if agent.Status != domain.AgentStatusActive {
		return false
	}
	for _, delivery := range query.Deliveries {
		if deliveryNeedsModel(agent, delivery) {
			return true
		}
	}
	if queuedInputNeedsModel(agent, query.Deliveries) {
		return true
	}
	for _, action := range query.Actions {
		if modelAgentDefinition(agent.Definition) && strings.HasPrefix(action.Action.Request.Type, "tool.") && action.Ready {
			return true
		}
		if action.Action.Request.Type != "model.generate" {
			continue
		}
		if action.Action.Status != domain.ActionStatusPending &&
			(action.Action.Status != domain.ActionStatusUnknown || action.Action.RecoveryPolicy != domain.RecoveryPolicySafeRetry) {
			continue
		}
		// The handler is deliberately unbound until configuration is loaded.
		blocked := false
		for _, reason := range action.BlockedBy {
			if reason.Code != core.BlockHandlerUnavailable {
				blocked = true
				break
			}
		}
		if !blocked {
			return true
		}
	}
	return false
}

func deliveryNeedsModel(agent core.AgentSnapshot, delivery core.DeliveryQuery) bool {
	if !delivery.Ready || !modelAgentDefinition(agent.Definition) {
		return false
	}
	if modelRequestEvent(agent, delivery.Event) {
		return true
	}
	if waitingResolutionMatches(agent, delivery) {
		return delivery.Event.Payload["decision"] == string(core.ResolutionRetry)
	}
	if !waitingResultMatches(agent, delivery) {
		return false
	}
	payload := delivery.Event.Payload
	actionType, _ := payload["action_type"].(string)
	if strings.HasPrefix(actionType, "tool.") {
		return payload["status"] == "succeeded" || payload["status"] == "failed"
	}
	if actionType != "model.generate" || payload["status"] != "succeeded" {
		return false
	}
	result, _ := payload["result"].(map[string]any)
	toolCalls, _ := result["tool_calls"].([]any)
	return len(toolCalls) > 0
}

func waitingResultMatches(agent core.AgentSnapshot, delivery core.DeliveryQuery) bool {
	return delivery.Event.Type == "action.result" && waitingActionMatches(agent, delivery)
}

func waitingResolutionMatches(agent core.AgentSnapshot, delivery core.DeliveryQuery) bool {
	return delivery.Event.Type == core.ActionResolutionEventType && waitingActionMatches(agent, delivery)
}

func waitingActionMatches(agent core.AgentSnapshot, delivery core.DeliveryQuery) bool {
	if !delivery.Ready || !modelAgentDefinition(agent.Definition) ||
		agent.State["request_status"] != "waiting" {
		return false
	}
	payload := delivery.Event.Payload
	if actionID, ok := payload["action_id"].(string); !ok || actionID == "" || agent.State["waiting_action_id"] != actionID {
		return false
	}
	executionID := agent.State["waiting_execution_id"]
	if executionID == nil {
		executionID = agent.State["request_execution_id"]
	}
	if resultExecution, ok := payload["execution_id"].(string); !ok || resultExecution == "" || resultExecution != executionID {
		return false
	}
	waitingType := agent.State["waiting_action_type"]
	if waitingType == nil {
		waitingType = "model.generate"
	}
	actionType, _ := payload["action_type"].(string)
	return waitingType == actionType
}

// 当前模型结果可能在同一次run中解锁后续输入，预检也要覆盖这些输入。
func queuedInputNeedsModel(agent core.AgentSnapshot, deliveries []core.DeliveryQuery) bool {
	queued, resultPending := false, false
	for _, delivery := range deliveries {
		if modelRequestEvent(agent, delivery.Event) && delivery.Delivery.Status == domain.DeliveryStatusPending {
			blocked := false
			for _, reason := range delivery.BlockedBy {
				if reason.Code != core.BlockAgentWaiting && reason.Code != core.BlockEarlierInput {
					blocked = true
					break
				}
			}
			queued = queued || !blocked
		}
		if waitingResultMatches(agent, delivery) && delivery.Event.Payload["action_type"] == "model.generate" {
			status := delivery.Event.Payload["status"]
			resultPending = resultPending || status == "succeeded" || status == "failed"
		}
		if waitingResolutionMatches(agent, delivery) && delivery.Event.Payload["decision"] == string(core.ResolutionAbandon) {
			resultPending = true
		}
	}
	return queued && resultPending
}
