package main

import (
	"agent-runtime/core"
	"agent-runtime/domain"
	"context"
)

// Check before advancing deliveries so missing model configuration leaves the
// request queued. Tool work needs a model for the following turn; a final text
// result can still be delivered without model configuration.
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
		for _, delivery := range query.Deliveries {
			if deliveryNeedsModel(agent, delivery) {
				return true, nil
			}
		}
		for _, action := range query.Actions {
			if agent.Definition == (domain.DefinitionRef{ID: "main", Version: "1"}) &&
				action.Action.Request.Type == agentStatusActionType && action.Ready {
				return true, nil
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
				return true, nil
			}
		}
	}
	return false, nil
}

func deliveryNeedsModel(agent core.AgentSnapshot, delivery core.DeliveryQuery) bool {
	if !delivery.Ready || agent.Definition != (domain.DefinitionRef{ID: "main", Version: "1"}) {
		return false
	}
	if delivery.Event.Type == "main.request" {
		return true
	}
	if delivery.Event.Type != "action.result" || agent.State["request_status"] != "waiting" {
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
	actionType, _ := payload["action_type"].(string)
	if waitingType := agent.State["waiting_action_type"]; waitingType != nil && waitingType != actionType {
		return false
	}
	if actionType == agentStatusActionType {
		return payload["status"] == "succeeded" || payload["status"] == "failed"
	}
	if actionType != "model.generate" || payload["status"] != "succeeded" {
		return false
	}
	result, _ := payload["result"].(map[string]any)
	toolCalls, _ := result["tool_calls"].([]any)
	return len(toolCalls) > 0
}
