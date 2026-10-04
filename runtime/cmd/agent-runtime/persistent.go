package main

import (
	"agent-runtime/core"
	"agent-runtime/domain"
	"context"
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
		for _, delivery := range query.Deliveries {
			if deliveryNeedsModel(agent, delivery) {
				return true, nil
			}
		}
		if queuedInputNeedsModel(agent, query.Deliveries) {
			return true, nil
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
	if !waitingResultMatches(agent, delivery) {
		return false
	}
	payload := delivery.Event.Payload
	actionType, _ := payload["action_type"].(string)
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

func waitingResultMatches(agent core.AgentSnapshot, delivery core.DeliveryQuery) bool {
	if !delivery.Ready || agent.Definition != (domain.DefinitionRef{ID: "main", Version: "1"}) ||
		delivery.Event.Type != "action.result" || agent.State["request_status"] != "waiting" {
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
		if delivery.Event.Type == "main.request" && delivery.Delivery.Status == domain.DeliveryStatusPending {
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
	}
	return queued && resultPending
}
