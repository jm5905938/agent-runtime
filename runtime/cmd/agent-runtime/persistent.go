package main

import (
	"agent-runtime/core"
	"agent-runtime/domain"
	"context"
)

// Check before advancing deliveries so missing model configuration leaves the
// request queued. Completed actions and result-only deliveries need no model.
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
			if delivery.Ready && delivery.Event.Type == "main.request" &&
				agent.Definition == (domain.DefinitionRef{ID: "main", Version: "1"}) {
				return true, nil
			}
		}
		for _, action := range query.Actions {
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
