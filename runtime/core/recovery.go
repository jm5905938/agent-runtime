package core

import (
	"context"
	"fmt"
	"strings"
	"time"

	"agent-runtime/codec"
	"agent-runtime/domain"
)

func (s *memoryRecoverySession) Recover(ctx context.Context) (RecoveryReport, error) {
	if err := s.lock(ctx, false); err != nil {
		return RecoveryReport{}, err
	}
	defer s.backend.mu.Unlock()
	if s.ready {
		return cloneRecoveryReport(s.report), nil
	}
	store := s.backend.store
	if err := store.lock(ctx); err != nil {
		return RecoveryReport{}, err
	}
	defer store.mu.Unlock()
	if err := validateRecoveryRecords(store); err != nil {
		return RecoveryReport{}, err
	}
	if err := ctx.Err(); err != nil {
		return RecoveryReport{}, err
	}
	now := time.Now().UTC()
	report := RecoveryReport{}
	//关联已全部校验，转换不再包含可失败操作
	for _, key := range store.deliveryOrder {
		delivery := store.deliveries[key]
		if delivery.Status != domain.DeliveryStatusRunning {
			continue
		}
		execution := store.executions[delivery.ExecutionID]
		attempts := store.attempts[execution.ID]
		attempt := &attempts[len(attempts)-1]
		attempt.Status, attempt.FinishedAt = domain.AttemptStatusInterrupted, &now
		attempt.Error = &domain.Failure{Kind: domain.ErrorKindInterrupted, Message: "execution interrupted before commit"}
		execution.Status = domain.ExecutionStatusPending
		execution.StartedAt, execution.FinishedAt, execution.Result, execution.Error = nil, nil, nil, ""
		delivery.Status = domain.DeliveryStatusPending
		store.executions[execution.ID], store.deliveries[key] = execution, delivery
		delete(store.claimVersions, attempt.ID)
		report.RequeuedDeliveries = append(report.RequeuedDeliveries, key)
	}
	for _, id := range store.actionOrder {
		action := store.actions[id]
		if action.Status == domain.ActionStatusRunning {
			attempts := store.actionAttempts[id]
			attempt := &attempts[len(attempts)-1]
			failure := domain.Failure{Kind: domain.ErrorKindInterrupted, Message: "action interrupted before result was saved"}
			attempt.Status, attempt.FinishedAt, attempt.Error = domain.ActionStatusUnknown, &now, memoryCloneFailure(&failure)
			action.Status, action.LastError = domain.ActionStatusUnknown, memoryCloneFailure(&failure)
			store.actions[id] = action
		}
		if action.Status == domain.ActionStatusUnknown {
			report.UnknownActions = append(report.UnknownActions, id)
			if action.RecoveryPolicy == domain.RecoveryPolicySafeRetry && action.AttemptCount < action.MaxAttempts {
				report.RetryableActions = append(report.RetryableActions, id)
			}
		}
	}
	s.report, s.ready = report, true
	return cloneRecoveryReport(report), nil
}

func recoveryConflict(kind string, id any) error {
	return fmt.Errorf("recovery: invalid %s %v: %w", kind, id, ErrStoreConflict)
}

func validateRecoveryRecords(s *MemoryStore) error {
	for id, agent := range s.agents {
		if id == "" || agent.ID != id || agent.Definition.Validate() != nil || validateAgentStatus(agent.Status) != nil {
			return recoveryConflict("agent", id)
		}
		if _, err := codec.Encode(agent); err != nil {
			return recoveryConflict("agent data", id)
		}
	}
	for id, event := range s.events {
		if event.ID != id || validateStoredEvent(event) != nil {
			return recoveryConflict("event", id)
		}
	}
	seenDeliveries := make(map[domain.DeliveryKey]bool)
	seenExecutions := make(map[domain.ID]bool)
	runningAgents := make(map[domain.ID]bool)
	for _, key := range s.deliveryOrder {
		delivery, exists := s.deliveries[key]
		_, hasAgent := s.agents[key.AgentID]
		_, hasEvent := s.events[key.EventID]
		if !exists || seenDeliveries[key] || !hasAgent || !hasEvent || delivery.Key != key || delivery.ExecutionID == "" || seenExecutions[delivery.ExecutionID] {
			return recoveryConflict("delivery", key)
		}
		seenDeliveries[key], seenExecutions[delivery.ExecutionID] = true, true
		execution, exists := s.executions[delivery.ExecutionID]
		if !exists && delivery.Status == domain.DeliveryStatusPending {
			continue
		}
		if !exists || execution.ID != delivery.ExecutionID || execution.AgentID != key.AgentID || execution.EventID != key.EventID || string(execution.Status) != string(delivery.Status) {
			return recoveryConflict("delivery execution", key)
		}
		if delivery.Status == domain.DeliveryStatusRunning {
			if runningAgents[key.AgentID] {
				return recoveryConflict("concurrent execution", key.AgentID)
			}
			runningAgents[key.AgentID] = true
		}
	}
	if len(seenDeliveries) != len(s.deliveries) {
		return recoveryConflict("delivery order", "")
	}
	attemptIDs := make(map[domain.ID]bool)
	for id, execution := range s.executions {
		if !seenExecutions[id] || execution.ID != id {
			return recoveryConflict("execution", id)
		}
		if err := validateRecoveryExecution(s, execution, attemptIDs); err != nil {
			return err
		}
	}
	for id := range s.attempts {
		if _, exists := s.executions[id]; !exists {
			return recoveryConflict("orphan attempts", id)
		}
	}
	for id := range s.claimVersions {
		if !attemptIDs[id] {
			return recoveryConflict("claim version", id)
		}
	}
	seenActions, resultEvents := make(map[domain.ID]bool), make(map[domain.ID]bool)
	for _, id := range s.actionOrder {
		action, exists := s.actions[id]
		if !exists || seenActions[id] || action.Request.ID != id || resultEvents[action.ResultEventID] {
			return recoveryConflict("action", id)
		}
		seenActions[id], resultEvents[action.ResultEventID] = true, true
		if err := validateRecoveryAction(s, action, attemptIDs); err != nil {
			return err
		}
	}
	if len(seenActions) != len(s.actions) {
		return recoveryConflict("action order", "")
	}
	for id := range s.actionAttempts {
		if !seenActions[id] {
			return recoveryConflict("orphan action attempts", id)
		}
	}
	return nil
}

func validateRecoveryExecution(s *MemoryStore, execution domain.Execution, ids map[domain.ID]bool) error {
	id := execution.ID
	attempts := s.attempts[id]
	if len(attempts) == 0 || uint64(len(attempts)) != execution.AttemptCount {
		return recoveryConflict("execution attempts", id)
	}
	if _, err := codec.Encode(execution); err != nil {
		return recoveryConflict("execution data", id)
	}
	for i, attempt := range attempts {
		if attempt.ID == "" || ids[attempt.ID] || attempt.ExecutionID != id || attempt.Number != uint64(i)+1 {
			return recoveryConflict("attempt identity", attempt.ID)
		}
		ids[attempt.ID] = true
		if attempt.Error != nil && validateStoredFailure(*attempt.Error) != nil {
			return recoveryConflict("attempt failure", attempt.ID)
		}
		switch attempt.Status {
		case domain.AttemptStatusRunning:
			version, exists := s.claimVersions[attempt.ID]
			if i != len(attempts)-1 || execution.Status != domain.ExecutionStatusRunning || attempt.FinishedAt != nil || attempt.Error != nil || !exists || version != s.agents[execution.AgentID].StateVersion {
				return recoveryConflict("running attempt", attempt.ID)
			}
		case domain.AttemptStatusSucceeded:
			if i != len(attempts)-1 || execution.Status != domain.ExecutionStatusCompleted || attempt.FinishedAt == nil || attempt.Error != nil {
				return recoveryConflict("succeeded attempt", attempt.ID)
			}
		case domain.AttemptStatusFailed, domain.AttemptStatusInterrupted:
			if attempt.FinishedAt == nil || attempt.Error == nil {
				return recoveryConflict("failed attempt", attempt.ID)
			}
		default:
			return recoveryConflict("attempt status", attempt.ID)
		}
	}
	last := attempts[len(attempts)-1]
	switch execution.Status {
	case domain.ExecutionStatusRunning:
		if last.Status != domain.AttemptStatusRunning || execution.StartedAt == nil || execution.FinishedAt != nil || execution.Result != nil || execution.Error != "" {
			return recoveryConflict("running execution", id)
		}
	case domain.ExecutionStatusPending, domain.ExecutionStatusFailed:
		if (last.Status != domain.AttemptStatusFailed && last.Status != domain.AttemptStatusInterrupted) || execution.Result != nil {
			return recoveryConflict("incomplete execution", id)
		}
		if execution.Status == domain.ExecutionStatusFailed && execution.FinishedAt == nil {
			return recoveryConflict("failed execution", id)
		}
	case domain.ExecutionStatusCompleted:
		if last.Status != domain.AttemptStatusSucceeded || execution.Result == nil || execution.FinishedAt == nil || execution.Error != "" {
			return recoveryConflict("completed execution", id)
		}
		seen := make(map[domain.ID]bool)
		for _, request := range execution.Result.Actions {
			action, exists := s.actions[request.ID]
			equal, err := sameJSONValue(request, action.Request)
			if !exists || seen[request.ID] || err != nil || !equal || action.Request.ExecutionID == nil || *action.Request.ExecutionID != id {
				return recoveryConflict("execution action", request.ID)
			}
			seen[request.ID] = true
		}
	default:
		return recoveryConflict("execution status", id)
	}
	return nil
}

func validateRecoveryAction(s *MemoryStore, action domain.ActionRecord, ids map[domain.ID]bool) error {
	id := action.Request.ID
	if id == "" || strings.TrimSpace(action.Request.Type) == "" || action.Request.ExecutionID == nil || action.ResultEventID == "" ||
		strings.TrimSpace(action.HandlerVersion) == "" || strings.TrimSpace(action.IdempotencyKey) == "" || action.MaxAttempts == 0 || action.AttemptCount > action.MaxAttempts ||
		(action.RecoveryPolicy != domain.RecoveryPolicyManual && action.RecoveryPolicy != domain.RecoveryPolicySafeRetry) {
		return recoveryConflict("action metadata", id)
	}
	execution, exists := s.executions[*action.Request.ExecutionID]
	if !exists || execution.AgentID != action.AgentID || execution.Status != domain.ExecutionStatusCompleted || execution.Result == nil {
		return recoveryConflict("action source", id)
	}
	found := false
	for _, request := range execution.Result.Actions {
		found = found || request.ID == id
	}
	if !found {
		return recoveryConflict("action source result", id)
	}
	if _, err := codec.Encode(action); err != nil {
		return recoveryConflict("action data", id)
	}
	if action.LastError != nil && validateStoredFailure(*action.LastError) != nil {
		return recoveryConflict("action failure", id)
	}
	attempts := s.actionAttempts[id]
	if uint64(len(attempts)) != action.AttemptCount {
		return recoveryConflict("action attempts", id)
	}
	for i, attempt := range attempts {
		if attempt.ID == "" || ids[attempt.ID] || attempt.ActionID != id || attempt.Number != uint64(i)+1 ||
			(i < len(attempts)-1 && attempt.Status != domain.ActionStatusUnknown) ||
			(i == len(attempts)-1 && attempt.Status != action.Status) {
			return recoveryConflict("action attempt", attempt.ID)
		}
		ids[attempt.ID] = true
		if attempt.Error != nil && validateStoredFailure(*attempt.Error) != nil {
			return recoveryConflict("action attempt failure", attempt.ID)
		}
		if attempt.Status == domain.ActionStatusRunning {
			if attempt.FinishedAt != nil || attempt.Error != nil {
				return recoveryConflict("running action attempt", attempt.ID)
			}
		} else if attempt.FinishedAt == nil || (attempt.Status != domain.ActionStatusSucceeded && attempt.Error == nil) {
			return recoveryConflict("finished action attempt", attempt.ID)
		}
	}
	event, hasEvent := s.events[action.ResultEventID]
	_, hasDelivery := s.deliveries[domain.DeliveryKey{AgentID: action.AgentID, EventID: action.ResultEventID}]
	switch action.Status {
	case domain.ActionStatusPending:
		if action.AttemptCount != 0 || action.Result != nil || action.LastError != nil || hasEvent {
			return recoveryConflict("pending action", id)
		}
	case domain.ActionStatusRunning, domain.ActionStatusUnknown:
		if action.AttemptCount == 0 || action.Result != nil || hasEvent || (action.Status == domain.ActionStatusUnknown && action.LastError == nil) {
			return recoveryConflict("incomplete action", id)
		}
	case domain.ActionStatusSucceeded, domain.ActionStatusFailed:
		if action.AttemptCount == 0 || action.Result == nil || action.Result.Status != action.Status || !hasEvent || !hasDelivery {
			return recoveryConflict("completed action", id)
		}
		completion := ActionCompletion{Token: ActionToken{ActionID: id, AttemptNumber: action.AttemptCount}, Result: *action.Result, Event: event}
		if err := validateActionCompletion(action, completion); err != nil {
			return recoveryConflict("action result", id)
		}
	default:
		return recoveryConflict("action status", id)
	}
	return nil
}
