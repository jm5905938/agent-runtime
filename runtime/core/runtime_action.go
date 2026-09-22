package core

import (
	"agent-runtime/codec"
	"agent-runtime/domain"
	"context"
	"errors"
	"fmt"
	"time"
)

func (r *Runtime) executeAction(ctx context.Context, record domain.ActionRecord) error {
	agent, err := r.store.LoadAgent(ctx, record.AgentID)
	if err != nil {
		return &storeFailureError{cause: err}
	}
	if agent.Status != domain.AgentStatusActive {
		return fmt.Errorf("%w: agent %s 当前状态 %s", ErrAgentUnavailable, agent.ID, agent.Status)
	}
	handler, err := r.executor.handlerFor(record)
	if err != nil {
		return err
	}
	claim, err := r.store.ClaimAction(ctx, record.Request.ID)
	if err != nil {
		if errors.Is(err, ErrStoreConflict) || errors.Is(err, ErrStoreStaleClaim) {
			latest, readErr := r.store.LoadAction(ctx, record.Request.ID)
			if readErr != nil {
				return &storeFailureError{cause: readErr}
			}
			if latest.Action.Status != domain.ActionStatusPending {
				return ErrExecutionInProgress
			}
		}
		return &storeFailureError{cause: err}
	}
	if err := ctx.Err(); err != nil {
		if saveErr := r.recordActionUnknown(ctx, claim.Token, domain.Failure{
			Kind: domain.ErrorKindInterrupted, Message: recordText(err.Error()),
		}); saveErr != nil {
			return &storeFailureError{cause: errors.Join(err, saveErr)}
		}
		return err
	}
	output, cause, unknown := callHandler(handler, claim.Record.Request)
	if cause == nil {
		if err := codec.ValidateData(output); err != nil {
			cause, unknown = fmt.Errorf("handler 返回不支持的业务数据: %w", err), true
		}
	}
	if unknown {
		err := r.recordActionUnknown(ctx, claim.Token, domain.Failure{
			Kind: domain.ErrorKindUnknown, Message: recordText(cause.Error()),
		})
		if err != nil {
			return &storeFailureError{cause: err}
		}
		return nil
	}
	record = claim.Record
	result := domain.ActionResult{ActionID: record.Request.ID, EventID: record.ResultEventID,
		Status: domain.ActionStatusSucceeded, Output: cloneMap(output)}
	payload := map[string]any{
		"action_id": string(record.Request.ID), "action_type": record.Request.Type,
		"execution_id": string(*record.Request.ExecutionID), "status": string(result.Status),
	}
	if cause != nil {
		result.Status = domain.ActionStatusFailed
		result.Output = nil
		result.Error = &domain.Failure{Kind: domain.ErrorKindBusiness, Message: recordText(fmt.Sprintf("%T: %v", cause, cause))}
		payload["status"], payload["error"] = string(result.Status), result.Error.Message
	} else {
		payload["result"] = cloneMap(result.Output)
	}
	event := domain.Event{ID: record.ResultEventID, Type: "action.result", Payload: payload, CreatedAt: time.Now().UTC()}
	_, err = r.store.CompleteAction(ctx, ActionCompletion{Token: claim.Token, Result: result, Event: event})
	if err != nil {
		return &storeFailureError{cause: err}
	}
	return err
}

func (r *Runtime) recordActionUnknown(ctx context.Context, token ActionToken, failure domain.Failure) error {
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	return r.store.RecordActionUnknown(cleanup, token, failure)
}

func callHandler(handler ActionHandler, action domain.Action) (output map[string]any, err error, unknown bool) {
	defer func() {
		if recovered := recover(); recovered != nil {
			output, err, unknown = nil, fmt.Errorf("handler panic: %v", recovered), true
		}
	}()
	output, err = handler.Execute(cloneActions([]domain.Action{action})[0])
	return
}
