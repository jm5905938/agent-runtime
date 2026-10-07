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
	return r.executeActionWithCancel(ctx, record, false)
}

func (r *Runtime) executeActionWithCancel(ctx context.Context, record domain.ActionRecord, confirmation bool) error {
	agent, err := r.store.LoadAgent(ctx, record.AgentID)
	if err != nil {
		return &storeFailureError{cause: err}
	}
	if agent.Status != domain.AgentStatusActive {
		return fmt.Errorf("%w: agent %s当前状态%s", ErrAgentUnavailable, agent.ID, agent.Status)
	}
	if cancelled, err := r.subagentCancelled(ctx, agent.ID); err != nil {
		return &storeFailureError{cause: err}
	} else if cancelled && !confirmation {
		return ErrActionNotReady
	}
	if record.Status == domain.ActionStatusUnknown {
		event, err := r.store.LoadEvent(ctx, actionResolutionEventID(record.Request.ID))
		if err != nil && !errors.Is(err, ErrStoreNotFound) {
			return &storeFailureError{cause: err}
		}
		if err == nil && resolutionFromEvent(record, *event) != nil {
			return ErrActionNotReady
		}
	}
	handler, err := r.executor.handlerFor(record)
	if err != nil {
		return err
	}
	if gate, ok := handler.(ActionGate); ok {
		blocked, err := gate.ActionBlockedBy(ctx, record)
		if err != nil {
			return &storeFailureError{cause: err}
		}
		if len(blocked) > 0 {
			return ErrActionNotReady
		}
	}
	var claim *ActionClaim
	if confirmation {
		claim, err = r.store.(SubagentStore).ClaimSubagentCancelAction(ctx, record.Request.ID)
	} else {
		claim, err = r.store.ClaimAction(ctx, record.Request.ID)
	}
	if err != nil {
		if errors.Is(err, ErrActionNotReady) {
			return err
		}
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
	output, cause, unknown := callClaimHandler(ctx, handler, *claim)
	if cause == nil {
		if err := codec.ValidateData(output); err != nil {
			cause, unknown = fmt.Errorf("handler返回不支持的业务数据: %w", err), true
		}
	}
	if unknown {
		kind := domain.ErrorKindUnknown
		if errors.Is(cause, context.Canceled) || errors.Is(cause, context.DeadlineExceeded) {
			kind = domain.ErrorKindInterrupted
		}
		err := r.recordActionUnknown(ctx, claim.Token, domain.Failure{
			Kind: kind, Message: recordText(cause.Error()),
		})
		if err != nil {
			return &storeFailureError{cause: errors.Join(err, ctx.Err())}
		}
		if _, storageFailure := cause.(*storeFailureError); storageFailure {
			return cause
		}
		return ctx.Err()
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
	// 外部调用已经得出结果，调用者取消不应使已确认的结果丢失。
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	_, err = r.store.CompleteAction(cleanup, ActionCompletion{Token: claim.Token, Result: result, Event: event})
	if err != nil {
		return &storeFailureError{cause: errors.Join(err, ctx.Err())}
	}
	return ctx.Err()
}

func callClaimHandler(ctx context.Context, handler ActionHandler, claim ActionClaim) (output map[string]any, err error, unknown bool) {
	claimed, ok := handler.(ClaimActionHandler)
	if !ok {
		return callHandler(ctx, handler, claim.Record.Request)
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			output, err, unknown = nil, fmt.Errorf("handler异常: %v", recovered), true
		}
	}()
	claim.Record = memoryCloneActionRecord(claim.Record)
	output, err = claimed.ExecuteClaim(ctx, claim)
	_, storageFailure := err.(*storeFailureError)
	unknown = storageFailure || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
	return
}

func (r *Runtime) recordActionUnknown(ctx context.Context, token ActionToken, failure domain.Failure) error {
	cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
	defer cancel()
	return r.store.RecordActionUnknown(cleanup, token, failure)
}

func callHandler(ctx context.Context, handler ActionHandler, action domain.Action) (output map[string]any, err error, unknown bool) {
	defer func() {
		if recovered := recover(); recovered != nil {
			output, err, unknown = nil, fmt.Errorf("handler异常: %v", recovered), true
		}
	}()
	action = cloneActions([]domain.Action{action})[0]
	if contextual, ok := handler.(ContextActionHandler); ok {
		output, err = contextual.ExecuteContext(ctx, action)
	} else {
		output, err = handler.Execute(action)
	}
	unknown = errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
	return
}
