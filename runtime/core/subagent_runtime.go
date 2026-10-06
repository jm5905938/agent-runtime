package core

import (
	"agent-runtime/codec"
	"agent-runtime/domain"
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"
)

var ErrActionNotReady = errors.New("action暂时不能执行")

type ActionGate interface {
	ActionBlockedBy(context.Context, domain.ActionRecord) ([]BlockReason, error)
}

type ClaimActionHandler interface {
	ExecuteClaim(context.Context, ActionClaim) (map[string]any, error)
}

type subagentHandler struct {
	runtime *Runtime
	kind    string
}

func (r *Runtime) RegisterSubagentTools() error {
	for _, kind := range []string{SubagentSpawnActionType, SubagentWaitActionType, SubagentCancelActionType} {
		if err := r.executor.RegisterWithOptions(kind, subagentHandler{runtime: r, kind: kind}, HandlerOptions{
			Version: "1", RecoveryPolicy: domain.RecoveryPolicySafeRetry, MaxAttempts: math.MaxUint64,
		}); err != nil {
			return err
		}
	}
	return nil
}

func (h subagentHandler) Execute(domain.Action) (map[string]any, error) {
	return nil, fmt.Errorf("subagent操作需要持久action claim")
}

func (h subagentHandler) ActionBlockedBy(ctx context.Context, action domain.ActionRecord) ([]BlockReason, error) {
	if h.kind != SubagentWaitActionType {
		return nil, nil
	}
	store, ok := h.runtime.store.(SubagentStore)
	if !ok {
		return nil, nil
	}
	id, err := subagentTaskID(action.Request.Payload)
	if err != nil {
		return nil, nil
	}
	task, err := store.LoadSubagentTask(ctx, id)
	if errors.Is(err, ErrStoreNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if task.ParentAgentID != action.AgentID || task.Result != nil {
		return nil, nil
	}
	message := "等待subagent任务完成"
	if task.CancelRequested {
		message = "subagent取消已登记，等待在途操作确认"
	}
	return []BlockReason{{Code: BlockSubagentWaiting, Message: message}}, nil
}

func subagentTaskID(payload map[string]any) (domain.ID, error) {
	id, ok := payload["task_id"].(string)
	if !ok || strings.TrimSpace(id) == "" || len(payload) != 1 {
		return "", fmt.Errorf("subagent操作需要task_id")
	}
	return domain.ID(id), nil
}

func (h subagentHandler) ExecuteClaim(ctx context.Context, claim ActionClaim) (map[string]any, error) {
	store, ok := h.runtime.store.(SubagentStore)
	if !ok {
		return nil, fmt.Errorf("store不支持subagent")
	}
	action := claim.Record.Request
	if h.kind == SubagentSpawnActionType {
		if task, err := store.LoadSubagentTask(ctx, action.ID); err == nil {
			if task.ParentAgentID != claim.Record.AgentID {
				return nil, ErrStoreConflict
			}
			return subagentHandle(*task), nil
		} else if !errors.Is(err, ErrStoreNotFound) {
			return nil, &storeFailureError{cause: err}
		}
		message, ok := action.Payload["message"].(string)
		if !ok || strings.TrimSpace(message) == "" || len(message) > 64*1024 {
			return nil, fmt.Errorf("subagent任务需要64KiB以内的非空message")
		}
		for key, value := range action.Payload {
			if key != "message" && key != "context" && key != "name" {
				return nil, fmt.Errorf("subagent创建参数无效%s", key)
			}
			if _, ok := value.(string); !ok {
				return nil, fmt.Errorf("subagent创建参数%s需要字符串", key)
			}
		}
		contextText, _ := action.Payload["context"].(string)
		if len(contextText)+len(message) > 64*1024 {
			return nil, fmt.Errorf("subagent任务与上下文超过64KiB")
		}
		ref := domain.DefinitionRef{ID: "subagent", Version: "1"}
		h.runtime.mu.Lock()
		err := h.runtime.bindingError(ref)
		h.runtime.mu.Unlock()
		if err != nil {
			return nil, err
		}
		name, _ := action.Payload["name"].(string)
		if strings.TrimSpace(name) == "" {
			name = "subagent"
		}
		child := domain.NewAgentInstance(name)
		child.Definition, child.Status = ref, domain.AgentStatusActive
		event := domain.NewEvent("subagent.request", map[string]any{"message": message, "context": contextText})
		task, err := store.SpawnSubagent(ctx, SubagentSpawn{Token: claim.Token, Child: child, Event: event})
		if err != nil {
			if errors.Is(err, ErrStoreConflict) || errors.Is(err, ErrStoreStaleClaim) {
				return nil, err
			}
			return nil, &storeFailureError{cause: err}
		}
		return subagentHandle(*task), nil
	}
	id, err := subagentTaskID(action.Payload)
	if err != nil {
		return nil, err
	}
	task, err := store.LoadSubagentTask(ctx, id)
	if err != nil {
		if errors.Is(err, ErrStoreNotFound) {
			return nil, err
		}
		return nil, &storeFailureError{cause: err}
	}
	if task.ParentAgentID != claim.Record.AgentID {
		return nil, fmt.Errorf("只能操作当前agent的子任务")
	}
	if h.kind == SubagentCancelActionType {
		task, err = h.runtime.CancelSubagentTaskContext(ctx, claim.Record.AgentID, id)
		if err != nil {
			return nil, &storeFailureError{cause: err}
		}
		output := subagentHandle(*task)
		output["cancel_requested"] = task.CancelRequested
		return output, nil
	}
	if task.Result == nil {
		return nil, ErrActionNotReady
	}
	return subagentToolResult(*task)
}

func subagentHandle(task domain.SubagentTask) map[string]any {
	return map[string]any{"task_id": string(task.ID), "child_id": string(task.ChildAgentID)}
}

func subagentToolResult(task domain.SubagentTask) (map[string]any, error) {
	output := subagentHandle(task)
	output["task_status"], output["output"] = string(task.Result.Status), cloneMap(task.Result.Output)
	if task.Result.Error != nil {
		output["error"] = map[string]any{"kind": string(task.Result.Error.Kind), "message": task.Result.Error.Message}
	}
	data, err := codec.Encode(output)
	if err != nil {
		return nil, err
	}
	if len(data) <= 8*1024 {
		return output, nil
	}
	message, _ := task.Result.Output["message"].(string)
	output["output"] = map[string]any{"message": string([]rune(message)[:min(len([]rune(message)), 512)]), "truncated": true}
	output["result_reference"] = "subagent-task:" + string(task.ID)
	if task.Result.Error != nil {
		text := []rune(task.Result.Error.Message)
		output["error"] = map[string]any{"kind": string(task.Result.Error.Kind), "message": string(text[:min(len(text), 512)])}
	}
	return output, nil
}

func (r *Runtime) AgentTreeContext(ctx context.Context, rootID domain.ID) ([]domain.ID, error) {
	done, err := r.enter()
	if err != nil {
		return nil, err
	}
	defer done()
	if _, err := r.store.LoadAgent(ctx, rootID); err != nil {
		return nil, err
	}
	ids := []domain.ID{rootID}
	store, ok := r.store.(SubagentStore)
	if !ok {
		return ids, nil
	}
	tasks, err := store.ListSubagentTasks(ctx)
	if err != nil {
		return nil, err
	}
	for _, task := range tasks {
		if task.ParentAgentID == rootID {
			ids = append(ids, task.ChildAgentID)
		}
	}
	return ids, nil
}

func (r *Runtime) CancelSubagentTaskContext(ctx context.Context, parentID, taskID domain.ID) (*domain.SubagentTask, error) {
	done, err := r.enter()
	if err != nil {
		return nil, err
	}
	defer done()
	store, ok := r.store.(SubagentStore)
	if !ok {
		return nil, fmt.Errorf("store不支持subagent")
	}
	task, err := store.RequestSubagentCancel(ctx, parentID, taskID)
	if err != nil || task.Result != nil {
		return task, err
	}
	if _, err := r.settleSubagentCancel(ctx, *task); err != nil {
		return nil, err
	}
	return store.LoadSubagentTask(ctx, taskID)
}

func (r *Runtime) settleSubagentCancel(ctx context.Context, task domain.SubagentTask) (bool, error) {
	actions, err := r.store.ListActions(ctx, domain.ActionStatusPending, domain.ActionStatusUnknown)
	if err != nil {
		return false, err
	}
	progress := false
	for _, action := range actions {
		if action.AgentID != task.ChildAgentID {
			continue
		}
		if action.Status == domain.ActionStatusUnknown {
			if action.RecoveryPolicy != domain.RecoveryPolicySafeRetry || action.AttemptCount >= action.MaxAttempts {
				continue
			}
			err := r.executeActionWithCancel(ctx, action, true)
			if errors.Is(err, ErrActionNotReady) || errors.Is(err, ErrHandlerUnavailable) || errors.Is(err, ErrAgentUnavailable) || errors.Is(err, ErrExecutionInProgress) {
				continue
			}
			if err != nil {
				return false, err
			}
			progress = true
			continue
		}
		claim, err := r.store.(SubagentStore).ClaimSubagentCancelAction(ctx, action.Request.ID)
		if errors.Is(err, ErrStoreConflict) || errors.Is(err, ErrStoreStaleClaim) {
			continue
		}
		if err != nil {
			return false, err
		}
		failure := &domain.Failure{Kind: domain.ErrorKindBusiness, Message: "任务已取消，action未执行"}
		result := domain.ActionResult{ActionID: action.Request.ID, EventID: action.ResultEventID, Status: domain.ActionStatusFailed, Error: failure}
		event := domain.Event{ID: action.ResultEventID, Type: "action.result", CreatedAt: time.Now().UTC(), Payload: map[string]any{
			"action_id": string(action.Request.ID), "execution_id": string(*action.Request.ExecutionID), "action_type": action.Request.Type, "status": "failed", "error": failure.Message,
		}}
		cleanup, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		_, err = r.store.CompleteAction(cleanup, ActionCompletion{Token: claim.Token, Result: result, Event: event})
		cancel()
		if err != nil {
			return false, err
		}
		progress = true
	}
	err = r.store.(SubagentStore).FinishSubagentCancel(ctx, task.ID)
	if errors.Is(err, ErrSubagentBusy) {
		return progress, nil
	}
	return err == nil, err
}

func (r *Runtime) subagentCancelled(ctx context.Context, agentID domain.ID) (bool, error) {
	store, ok := r.store.(SubagentStore)
	if !ok {
		return false, nil
	}
	tasks, err := store.ListSubagentTasks(ctx)
	if err != nil {
		return false, err
	}
	for _, task := range tasks {
		if task.ChildAgentID == agentID && task.CancelRequested && task.Result == nil {
			return true, nil
		}
	}
	return false, nil
}
