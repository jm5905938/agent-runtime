package core

import (
	"agent-runtime/codec"
	"agent-runtime/domain"
	"fmt"
	"sync"
)

//外部能力接口
type ActionHandler interface {
	Execute(action domain.Action) (map[string]any, error)
}

//原样返回action数据
type EchoHandler struct{}

func (EchoHandler) Execute(action domain.Action) (map[string]any, error) {
	return cloneMap(action.Payload), nil
}

//执行action并生成结果事件，已完成的结果直接复用
type Executor struct {
	mu       sync.Mutex
	handlers map[string]ActionHandler
	options  map[string]HandlerOptions
	results  map[domain.ID]domain.Event
	statuses map[domain.ID]domain.ActionStatus
}

func NewExecutor() *Executor {
	return &Executor{handlers: make(map[string]ActionHandler), options: make(map[string]HandlerOptions), results: make(map[domain.ID]domain.Event), statuses: make(map[domain.ID]domain.ActionStatus)}
}

func (e *Executor) Register(actionType string, handler ActionHandler) error {
	return e.RegisterWithOptions(actionType, handler, HandlerOptions{
		Version: "1", RecoveryPolicy: domain.RecoveryPolicyManual, MaxAttempts: 1,
	})
}

func (e *Executor) Execute(action domain.Action) (domain.Event, error) {
	if err := validateResult(ExecutionResult{Actions: []domain.Action{action}}); err != nil {
		return domain.Event{}, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if event, exists := e.results[action.ID]; exists {
		return cloneEvent(event), nil
	}
	if e.statuses[action.ID] == domain.ActionStatusUnknown {
		return domain.Event{}, fmt.Errorf("Action %s 的结果未知，需要确认后处理", action.ID)
	}
	e.statuses[action.ID] = domain.ActionStatusRunning
	payload := map[string]any{"action_id": string(action.ID), "action_type": action.Type, "execution_id": ""}
	if action.ExecutionID != nil {
		payload["execution_id"] = string(*action.ExecutionID)
	}
	handler, exists := e.handlers[action.Type]
	if !exists {
		payload["status"] = "failed"
		payload["error"] = fmt.Sprintf("未注册 Action 类型 %s", action.Type)
	} else if result, err := handler.Execute(cloneActions([]domain.Action{action})[0]); err != nil {
		payload["status"] = "failed"
		payload["error"] = recordText(fmt.Sprintf("%T: %v", err, err))
	} else {
		if err := codec.ValidateData(result); err != nil {
			e.statuses[action.ID] = domain.ActionStatusUnknown
			return domain.Event{}, fmt.Errorf("Action %s 返回不支持的业务数据，结果未知: %w", action.ID, err)
		}
		payload["status"] = "succeeded"
		payload["result"] = cloneMap(result)
	}
	event := domain.NewEvent("action.result", payload)
	e.results[action.ID] = event
	e.statuses[action.ID] = domain.ActionStatus(payload["status"].(string))
	return cloneEvent(event), nil
}
