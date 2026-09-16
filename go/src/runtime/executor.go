package runtime

import (
	"agent-runtime-go/src/domain"
	"fmt"
	"sync"
)

// ActionHandler 是一种外部能力的具体实现，例如发消息或写文件。
type ActionHandler interface {
	Execute(action domain.Action) (map[string]any, error)
}

// EchoHandler 是最小示例：直接返回 Action 携带的数据。
type EchoHandler struct{}

func (EchoHandler) Execute(action domain.Action) (map[string]any, error) {
	return copyMap(action.Payload), nil
}

// Executor 执行 Action，并把结果包装为 action.result Event。
// 已完成 Action 的结果会被缓存，因此同一 Action ID 不会重复执行。
type Executor struct {
	mu       sync.Mutex
	handlers map[string]ActionHandler
	results  map[domain.ID]domain.Event
	statuses map[domain.ID]string
}

func NewExecutor() *Executor {
	return &Executor{handlers: make(map[string]ActionHandler), results: make(map[domain.ID]domain.Event), statuses: make(map[domain.ID]string)}
}

func (e *Executor) Register(actionType string, handler ActionHandler) error {
	if handler == nil {
		return fmt.Errorf("注册 Action 类型 %s: handler 不能为空", actionType)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, exists := e.handlers[actionType]; exists {
		return fmt.Errorf("Action 类型 %s 已存在", actionType)
	}
	e.handlers[actionType] = handler
	return nil
}

func (e *Executor) Execute(action domain.Action) (domain.Event, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if event, exists := e.results[action.ID]; exists {
		return event, nil
	}
	if e.statuses[action.ID] == "unknown" {
		return domain.Event{}, fmt.Errorf("Action %s 的结果未知，需要确认后处理", action.ID)
	}
	e.statuses[action.ID] = "running"
	payload := map[string]any{"action_id": string(action.ID), "action_type": action.Type, "execution_id": ""}
	if action.ExecutionID != nil {
		payload["execution_id"] = string(*action.ExecutionID)
	}
	handler, exists := e.handlers[action.Type]
	if !exists {
		payload["status"] = "failed"
		payload["error"] = fmt.Sprintf("未注册 Action 类型 %s", action.Type)
	} else if result, err := handler.Execute(action); err != nil {
		payload["status"] = "failed"
		payload["error"] = fmt.Sprintf("%T: %v", err, err)
	} else {
		payload["status"] = "succeeded"
		payload["result"] = result
	}
	event := domain.NewEvent("action.result", payload)
	e.results[action.ID] = event
	e.statuses[action.ID] = payload["status"].(string)
	return event, nil
}
