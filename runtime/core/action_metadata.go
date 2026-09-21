package core

import (
	"agent-runtime/codec"
	"agent-runtime/domain"
	"errors"
	"fmt"
	"reflect"
	"strings"
)

var ErrHandlerUnavailable = errors.New("executor: handler unavailable")

type HandlerOptions struct {
	Version        string
	RecoveryPolicy domain.RecoveryPolicy
	MaxAttempts    uint64
}

func (e *Executor) RegisterWithOptions(actionType string, handler ActionHandler, options HandlerOptions) error {
	if strings.TrimSpace(actionType) == "" || strings.TrimSpace(options.Version) == "" {
		return fmt.Errorf("注册 Action: type/version 不能为空")
	}
	if nilHandler(handler) {
		return fmt.Errorf("注册 Action 类型 %s: handler 不能为空", actionType)
	}
	if options.RecoveryPolicy != domain.RecoveryPolicyManual && options.RecoveryPolicy != domain.RecoveryPolicySafeRetry {
		return fmt.Errorf("注册 Action 类型 %s: 无效恢复策略 %q", actionType, options.RecoveryPolicy)
	}
	if options.MaxAttempts == 0 {
		return fmt.Errorf("注册 Action 类型 %s: 最大尝试次数必须大于零", actionType)
	}
	if _, err := codec.Encode(struct {
		Type    string
		Options HandlerOptions
	}{actionType, options}); err != nil {
		return fmt.Errorf("注册 Action 元数据: %w", err)
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, exists := e.handlers[actionType]; exists {
		return fmt.Errorf("Action 类型 %s 已存在", actionType)
	}
	e.handlers[actionType] = handler
	e.options[actionType] = options
	return nil
}

func (e *Executor) prepareActions(agentID, executionID domain.ID, actions []domain.Action) ([]domain.ActionRecord, error) {
	if err := validateResult(ExecutionResult{Actions: actions}); err != nil {
		return nil, err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	records := make([]domain.ActionRecord, 0, len(actions))
	seen := make(map[domain.ID]bool, len(actions))
	for _, action := range cloneActions(actions) {
		if seen[action.ID] {
			return nil, fmt.Errorf("重复 Action ID %s", action.ID)
		}
		seen[action.ID] = true
		options, exists := e.options[action.Type]
		if !exists {
			return nil, fmt.Errorf("未注册 Action 类型 %s", action.Type)
		}
		resultEventID, err := domain.NewID()
		if err != nil {
			return nil, fmt.Errorf("Action %s 结果 Event ID: %w", action.ID, err)
		}
		action.BindExecution(executionID)
		records = append(records, domain.ActionRecord{
			Request: action, AgentID: agentID, HandlerVersion: options.Version,
			RecoveryPolicy: options.RecoveryPolicy, IdempotencyKey: string(action.ID),
			MaxAttempts: options.MaxAttempts, Status: domain.ActionStatusPending,
			ResultEventID: resultEventID,
		})
	}
	return records, nil
}

func (e *Executor) handlerFor(record domain.ActionRecord) (ActionHandler, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	handler, exists := e.handlers[record.Request.Type]
	if !exists {
		return nil, fmt.Errorf("%w: 未注册 Action 类型 %s", ErrHandlerUnavailable, record.Request.Type)
	}
	options := e.options[record.Request.Type]
	if options.Version != record.HandlerVersion {
		return nil, fmt.Errorf("%w: Action 类型 %s 需要 Handler 版本 %s，当前版本 %s",
			ErrHandlerUnavailable, record.Request.Type, record.HandlerVersion, options.Version)
	}
	return handler, nil
}

func nilHandler(handler ActionHandler) bool {
	if handler == nil {
		return true
	}
	value := reflect.ValueOf(handler)
	switch value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	}
	return false
}
