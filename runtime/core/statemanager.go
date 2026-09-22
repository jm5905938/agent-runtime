package core

import (
	"agent-runtime/codec"
	"agent-runtime/domain"
	"fmt"
	"math"
)

//校验并合并状态增量
type StateManager struct{}

func (StateManager) Apply(agent *domain.AgentInstance, result ExecutionResult) error {
	if agent == nil {
		return fmt.Errorf("提交状态: agent 不能为空")
	}
	if _, err := codec.Encode(agent); err != nil {
		return fmt.Errorf("原agent记录: %w", err)
	}
	if err := validateResult(result); err != nil {
		return err
	}
	if agent.StateVersion == math.MaxUint64 {
		return fmt.Errorf("提交状态: 状态版本已耗尽")
	}
	state := cloneMap(agent.State)
	if state == nil {
		state = make(map[string]any)
	}
	for key, value := range result.StateUpdate {
		state[key] = cloneValue(value)
	}
	agent.State = state
	agent.StateVersion++
	return nil
}

func validateResult(result ExecutionResult) error {
	if err := codec.ValidateData(result.StateUpdate); err != nil {
		return fmt.Errorf("状态更新: %w", err)
	}
	for _, action := range result.Actions {
		if action.ID == "" || action.Type == "" {
			return fmt.Errorf("action id/type 不能为空")
		}
		if err := codec.ValidateData(action.Payload); err != nil {
			return fmt.Errorf("action %s 输入: %w", action.ID, err)
		}
	}
	if _, err := codec.Encode(result); err != nil {
		return fmt.Errorf("执行结果记录: %w", err)
	}
	return nil
}
