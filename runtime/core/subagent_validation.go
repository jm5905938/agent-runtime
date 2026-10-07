package core

import (
	"agent-runtime/codec"
	"agent-runtime/domain"
	"fmt"
	"strings"
)

const (
	SubagentSpawnActionType  = "tool.spawn_subagent"
	SubagentWaitActionType   = "tool.wait_subagent"
	SubagentCancelActionType = "tool.cancel_subagent"
)

func ValidateSubagentResult(result *domain.SubagentResult) error {
	if result == nil {
		return nil
	}
	if result.Output == nil {
		return fmt.Errorf("subagent结果需要output对象")
	}
	switch result.Status {
	case domain.SubagentStatusSucceeded:
		if result.Error != nil {
			return fmt.Errorf("成功的subagent结果不能包含error")
		}
	case domain.SubagentStatusFailed:
		if result.Error == nil {
			return fmt.Errorf("失败的subagent结果需要error")
		}
	case domain.SubagentStatusCancelled:
	default:
		return fmt.Errorf("subagent结果状态无效%q", result.Status)
	}
	if result.Error != nil {
		if strings.TrimSpace(result.Error.Message) == "" {
			return fmt.Errorf("subagent错误信息不能为空")
		}
		if err := ValidateFailure(*result.Error); err != nil {
			return err
		}
	}
	_, err := codec.Encode(result)
	return err
}

func ValidateSubagentSpawn(action domain.ActionRecord, spawn SubagentSpawn) error {
	if action.Request.Type != SubagentSpawnActionType || action.Request.ID != spawn.Token.ActionID ||
		spawn.Child.ID == "" || spawn.Child.ID == action.AgentID || spawn.Child.Status != domain.AgentStatusActive || spawn.Child.StateVersion != 0 {
		return fmt.Errorf("subagent创建关联无效: %w", ErrStoreConflict)
	}
	if err := spawn.Child.Definition.Validate(); err != nil {
		return err
	}
	if _, err := codec.Encode(spawn.Child); err != nil {
		return err
	}
	return ValidateEvent(spawn.Event)
}

func TerminateSubagentAgent(agent *domain.AgentInstance) error {
	manager := LifecycleManager{}
	if agent.Status == domain.AgentStatusActive || agent.Status == domain.AgentStatusPaused {
		if err := manager.Transition(agent, domain.AgentStatusTerminating); err != nil {
			return err
		}
	}
	return manager.Transition(agent, domain.AgentStatusTerminated)
}

func SubagentActionUnresolved(action domain.ActionRecord, events []domain.Event) bool {
	switch action.Status {
	case domain.ActionStatusSucceeded, domain.ActionStatusFailed:
		return false
	case domain.ActionStatusUnknown:
		for _, event := range events {
			if event.ID != actionResolutionEventID(action.Request.ID) {
				continue
			}
			resolution := resolutionFromEvent(action, event)
			if resolution != nil {
				return false
			}
		}
	}
	return true
}

func cloneSubagentResult(source *domain.SubagentResult) *domain.SubagentResult {
	if source == nil {
		return nil
	}
	return &domain.SubagentResult{Status: source.Status, Output: cloneMap(source.Output), Error: memoryCloneFailure(source.Error)}
}

func cloneSubagentTask(task domain.SubagentTask) domain.SubagentTask {
	task.Result = cloneSubagentResult(task.Result)
	return task
}
