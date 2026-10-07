package core

import "agent-runtime/domain"

func validateSubagentRecovery(s *MemoryStore) error {
	children := make(map[domain.ID]bool)
	for id, task := range s.tasks {
		action, sourceExists := s.actions[id]
		child, childExists := s.agents[task.ChildAgentID]
		_, parentExists := s.agents[task.ParentAgentID]
		_, eventExists := s.events[task.InitialEventID]
		_, deliveryExists := s.deliveries[domain.DeliveryKey{AgentID: task.ChildAgentID, EventID: task.InitialEventID}]
		if id == "" || task.ID != id || !sourceExists || action.Request.Type != SubagentSpawnActionType || action.AgentID != task.ParentAgentID ||
			!childExists || !parentExists || task.ChildAgentID == task.ParentAgentID || children[task.ChildAgentID] || !eventExists || !deliveryExists {
			return recoveryConflict("subagent任务关联", id)
		}
		children[task.ChildAgentID] = true
		if task.Result == nil {
			if child.Status == domain.AgentStatusTerminated || task.CompletionExecutionID != "" {
				return recoveryConflict("未完成的subagent任务", id)
			}
			continue
		}
		if ValidateSubagentResult(task.Result) != nil || child.Status != domain.AgentStatusTerminated || s.childActionsUnresolved(child.ID) {
			return recoveryConflict("subagent任务结果", id)
		}
		if task.CompletionExecutionID == "" {
			if !task.CancelRequested || task.Result.Status != domain.SubagentStatusCancelled {
				return recoveryConflict("subagent取消结果", id)
			}
			continue
		}
		execution, exists := s.executions[task.CompletionExecutionID]
		if !exists || execution.AgentID != child.ID {
			return recoveryConflict("subagent完成execution", id)
		}
		if execution.Status == domain.ExecutionStatusFailed {
			attempts := s.attempts[execution.ID]
			if len(attempts) == 0 || attempts[len(attempts)-1].Status != domain.AttemptStatusFailed ||
				task.Result.Status != domain.SubagentStatusFailed || task.Result.Error == nil || task.Result.Error.Message != execution.Error || len(task.Result.Output) != 0 {
				return recoveryConflict("subagent失败execution", id)
			}
			if equal, err := sameJSONValue(task.Result.Error, attempts[len(attempts)-1].Error); err != nil || !equal {
				return recoveryConflict("subagent失败信息", id)
			}
		} else if execution.Status != domain.ExecutionStatusCompleted || execution.Result == nil || execution.Result.TaskResult == nil {
			return recoveryConflict("subagent完成execution", id)
		} else if equal, err := sameJSONValue(task.Result, execution.Result.TaskResult); err != nil || !equal {
			return recoveryConflict("subagent完成结果", id)
		}
	}
	for _, task := range s.tasks {
		if children[task.ParentAgentID] {
			return recoveryConflict("递归subagent任务", task.ID)
		}
	}
	for _, execution := range s.executions {
		if execution.Result != nil && execution.Result.TaskResult != nil {
			task := s.taskForChild(execution.AgentID)
			if task == nil || task.CompletionExecutionID != execution.ID || task.Result == nil {
				return recoveryConflict("孤立的subagent完成结果", execution.ID)
			}
		}
	}
	return nil
}
