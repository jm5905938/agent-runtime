package main

import (
	"agent-runtime/core"
	"agent-runtime/domain"
	"context"
	"encoding/json"
	"fmt"
	"io"
)

func runCancelCommand(ctx context.Context, options commandOptions, output io.Writer, open backendOpener) error {
	var task *domain.SubagentTask
	if err := withConversation(ctx, options, open, false, func(runtime *core.Runtime, agent core.AgentSnapshot, _ func(context.Context) error) error {
		var err error
		task, err = runtime.CancelSubagentTaskContext(ctx, agent.ID, options.taskID)
		return err
	}); err != nil {
		return err
	}
	if options.asJSON {
		return json.NewEncoder(output).Encode(struct {
			Command string               `json:"command"`
			Task    *domain.SubagentTask `json:"task"`
		}{"cancel", task})
	}
	status := "已请求取消，等待当前操作确认"
	if task.Result != nil {
		status = "已结束，状态=" + string(task.Result.Status)
		if task.Result.Status == domain.SubagentStatusCancelled {
			status = "已取消，用resume或run继续main"
		}
	}
	_, err := fmt.Fprintf(output, "subagent任务%s%s\n", options.taskID, status)
	return err
}

func cancelledUnknownAction(query core.AgentQuery, action *core.ActionQuery) bool {
	if action == nil {
		return false
	}
	for _, task := range query.Tasks {
		if task.ChildAgentID == action.Action.AgentID && task.CancelRequested && task.Result == nil {
			return true
		}
	}
	return false
}

func treeUnknownConversationAction(ctx context.Context, runtime *core.Runtime, rootID domain.ID) (*core.ActionQuery, error) {
	ids, err := runtime.AgentTreeContext(ctx, rootID)
	if err != nil {
		return nil, err
	}
	for _, id := range ids {
		query, err := runtime.QueryAgentContext(ctx, id)
		if err != nil {
			return nil, err
		}
		if action := waitingUnknownConversationAction(query); action != nil {
			return action, nil
		}
	}
	return nil, nil
}

func conversationUnknownAction(ctx context.Context, options commandOptions, open backendOpener) (action *core.ActionQuery, err error) {
	options.request.Command = "status"
	err = withConversation(ctx, options, open, false, func(runtime *core.Runtime, agent core.AgentSnapshot, _ func(context.Context) error) error {
		action, err = treeUnknownConversationAction(ctx, runtime, agent.ID)
		return err
	})
	return action, err
}

func retryConversationTree(ctx context.Context, runtime *core.Runtime, rootID domain.ID, decision core.ResolutionDecision, prepare func(context.Context) error) error {
	ids, err := runtime.AgentTreeContext(ctx, rootID)
	if err != nil {
		return err
	}
	for _, id := range ids {
		if err := retryConversationDelivery(ctx, runtime, id, decision, prepare); err != nil {
			return err
		}
	}
	return nil
}
