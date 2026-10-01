package main

import (
	"agent-runtime/cli"
	"agent-runtime/core"
	"encoding/json"
	"fmt"
	"io"
	"strings"
)

func writeCommandResult(output io.Writer, result cli.Result) error {
	var text strings.Builder
	fmt.Fprintf(&text, "command: %s\n", result.Command)
	switch result.Command {
	case "init":
		if err := writeAgentSummary(&text, *result.Agent); err != nil {
			return err
		}
	case "submit":
		submission := result.Submission
		fmt.Fprintf(&text, "agent: %s\nevent: %s\nexecution: %s\ndelivery: %s\nduplicate: %t\n",
			submission.Delivery.Key.AgentID, submission.Delivery.Key.EventID,
			submission.Delivery.ExecutionID, submission.Delivery.Status, submission.Duplicate)
	case "retry":
		fmt.Fprintf(&text, "agent: %s\nevent: %s\ndelivery: pending\n", result.Retry.AgentID, result.Retry.EventID)
	case "status":
		if result.Query != nil {
			if err := writeAgentQuery(&text, *result.Query); err != nil {
				return err
			}
		} else {
			fmt.Fprintf(&text, "agents: %d\n", len(result.Agents))
			for _, agent := range result.Agents {
				if err := writeAgentSummary(&text, agent); err != nil {
					return err
				}
			}
		}
	case "run":
		if err := writeJSONValue(&text, "before", result.Run.Before); err != nil {
			return err
		}
		if err := writeJSONValue(&text, "after", result.Run.After); err != nil {
			return err
		}
		for _, query := range result.Run.Agents {
			if err := writeAgentQuery(&text, query); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("未知结果命令%s", result.Command)
	}
	if err := writeJSONValue(&text, "startup_recovery", result.StartupRecovery); err != nil {
		return err
	}
	_, err := io.WriteString(output, text.String())
	return err
}

func writeAgentSummary(output *strings.Builder, agent core.AgentSnapshot) error {
	fmt.Fprintf(output, "agent: %s name=%q definition=%s@%s status=%s state_version=%d\n",
		agent.ID, agent.Name, agent.Definition.ID, agent.Definition.Version, agent.Status, agent.StateVersion)
	if agent.BindingError != "" {
		fmt.Fprintf(output, "binding_error: %q\n", agent.BindingError)
	}
	return writeJSONValue(output, "state", agent.State)
}

func writeAgentQuery(output *strings.Builder, query core.AgentQuery) error {
	if err := writeAgentSummary(output, query.Agent); err != nil {
		return err
	}
	for _, item := range query.Deliveries {
		fmt.Fprintf(output, "delivery: event=%s type=%s status=%s execution=%s ready=%t\n",
			item.Event.ID, item.Event.Type, item.Delivery.Status, item.Delivery.ExecutionID, item.Ready)
		if err := writeJSONValue(output, "payload", item.Event.Payload); err != nil {
			return err
		}
		if item.Execution != nil {
			fmt.Fprintf(output, "execution: status=%s attempts=%d error=%q\n", item.Execution.Status,
				item.Execution.AttemptCount, item.Execution.Error)
		}
		for _, attempt := range item.Attempts {
			if err := writeJSONValue(output, "attempt", attempt); err != nil {
				return err
			}
		}
		writeBlockReasons(output, item.BlockedBy)
	}
	for _, item := range query.Actions {
		if err := writeJSONValue(output, "action", item.Action); err != nil {
			return err
		}
		fmt.Fprintf(output, "ready: %t\n", item.Ready)
		for _, attempt := range item.Attempts {
			if err := writeJSONValue(output, "action_attempt", attempt); err != nil {
				return err
			}
		}
		writeBlockReasons(output, item.BlockedBy)
	}
	return nil
}

func writeBlockReasons(output *strings.Builder, reasons []core.BlockReason) {
	for _, reason := range reasons {
		fmt.Fprintf(output, "blocked: %s %q\n", reason.Code, reason.Message)
	}
}

func writeJSONValue(output *strings.Builder, label string, value any) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	fmt.Fprintf(output, "%s: %s\n", label, data)
	return nil
}
