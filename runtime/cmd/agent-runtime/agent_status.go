package main

import (
	"agent-runtime/core"
	"agent-runtime/domain"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

const agentStatusActionType = "tool.agent_status"

type agentStatusHandler struct {
	runtime *core.Runtime
	ctx     context.Context
}

func registerAgentStatus(ctx context.Context, runtime *core.Runtime) error {
	return runtime.Executor().RegisterWithOptions(agentStatusActionType, agentStatusHandler{runtime: runtime, ctx: ctx}, core.HandlerOptions{
		Version: "1", RecoveryPolicy: domain.RecoveryPolicySafeRetry, MaxAttempts: 3,
	})
}

func (handler agentStatusHandler) Execute(action domain.Action) (map[string]any, error) {
	id, ok := action.Payload["agent_id"].(string)
	if !ok || len(action.Payload) != 1 || strings.TrimSpace(id) == "" || !utf8.ValidString(id) || len(id) > 1024 {
		return nil, errors.New("agent_status需要有效的agent_id字符串")
	}
	if handler.runtime == nil {
		return nil, errors.New("agent_status未绑定runtime")
	}
	ctx := handler.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	snapshot, err := handler.runtime.AgentContext(ctx, domain.ID(id))
	if err != nil {
		return nil, fmt.Errorf("agent_status查询失败: %w", err)
	}
	result := map[string]any{
		"id": agentStatusText(string(snapshot.ID)), "name": agentStatusText(snapshot.Name),
		"definition": map[string]any{"id": agentStatusText(snapshot.Definition.ID), "version": agentStatusText(snapshot.Definition.Version)},
		"status":     agentStatusText(string(snapshot.Status)), "state_version": snapshot.StateVersion,
	}
	if status, ok := snapshot.State["request_status"].(string); ok {
		result["request_status"] = agentStatusText(status)
	}
	if snapshot.BindingError != "" {
		result["binding_error"] = agentStatusText(snapshot.BindingError)
	}
	encoded, err := json.Marshal(result)
	if err != nil || len(encoded) > 8*1024 {
		return nil, errors.New("agent_status结果超过8KiB")
	}
	return result, nil
}

// Count encoded bytes so control characters and HTML escapes also fit the
// worker protocol budget. Stop only between UTF-8 code points.
func agentStatusText(value string) string {
	size := 2 // JSON string quotes.
	for offset, character := range value {
		encoded, _ := json.Marshal(string(character))
		if size+len(encoded)-2 > 1024 {
			return value[:offset]
		}
		size += len(encoded) - 2
	}
	return value
}
