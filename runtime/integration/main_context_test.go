package integration_test

import (
	"agent-runtime/core"
	"agent-runtime/domain"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

// Exercise the actual Go/Python protocol near its 1 MiB frame limit. A tool
// continuation duplicates the current turn into both state and model input.
func TestMainToolContinuationFitsWorkerFramesAndPreservesHistoryOnFailure(t *testing.T) {
	// Exercise the byte limit independently of the lower default character
	// budget, including a system prefix in the complete message array.
	t.Setenv("LLM_MAX_PROMPT_CHARS", "393216")
	systemPrompt := "你是MainAgent。" + strings.Repeat("s", 1024)
	t.Setenv("LLM_SYSTEM_PROMPT", systemPrompt)
	p := openPythonRuntime(t, core.NewMemoryRecoveryStore(), core.EchoHandler{}, domain.RecoveryPolicySafeRetry)
	var history []any
	for range 3 {
		history = append(history,
			map[string]any{"role": "user", "content": strings.Repeat("u", 64*1024-256)},
			map[string]any{"role": "assistant", "content": strings.Repeat("a", 64*1024-256)},
		)
	}
	historyJSON, err := json.Marshal(history)
	if err != nil || len(historyJSON) > 384*1024 || len(historyJSON) < 380*1024 {
		t.Fatalf("history fixture is not near its budget: bytes=%d err=%v", len(historyJSON), err)
	}
	input := core.ExecutionContext{
		Agent: core.AgentSnapshot{
			ID: "main-frame-budget", Name: "main", Definition: domain.DefinitionRef{ID: "main", Version: "1"},
			Status: domain.AgentStatusActive, StateVersion: 6,
			State: map[string]any{"request_status": "succeeded", "messages": history, "pending_message": nil, "result": "previous reply"},
		},
		Event:       domain.NewEvent("main.request", map[string]any{"message": strings.Repeat("q", 64*1024-2)}),
		ExecutionID: "request-execution", AttemptID: "request-attempt",
	}
	request, err := p.runner.Run(input)
	if err != nil || len(request.Actions) != 1 || request.Actions[0].Type != "model.generate" {
		t.Fatalf("large request did not cross the worker protocol: actions=%d err=%v", len(request.Actions), err)
	}
	if !reflect.DeepEqual(request.StateUpdate["messages"], history) {
		t.Fatal("model input trimming changed saved history")
	}
	resultInput := func(state map[string]any, executionID string, payload map[string]any) core.ExecutionContext {
		payload["action_id"] = state["waiting_action_id"]
		payload["action_type"] = state["waiting_action_type"]
		payload["execution_id"] = state["waiting_execution_id"]
		return core.ExecutionContext{
			Agent: core.AgentSnapshot{
				ID: input.Agent.ID, Name: input.Agent.Name, Definition: input.Agent.Definition,
				Status: domain.AgentStatusActive, State: state, StateVersion: 7,
			},
			Event: domain.NewEvent("action.result", payload), ExecutionID: domain.ID(executionID), AttemptID: domain.ID(executionID + "-attempt"),
		}
	}
	tool, err := p.runner.Run(resultInput(request.StateUpdate, "tool-request-execution", map[string]any{
		"status": "succeeded",
		"result": map[string]any{
			"message": strings.Repeat("m", 58*1024),
			"tool_calls": []any{map[string]any{
				"id": "status-call", "type": "function",
				"function": map[string]any{"name": "agent_status", "arguments": "{}"},
			}},
		},
	}))
	if err != nil || len(tool.Actions) != 1 || tool.Actions[0].Type != "tool.agent_status" {
		t.Fatalf("large pending tool trace was rejected: actions=%d err=%v", len(tool.Actions), err)
	}
	continued, err := p.runner.Run(resultInput(tool.StateUpdate, "continuation-execution", map[string]any{
		"status": "succeeded", "result": map[string]any{"id": string(input.Agent.ID), "name": strings.Repeat("s", 3*1024)},
	}))
	if err != nil || len(continued.Actions) != 1 || continued.Actions[0].Type != "model.generate" {
		t.Fatalf("large model continuation did not cross the worker protocol: actions=%d err=%v", len(continued.Actions), err)
	}
	frame, err := json.Marshal(map[string]any{"version": 1, "id": "continuation-execution-attempt", "result": continued})
	if err != nil || len(frame)+1 >= 1<<20 || len(frame) < 900*1024 {
		t.Fatalf("continuation fixture did not exercise a near-limit frame: bytes=%d err=%v", len(frame), err)
	}
	modelInput, err := json.Marshal(continued.Actions[0].Payload["messages"])
	if err != nil || len(modelInput) > 384*1024 {
		t.Fatalf("continuation exceeded the model input budget: bytes=%d err=%v", len(modelInput), err)
	}
	messages := continued.Actions[0].Payload["messages"].([]any)
	if first := messages[0].(map[string]any); first["role"] != "system" || first["content"] != systemPrompt {
		t.Fatal("continuation lost its system prompt prefix")
	}
	if !reflect.DeepEqual(continued.StateUpdate["messages"], history) ||
		continued.StateUpdate["request_execution_id"] != string(input.ExecutionID) ||
		continued.StateUpdate["waiting_execution_id"] != "continuation-execution" {
		t.Fatal("continuation lost saved history or action correlation")
	}
	failed, err := p.runner.Run(resultInput(continued.StateUpdate, "failure-execution", map[string]any{"status": "failed", "error": "model unavailable"}))
	if err != nil || len(failed.Actions) != 0 || failed.StateUpdate["request_status"] != "failed" ||
		failed.StateUpdate["pending_message"] != nil || !reflect.DeepEqual(failed.StateUpdate["messages"], history) {
		t.Fatalf("failed continuation changed completed history or left pending work: err=%v", err)
	}
}
