package main

import (
	"agent-runtime/core"
	"agent-runtime/domain"
	"agent-runtime/model"
	pythonrunner "agent-runtime/python"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

type chatResult struct {
	AgentID         domain.ID `json:"agent_id"`
	Status          string    `json:"status"`
	Result          string    `json:"result"`
	Executions      int       `json:"executions"`
	Actions         int       `json:"actions"`
	RequestEventID  domain.ID `json:"request_event_id,omitempty"`
	Queued          bool      `json:"queued,omitempty"`
	WaitingActionID domain.ID `json:"waiting_action_id,omitempty"`
}

func defaultEnvFile() string {
	if _, err := os.Stat(".env"); !errors.Is(err, os.ErrNotExist) {
		return ".env"
	}
	if source := defaultPythonSource(); source != "" {
		return filepath.Join(filepath.Dir(filepath.Dir(source)), ".env")
	}
	return ".env"
}

func runMainAgent(ctx context.Context, message string, options pythonrunner.Options, config model.Config) (output chatResult, err error) {
	handler, err := model.NewHandler(config)
	if err != nil {
		return output, err
	}
	session, err := openRuntimeSession(ctx, core.NewMemoryRecoveryStore(), promptRunnerOptions(options, config))
	if err != nil {
		return output, err
	}
	defer func() { err = errors.Join(err, session.close()) }()
	if err := registerMain(session.runtime, session.runner); err != nil {
		return output, err
	}
	if err := registerAgentStatus(ctx, session.runtime); err != nil {
		return output, err
	}
	if err := session.runtime.Executor().RegisterWithOptions(
		getCurrentTimeActionType,
		getCurrentTimeHandler{},
		core.HandlerOptions{
			Version:        "1",
			RecoveryPolicy: domain.RecoveryPolicySafeRetry,
			MaxAttempts:    3,
		},
	); err != nil {
		return output, err
	}
	if err := session.runtime.Executor().RegisterWithOptions(
		getCurrentDateActionType,
		getCurrentDateHandler{},
		core.HandlerOptions{
			Version:        "1",
			RecoveryPolicy: domain.RecoveryPolicySafeRetry,
			MaxAttempts:    3,
		},
	); err != nil {
		return output, err
	}
	if err := session.runtime.Executor().RegisterWithOptions(
		readFileActionType,
		readFileHandler{rootDir: options.SourceDir},
		core.HandlerOptions{
			Version:        "1",
			RecoveryPolicy: domain.RecoveryPolicySafeRetry,
			MaxAttempts:    3,
		},
	); err != nil {
		return output, err
	}
	if err := session.runtime.Executor().RegisterWithOptions(
		writeFileActionType,
		writeFileHandler{rootDir: options.SourceDir},
		core.HandlerOptions{
			Version:        "1",
			RecoveryPolicy: domain.RecoveryPolicyManual,
			MaxAttempts:    1,
		},
	); err != nil {
		return output, err
	}
	if err := session.runtime.Executor().Register("model.generate", handler); err != nil {
		return output, err
	}
	agent, err := session.runtime.CreateAgentContext(ctx, "main", domain.DefinitionRef{ID: "main", Version: "1"}, nil)
	if err != nil {
		return output, err
	}
	request := domain.NewEvent("main.request", map[string]any{"message": message})
	if _, err := session.runtime.SubmitContext(ctx, agent.ID, request); err != nil {
		return output, err
	}
	if err := session.runtime.RunUntilIdleContext(ctx); err != nil {
		return output, err
	}
	snapshot, err := session.runtime.AgentContext(ctx, agent.ID)
	if err != nil {
		return output, err
	}
	status, _ := snapshot.State["request_status"].(string)
	reply, ok := snapshot.State["result"].(string)
	if status != "succeeded" || !ok {
		if failure, ok := snapshot.State["error"].(string); ok && failure != "" {
			return output, fmt.Errorf("main agent请求失败: %s", failure)
		}
		return output, fmt.Errorf("main agent未取得回复: status=%q", status)
	}
	executions, err := session.runtime.ExecutionsContext(ctx)
	if err != nil {
		return output, err
	}
	actions, err := session.runtime.ActionsContext(ctx)
	if err != nil {
		return output, err
	}
	return chatResult{AgentID: agent.ID, Status: status, Result: reply, Executions: len(executions), Actions: len(actions)}, nil
}

func promptRunnerOptions(options pythonrunner.Options, config model.Config) pythonrunner.Options {
	env := make(map[string]string, len(options.Env)+2)
	for key, value := range options.Env {
		env[key] = value
	}
	maxPromptChars := config.MaxPromptChars
	if maxPromptChars == 0 {
		maxPromptChars = model.DefaultMaxPromptChars
	}
	env["LLM_SYSTEM_PROMPT"] = config.SystemPrompt
	env["LLM_MAX_PROMPT_CHARS"] = strconv.Itoa(maxPromptChars)
	options.Env = env
	return options
}
