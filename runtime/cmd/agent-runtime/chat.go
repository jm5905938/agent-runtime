package main

import (
	"agent-runtime/domain"
	"agent-runtime/model"
	pythonrunner "agent-runtime/python"
	"errors"
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
