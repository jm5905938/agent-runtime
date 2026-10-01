package main

import (
	"agent-runtime/core"
	"agent-runtime/domain"
	pythonrunner "agent-runtime/python"
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	code := runCommand(ctx, os.Args[1:], os.Stdout, os.Stderr)
	cancel()
	os.Exit(code)
}

func defaultPythonSource() string {
	for _, candidate := range []string{"python/src", "../python/src"} {
		if info, err := os.Stat(filepath.Join(candidate, "agent_runtime", "worker.py")); err == nil && !info.IsDir() {
			absolute, err := filepath.Abs(candidate)
			if err == nil {
				return absolute
			}
		}
	}
	return ""
}

type echoResult struct {
	AgentID            domain.ID `json:"agent_id"`
	Status             string    `json:"status"`
	Result             string    `json:"result"`
	RequestEventID     domain.ID `json:"request_event_id"`
	RequestExecutionID domain.ID `json:"request_execution_id"`
	ActionID           domain.ID `json:"action_id"`
	ResultEventID      domain.ID `json:"result_event_id"`
	Executions         int       `json:"executions"`
	Actions            int       `json:"actions"`
}

type runtimeSession struct {
	runtime *core.Runtime
	runner  *pythonrunner.Runner
}

func openRuntimeSession(ctx context.Context, backend core.RecoveryStore, options pythonrunner.Options) (*runtimeSession, error) {
	runner, err := pythonrunner.NewRunner(options)
	if err != nil {
		return nil, err
	}
	runtime, err := core.OpenRuntime(ctx, backend)
	if err != nil {
		var openErr *core.RuntimeOpenError
		if errors.As(err, &openErr) {
			cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			err = errors.Join(err, openErr.Close(cleanup))
			cancel()
		}
		return nil, errors.Join(err, runner.Close())
	}
	session := &runtimeSession{runtime: runtime, runner: runner}
	if err := registerEcho(runtime, runner); err != nil {
		return nil, errors.Join(err, session.close())
	}
	return session, nil
}

func (s *runtimeSession) close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.runtime.Close(ctx); err != nil {
		return err
	}
	return s.runner.Close()
}

func runEcho(ctx context.Context, message string, options pythonrunner.Options) (output echoResult, err error) {
	backend := core.NewMemoryRecoveryStore()
	session, err := openRuntimeSession(ctx, backend, options)
	if err != nil {
		return output, err
	}
	defer func() {
		err = errors.Join(err, session.close())
	}()
	agent, err := session.runtime.CreateAgentContext(ctx, "echo", domain.DefinitionRef{ID: "echo", Version: "1"}, nil)
	if err != nil {
		return output, err
	}
	request := domain.NewEvent("echo.request", map[string]any{"message": message})
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
	messageResult, ok := snapshot.State["result"].(string)
	if status != "succeeded" || !ok {
		//没有可执行工作不等于请求成功
		attempts, queryErr := session.runtime.AttemptsContext(ctx)
		if queryErr != nil {
			return output, queryErr
		}
		for _, attempt := range attempts {
			if attempt.Error != nil {
				return output, fmt.Errorf("echo %s: %s: %s", status, attempt.Error.Kind, attempt.Error.Message)
			}
		}
		return output, fmt.Errorf("echo未成功: status=%q", status)
	}
	executions, err := session.runtime.ExecutionsContext(ctx)
	if err != nil {
		return output, err
	}
	actions, err := session.runtime.ActionsContext(ctx)
	if err != nil {
		return output, err
	}
	requestExecutionID, _ := snapshot.State["request_execution_id"].(string)
	var actionID domain.ID
	for id, action := range actions {
		if action.ExecutionID != nil && *action.ExecutionID == domain.ID(requestExecutionID) {
			actionID = id
			break
		}
	}
	resultEventID, _ := snapshot.State["result_event_id"].(string)
	return echoResult{
		AgentID: agent.ID, Status: status, Result: messageResult,
		RequestEventID: request.ID, RequestExecutionID: domain.ID(requestExecutionID),
		ActionID: actionID, ResultEventID: domain.ID(resultEventID),
		Executions: len(executions), Actions: len(actions),
	}, nil
}
