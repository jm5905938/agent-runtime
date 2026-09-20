package core

import (
	"agent-runtime/domain"
	"errors"
	"testing"
)

type functionRunner func(ExecutionContext) (ExecutionResult, error)

func (f functionRunner) Run(
	context ExecutionContext,
) (ExecutionResult, error) {
	return f(context)
}

type resultRunner struct{}

func (resultRunner) Run(
	context ExecutionContext,
) (ExecutionResult, error) {

	if context.Event.Type == "action.result" {
		return ExecutionResult{
			StateUpdate: map[string]any{
				"action_status": context.Event.Payload["status"],
			},
		}, nil
	}

	return ExecutionResult{
		StateUpdate: map[string]any{
			"count": 1,
		},
		Actions: []domain.Action{
			domain.NewAction(
				"echo",
				map[string]any{
					"message": "hello",
				},
			),
		},
	}, nil
}

func TestRunUntilIdleProcessesActionResult(t *testing.T) {
	runtime := NewRuntime()

	if err := runtime.Executor().Register(
		"echo",
		EchoHandler{},
	); err != nil {
		t.Fatal(err)
	}

	agent := domain.NewAgentInstance("test")

	if err := runtime.Register(
		&agent,
		resultRunner{},
	); err != nil {
		t.Fatal(err)
	}

	if err := runtime.Submit(
		agent.ID,
		domain.NewEvent("start", nil),
	); err != nil {
		t.Fatal(err)
	}

	if err := runtime.RunUntilIdle(); err != nil {
		t.Fatal(err)
	}

	// 从 Runtime 获取内部 Agent 状态
	snapshot, err := runtime.Agent(agent.ID)
	if err != nil {
		t.Fatal(err)
	}

	if snapshot.State["count"] != 1 ||
		snapshot.State["action_status"] != "succeeded" {

		t.Fatalf(
			"unexpected agent state: %#v",
			snapshot.State,
		)
	}

	if len(runtime.Executions()) != 2 ||
		len(runtime.Actions()) != 1 {

		t.Fatalf(
			"executions/actions = %d/%d, want 2/1",
			len(runtime.Executions()),
			len(runtime.Actions()),
		)
	}
}

func TestProcessFailureRecordsExecution(t *testing.T) {
	runtime := NewRuntime()

	agent := domain.NewAgentInstance("broken")

	expected := errors.New("runner failed")

	if err := runtime.Register(
		&agent,
		functionRunner(
			func(ExecutionContext) (
				ExecutionResult,
				error,
			) {
				return ExecutionResult{}, expected
			},
		),
	); err != nil {
		t.Fatal(err)
	}

	_, err := runtime.Process(
		agent.ID,
		domain.NewEvent("start", nil),
	)

	if !errors.Is(err, expected) {
		t.Fatalf(
			"Process error = %v, want %v",
			err,
			expected,
		)
	}

	for _, execution := range runtime.Executions() {

		if execution.Status != domain.ExecutionStatusFailed ||
			execution.Error != "runner failed" ||
			execution.FinishedAt == nil {

			t.Fatalf(
				"failed execution = %#v",
				execution,
			)
		}

		return
	}

	t.Fatal("no execution recorded")
}

func TestRegisteredAgentIsIsolatedFromCaller(t *testing.T) {
	runtime := NewRuntime()

	agent := domain.NewAgentInstance("test")

	agent.State["count"] = 1

	if err := runtime.Register(
		&agent,
		functionRunner(
			func(ExecutionContext) (
				ExecutionResult,
				error,
			) {
				return ExecutionResult{}, nil
			},
		),
	); err != nil {
		t.Fatal(err)
	}

	// 修改外部持有的 Agent
	agent.State["count"] = 999

	snapshot, err := runtime.Agent(agent.ID)

	if err != nil {
		t.Fatal(err)
	}

	if snapshot.State["count"] != 1 {

		t.Fatalf(
			"runtime state changed through caller reference: %#v",
			snapshot.State,
		)
	}
}

func TestRunnerCannotMutatePersistentAgentState(t *testing.T) {
	runtime := NewRuntime()

	agent := domain.NewAgentInstance("test")

	agent.State["count"] = 1

	if err := runtime.Register(
		&agent,
		functionRunner(
			func(
				ctx ExecutionContext,
			) (
				ExecutionResult,
				error,
			) {

				// 尝试修改 Execution Snapshot
				ctx.Agent.State["count"] = 999

				return ExecutionResult{}, nil
			},
		),
	); err != nil {
		t.Fatal(err)
	}

	if _, err := runtime.Process(
		agent.ID,
		domain.NewEvent("test", nil),
	); err != nil {
		t.Fatal(err)
	}

	snapshot, err := runtime.Agent(agent.ID)

	if err != nil {
		t.Fatal(err)
	}

	if snapshot.State["count"] != 1 {

		t.Fatalf(
			"runner modified persistent state: %#v",
			snapshot.State,
		)
	}
}

func TestLifecycleRejectsIllegalTransition(t *testing.T) {

	agent := domain.NewAgentInstance("test")

	if err := (LifecycleManager{}).Transition(
		&agent,
		domain.AgentStatusPaused,
	); err == nil {

		t.Fatal("expected transition error")
	}

	if agent.Status != domain.AgentStatusCreated {

		t.Fatalf(
			"status changed to %s",
			agent.Status,
		)
	}
}
