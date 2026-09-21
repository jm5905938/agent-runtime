package core

import (
	"agent-runtime/domain"
	"math"
	"reflect"
	"testing"
)

func TestCommitPublishesStateActionsAndCompletionTogether(t *testing.T) {
	runtime := NewRuntime()
	agent := domain.NewAgentInstance("commit")
	var executionID, attemptID domain.ID
	action := domain.NewAction("echo", map[string]any{"message": "hello"})
	if err := runtime.Executor().Register("echo", EchoHandler{}); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Register(&agent, functionRunner(func(ctx ExecutionContext) (ExecutionResult, error) {
		executionID, attemptID = ctx.ExecutionID, ctx.AttemptID
		return ExecutionResult{StateUpdate: map[string]any{"waiting_for": string(action.ID)}, Actions: []domain.Action{action}}, nil
	})); err != nil {
		t.Fatal(err)
	}
	result, err := runtime.Process(agent.ID, domain.NewEvent("run", nil))
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := runtime.Agent(agent.ID)
	if err != nil {
		t.Fatal(err)
	}

	//业务数据和完成记录一起可见
	saved := runtime.Executions()[executionID]
	savedAttempt := runtime.Attempts()[attemptID]
	if snapshot.StateVersion != 1 || snapshot.State["waiting_for"] != string(action.ID) ||
		saved.Status != domain.ExecutionStatusCompleted || saved.Result == nil || saved.FinishedAt == nil ||
		savedAttempt.Status != domain.AttemptStatusSucceeded || savedAttempt.FinishedAt == nil {
		t.Fatalf("incomplete commit: agent=%+v execution=%+v attempt=%+v", agent, saved, savedAttempt)
	}
	if !reflect.DeepEqual(*saved.Result, result) || !saved.FinishedAt.Equal(*savedAttempt.FinishedAt) {
		t.Fatal("completion records disagree with the committed result")
	}
	if len(runtime.Actions()) != 1 {
		t.Fatal("committed action is missing")
	}
	storedAction := runtime.Actions()[action.ID]
	if storedAction.ExecutionID == nil || *storedAction.ExecutionID != executionID ||
		result.Actions[0].ExecutionID == nil || *result.Actions[0].ExecutionID != executionID {
		t.Fatal("committed action lost its execution identity")
	}
}

func TestProcessCommitFailureDoesNotPublishBusinessChanges(t *testing.T) {
	for _, scenario := range []string{"duplicate action", "state version exhausted"} {
		t.Run(scenario, func(t *testing.T) {
			runtime := NewRuntime()
			agent := domain.NewAgentInstance("rejected commit")
			agent.State = map[string]any{"value": "original"}
			if scenario == "state version exhausted" {
				agent.StateVersion = math.MaxUint64
			}
			action := domain.NewAction("echo", map[string]any{"message": "hello"})
			actions := []domain.Action{action}
			if scenario == "duplicate action" {
				actions = append(actions, action)
			}
			if err := runtime.Register(&agent, functionRunner(func(ExecutionContext) (ExecutionResult, error) {
				return ExecutionResult{StateUpdate: map[string]any{"value": "changed"}, Actions: actions}, nil
			})); err != nil {
				t.Fatal(err)
			}
			handlerCalls := 0
			if err := runtime.Executor().Register("echo", handlerFunc(func(domain.Action) (map[string]any, error) {
				handlerCalls++
				return nil, nil
			})); err != nil {
				t.Fatal(err)
			}

			result, err := runtime.Process(agent.ID, domain.NewEvent("run", nil))
			if err == nil || result.StateUpdate != nil || len(result.Actions) != 0 {
				t.Fatalf("rejected commit returned a result: %+v, %v", result, err)
			}
			snapshot, err := runtime.Agent(agent.ID)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(snapshot.State, agent.State) || snapshot.StateVersion != agent.StateVersion ||
				len(runtime.Actions()) != 0 {
				t.Fatalf("rejected commit published business changes: %+v", snapshot)
			}
			if len(runtime.Executions()) != 1 || len(runtime.Attempts()) != 1 {
				t.Fatal("rejected commit did not retain its execution attempt")
			}
			executionStatus, attemptStatus := domain.ExecutionStatusFailed, domain.AttemptStatusFailed
			if scenario == "state version exhausted" {
				executionStatus, attemptStatus = domain.ExecutionStatusRunning, domain.AttemptStatusRunning
			}
			for _, execution := range runtime.Executions() {
				if execution.Status != executionStatus || execution.Result != nil {
					t.Fatalf("rejected commit has incorrect execution status: %+v", execution)
				}
			}
			for _, attempt := range runtime.Attempts() {
				if attempt.Status != attemptStatus {
					t.Fatalf("rejected commit has incorrect attempt status: %+v", attempt)
				}
				if attemptStatus == domain.AttemptStatusFailed && (attempt.Error == nil ||
					attempt.Error.Kind != domain.ErrorKindRuntime || attempt.FinishedAt == nil) {
					t.Fatalf("rejected commit has incorrect attempt status: %+v", attempt)
				}
			}
			if err := runtime.RunUntilIdle(); err != nil {
				t.Fatal(err)
			}
			if handlerCalls != 0 {
				t.Fatal("rejected commit reached the action handler")
			}
		})
	}
}
