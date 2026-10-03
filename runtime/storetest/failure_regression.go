package storetest

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"agent-runtime/core"
	"agent-runtime/domain"
)

type failureContractSnapshot struct {
	Agents     []domain.AgentInstance
	Deliveries []domain.Delivery
	Events     []domain.Event
	Executions []*core.StoredExecution
	Actions    []*core.StoredAction
}

func snapshotFailureContract(t *testing.T, s core.StateStore) failureContractSnapshot {
	t.Helper()
	ctx := context.Background()
	var snapshot failureContractSnapshot
	var err error
	snapshot.Agents, err = s.ListAgents(ctx)
	must(t, err)
	snapshot.Deliveries, err = s.ListDeliveries(ctx)
	must(t, err)
	for _, delivery := range snapshot.Deliveries {
		event, err := s.LoadEvent(ctx, delivery.Key.EventID)
		must(t, err)
		snapshot.Events = append(snapshot.Events, *event)
		execution, err := s.LoadExecution(ctx, delivery.ExecutionID)
		if err != nil && !errors.Is(err, core.ErrStoreNotFound) {
			t.Fatal(err)
		}
		snapshot.Executions = append(snapshot.Executions, execution)
	}
	actions, err := s.ListActions(ctx)
	must(t, err)
	for _, action := range actions {
		stored, err := s.LoadAction(ctx, action.Request.ID)
		must(t, err)
		snapshot.Actions = append(snapshot.Actions, stored)
	}
	return snapshot
}

type failureOperation struct {
	name        string
	interrupted bool
	action      bool
	unknown     bool
}

var failureOperations = []failureOperation{
	{name: "execution_failed"},
	{name: "execution_interrupted", interrupted: true},
	{name: "action_failed", action: true},
	{name: "action_unknown", action: true, unknown: true},
}

func prepareFailureOperation(t *testing.T, s core.StateStore, agentID domain.ID, operation failureOperation) (
	func(domain.Failure) error, domain.ID, func(domain.Failure),
) {
	t.Helper()
	ctx := context.Background()
	if !operation.action {
		agent := createAgent(t, s, agentID)
		claim := newExecution(t, s, agent.ID)
		return func(failure domain.Failure) error {
				return s.FailExecution(ctx, core.ExecutionFailure{Token: claim.Token, Failure: failure, Interrupted: operation.interrupted})
			}, "", func(failure domain.Failure) {
				stored, err := s.LoadExecution(ctx, claim.Token.ExecutionID)
				must(t, err)
				status := domain.AttemptStatusFailed
				if operation.interrupted {
					status = domain.AttemptStatusInterrupted
				}
				if stored.Execution.Status != domain.ExecutionStatusFailed || len(stored.Attempts) != 1 ||
					stored.Attempts[0].Status != status || stored.Attempts[0].FinishedAt == nil {
					t.Fatalf("failure未正确结束execution和attempt: %+v", stored)
				}
				sameJSON(t, stored.Attempts[0].Error, &failure)
			}
	}
	claim := actionRegressionClaim(t, s, agentID, 1)
	return func(failure domain.Failure) error {
			if operation.unknown {
				return s.RecordActionUnknown(ctx, claim.Token, failure)
			}
			input := completion(claim, "")
			input.Result.Status = domain.ActionStatusFailed
			input.Result.Output = nil
			input.Result.Error = &failure
			input.Event.Payload["status"] = string(domain.ActionStatusFailed)
			delete(input.Event.Payload, "result")
			input.Event.Payload["error"] = failure.Message
			_, err := s.CompleteAction(ctx, input)
			return err
		}, claim.Record.ResultEventID, func(failure domain.Failure) {
			stored, err := s.LoadAction(ctx, claim.Record.Request.ID)
			must(t, err)
			status := domain.ActionStatusFailed
			if operation.unknown {
				status = domain.ActionStatusUnknown
			}
			if stored.Action.Status != status || len(stored.Attempts) != 1 || stored.Attempts[0].Status != status ||
				stored.Attempts[0].FinishedAt == nil {
				t.Fatalf("failure未正确结束action和attempt: %+v", stored)
			}
			sameJSON(t, stored.Action.LastError, &failure)
			sameJSON(t, stored.Attempts[0].Error, &failure)
			if !operation.unknown {
				if stored.Action.Result == nil {
					t.Fatal("failed action未保存结果")
				}
				sameJSON(t, stored.Action.Result.Error, &failure)
				event, err := s.LoadEvent(ctx, claim.Record.ResultEventID)
				must(t, err)
				if event.Payload["error"] != failure.Message {
					t.Fatalf("结果event未保存failure原因: %+v", event)
				}
			}
		}
}

func assertFailureContractUnchanged(t *testing.T, s core.StateStore, before failureContractSnapshot) {
	t.Helper()
	if after := snapshotFailureContract(t, s); !reflect.DeepEqual(after, before) {
		t.Fatal("failure操作改变agent/execution/action/attempt/event/delivery快照")
	}
}

func reopenFailureContract(t, sessionOwner *testing.T, s core.RecoverySession, backend core.RecoveryStore) core.RecoverySession {
	t.Helper()
	before := snapshotFailureContract(t, s)
	must(t, s.Close(context.Background()))
	reopened := openSession(sessionOwner, backend)
	_, err := reopened.Recover(context.Background())
	must(t, err)
	assertFailureContractUnchanged(t, reopened, before)
	return reopened
}

func testFailureValidation(t *testing.T, backend core.RecoveryStore) {
	ctx := context.Background()
	s := readySession(t, backend)
	sessionOwner := t
	for operationIndex, operation := range failureOperations {
		for invalidIndex, invalid := range []struct {
			name string
			kind domain.ErrorKind
		}{
			{name: "empty_kind", kind: ""},
			{name: "invalid_kind", kind: "invalid"},
			{name: "blank_kind", kind: " \t"},
		} {
			t.Run(operation.name+"/"+invalid.name, func(t *testing.T) {
				apply, resultEventID, verify := prepareFailureOperation(t, s,
					domain.ID(fmt.Sprintf("invalid-%d-%d", operationIndex, invalidIndex)), operation)
				before := snapshotFailureContract(t, s)
				if err := apply(domain.Failure{Kind: invalid.kind, Message: "非法failure"}); err == nil {
					t.Error("接受无效failure kind")
				}
				assertFailureContractUnchanged(t, s, before)
				if resultEventID != "" {
					if _, err := s.LoadEvent(ctx, resultEventID); !errors.Is(err, core.ErrStoreNotFound) {
						t.Fatalf("拒绝failure后留下结果event: %v", err)
					}
				}
				valid := domain.Failure{Kind: domain.ErrorKindBusiness, Message: "合法failure"}
				must(t, apply(valid))
				verify(valid)
				if operation.action && !operation.unknown {
					completed := snapshotFailureContract(t, s)
					if err := apply(domain.Failure{Kind: invalid.kind, Message: valid.Message}); err == nil {
						t.Error("已完成action接受非法failure kind")
					}
					assertFailureContractUnchanged(t, s, completed)
					must(t, apply(valid))
					assertFailureContractUnchanged(t, s, completed)
				}
				s = reopenFailureContract(t, sessionOwner, s, backend)
			})
		}
	}
}

func testFailureValidKinds(t *testing.T, backend core.RecoveryStore) {
	s := readySession(t, backend)
	sessionOwner := t
	for operationIndex, operation := range failureOperations {
		for _, kind := range []domain.ErrorKind{
			domain.ErrorKindBusiness, domain.ErrorKindRuntime, domain.ErrorKindInterrupted, domain.ErrorKindUnknown,
		} {
			t.Run(operation.name+"/"+string(kind), func(t *testing.T) {
				apply, _, verify := prepareFailureOperation(t, s,
					domain.ID(fmt.Sprintf("valid-%d-%s", operationIndex, kind)), operation)
				failure := domain.Failure{Kind: kind, Message: "保留合法failure种类和原因"}
				must(t, apply(failure))
				verify(failure)
				s = reopenFailureContract(t, sessionOwner, s, backend)
			})
		}
	}
}
