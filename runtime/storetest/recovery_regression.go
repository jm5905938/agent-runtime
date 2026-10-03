package storetest

import (
	"context"
	"testing"

	"agent-runtime/core"
	"agent-runtime/domain"
)

func testRecoveryReportsExistingUnknown(t *testing.T, backend core.RecoveryStore) {
	ctx := context.Background()
	s := readySession(t, backend)
	agent := createAgent(t, s, "agent")
	source := newExecution(t, s, agent.ID)
	safe, manual, exhausted, running := newAction(source), newAction(source), newAction(source), newAction(source)
	manual.RecoveryPolicy = domain.RecoveryPolicyManual
	exhausted.MaxAttempts = 1
	_, err := s.CommitExecution(ctx, core.ExecutionCommit{Token: source.Token, Actions: []domain.ActionRecord{safe, manual, exhausted, running}})
	must(t, err)
	failure := domain.Failure{Kind: domain.ErrorKindUnknown, Message: "外部处理结果无法确认，保留原始原因"}
	before := make(map[domain.ID]*core.StoredAction)
	for _, action := range []domain.ActionRecord{safe, manual, exhausted} {
		claim := claimAction(t, s, action.Request.ID)
		must(t, s.RecordActionUnknown(ctx, claim.Token, failure))
		before[action.Request.ID], err = s.LoadAction(ctx, action.Request.ID)
		must(t, err)
	}
	claimAction(t, s, running.Request.ID)
	want := core.RecoveryReport{
		UnknownActions:   []domain.ID{safe.Request.ID, manual.Request.ID, exhausted.Request.ID, running.Request.ID},
		RetryableActions: []domain.ID{safe.Request.ID, running.Request.ID},
	}
	for i := range 3 {
		must(t, s.Close(ctx))
		s = openSession(t, backend)
		report, err := s.Recover(ctx)
		must(t, err)
		sameJSON(t, report, want)
		for id, original := range before {
			action, err := s.LoadAction(ctx, id)
			must(t, err)
			sameJSON(t, action, original)
		}
		if i == 0 {
			before[running.Request.ID], err = s.LoadAction(ctx, running.Request.ID)
			must(t, err)
		}
	}
}

func testRecoveryAfterAttemptHistory(t *testing.T, backend core.RecoveryStore) {
	ctx := context.Background()
	s := readySession(t, backend)
	agent := createAgent(t, s, "agent")
	source := newExecution(t, s, agent.ID)
	action := newAction(source)
	action.MaxAttempts = 12
	_, err := s.CommitExecution(ctx, core.ExecutionCommit{Token: source.Token, Actions: []domain.ActionRecord{action}})
	must(t, err)
	failure := domain.Failure{Kind: domain.ErrorKindUnknown, Message: "保留第1至11次尝试历史"}
	for i := range 12 {
		claim := claimAction(t, s, action.Request.ID)
		if i < 11 {
			must(t, s.RecordActionUnknown(ctx, claim.Token, failure))
		}
	}
	claim := newExecution(t, s, agent.ID)
	for range 11 {
		must(t, s.FailExecution(ctx, core.ExecutionFailure{Token: claim.Token, Failure: failure}))
		must(t, s.RequeueDelivery(ctx, claim.Token.Delivery))
		claim = claimExecution(t, s, claim.Token.Delivery)
	}
	beforeExecution, err := s.LoadExecution(ctx, claim.Token.ExecutionID)
	must(t, err)
	beforeAction, err := s.LoadAction(ctx, action.Request.ID)
	must(t, err)
	must(t, s.Close(ctx))
	s = openSession(t, backend)
	report, err := s.Recover(ctx)
	must(t, err)
	sameJSON(t, report, core.RecoveryReport{
		RequeuedDeliveries: []domain.DeliveryKey{claim.Token.Delivery},
		UnknownActions:     []domain.ID{action.Request.ID},
	})
	afterExecution, err := s.LoadExecution(ctx, claim.Token.ExecutionID)
	must(t, err)
	afterAction, err := s.LoadAction(ctx, action.Request.ID)
	must(t, err)
	if len(afterExecution.Attempts) != 12 || len(afterAction.Attempts) != 12 {
		t.Fatal("恢复丢失12次尝试历史")
	}
	sameJSON(t, afterExecution.Attempts[:11], beforeExecution.Attempts[:11])
	sameJSON(t, afterAction.Attempts[:11], beforeAction.Attempts[:11])
	lastExecution, lastAction := afterExecution.Attempts[11], afterAction.Attempts[11]
	if lastExecution.Number != 12 || lastExecution.Status != domain.AttemptStatusInterrupted || lastExecution.FinishedAt == nil || lastExecution.Error == nil ||
		lastAction.Number != 12 || lastAction.Status != domain.ActionStatusUnknown || lastAction.FinishedAt == nil || lastAction.Error == nil {
		t.Fatalf("恢复未正确转换第12次尝试: %+v, %+v", lastExecution, lastAction)
	}
	must(t, s.Close(ctx))
	s = openSession(t, backend)
	report, err = s.Recover(ctx)
	must(t, err)
	sameJSON(t, report, core.RecoveryReport{UnknownActions: []domain.ID{action.Request.ID}})
	reopenedExecution, err := s.LoadExecution(ctx, claim.Token.ExecutionID)
	must(t, err)
	reopenedAction, err := s.LoadAction(ctx, action.Request.ID)
	must(t, err)
	sameJSON(t, reopenedExecution, afterExecution)
	sameJSON(t, reopenedAction, afterAction)
}
