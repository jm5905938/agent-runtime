package storetest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"agent-runtime/core"
	"agent-runtime/domain"
)

func actionRegressionClaim(t *testing.T, s core.StateStore, agentID domain.ID, maxAttempts uint64) *core.ActionClaim {
	t.Helper()
	agent := createAgent(t, s, agentID)
	execution := newExecution(t, s, agent.ID)
	action := newAction(execution)
	action.MaxAttempts = maxAttempts
	_, err := s.CommitExecution(context.Background(), core.ExecutionCommit{Token: execution.Token, Actions: []domain.ActionRecord{action}})
	must(t, err)
	return claimAction(t, s, action.Request.ID)
}

func testActionCompletedStaleToken(t *testing.T, backend core.RecoveryStore) {
	ctx := context.Background()
	s := readySession(t, backend)
	first := actionRegressionClaim(t, s, "agent", 2)
	must(t, s.RecordActionUnknown(ctx, first.Token, domain.Failure{Kind: domain.ErrorKindUnknown, Message: "第一次结果未知"}))
	second := claimAction(t, s, first.Record.Request.ID)
	input := completion(second, "hello")
	_, err := s.CompleteAction(ctx, input)
	must(t, err)
	before, err := s.LoadAction(ctx, first.Record.Request.ID)
	must(t, err)
	eventBefore, err := s.LoadEvent(ctx, input.Event.ID)
	must(t, err)
	deliveriesBefore, err := s.ListDeliveries(ctx)
	must(t, err)
	stale := input
	stale.Token = first.Token
	if _, err := s.CompleteAction(ctx, stale); !errors.Is(err, core.ErrStoreStaleClaim) {
		t.Fatalf("已完成action接受旧attempt的相同结果: %v", err)
	}
	duplicate := input
	duplicate.Event.CreatedAt = duplicate.Event.CreatedAt.Add(time.Hour)
	_, err = s.CompleteAction(ctx, duplicate)
	must(t, err)
	stale = completion(first, "不同结果")
	if _, err := s.CompleteAction(ctx, stale); !errors.Is(err, core.ErrStoreStaleClaim) {
		t.Fatalf("已完成action接受旧attempt的不同结果: %v", err)
	}
	after, err := s.LoadAction(ctx, first.Record.Request.ID)
	must(t, err)
	eventAfter, err := s.LoadEvent(ctx, input.Event.ID)
	must(t, err)
	deliveriesAfter, err := s.ListDeliveries(ctx)
	must(t, err)
	sameJSON(t, after, before)
	sameJSON(t, eventAfter, eventBefore)
	sameJSON(t, deliveriesAfter, deliveriesBefore)
}

func numericCompletion(claim *core.ActionClaim, resultNumber, eventNumber string) core.ActionCompletion {
	input := completion(claim, "hello")
	input.Result.Output = map[string]any{"number": json.Number(resultNumber)}
	input.Event.Payload["result"] = map[string]any{"number": json.Number(eventNumber)}
	return input
}

func testActionNumericEquality(t *testing.T, backend core.RecoveryStore) {
	ctx := context.Background()
	s := readySession(t, backend)
	for i, test := range []struct {
		name       string
		number     string
		equivalent string
		different  string
	}{
		{"integer", "1", "1.0", "2"},
		{"exponent", "1.0", "1e0", "1.0000000000000000000001"},
		{"large_integer", "9007199254740993", "9007199254740993e0", "9007199254740992"},
		{"decimal", "0.123456789012345678901", "123456789012345678901e-21", "0.123456789012345678902"},
		{"large_exponent", "1e1000000000000000", "10e999999999999999", "1e1000000000000001"},
		{"negative_zero", "-0", "0e1000000000000000", "-0.000000000000000000001"},
	} {
		t.Run(test.name, func(t *testing.T) {
			claim := actionRegressionClaim(t, s, domain.ID(fmt.Sprintf("agent-%d", i)), 1)
			before, err := s.LoadAction(ctx, claim.Record.Request.ID)
			must(t, err)
			deliveriesBefore, err := s.ListDeliveries(ctx)
			must(t, err)
			if _, err := s.CompleteAction(ctx, numericCompletion(claim, test.number, test.different)); !errors.Is(err, core.ErrStoreConflict) {
				t.Fatalf("action首次完成接受不同数值: %v", err)
			}
			afterRejected, err := s.LoadAction(ctx, claim.Record.Request.ID)
			must(t, err)
			sameJSON(t, afterRejected, before)
			if _, err := s.LoadEvent(ctx, claim.Record.ResultEventID); !errors.Is(err, core.ErrStoreNotFound) {
				t.Fatalf("不一致数值留下结果event: %v", err)
			}
			deliveriesAfter, err := s.ListDeliveries(ctx)
			must(t, err)
			sameJSON(t, deliveriesAfter, deliveriesBefore)
			input := numericCompletion(claim, test.number, test.equivalent)
			_, err = s.CompleteAction(ctx, input)
			must(t, err)
			completed, err := s.LoadAction(ctx, claim.Record.Request.ID)
			must(t, err)
			eventBefore, err := s.LoadEvent(ctx, input.Event.ID)
			must(t, err)
			deliveriesBefore, err = s.ListDeliveries(ctx)
			must(t, err)
			_, err = s.CompleteAction(ctx, numericCompletion(claim, test.equivalent, test.number))
			must(t, err)
			if _, err := s.CompleteAction(ctx, numericCompletion(claim, test.different, test.different)); !errors.Is(err, core.ErrStoreConflict) {
				t.Fatalf("action重复完成接受不同数值: %v", err)
			}
			after, err := s.LoadAction(ctx, claim.Record.Request.ID)
			must(t, err)
			eventAfter, err := s.LoadEvent(ctx, input.Event.ID)
			must(t, err)
			deliveriesAfter, err = s.ListDeliveries(ctx)
			must(t, err)
			sameJSON(t, after, completed)
			sameJSON(t, eventAfter, eventBefore)
			sameJSON(t, deliveriesAfter, deliveriesBefore)
		})
	}
}

func testActionAttemptOrder(t *testing.T, backend core.RecoveryStore) {
	ctx := context.Background()
	s := readySession(t, backend)
	claim := actionRegressionClaim(t, s, "agent", 12)
	for i := uint64(1); i <= 12; i++ {
		if i > 1 {
			claim = claimAction(t, s, claim.Record.Request.ID)
		}
		must(t, s.RecordActionUnknown(ctx, claim.Token, domain.Failure{Kind: domain.ErrorKindUnknown, Message: fmt.Sprintf("第%d次结果未知", i)}))
	}
	saved, err := s.LoadAction(ctx, claim.Record.Request.ID)
	must(t, err)
	if len(saved.Attempts) != 12 || saved.Action.AttemptCount != 12 {
		t.Fatalf("action尝试历史数量不完整: %+v", saved)
	}
	for i, attempt := range saved.Attempts {
		number := uint64(i + 1)
		if attempt.Number != number || attempt.Status != domain.ActionStatusUnknown || attempt.Error == nil || attempt.Error.Message != fmt.Sprintf("第%d次结果未知", number) {
			t.Fatalf("action尝试历史未按数值排序或丢失内容，第%d项为%+v", number, attempt)
		}
	}
}

func testActionResultEventValidation(t *testing.T, backend core.RecoveryStore) {
	ctx := context.Background()
	s := readySession(t, backend)
	for i, test := range []struct {
		name string
		edit func(*domain.Event)
	}{
		{"year_out_of_range", func(e *domain.Event) { e.CreatedAt = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC) }},
		{"negative_year", func(e *domain.Event) { e.CreatedAt = time.Date(-1, 1, 1, 0, 0, 0, 0, time.UTC) }},
		{"utc_year_out_of_range", func(e *domain.Event) {
			e.CreatedAt = time.Date(9999, 12, 31, 23, 0, 0, 0, time.FixedZone("test", -3600))
		}},
		{"invalid_payload_text", func(e *domain.Event) { e.Payload["result"] = map[string]any{"text": string([]byte{0xff})} }},
	} {
		t.Run(test.name, func(t *testing.T) {
			claim := actionRegressionClaim(t, s, domain.ID(fmt.Sprintf("agent-%d", i)), 1)
			before, err := s.LoadAction(ctx, claim.Record.Request.ID)
			must(t, err)
			deliveriesBefore, err := s.ListDeliveries(ctx)
			must(t, err)
			input := completion(claim, "hello")
			test.edit(&input.Event)
			if _, err := s.CompleteAction(ctx, input); err == nil {
				t.Fatal("action完成接受非法结果event")
			}
			after, err := s.LoadAction(ctx, claim.Record.Request.ID)
			must(t, err)
			sameJSON(t, after, before)
			deliveriesAfter, err := s.ListDeliveries(ctx)
			must(t, err)
			sameJSON(t, deliveriesAfter, deliveriesBefore)
			if _, err := s.LoadEvent(ctx, claim.Record.ResultEventID); !errors.Is(err, core.ErrStoreNotFound) {
				t.Fatalf("非法结果event留下记录: %v", err)
			}
			_, err = s.CompleteAction(ctx, completion(claim, "hello"))
			must(t, err)
			completed, err := s.LoadAction(ctx, claim.Record.Request.ID)
			must(t, err)
			eventBefore, err := s.LoadEvent(ctx, claim.Record.ResultEventID)
			must(t, err)
			deliveriesBefore, err = s.ListDeliveries(ctx)
			must(t, err)
			if _, err := s.CompleteAction(ctx, input); err == nil {
				t.Fatal("action幂等完成接受非法结果event")
			}
			after, err = s.LoadAction(ctx, claim.Record.Request.ID)
			must(t, err)
			sameJSON(t, after, completed)
			eventAfter, err := s.LoadEvent(ctx, claim.Record.ResultEventID)
			must(t, err)
			deliveriesAfter, err = s.ListDeliveries(ctx)
			must(t, err)
			sameJSON(t, eventAfter, eventBefore)
			sameJSON(t, deliveriesAfter, deliveriesBefore)
		})
	}
}
