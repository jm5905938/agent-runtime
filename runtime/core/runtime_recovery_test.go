package core

import (
	"agent-runtime/domain"
	"context"
	"errors"
	"fmt"
	"testing"
)

type canceledClaimStore struct {
	RecoveryStore
	cancel    context.CancelFunc
	recordErr error
}

func (s canceledClaimStore) OpenSession(ctx context.Context) (RecoverySession, error) {
	session, err := s.RecoveryStore.OpenSession(ctx)
	if err != nil {
		return nil, err
	}
	return canceledClaimSession{session, s.cancel, s.recordErr}, nil
}

type canceledClaimSession struct {
	RecoverySession
	cancel    context.CancelFunc
	recordErr error
}

func (s canceledClaimSession) ClaimAction(ctx context.Context, id domain.ID) (*ActionClaim, error) {
	claim, err := s.RecoverySession.ClaimAction(ctx, id)
	s.cancel()
	return claim, err
}

func (s canceledClaimSession) RecordActionUnknown(ctx context.Context, token ActionToken, failure domain.Failure) error {
	if s.recordErr != nil {
		return s.recordErr
	}
	return s.RecoverySession.RecordActionUnknown(ctx, token, failure)
}

func TestActionCanceledAfterClaimDoesNotCallHandler(t *testing.T) {
	for _, recordErr := range []error{nil, errors.New("save interrupted action fault")} {
		t.Run(fmt.Sprint(recordErr), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			runtime, err := OpenRuntime(ctx, canceledClaimStore{NewMemoryRecoveryStore(), cancel, recordErr})
			if err != nil {
				t.Fatal(err)
			}
			defer runtime.Close(context.Background())
			calls := 0
			if err := runtime.Executor().Register("echo", handlerFunc(func(domain.Action) (map[string]any, error) {
				calls++
				return nil, nil
			})); err != nil {
				t.Fatal(err)
			}
			agent := domain.NewAgentInstance("canceled action")
			if err := runtime.Register(&agent, resultRunner{}); err != nil {
				t.Fatal(err)
			}
			result, err := runtime.Process(agent.ID, domain.NewEvent("start", nil))
			if err != nil {
				t.Fatal(err)
			}
			err = runtime.RunUntilIdleContext(ctx)
			if !errors.Is(err, context.Canceled) || (recordErr != nil && !errors.Is(err, recordErr)) || calls != 0 {
				t.Fatalf("canceled claim: err=%v, calls=%d", err, calls)
			}
			record, err := runtime.ActionContext(context.Background(), result.Actions[0].ID)
			if err != nil {
				t.Fatal(err)
			}
			if recordErr == nil {
				if record.Action.Status != domain.ActionStatusUnknown || record.Action.LastError == nil ||
					record.Action.LastError.Kind != domain.ErrorKindInterrupted || len(record.Attempts) != 1 || record.Attempts[0].FinishedAt == nil {
					t.Fatalf("interruption not saved: %+v", record)
				}
			} else if record.Action.Status != domain.ActionStatusRunning {
				t.Fatalf("failed save changed action status: %s", record.Action.Status)
			}
			if err := runtime.RunUntilIdle(); err != nil || calls != 0 {
				t.Fatalf("manual action called after cancellation: err=%v, calls=%d", err, calls)
			}
		})
	}
}

type cleanupFaultStore struct {
	RecoveryStore
	recoverErr error
	closeErr   error
}

func (s cleanupFaultStore) OpenSession(ctx context.Context) (RecoverySession, error) {
	session, err := s.RecoveryStore.OpenSession(ctx)
	if err != nil {
		return nil, err
	}
	return &cleanupFaultSession{RecoverySession: session, recoverErr: s.recoverErr, closeErr: s.closeErr}, nil
}

type cleanupFaultSession struct {
	RecoverySession
	recoverErr error
	closeErr   error
}

func (s *cleanupFaultSession) Recover(context.Context) (RecoveryReport, error) {
	return RecoveryReport{}, s.recoverErr
}

func (s *cleanupFaultSession) Close(ctx context.Context) error {
	if s.closeErr != nil {
		err := s.closeErr
		s.closeErr = nil
		return err
	}
	return s.RecoverySession.Close(ctx)
}

func TestOpenRuntimeCleanupFailureCanBeRetried(t *testing.T) {
	ctx := context.Background()
	backend := NewMemoryRecoveryStore()
	recoverErr, closeErr := errors.New("recover fault"), errors.New("close fault")
	runtime, err := OpenRuntime(ctx, cleanupFaultStore{backend, recoverErr, closeErr})
	var pending *RuntimeOpenError
	if runtime != nil || !errors.As(err, &pending) || !errors.Is(err, recoverErr) || !errors.Is(err, closeErr) {
		t.Fatalf("startup error lost cleanup handle or causes: runtime=%v, err=%v", runtime, err)
	}
	if _, err := backend.OpenSession(ctx); !errors.Is(err, ErrStoreOwned) {
		t.Fatalf("failed cleanup released ownership: %v", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := pending.Close(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled cleanup: %v", err)
	}
	if err := pending.Close(ctx); err != nil {
		t.Fatal(err)
	}
	runtime, err = OpenRuntime(ctx, backend)
	if err != nil {
		t.Fatal(err)
	}
	defer runtime.Close(ctx)
	if err := pending.Close(ctx); err != nil {
		t.Fatalf("repeated cleanup: %v", err)
	}
	if _, err := backend.OpenSession(ctx); !errors.Is(err, ErrStoreOwned) {
		t.Fatalf("old cleanup released new owner: %v", err)
	}
}
