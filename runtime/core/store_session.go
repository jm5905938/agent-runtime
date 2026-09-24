package core

import (
	"context"
	"sync"

	"agent-runtime/domain"
)

//管理存储的独占访问
type memoryRecoveryStore struct {
	mu    sync.Mutex
	store *MemoryStore
	owner *memoryRecoverySession
}

//一次存储占用，记录恢复和关闭状态
type memoryRecoverySession struct {
	backend *memoryRecoveryStore
	ready   bool
	closed  bool
	report  RecoveryReport
}

var _ RecoverySession = (*memoryRecoverySession)(nil)

//内存恢复后端，只通过独占会话访问
func NewMemoryRecoveryStore() RecoveryStore {
	return &memoryRecoveryStore{store: NewMemoryStore()}
}

func (b *memoryRecoveryStore) OpenSession(ctx context.Context) (RecoverySession, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if b.owner != nil {
		return nil, ErrStoreOwned
	}
	session := &memoryRecoverySession{backend: b}
	b.owner = session
	return session, nil
}

func (s *memoryRecoverySession) lock(ctx context.Context, write bool) error {
	s.backend.mu.Lock()
	var err error
	switch {
	case s.closed:
		err = ErrStoreClosed
	case ctx.Err() != nil:
		err = ctx.Err()
	case write && !s.ready:
		err = ErrRecoveryRequired
	}
	if err != nil {
		s.backend.mu.Unlock()
	}
	return err
}

func (s *memoryRecoverySession) Close(ctx context.Context) error {
	s.backend.mu.Lock()
	defer s.backend.mu.Unlock()
	if s.closed {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.closed = true
	s.backend.owner = nil
	return nil
}

func memorySessionCall[T any](s *memoryRecoverySession, ctx context.Context, write bool, call func(*MemoryStore) (T, error)) (T, error) {
	if err := s.lock(ctx, write); err != nil {
		var zero T
		return zero, err
	}
	defer s.backend.mu.Unlock()
	return call(s.backend.store)
}

func memorySessionWrite(s *memoryRecoverySession, ctx context.Context, call func(*MemoryStore) error) error {
	_, err := memorySessionCall(s, ctx, true, func(store *MemoryStore) (struct{}, error) {
		return struct{}{}, call(store)
	})
	return err
}

func (s *memoryRecoverySession) CreateAgent(ctx context.Context, agent domain.AgentInstance) error {
	return memorySessionWrite(s, ctx, func(store *MemoryStore) error { return store.CreateAgent(ctx, agent) })
}

func (s *memoryRecoverySession) LoadAgent(ctx context.Context, id domain.ID) (*domain.AgentInstance, error) {
	return memorySessionCall(s, ctx, false, func(store *MemoryStore) (*domain.AgentInstance, error) { return store.LoadAgent(ctx, id) })
}

func (s *memoryRecoverySession) ListAgents(ctx context.Context) ([]domain.AgentInstance, error) {
	return memorySessionCall(s, ctx, false, func(store *MemoryStore) ([]domain.AgentInstance, error) { return store.ListAgents(ctx) })
}

func (s *memoryRecoverySession) ReceiveEvent(ctx context.Context, id domain.ID, event domain.Event) (ReceivedEvent, error) {
	return memorySessionCall(s, ctx, true, func(store *MemoryStore) (ReceivedEvent, error) { return store.ReceiveEvent(ctx, id, event) })
}

func (s *memoryRecoverySession) LoadEvent(ctx context.Context, id domain.ID) (*domain.Event, error) {
	return memorySessionCall(s, ctx, false, func(store *MemoryStore) (*domain.Event, error) { return store.LoadEvent(ctx, id) })
}

func (s *memoryRecoverySession) LoadDelivery(ctx context.Context, key domain.DeliveryKey) (*domain.Delivery, error) {
	return memorySessionCall(s, ctx, false, func(store *MemoryStore) (*domain.Delivery, error) { return store.LoadDelivery(ctx, key) })
}

func (s *memoryRecoverySession) ListDeliveries(ctx context.Context, statuses ...domain.DeliveryStatus) ([]domain.Delivery, error) {
	return memorySessionCall(s, ctx, false, func(store *MemoryStore) ([]domain.Delivery, error) { return store.ListDeliveries(ctx, statuses...) })
}

func (s *memoryRecoverySession) ClaimExecution(ctx context.Context, key domain.DeliveryKey) (*ExecutionClaim, error) {
	return memorySessionCall(s, ctx, true, func(store *MemoryStore) (*ExecutionClaim, error) { return store.ClaimExecution(ctx, key) })
}

func (s *memoryRecoverySession) CommitExecution(ctx context.Context, commit ExecutionCommit) (domain.ExecutionResult, error) {
	return memorySessionCall(s, ctx, true, func(store *MemoryStore) (domain.ExecutionResult, error) { return store.CommitExecution(ctx, commit) })
}

func (s *memoryRecoverySession) FailExecution(ctx context.Context, failure ExecutionFailure) error {
	return memorySessionWrite(s, ctx, func(store *MemoryStore) error { return store.FailExecution(ctx, failure) })
}

func (s *memoryRecoverySession) RequeueDelivery(ctx context.Context, key domain.DeliveryKey) error {
	return memorySessionWrite(s, ctx, func(store *MemoryStore) error { return store.RequeueDelivery(ctx, key) })
}

func (s *memoryRecoverySession) LoadExecution(ctx context.Context, id domain.ID) (*StoredExecution, error) {
	return memorySessionCall(s, ctx, false, func(store *MemoryStore) (*StoredExecution, error) { return store.LoadExecution(ctx, id) })
}

func (s *memoryRecoverySession) ClaimAction(ctx context.Context, id domain.ID) (*ActionClaim, error) {
	return memorySessionCall(s, ctx, true, func(store *MemoryStore) (*ActionClaim, error) { return store.ClaimAction(ctx, id) })
}

func (s *memoryRecoverySession) CompleteAction(ctx context.Context, completion ActionCompletion) (domain.ActionResult, error) {
	return memorySessionCall(s, ctx, true, func(store *MemoryStore) (domain.ActionResult, error) { return store.CompleteAction(ctx, completion) })
}

func (s *memoryRecoverySession) RecordActionUnknown(ctx context.Context, token ActionToken, failure domain.Failure) error {
	return memorySessionWrite(s, ctx, func(store *MemoryStore) error { return store.RecordActionUnknown(ctx, token, failure) })
}

func (s *memoryRecoverySession) LoadAction(ctx context.Context, id domain.ID) (*StoredAction, error) {
	return memorySessionCall(s, ctx, false, func(store *MemoryStore) (*StoredAction, error) { return store.LoadAction(ctx, id) })
}

func (s *memoryRecoverySession) ListActions(ctx context.Context, statuses ...domain.ActionStatus) ([]domain.ActionRecord, error) {
	return memorySessionCall(s, ctx, false, func(store *MemoryStore) ([]domain.ActionRecord, error) { return store.ListActions(ctx, statuses...) })
}
