package core

import (
	"context"
	"errors"

	"agent-runtime/domain"
)

var (
	ErrStoreNotFound = errors.New("store中找不到记录")
	ErrStoreConflict = errors.New("store身份或状态冲突")
	ErrStoreStaleClaim = errors.New("store的claim已过期")
)

type ReceivedEvent struct {
	Delivery  domain.Delivery
	Duplicate bool
}

type ExecutionToken struct {
	Delivery             domain.DeliveryKey
	ExecutionID          domain.ID
	AttemptID            domain.ID
	ExpectedStateVersion uint64
}

type ExecutionClaim struct {
	Token   ExecutionToken
	Agent   domain.AgentInstance
	Event   domain.Event
	Attempt domain.Attempt
}

type ExecutionCommit struct {
	Token       ExecutionToken
	StateUpdate map[string]any
	Actions     []domain.ActionRecord
}

type ExecutionFailure struct {
	Token       ExecutionToken
	Failure     domain.Failure
	Interrupted bool
}

type StoredExecution struct {
	Execution domain.Execution
	Attempts  []domain.Attempt
}

type ActionToken struct {
	ActionID      domain.ID
	AttemptNumber uint64
}

type ActionClaim struct {
	Token  ActionToken
	Record domain.ActionRecord
}

type StoredAction struct {
	Action   domain.ActionRecord
	Attempts []domain.ActionAttempt
}

type ActionCompletion struct {
	Token  ActionToken
	Result domain.ActionResult
	Event  domain.Event
}

type StateStore interface {
	CreateAgent(ctx context.Context, agent domain.AgentInstance) error
	LoadAgent(ctx context.Context, agentID domain.ID) (*domain.AgentInstance, error)
	ListAgents(ctx context.Context) ([]domain.AgentInstance, error)

	ReceiveEvent(ctx context.Context, agentID domain.ID, event domain.Event) (ReceivedEvent, error)
	LoadEvent(ctx context.Context, eventID domain.ID) (*domain.Event, error)
	LoadDelivery(ctx context.Context, key domain.DeliveryKey) (*domain.Delivery, error)
	ListDeliveries(ctx context.Context, statuses ...domain.DeliveryStatus) ([]domain.Delivery, error)

	ClaimExecution(ctx context.Context, key domain.DeliveryKey) (*ExecutionClaim, error)
	CommitExecution(ctx context.Context, commit ExecutionCommit) (domain.ExecutionResult, error)
	FailExecution(ctx context.Context, failure ExecutionFailure) error
	RequeueDelivery(ctx context.Context, key domain.DeliveryKey) error
	LoadExecution(ctx context.Context, executionID domain.ID) (*StoredExecution, error)

	ClaimAction(ctx context.Context, actionID domain.ID) (*ActionClaim, error)
	CompleteAction(ctx context.Context, completion ActionCompletion) (domain.ActionResult, error)
	RecordActionUnknown(ctx context.Context, token ActionToken, failure domain.Failure) error
	LoadAction(ctx context.Context, actionID domain.ID) (*StoredAction, error)
	ListActions(ctx context.Context, statuses ...domain.ActionStatus) ([]domain.ActionRecord, error)
}
