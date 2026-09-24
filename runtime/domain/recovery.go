package domain

import "time"

type DeliveryKey struct {
	AgentID ID `json:"agent_id"`
	EventID ID `json:"event_id"`
}

type DeliveryStatus string

const (
	DeliveryStatusPending   DeliveryStatus = "pending"
	DeliveryStatusRunning   DeliveryStatus = "running"
	DeliveryStatusCompleted DeliveryStatus = "completed"
	DeliveryStatusFailed    DeliveryStatus = "failed"
)

type Delivery struct {
	Key         DeliveryKey    `json:"key"`
	ExecutionID ID             `json:"execution_id"`
	Status      DeliveryStatus `json:"status"`
}

type ErrorKind string

const (
	ErrorKindBusiness    ErrorKind = "business"
	ErrorKindRuntime     ErrorKind = "runtime"
	ErrorKindInterrupted ErrorKind = "interrupted"
	ErrorKindUnknown     ErrorKind = "unknown"
)

type Failure struct {
	Kind    ErrorKind `json:"kind"`
	Message string    `json:"message"`
}

type AttemptStatus string

const (
	AttemptStatusRunning     AttemptStatus = "running"
	AttemptStatusSucceeded   AttemptStatus = "succeeded"
	AttemptStatusFailed      AttemptStatus = "failed"
	AttemptStatusInterrupted AttemptStatus = "interrupted"
)

type Attempt struct {
	ID          ID            `json:"id"`
	ExecutionID ID            `json:"execution_id"`
	Number      uint64        `json:"number"`
	Status      AttemptStatus `json:"status"`
	StartedAt   time.Time     `json:"started_at"`
	FinishedAt  *time.Time    `json:"finished_at,omitempty"`
	Error       *Failure      `json:"error,omitempty"`
}

type ActionStatus string

const (
	ActionStatusPending   ActionStatus = "pending"
	ActionStatusRunning   ActionStatus = "running"
	ActionStatusSucceeded ActionStatus = "succeeded"
	ActionStatusFailed    ActionStatus = "failed"
	ActionStatusUnknown   ActionStatus = "unknown"
)

type RecoveryPolicy string

const (
	RecoveryPolicyManual    RecoveryPolicy = "manual"
	RecoveryPolicySafeRetry RecoveryPolicy = "safe_retry"
)

type ActionRecord struct {
	Request        Action         `json:"request"`
	AgentID        ID             `json:"agent_id"`
	HandlerVersion string         `json:"handler_version"`
	RecoveryPolicy RecoveryPolicy `json:"recovery_policy"`
	IdempotencyKey string         `json:"idempotency_key"`
	MaxAttempts    uint64         `json:"max_attempts"`
	Status         ActionStatus   `json:"status"`
	AttemptCount   uint64         `json:"attempt_count"`
	ResultEventID  ID             `json:"result_event_id"`
	Result         *ActionResult  `json:"result,omitempty"`
	LastError      *Failure       `json:"last_error,omitempty"`
}

type ActionResult struct {
	ActionID ID             `json:"action_id"`
	EventID  ID             `json:"event_id"`
	Status   ActionStatus   `json:"status"`
	Output   map[string]any `json:"output"`
	Error    *Failure       `json:"error,omitempty"`
}

type ActionAttempt struct {
	ID         ID           `json:"id"`
	ActionID   ID           `json:"action_id"`
	Number     uint64       `json:"number"`
	Status     ActionStatus `json:"status"`
	StartedAt  time.Time    `json:"started_at"`
	FinishedAt *time.Time   `json:"finished_at,omitempty"`
	Error      *Failure     `json:"error,omitempty"`
}
