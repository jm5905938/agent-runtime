package core

import (
	"agent-runtime/domain"
	"context"
	"errors"
	"slices"
)

var (
	ErrStoreOwned       = errors.New("store: owner already active")
	ErrStoreClosed      = errors.New("store: session closed")
	ErrRecoveryRequired = errors.New("store: recovery required")
)

//恢复会话入口
type RecoveryStore interface {
	OpenSession(ctx context.Context) (RecoverySession, error)
}

//独占会话，恢复成功后才允许业务写入
type RecoverySession interface {
	StateStore
	Recover(ctx context.Context) (RecoveryReport, error)
	Close(ctx context.Context) error
}

//恢复事务的结果，不代表任务已完成
type RecoveryReport struct {
	RequeuedDeliveries []domain.DeliveryKey `json:"requeued_deliveries"`
	UnknownActions     []domain.ID          `json:"unknown_actions"`
	RetryableActions   []domain.ID          `json:"retryable_actions"`
}

func cloneRecoveryReport(report RecoveryReport) RecoveryReport {
	return RecoveryReport{
		RequeuedDeliveries: slices.Clone(report.RequeuedDeliveries),
		UnknownActions:     slices.Clone(report.UnknownActions),
		RetryableActions:   slices.Clone(report.RetryableActions),
	}
}
