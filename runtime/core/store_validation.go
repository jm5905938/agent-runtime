package core

import (
	"fmt"
	"strings"

	"agent-runtime/domain"
)

func ValidateEvent(event domain.Event) error {
	return validateStoredEvent(event)
}

// 校验各action状态共用的元数据，保留有效字段原文
func ValidateActionMetadata(action domain.ActionRecord) error {
	if strings.TrimSpace(string(action.Request.ID)) == "" || strings.TrimSpace(action.Request.Type) == "" ||
		strings.TrimSpace(string(action.AgentID)) == "" || action.Request.ExecutionID == nil || strings.TrimSpace(string(*action.Request.ExecutionID)) == "" ||
		strings.TrimSpace(action.HandlerVersion) == "" || strings.TrimSpace(action.IdempotencyKey) == "" ||
		action.MaxAttempts == 0 || strings.TrimSpace(string(action.ResultEventID)) == "" {
		return fmt.Errorf("action %s元数据无效: %w", action.Request.ID, ErrStoreConflict)
	}
	if action.RecoveryPolicy != domain.RecoveryPolicyManual && action.RecoveryPolicy != domain.RecoveryPolicySafeRetry {
		return fmt.Errorf("action %s恢复策略无效: %w", action.Request.ID, ErrStoreConflict)
	}
	return nil
}

func ValidateNewActionRecord(action domain.ActionRecord, execution domain.Execution) error {
	if err := ValidateActionMetadata(action); err != nil {
		return err
	}
	if action.AgentID != execution.AgentID || *action.Request.ExecutionID != execution.ID ||
		action.Status != domain.ActionStatusPending || action.AttemptCount != 0 || action.Result != nil || action.LastError != nil {
		return fmt.Errorf("action %s初始记录或关联无效: %w", action.Request.ID, ErrStoreConflict)
	}
	return nil
}
