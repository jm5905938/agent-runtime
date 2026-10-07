package core

import (
	"fmt"
	"strings"

	"agent-runtime/codec"
	"agent-runtime/domain"
)

func ValidateEvent(event domain.Event) error {
	return validateStoredEvent(event)
}

// 校验各action状态共用的元数据，保留有效字段原文
func ValidateActionMetadata(action domain.ActionRecord) error {
	if strings.TrimSpace(string(action.Request.ID)) == "" || strings.TrimSpace(action.Request.Type) == "" ||
		strings.TrimSpace(string(action.AgentID)) == "" || action.Request.ExecutionID == nil || strings.TrimSpace(string(*action.Request.ExecutionID)) == "" ||
		strings.TrimSpace(action.HandlerVersion) == "" ||
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

func ValidateActionCompletion(action domain.ActionRecord, completion ActionCompletion) error {
	if err := ValidateActionMetadata(action); err != nil {
		return err
	}
	result, event := completion.Result, completion.Event
	if err := ValidateEvent(event); err != nil {
		return err
	}
	if result.ActionID != action.Request.ID || result.EventID != action.ResultEventID || event.ID != action.ResultEventID || event.Type != "action.result" {
		return fmt.Errorf("action结果身份不匹配: %w", ErrStoreConflict)
	}
	if result.Status != domain.ActionStatusSucceeded && result.Status != domain.ActionStatusFailed {
		return fmt.Errorf("action结果不是最终状态: %w", ErrStoreConflict)
	}
	if (result.Status == domain.ActionStatusSucceeded && result.Error != nil) || (result.Status == domain.ActionStatusFailed && result.Error == nil) {
		return fmt.Errorf("action结果与错误不一致: %w", ErrStoreConflict)
	}
	if result.Error != nil {
		if err := validateStoredFailure(*result.Error); err != nil {
			return err
		}
	}
	if _, err := codec.Encode(completion); err != nil {
		return err
	}
	expected := map[string]any{"action_id": string(action.Request.ID), "action_type": action.Request.Type,
		"execution_id": string(*action.Request.ExecutionID), "status": string(result.Status)}
	if result.Status == domain.ActionStatusSucceeded {
		expected["result"] = result.Output
	} else {
		expected["error"] = result.Error.Message
	}
	equal, err := sameJSONValue(expected, event.Payload)
	if err != nil {
		return err
	}
	if !equal {
		return fmt.Errorf("action结果event内容不匹配: %w", ErrStoreConflict)
	}
	return nil
}
