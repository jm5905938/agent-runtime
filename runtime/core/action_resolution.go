package core

import (
	"agent-runtime/domain"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

type ResolutionDecision string

const (
	ResolutionRetry           ResolutionDecision = "retry"
	ResolutionAbandon         ResolutionDecision = "abandon"
	ActionResolutionEventType                    = "action.resolution"
)

// 人工决定与未知的外部结果分开保存，原Action及Attempt保持unknown。
type ActionResolution struct {
	ActionID domain.ID          `json:"action_id"`
	EventID  domain.ID          `json:"event_id"`
	Decision ResolutionDecision `json:"decision"`
	Reason   string             `json:"reason"`
}

type ActionResolutionReceipt struct {
	Resolution ActionResolution `json:"resolution"`
	Delivery   domain.Delivery  `json:"delivery"`
	Duplicate  bool             `json:"duplicate"`
}

func actionResolutionEventID(actionID domain.ID) domain.ID {
	return domain.ID(fmt.Sprintf("action-resolution/%x", sha256.Sum256([]byte(actionID))))
}

func validResolutionReason(reason string) bool {
	return utf8.ValidString(reason) && strings.TrimSpace(reason) != "" && utf8.RuneCountInString(reason) <= 1024
}

// ResolveAction只登记决定；run随后处理控制事件并创建重试Action或结束本轮。
// 相同Action的完整决定幂等，即使控制事件已经处理完毕也可以重复查询receipt。
func (r *Runtime) ResolveAction(ctx context.Context, actionID domain.ID, decision ResolutionDecision, reason string) (ActionResolutionReceipt, error) {
	done, err := r.enter()
	if err != nil {
		return ActionResolutionReceipt{}, err
	}
	defer done()
	if !utf8.ValidString(string(actionID)) || strings.TrimSpace(string(actionID)) == "" {
		return ActionResolutionReceipt{}, fmt.Errorf("action_id必须是非空UTF-8文本")
	}
	if decision != ResolutionRetry && decision != ResolutionAbandon {
		return ActionResolutionReceipt{}, fmt.Errorf("人工处理决定仅支持retry或abandon")
	}
	if !validResolutionReason(reason) {
		return ActionResolutionReceipt{}, fmt.Errorf("reason必须是非空UTF-8文本且不超过1024个字符")
	}
	saved, err := r.store.LoadAction(ctx, actionID)
	if err != nil {
		return ActionResolutionReceipt{}, err
	}
	action := saved.Action
	if action.Request.ExecutionID == nil {
		return ActionResolutionReceipt{}, fmt.Errorf("action缺少来源execution: %w", ErrStoreConflict)
	}
	if action.Status != domain.ActionStatusUnknown {
		return ActionResolutionReceipt{}, fmt.Errorf("仅能处理manual策略且结果unknown的model.generate: %w", ErrStoreConflict)
	}
	agent, err := r.store.LoadAgent(ctx, action.AgentID)
	if err != nil {
		return ActionResolutionReceipt{}, err
	}
	if agent.Definition != (domain.DefinitionRef{ID: "main", Version: "1"}) && agent.Definition != (domain.DefinitionRef{ID: "subagent", Version: "1"}) {
		return ActionResolutionReceipt{}, fmt.Errorf("仅能处理main的模型调用: %w", ErrStoreConflict)
	}
	resolution := ActionResolution{ActionID: actionID, EventID: actionResolutionEventID(actionID), Decision: decision, Reason: reason}
	payload := map[string]any{
		"action_id": string(actionID), "action_type": action.Request.Type,
		"execution_id": string(*action.Request.ExecutionID), "decision": string(decision), "reason": reason,
	}
	if decision == ResolutionRetry {
		payload["retry_payload"] = cloneMap(action.Request.Payload)
	}
	event := domain.Event{ID: resolution.EventID, Type: ActionResolutionEventType, Payload: payload, CreatedAt: time.Now().UTC()}
	// 已有决定是只读查询，后续投递占用执行锁时也能返回原receipt。
	if receipt, exists, err := r.existingResolution(ctx, action.AgentID, event, resolution); exists || err != nil {
		return receipt, err
	}
	gate := r.executionGate(action.AgentID)
	if !gate.TryLock() {
		return ActionResolutionReceipt{}, ErrExecutionInProgress
	}
	defer gate.Unlock()
	// 首次读取与拿锁之间，另一个调用可能已经登记决定。
	if receipt, exists, err := r.existingResolution(ctx, action.AgentID, event, resolution); exists || err != nil {
		return receipt, err
	}
	agent, err = r.store.LoadAgent(ctx, action.AgentID)
	if err != nil {
		return ActionResolutionReceipt{}, err
	}
	cancelled, err := r.subagentCancelled(ctx, agent.ID)
	if err != nil {
		return ActionResolutionReceipt{}, err
	}
	model := action.Request.Type == "model.generate" && action.RecoveryPolicy == domain.RecoveryPolicyManual
	if !model && !(cancelled && agent.Definition == (domain.DefinitionRef{ID: "subagent", Version: "1"}) && decision == ResolutionAbandon) {
		return ActionResolutionReceipt{}, fmt.Errorf("仅能处理manual策略且结果unknown的model.generate: %w", ErrStoreConflict)
	}
	if cancelled && decision == ResolutionRetry {
		return ActionResolutionReceipt{}, fmt.Errorf("已请求取消的subagent不能重试模型调用: %w", ErrStoreConflict)
	}
	if agent.Status != domain.AgentStatusActive {
		return ActionResolutionReceipt{}, fmt.Errorf("%w: agent %s当前状态%s", ErrAgentUnavailable, agent.ID, agent.Status)
	}
	waitingExecution := agent.State["waiting_execution_id"]
	if waitingExecution == nil {
		waitingExecution = agent.State["request_execution_id"]
	}
	waitingType := agent.State["waiting_action_type"]
	if waitingType == nil {
		waitingType = "model.generate"
	}
	if agent.State["request_status"] != "waiting" || agent.State["waiting_action_id"] != string(actionID) ||
		waitingType != action.Request.Type || waitingExecution != string(*action.Request.ExecutionID) {
		return ActionResolutionReceipt{}, fmt.Errorf("action不属于main当前等待的模型调用: %w", ErrStoreConflict)
	}
	// 拒绝已经领取但尚未提交的execution。
	deliveries, err := r.store.ListDeliveries(ctx, domain.DeliveryStatusRunning)
	if err != nil {
		return ActionResolutionReceipt{}, err
	}
	for _, delivery := range deliveries {
		if delivery.Key.AgentID == agent.ID {
			return ActionResolutionReceipt{}, ErrExecutionInProgress
		}
	}
	received, err := r.receiveEvent(ctx, action.AgentID, event)
	if err != nil {
		return ActionResolutionReceipt{}, err
	}
	return ActionResolutionReceipt{Resolution: resolution, Delivery: received.Delivery, Duplicate: received.Duplicate}, nil
}

func (r *Runtime) existingResolution(ctx context.Context, agentID domain.ID, event domain.Event, resolution ActionResolution) (ActionResolutionReceipt, bool, error) {
	saved, err := r.store.LoadEvent(ctx, event.ID)
	if errors.Is(err, ErrStoreNotFound) {
		return ActionResolutionReceipt{}, false, nil
	}
	if err != nil {
		return ActionResolutionReceipt{}, false, err
	}
	equal, err := SameEventContent(*saved, event)
	if err != nil {
		return ActionResolutionReceipt{}, true, err
	}
	if !equal {
		return ActionResolutionReceipt{}, true, fmt.Errorf("action已登记不同的人工处理决定: %w", ErrStoreConflict)
	}
	delivery, err := r.store.LoadDelivery(ctx, domain.DeliveryKey{AgentID: agentID, EventID: event.ID})
	if err != nil {
		return ActionResolutionReceipt{}, true, err
	}
	return ActionResolutionReceipt{Resolution: resolution, Delivery: *delivery, Duplicate: true}, true, nil
}

func resolutionFromEvent(action domain.ActionRecord, event domain.Event) *ActionResolution {
	if action.Request.ExecutionID == nil || action.Status != domain.ActionStatusUnknown ||
		event.ID != actionResolutionEventID(action.Request.ID) || event.Type != ActionResolutionEventType ||
		event.Payload["action_id"] != string(action.Request.ID) || event.Payload["action_type"] != action.Request.Type ||
		event.Payload["execution_id"] != string(*action.Request.ExecutionID) {
		return nil
	}
	decision, _ := event.Payload["decision"].(string)
	reason, _ := event.Payload["reason"].(string)
	if (ResolutionDecision(decision) != ResolutionRetry && ResolutionDecision(decision) != ResolutionAbandon) || !validResolutionReason(reason) {
		return nil
	}
	if (action.Request.Type != "model.generate" || action.RecoveryPolicy != domain.RecoveryPolicyManual) && ResolutionDecision(decision) != ResolutionAbandon {
		return nil
	}
	return &ActionResolution{ActionID: action.Request.ID, EventID: event.ID, Decision: ResolutionDecision(decision), Reason: reason}
}
