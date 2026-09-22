package core

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"time"

	"agent-runtime/codec"
	"agent-runtime/domain"
)

//内存事务，进程退出后数据丢失
type MemoryStore struct {
	mu             sync.Mutex
	agents         map[domain.ID]domain.AgentInstance
	events         map[domain.ID]domain.Event
	deliveries     map[domain.DeliveryKey]domain.Delivery
	deliveryOrder  []domain.DeliveryKey
	executions     map[domain.ID]domain.Execution
	attempts       map[domain.ID][]domain.Attempt
	claimVersions  map[domain.ID]uint64
	actions        map[domain.ID]domain.ActionRecord
	actionOrder    []domain.ID
	actionAttempts map[domain.ID][]domain.ActionAttempt
}

var _ StateStore = (*MemoryStore)(nil)

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		agents: make(map[domain.ID]domain.AgentInstance), events: make(map[domain.ID]domain.Event),
		deliveries: make(map[domain.DeliveryKey]domain.Delivery), executions: make(map[domain.ID]domain.Execution),
		attempts: make(map[domain.ID][]domain.Attempt), claimVersions: make(map[domain.ID]uint64), actions: make(map[domain.ID]domain.ActionRecord),
		actionAttempts: make(map[domain.ID][]domain.ActionAttempt),
	}
}

func (s *MemoryStore) lock(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	if err := ctx.Err(); err != nil {
		s.mu.Unlock()
		return err
	}
	return nil
}

func (s *MemoryStore) CreateAgent(ctx context.Context, agent domain.AgentInstance) error {
	if err := s.lock(ctx); err != nil {
		return err
	}
	defer s.mu.Unlock()
	if strings.TrimSpace(string(agent.ID)) == "" {
		return fmt.Errorf("agent id 不能为空")
	}
	if err := agent.Definition.Validate(); err != nil {
		return err
	}
	if err := validateAgentStatus(agent.Status); err != nil {
		return err
	}
	if _, err := codec.Encode(agent); err != nil {
		return fmt.Errorf("agent记录: %w", err)
	}
	if _, exists := s.agents[agent.ID]; exists {
		return fmt.Errorf("agent %s: %w", agent.ID, ErrStoreConflict)
	}
	s.agents[agent.ID] = cloneAgent(agent)
	return nil
}

func (s *MemoryStore) LoadAgent(ctx context.Context, agentID domain.ID) (*domain.AgentInstance, error) {
	if err := s.lock(ctx); err != nil {
		return nil, err
	}
	defer s.mu.Unlock()
	agent, exists := s.agents[agentID]
	if !exists {
		return nil, fmt.Errorf("agent %s: %w", agentID, ErrStoreNotFound)
	}
	result := cloneAgent(agent)
	return &result, nil
}

func (s *MemoryStore) ListAgents(ctx context.Context) ([]domain.AgentInstance, error) {
	if err := s.lock(ctx); err != nil {
		return nil, err
	}
	defer s.mu.Unlock()
	result := make([]domain.AgentInstance, 0, len(s.agents))
	for _, agent := range s.agents {
		result = append(result, cloneAgent(agent))
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ID < result[j].ID })
	return result, nil
}

func validateStoredEvent(event domain.Event) error {
	if strings.TrimSpace(string(event.ID)) == "" || strings.TrimSpace(event.Type) == "" {
		return fmt.Errorf("event id/type 不能为空")
	}
	if _, err := codec.Encode(event); err != nil {
		return fmt.Errorf("event记录: %w", err)
	}
	return nil
}

//准备事件，全部校验后统一保存
func (s *MemoryStore) prepareEvent(agentID domain.ID, event domain.Event) (ReceivedEvent, domain.Event, error) {
	if _, exists := s.agents[agentID]; !exists {
		return ReceivedEvent{}, domain.Event{}, fmt.Errorf("agent %s: %w", agentID, ErrStoreNotFound)
	}
	if err := validateStoredEvent(event); err != nil {
		return ReceivedEvent{}, domain.Event{}, err
	}
	if saved, exists := s.events[event.ID]; exists {
		equal, err := sameEventContent(saved, event)
		if err != nil {
			return ReceivedEvent{}, domain.Event{}, err
		}
		if !equal {
			return ReceivedEvent{}, domain.Event{}, fmt.Errorf("event %s 内容不同: %w", event.ID, ErrStoreConflict)
		}
		event = saved
	}
	key := domain.DeliveryKey{AgentID: agentID, EventID: event.ID}
	if delivery, exists := s.deliveries[key]; exists {
		return ReceivedEvent{Delivery: delivery, Duplicate: true}, cloneEvent(event), nil
	}
	executionID, err := domain.NewID()
	if err != nil {
		return ReceivedEvent{}, domain.Event{}, err
	}
	event.CreatedAt = event.CreatedAt.UTC()
	delivery := domain.Delivery{Key: key, ExecutionID: executionID, Status: domain.DeliveryStatusPending}
	return ReceivedEvent{Delivery: delivery}, cloneEvent(event), nil
}

func (s *MemoryStore) ReceiveEvent(ctx context.Context, agentID domain.ID, event domain.Event) (ReceivedEvent, error) {
	if err := s.lock(ctx); err != nil {
		return ReceivedEvent{}, err
	}
	defer s.mu.Unlock()
	if _, exists := s.events[event.ID]; !exists {
		for _, action := range s.actions {
			if action.ResultEventID == event.ID {
				return ReceivedEvent{}, fmt.Errorf("event %s 已保留为action结果: %w", event.ID, ErrStoreConflict)
			}
		}
	}
	received, saved, err := s.prepareEvent(agentID, event)
	if err != nil {
		return ReceivedEvent{}, err
	}
	s.events[event.ID] = saved
	s.deliveries[received.Delivery.Key] = received.Delivery
	if !received.Duplicate {
		s.deliveryOrder = append(s.deliveryOrder, received.Delivery.Key)
	}
	return received, nil
}

func (s *MemoryStore) LoadEvent(ctx context.Context, eventID domain.ID) (*domain.Event, error) {
	if err := s.lock(ctx); err != nil {
		return nil, err
	}
	defer s.mu.Unlock()
	event, exists := s.events[eventID]
	if !exists {
		return nil, fmt.Errorf("event %s: %w", eventID, ErrStoreNotFound)
	}
	result := cloneEvent(event)
	return &result, nil
}

func (s *MemoryStore) LoadDelivery(ctx context.Context, key domain.DeliveryKey) (*domain.Delivery, error) {
	if err := s.lock(ctx); err != nil {
		return nil, err
	}
	defer s.mu.Unlock()
	delivery, exists := s.deliveries[key]
	if !exists {
		return nil, fmt.Errorf("delivery %v: %w", key, ErrStoreNotFound)
	}
	return &delivery, nil
}

func (s *MemoryStore) ListDeliveries(ctx context.Context, statuses ...domain.DeliveryStatus) ([]domain.Delivery, error) {
	if err := s.lock(ctx); err != nil {
		return nil, err
	}
	defer s.mu.Unlock()
	filter := make(map[domain.DeliveryStatus]bool, len(statuses))
	for _, status := range statuses {
		filter[status] = true
	}
	result := make([]domain.Delivery, 0)
	for _, key := range s.deliveryOrder {
		delivery := s.deliveries[key]
		if len(filter) == 0 || filter[delivery.Status] {
			result = append(result, delivery)
		}
	}
	return result, nil
}

func (s *MemoryStore) ClaimExecution(ctx context.Context, key domain.DeliveryKey) (*ExecutionClaim, error) {
	if err := s.lock(ctx); err != nil {
		return nil, err
	}
	defer s.mu.Unlock()
	delivery, exists := s.deliveries[key]
	if !exists {
		return nil, fmt.Errorf("delivery %v: %w", key, ErrStoreNotFound)
	}
	switch delivery.Status {
	case domain.DeliveryStatusRunning:
		return nil, ErrExecutionInProgress
	case domain.DeliveryStatusFailed:
		return nil, ErrDeliveryFailed
	case domain.DeliveryStatusPending:
	default:
		return nil, fmt.Errorf("delivery %v 不可领取: %w", key, ErrStoreConflict)
	}
	agent := s.agents[key.AgentID]
	if agent.Status != domain.AgentStatusActive {
		return nil, fmt.Errorf("agent %s 状态为 %s: %w", agent.ID, agent.Status, ErrAgentUnavailable)
	}
	for _, other := range s.deliveries {
		if other.Key.AgentID == key.AgentID && other.Status == domain.DeliveryStatusRunning {
			return nil, ErrExecutionInProgress
		}
	}
	execution, exists := s.executions[delivery.ExecutionID]
	if !exists {
		execution = domain.Execution{ID: delivery.ExecutionID, AgentID: key.AgentID, EventID: key.EventID,
			Status: domain.ExecutionStatusPending, CreatedAt: time.Now().UTC()}
	}
	if execution.AttemptCount == math.MaxUint64 {
		return nil, fmt.Errorf("execution %s 尝试次数已耗尽: %w", execution.ID, ErrStoreConflict)
	}
	attemptID, err := domain.NewID()
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	attempt := domain.Attempt{ID: attemptID, ExecutionID: execution.ID, Number: execution.AttemptCount + 1,
		Status: domain.AttemptStatusRunning, StartedAt: now}
	execution.Status, execution.StartedAt, execution.FinishedAt = domain.ExecutionStatusRunning, &now, nil
	execution.Error, execution.AttemptCount = "", attempt.Number
	delivery.Status = domain.DeliveryStatusRunning
	s.executions[execution.ID] = execution
	s.attempts[execution.ID] = append(s.attempts[execution.ID], attempt)
	s.claimVersions[attempt.ID] = agent.StateVersion
	s.deliveries[key] = delivery
	return &ExecutionClaim{
		Token: ExecutionToken{Delivery: key, ExecutionID: execution.ID, AttemptID: attempt.ID, ExpectedStateVersion: agent.StateVersion},
		Agent: cloneAgent(agent), Event: cloneEvent(s.events[key.EventID]), Attempt: memoryCloneAttempt(attempt),
	}, nil
}

func (s *MemoryStore) executionForToken(token ExecutionToken) (domain.Delivery, domain.Execution, domain.Attempt, error) {
	delivery, exists := s.deliveries[token.Delivery]
	if !exists || delivery.ExecutionID != token.ExecutionID || delivery.Status != domain.DeliveryStatusRunning {
		return domain.Delivery{}, domain.Execution{}, domain.Attempt{}, ErrStoreStaleClaim
	}
	execution, exists := s.executions[token.ExecutionID]
	attempts := s.attempts[token.ExecutionID]
	if !exists || execution.Status != domain.ExecutionStatusRunning || len(attempts) == 0 ||
		execution.AgentID != token.Delivery.AgentID || execution.EventID != token.Delivery.EventID {
		return domain.Delivery{}, domain.Execution{}, domain.Attempt{}, ErrStoreStaleClaim
	}
	attempt := attempts[len(attempts)-1]
	agent, exists := s.agents[token.Delivery.AgentID]
	if !exists || agent.StateVersion != token.ExpectedStateVersion || s.claimVersions[attempt.ID] != token.ExpectedStateVersion || attempt.ID != token.AttemptID ||
		attempt.Status != domain.AttemptStatusRunning || attempt.Number != execution.AttemptCount {
		return domain.Delivery{}, domain.Execution{}, domain.Attempt{}, ErrStoreStaleClaim
	}
	return delivery, execution, attempt, nil
}

func (s *MemoryStore) CommitExecution(ctx context.Context, commit ExecutionCommit) (domain.ExecutionResult, error) {
	if err := s.lock(ctx); err != nil {
		return domain.ExecutionResult{}, err
	}
	defer s.mu.Unlock()
	delivery, execution, attempt, err := s.executionForToken(commit.Token)
	if err != nil {
		return domain.ExecutionResult{}, err
	}
	if _, err := codec.Encode(commit); err != nil {
		return domain.ExecutionResult{}, fmt.Errorf("提交execution记录: %w", err)
	}
	result := domain.ExecutionResult{StateUpdate: cloneMap(commit.StateUpdate)}
	if commit.Actions != nil {
		result.Actions = make([]domain.Action, 0, len(commit.Actions))
	}
	ids, eventIDs := make(map[domain.ID]bool), make(map[domain.ID]bool)
	for _, action := range commit.Actions {
		if err := s.validateNewAction(action, execution); err != nil {
			return domain.ExecutionResult{}, err
		}
		if ids[action.Request.ID] || eventIDs[action.ResultEventID] {
			return domain.ExecutionResult{}, fmt.Errorf("action或结果event id重复: %w", ErrStoreConflict)
		}
		ids[action.Request.ID], eventIDs[action.ResultEventID] = true, true
		result.Actions = append(result.Actions, cloneActions([]domain.Action{action.Request})[0])
	}
	agent := cloneAgent(s.agents[execution.AgentID])
	if err := (StateManager{}).Apply(&agent, result); err != nil {
		return domain.ExecutionResult{}, err
	}
	now := time.Now().UTC()
	attempt.Status, attempt.FinishedAt = domain.AttemptStatusSucceeded, &now
	execution.Status, execution.FinishedAt, execution.Error = domain.ExecutionStatusCompleted, &now, ""
	savedResult := cloneResult(result)
	execution.Result = &savedResult
	delivery.Status = domain.DeliveryStatusCompleted
	//统一发布本轮写入
	s.agents[agent.ID] = agent
	for _, action := range commit.Actions {
		s.actions[action.Request.ID] = memoryCloneActionRecord(action)
		s.actionOrder = append(s.actionOrder, action.Request.ID)
	}
	s.executions[execution.ID] = execution
	s.attempts[execution.ID][len(s.attempts[execution.ID])-1] = attempt
	s.deliveries[delivery.Key] = delivery
	return cloneResult(result), nil
}

func (s *MemoryStore) validateNewAction(action domain.ActionRecord, execution domain.Execution) error {
	if action.Request.ID == "" || action.Request.Type == "" || action.AgentID != execution.AgentID ||
		action.Request.ExecutionID == nil || *action.Request.ExecutionID != execution.ID ||
		action.Status != domain.ActionStatusPending || action.AttemptCount != 0 || action.Result != nil || action.LastError != nil ||
		strings.TrimSpace(action.HandlerVersion) == "" || strings.TrimSpace(action.IdempotencyKey) == "" ||
		action.MaxAttempts == 0 || action.ResultEventID == "" {
		return fmt.Errorf("action %s 初始记录或关联无效: %w", action.Request.ID, ErrStoreConflict)
	}
	if action.RecoveryPolicy != domain.RecoveryPolicyManual && action.RecoveryPolicy != domain.RecoveryPolicySafeRetry {
		return fmt.Errorf("action %s 恢复策略无效: %w", action.Request.ID, ErrStoreConflict)
	}
	if _, exists := s.actions[action.Request.ID]; exists {
		return fmt.Errorf("action %s 已存在: %w", action.Request.ID, ErrStoreConflict)
	}
	if _, exists := s.events[action.ResultEventID]; exists {
		return fmt.Errorf("action结果event %s 已存在: %w", action.ResultEventID, ErrStoreConflict)
	}
	for _, saved := range s.actions {
		if saved.ResultEventID == action.ResultEventID {
			return fmt.Errorf("action结果event %s 已保留: %w", action.ResultEventID, ErrStoreConflict)
		}
	}
	return nil
}

func validateStoredFailure(failure domain.Failure) error {
	switch failure.Kind {
	case domain.ErrorKindBusiness, domain.ErrorKindRuntime, domain.ErrorKindInterrupted, domain.ErrorKindUnknown:
	default:
		return fmt.Errorf("failure种类无效 %q", failure.Kind)
	}
	_, err := codec.Encode(failure)
	return err
}

func (s *MemoryStore) FailExecution(ctx context.Context, failure ExecutionFailure) error {
	if err := s.lock(ctx); err != nil {
		return err
	}
	defer s.mu.Unlock()
	delivery, execution, attempt, err := s.executionForToken(failure.Token)
	if err != nil {
		return err
	}
	if err := validateStoredFailure(failure.Failure); err != nil {
		return err
	}
	now := time.Now().UTC()
	attempt.Status, attempt.FinishedAt = domain.AttemptStatusFailed, &now
	if failure.Interrupted {
		attempt.Status = domain.AttemptStatusInterrupted
	}
	attempt.Error = memoryCloneFailure(&failure.Failure)
	execution.Status, execution.FinishedAt, execution.Error = domain.ExecutionStatusFailed, &now, failure.Failure.Message
	delivery.Status = domain.DeliveryStatusFailed
	s.executions[execution.ID] = execution
	s.attempts[execution.ID][len(s.attempts[execution.ID])-1] = attempt
	s.deliveries[delivery.Key] = delivery
	return nil
}

func (s *MemoryStore) RequeueDelivery(ctx context.Context, key domain.DeliveryKey) error {
	if err := s.lock(ctx); err != nil {
		return err
	}
	defer s.mu.Unlock()
	delivery, exists := s.deliveries[key]
	if !exists {
		return fmt.Errorf("delivery %v: %w", key, ErrStoreNotFound)
	}
	if delivery.Status != domain.DeliveryStatusFailed {
		return fmt.Errorf("只能重试失败的delivery %v: %w", key, ErrStoreConflict)
	}
	execution := s.executions[delivery.ExecutionID]
	execution.Status = domain.ExecutionStatusPending
	delivery.Status = domain.DeliveryStatusPending
	s.executions[execution.ID], s.deliveries[key] = execution, delivery
	return nil
}

func (s *MemoryStore) LoadExecution(ctx context.Context, executionID domain.ID) (*StoredExecution, error) {
	if err := s.lock(ctx); err != nil {
		return nil, err
	}
	defer s.mu.Unlock()
	execution, exists := s.executions[executionID]
	if !exists {
		return nil, fmt.Errorf("execution %s: %w", executionID, ErrStoreNotFound)
	}
	result := &StoredExecution{Execution: cloneExecution(execution), Attempts: make([]domain.Attempt, len(s.attempts[executionID]))}
	for i, attempt := range s.attempts[executionID] {
		result.Attempts[i] = memoryCloneAttempt(attempt)
	}
	return result, nil
}

func (s *MemoryStore) ClaimAction(ctx context.Context, actionID domain.ID) (*ActionClaim, error) {
	if err := s.lock(ctx); err != nil {
		return nil, err
	}
	defer s.mu.Unlock()
	action, exists := s.actions[actionID]
	if !exists {
		return nil, fmt.Errorf("action %s: %w", actionID, ErrStoreNotFound)
	}
	if action.Status != domain.ActionStatusPending &&
		!(action.Status == domain.ActionStatusUnknown && action.RecoveryPolicy == domain.RecoveryPolicySafeRetry) {
		return nil, fmt.Errorf("action %s 不可领取: %w", actionID, ErrStoreConflict)
	}
	if action.AttemptCount >= action.MaxAttempts || action.AttemptCount == math.MaxUint64 {
		return nil, fmt.Errorf("action %s 已达到尝试上限: %w", actionID, ErrStoreConflict)
	}
	attemptID, err := domain.NewID()
	if err != nil {
		return nil, err
	}
	action.Status, action.AttemptCount = domain.ActionStatusRunning, action.AttemptCount+1
	attempt := domain.ActionAttempt{ID: attemptID, ActionID: actionID, Number: action.AttemptCount,
		Status: domain.ActionStatusRunning, StartedAt: time.Now().UTC()}
	s.actions[actionID] = action
	s.actionAttempts[actionID] = append(s.actionAttempts[actionID], attempt)
	return &ActionClaim{Token: ActionToken{ActionID: actionID, AttemptNumber: action.AttemptCount}, Record: memoryCloneActionRecord(action)}, nil
}

func (s *MemoryStore) actionForToken(token ActionToken) (domain.ActionRecord, domain.ActionAttempt, error) {
	action, exists := s.actions[token.ActionID]
	attempts := s.actionAttempts[token.ActionID]
	if !exists || action.Status != domain.ActionStatusRunning || action.AttemptCount != token.AttemptNumber || len(attempts) == 0 {
		return domain.ActionRecord{}, domain.ActionAttempt{}, ErrStoreStaleClaim
	}
	attempt := attempts[len(attempts)-1]
	if attempt.Status != domain.ActionStatusRunning || attempt.Number != token.AttemptNumber {
		return domain.ActionRecord{}, domain.ActionAttempt{}, ErrStoreStaleClaim
	}
	return action, attempt, nil
}

func (s *MemoryStore) CompleteAction(ctx context.Context, completion ActionCompletion) (domain.ActionResult, error) {
	if err := s.lock(ctx); err != nil {
		return domain.ActionResult{}, err
	}
	defer s.mu.Unlock()
	action, exists := s.actions[completion.Token.ActionID]
	if !exists {
		return domain.ActionResult{}, ErrStoreNotFound
	}
	if err := validateActionCompletion(action, completion); err != nil {
		return domain.ActionResult{}, err
	}
	if action.Result != nil {
		if action.AttemptCount != completion.Token.AttemptNumber {
			return domain.ActionResult{}, ErrStoreStaleClaim
		}
		sameResult, err := sameJSONValue(*action.Result, completion.Result)
		if err != nil {
			return domain.ActionResult{}, err
		}
		sameEvent, err := sameEventContent(s.events[action.ResultEventID], completion.Event)
		if err != nil {
			return domain.ActionResult{}, err
		}
		if !sameResult || !sameEvent {
			return domain.ActionResult{}, ErrStoreConflict
		}
		return memoryCloneActionResult(*action.Result), nil
	}
	action, attempt, err := s.actionForToken(completion.Token)
	if err != nil {
		return domain.ActionResult{}, err
	}
	received, event, err := s.prepareEvent(action.AgentID, completion.Event)
	if err != nil {
		return domain.ActionResult{}, err
	}
	result := memoryCloneActionResult(completion.Result)
	action.Status, action.Result, action.LastError = result.Status, &result, memoryCloneFailure(result.Error)
	now := time.Now().UTC()
	attempt.Status, attempt.FinishedAt, attempt.Error = result.Status, &now, memoryCloneFailure(result.Error)
	s.actions[action.Request.ID] = action
	s.actionAttempts[action.Request.ID][len(s.actionAttempts[action.Request.ID])-1] = attempt
	s.events[event.ID] = event
	s.deliveries[received.Delivery.Key] = received.Delivery
	if !received.Duplicate {
		s.deliveryOrder = append(s.deliveryOrder, received.Delivery.Key)
	}
	return memoryCloneActionResult(result), nil
}

func validateActionCompletion(action domain.ActionRecord, completion ActionCompletion) error {
	result, event := completion.Result, completion.Event
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

func (s *MemoryStore) RecordActionUnknown(ctx context.Context, token ActionToken, failure domain.Failure) error {
	if err := s.lock(ctx); err != nil {
		return err
	}
	defer s.mu.Unlock()
	action, attempt, err := s.actionForToken(token)
	if err != nil {
		return err
	}
	if err := validateStoredFailure(failure); err != nil {
		return err
	}
	now := time.Now().UTC()
	action.Status, action.LastError = domain.ActionStatusUnknown, memoryCloneFailure(&failure)
	attempt.Status, attempt.FinishedAt, attempt.Error = domain.ActionStatusUnknown, &now, memoryCloneFailure(&failure)
	s.actions[action.Request.ID] = action
	s.actionAttempts[action.Request.ID][len(s.actionAttempts[action.Request.ID])-1] = attempt
	return nil
}

func (s *MemoryStore) LoadAction(ctx context.Context, actionID domain.ID) (*StoredAction, error) {
	if err := s.lock(ctx); err != nil {
		return nil, err
	}
	defer s.mu.Unlock()
	action, exists := s.actions[actionID]
	if !exists {
		return nil, fmt.Errorf("action %s: %w", actionID, ErrStoreNotFound)
	}
	result := &StoredAction{Action: memoryCloneActionRecord(action), Attempts: make([]domain.ActionAttempt, len(s.actionAttempts[actionID]))}
	for i, attempt := range s.actionAttempts[actionID] {
		attempt.FinishedAt, attempt.Error = cloneTime(attempt.FinishedAt), memoryCloneFailure(attempt.Error)
		result.Attempts[i] = attempt
	}
	return result, nil
}

func (s *MemoryStore) ListActions(ctx context.Context, statuses ...domain.ActionStatus) ([]domain.ActionRecord, error) {
	if err := s.lock(ctx); err != nil {
		return nil, err
	}
	defer s.mu.Unlock()
	filter := make(map[domain.ActionStatus]bool, len(statuses))
	for _, status := range statuses {
		filter[status] = true
	}
	result := make([]domain.ActionRecord, 0)
	for _, actionID := range s.actionOrder {
		action := s.actions[actionID]
		if len(filter) == 0 || filter[action.Status] {
			result = append(result, memoryCloneActionRecord(action))
		}
	}
	return result, nil
}

func memoryCloneFailure(failure *domain.Failure) *domain.Failure {
	if failure == nil {
		return nil
	}
	result := *failure
	return &result
}

func memoryCloneAttempt(attempt domain.Attempt) domain.Attempt {
	attempt.FinishedAt, attempt.Error = cloneTime(attempt.FinishedAt), memoryCloneFailure(attempt.Error)
	return attempt
}

func memoryCloneActionResult(result domain.ActionResult) domain.ActionResult {
	result.Output, result.Error = cloneMap(result.Output), memoryCloneFailure(result.Error)
	return result
}

func memoryCloneActionRecord(record domain.ActionRecord) domain.ActionRecord {
	record.Request = cloneActions([]domain.Action{record.Request})[0]
	record.LastError = memoryCloneFailure(record.LastError)
	if record.Result != nil {
		result := memoryCloneActionResult(*record.Result)
		record.Result = &result
	}
	return record
}
