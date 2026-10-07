package core

import (
	"agent-runtime/domain"
	"context"
	"fmt"
	"sort"
)

var _ SubagentStore = (*MemoryStore)(nil)

func (s *MemoryStore) taskForChild(childID domain.ID) *domain.SubagentTask {
	for _, task := range s.tasks {
		if task.ChildAgentID == childID {
			copy := cloneSubagentTask(task)
			return &copy
		}
	}
	return nil
}

func (s *MemoryStore) SpawnSubagent(ctx context.Context, spawn SubagentSpawn) (*domain.SubagentTask, error) {
	if err := s.lock(ctx); err != nil {
		return nil, err
	}
	defer s.mu.Unlock()
	action, _, err := s.actionForToken(spawn.Token)
	if err != nil {
		return nil, err
	}
	if err := ValidateSubagentSpawn(action, spawn); err != nil {
		return nil, err
	}
	if s.taskForChild(action.AgentID) != nil {
		return nil, fmt.Errorf("subagent不能递归委派: %w", ErrStoreConflict)
	}
	if task, exists := s.tasks[action.Request.ID]; exists {
		equal, err := SameEventContent(s.events[task.InitialEventID], spawn.Event)
		if err != nil || !equal || task.ChildAgentID != spawn.Child.ID || task.ParentAgentID != action.AgentID || task.InitialEventID != spawn.Event.ID {
			return nil, fmt.Errorf("重复subagent创建内容不一致: %w", ErrStoreConflict)
		}
		copy := cloneSubagentTask(task)
		return &copy, nil
	}
	if _, exists := s.agents[spawn.Child.ID]; exists {
		return nil, ErrStoreConflict
	}
	if _, exists := s.events[spawn.Event.ID]; exists {
		return nil, ErrStoreConflict
	}
	for _, record := range s.actions {
		if record.ResultEventID == spawn.Event.ID {
			return nil, ErrStoreConflict
		}
	}
	executionID, err := domain.NewID()
	if err != nil {
		return nil, err
	}
	task := domain.SubagentTask{ID: action.Request.ID, ParentAgentID: action.AgentID, ChildAgentID: spawn.Child.ID, InitialEventID: spawn.Event.ID}
	event := cloneEvent(spawn.Event)
	event.CreatedAt = event.CreatedAt.UTC()
	delivery := domain.Delivery{Key: domain.DeliveryKey{AgentID: spawn.Child.ID, EventID: event.ID}, ExecutionID: executionID, Status: domain.DeliveryStatusPending}
	s.agents[spawn.Child.ID] = cloneAgent(spawn.Child)
	s.events[event.ID], s.deliveries[delivery.Key] = event, delivery
	s.deliveryOrder = append(s.deliveryOrder, delivery.Key)
	s.tasks[task.ID] = task
	copy := cloneSubagentTask(task)
	return &copy, nil
}

func (s *MemoryStore) LoadSubagentTask(ctx context.Context, id domain.ID) (*domain.SubagentTask, error) {
	if err := s.lock(ctx); err != nil {
		return nil, err
	}
	defer s.mu.Unlock()
	task, exists := s.tasks[id]
	if !exists {
		return nil, ErrStoreNotFound
	}
	copy := cloneSubagentTask(task)
	return &copy, nil
}

func (s *MemoryStore) ListSubagentTasks(ctx context.Context) ([]domain.SubagentTask, error) {
	if err := s.lock(ctx); err != nil {
		return nil, err
	}
	defer s.mu.Unlock()
	tasks := make([]domain.SubagentTask, 0, len(s.tasks))
	for _, task := range s.tasks {
		tasks = append(tasks, cloneSubagentTask(task))
	}
	sort.Slice(tasks, func(i, j int) bool { return tasks[i].ID < tasks[j].ID })
	return tasks, nil
}

func (s *MemoryStore) RequestSubagentCancel(ctx context.Context, parentID, id domain.ID) (*domain.SubagentTask, error) {
	if err := s.lock(ctx); err != nil {
		return nil, err
	}
	defer s.mu.Unlock()
	task, exists := s.tasks[id]
	if !exists {
		return nil, ErrStoreNotFound
	}
	if task.ParentAgentID != parentID {
		return nil, fmt.Errorf("只能取消当前agent的子任务: %w", ErrStoreConflict)
	}
	if task.Result == nil {
		task.CancelRequested = true
		s.tasks[id] = task
	}
	copy := cloneSubagentTask(task)
	return &copy, nil
}

func (s *MemoryStore) childActionsUnresolved(childID domain.ID) bool {
	events := make([]domain.Event, 0)
	for _, action := range s.actions {
		if action.AgentID != childID {
			continue
		}
		if event, exists := s.events[actionResolutionEventID(action.Request.ID)]; exists {
			events = append(events, event)
		}
		if SubagentActionUnresolved(action, events) {
			return true
		}
	}
	return false
}

func (s *MemoryStore) prepareTaskCompletion(agent *domain.AgentInstance, executionID domain.ID, result *domain.SubagentResult) (*domain.SubagentTask, error) {
	if result == nil {
		return nil, nil
	}
	if err := ValidateSubagentResult(result); err != nil {
		return nil, err
	}
	task := s.taskForChild(agent.ID)
	if task == nil || task.Result != nil {
		return nil, fmt.Errorf("subagent完成关联无效: %w", ErrStoreConflict)
	}
	if s.childActionsUnresolved(agent.ID) {
		return nil, ErrSubagentBusy
	}
	if err := TerminateSubagentAgent(agent); err != nil {
		return nil, err
	}
	task.Result, task.CompletionExecutionID = cloneSubagentResult(result), executionID
	return task, nil
}

func (s *MemoryStore) FinishSubagentCancel(ctx context.Context, id domain.ID) error {
	if err := s.lock(ctx); err != nil {
		return err
	}
	defer s.mu.Unlock()
	task, exists := s.tasks[id]
	if !exists {
		return ErrStoreNotFound
	}
	if task.Result != nil {
		return nil
	}
	if !task.CancelRequested {
		return ErrStoreConflict
	}
	for _, delivery := range s.deliveries {
		if delivery.Key.AgentID == task.ChildAgentID && delivery.Status == domain.DeliveryStatusRunning {
			return ErrSubagentBusy
		}
	}
	if s.childActionsUnresolved(task.ChildAgentID) {
		return ErrSubagentBusy
	}
	agent := cloneAgent(s.agents[task.ChildAgentID])
	if err := TerminateSubagentAgent(&agent); err != nil {
		return err
	}
	task.Result = &domain.SubagentResult{Status: domain.SubagentStatusCancelled, Output: map[string]any{}, Error: &domain.Failure{Kind: domain.ErrorKindBusiness, Message: "任务已取消"}}
	s.tasks[id], s.agents[agent.ID] = task, agent
	return nil
}
