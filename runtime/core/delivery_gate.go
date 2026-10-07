package core

import (
	"agent-runtime/domain"
	"context"
	"fmt"
	"sync"
)

type deliveryNotReadyError struct {
	blocked []BlockReason
}

func (err *deliveryNotReadyError) Error() string {
	return fmt.Sprintf("%s: %s", ErrDeliveryNotReady, err.blocked[0].Message)
}

func (err *deliveryNotReadyError) Unwrap() error { return ErrDeliveryNotReady }

func (r *Runtime) executionGate(agentID domain.ID) *sync.Mutex {
	r.mu.Lock()
	defer r.mu.Unlock()
	gate := r.executionGates[agentID]
	if gate == nil {
		gate = &sync.Mutex{}
		r.executionGates[agentID] = gate
	}
	return gate
}

// 以store保存的接收顺序为准，结果事件也包含在列表中，由definition选择等待规则。
func (r *Runtime) earlierEvents(ctx context.Context, key domain.DeliveryKey) ([]domain.Event, error) {
	deliveries, err := r.store.ListDeliveries(ctx, domain.DeliveryStatusPending, domain.DeliveryStatusRunning)
	if err != nil {
		return nil, err
	}
	var earlier []domain.Event
	for _, delivery := range deliveries {
		if delivery.Key == key {
			break
		}
		if delivery.Key.AgentID != key.AgentID {
			continue
		}
		event, err := r.store.LoadEvent(ctx, delivery.Key.EventID)
		if err != nil {
			return nil, err
		}
		earlier = append(earlier, *event)
	}
	return earlier, nil
}

func deliveryBlockedBy(runner AgentRunner, agent AgentSnapshot, event domain.Event, earlier []domain.Event) (blocked []BlockReason, err error) {
	gate, ok := runner.(DeliveryGate)
	if !ok {
		return nil, nil
	}
	defer func() {
		if recovered := recover(); recovered != nil {
			blocked, err = nil, fmt.Errorf("投递等待规则异常: %v", recovered)
		}
	}()
	agent.State = cloneMap(agent.State)
	events := make([]domain.Event, len(earlier))
	for i, previous := range earlier {
		events[i] = cloneEvent(previous)
	}
	return append([]BlockReason(nil), gate.DeliveryBlockedBy(agent, cloneEvent(event), events)...), nil
}

func prepareDelivery(ctx context.Context, runner AgentRunner, agent AgentSnapshot, event domain.Event) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("准备投递异常: %v", recovered)
		}
	}()
	agent.State = cloneMap(agent.State)
	return runner.(DeliveryPreparer).PrepareDelivery(ctx, agent, cloneEvent(event))
}
