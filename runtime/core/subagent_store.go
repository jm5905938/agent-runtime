package core

import (
	"agent-runtime/domain"
	"context"
	"errors"
)

var ErrSubagentBusy = errors.New("subagent仍有未确认的在途操作")

type SubagentSpawn struct {
	Token ActionToken
	Child domain.AgentInstance
	Event domain.Event
}

type SubagentStore interface {
	SpawnSubagent(context.Context, SubagentSpawn) (*domain.SubagentTask, error)
	LoadSubagentTask(context.Context, domain.ID) (*domain.SubagentTask, error)
	ListSubagentTasks(context.Context) ([]domain.SubagentTask, error)
	RequestSubagentCancel(context.Context, domain.ID, domain.ID) (*domain.SubagentTask, error)
	FinishSubagentCancel(context.Context, domain.ID) error
	ClaimSubagentCancelAction(context.Context, domain.ID) (*ActionClaim, error)
}
