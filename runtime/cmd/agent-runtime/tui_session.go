package main

import (
	"agent-runtime/core"
	"agent-runtime/domain"
	"agent-runtime/model"
	"context"
	"errors"
	"io"
	"sync"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/x/term"
)

// 终端界面共用一个会话，退出时先等待后台操作，再释放存储所有权。
type tuiSession struct {
	ctx     context.Context
	cancel  context.CancelFunc
	runtime *core.Runtime
	agentID domain.ID
	prepare func(context.Context) error
	mu      sync.Mutex
	closed  bool
	work    sync.WaitGroup
}

func newTUISession(ctx context.Context, runtime *core.Runtime, agentID domain.ID, prepare func(context.Context) error) *tuiSession {
	ctx, cancel := context.WithCancel(ctx)
	return &tuiSession{ctx: ctx, cancel: cancel, runtime: runtime, agentID: agentID, prepare: prepare}
}

func (s *tuiSession) begin() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return context.Canceled
	}
	s.work.Add(1)
	return nil
}

func (s *tuiSession) close() {
	s.mu.Lock()
	s.closed = true
	s.cancel()
	s.mu.Unlock()
	s.work.Wait()
}

func (s *tuiSession) query() (core.AgentQuery, error) {
	if err := s.begin(); err != nil {
		return core.AgentQuery{}, err
	}
	defer s.work.Done()
	return s.runtime.QueryAgentContext(s.ctx, s.agentID)
}

func (s *tuiSession) submit(event domain.Event) error {
	if err := s.begin(); err != nil {
		return err
	}
	defer s.work.Done()
	_, err := s.runtime.SubmitContext(s.ctx, s.agentID, event)
	return err
}

func (s *tuiSession) run(ctx context.Context, resume bool, decision core.ResolutionDecision) error {
	if err := s.begin(); err != nil {
		return err
	}
	defer s.work.Done()
	if resume {
		unknown, err := treeUnknownConversationAction(ctx, s.runtime, s.agentID)
		if err != nil {
			return err
		}
		if unknown != nil {
			if decision == "" {
				return errors.New("模型调用结果未知，请选择重试或放弃")
			}
			if decision == core.ResolutionRetry {
				if err := s.prepare(ctx); err != nil {
					return err
				}
			}
			if _, err := s.runtime.ResolveAction(ctx, unknown.Action.Request.ID, decision, "用户在终端界面选择"+string(decision)); err != nil {
				return err
			}
		}
		if err := retryConversationTree(ctx, s.runtime, s.agentID, decision, s.prepare); err != nil {
			return err
		}
	}
	if needed, err := modelWorkPendingForTree(ctx, s.runtime, s.agentID); err != nil {
		return err
	} else if needed {
		if err := s.prepare(ctx); err != nil {
			return err
		}
	}
	return s.runtime.RunAgentTreeUntilIdleContext(ctx, s.agentID)
}

func (s *tuiSession) treeState() (bool, *core.ActionQuery, error) {
	if err := s.begin(); err != nil {
		return false, nil, err
	}
	defer s.work.Done()
	ids, err := s.runtime.AgentTreeContext(s.ctx, s.agentID)
	if err != nil {
		return false, nil, err
	}
	ready := false
	var unknown *core.ActionQuery
	for _, id := range ids {
		query, err := s.runtime.QueryAgentContext(s.ctx, id)
		if err != nil {
			return false, nil, err
		}
		ready = ready || tuiReady(query)
		if unknown == nil {
			unknown = waitingUnknownConversationAction(query)
		}
	}
	return ready, unknown, nil
}

func runTUI(ctx context.Context, options commandOptions, stdin io.Reader, stdout io.Writer, open backendOpener) error {
	input, inputOK := stdin.(interface{ Fd() uintptr })
	output, outputOK := stdout.(interface{ Fd() uintptr })
	if !inputOK || !outputOK || !term.IsTerminal(input.Fd()) || !term.IsTerminal(output.Fd()) {
		return errors.New("tui需要交互终端；脚本请使用chat --message或--json")
	}
	return withConversation(ctx, options, open, true, func(runtime *core.Runtime, agent core.AgentSnapshot, prepare func(context.Context) error) error {
		session := newTUISession(ctx, runtime, agent.ID, prepare)
		defer session.close()
		query, err := session.query()
		if err != nil {
			return err
		}
		view := newTUIModel(session, query, options.dataDir)
		view.treeReady, view.unknown, err = session.treeState()
		if err != nil {
			return err
		}
		if config, err := model.LoadConfig(options.envFile); err == nil {
			view.modelName = config.Model
		}
		program := tea.NewProgram(view, tea.WithContext(session.ctx),
			tea.WithInput(stdin), tea.WithOutput(stdout), tea.WithAltScreen())
		_, err = program.Run()
		if errors.Is(err, tea.ErrProgramKilled) && ctx.Err() != nil {
			return nil
		}
		return err
	})
}
