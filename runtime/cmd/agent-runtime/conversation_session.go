package main

import (
	"agent-runtime/cli"
	"agent-runtime/core"
	"agent-runtime/domain"
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

var errNoMainAgent = errors.New("尚未创建MainAgent，运行chat即可开始对话")

func defaultDataDir() string {
	if source := defaultPythonSource(); source != "" {
		return filepath.Join(filepath.Dir(filepath.Dir(source)), ".agent-runtime")
	}
	return ".agent-runtime"
}

// 每次操作只持有一个短会话；读取下一条用户输入时释放存储所有权。
func withConversation(ctx context.Context, options commandOptions, open backendOpener, create bool, fn func(*core.Runtime, core.AgentSnapshot, func(context.Context) error) error) (err error) {
	cleanup := &conversationCleanup{}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		if closeErr := cleanup.close(closeCtx); closeErr != nil {
			err = &conversationCleanupError{cause: errors.Join(err, closeErr), cleanup: cleanup}
		}
	}()
	if open == nil {
		return cli.ErrBackendUnavailable
	}
	if options.dataDir == "" {
		options.dataDir = defaultDataDir()
	}
	backend, err := open(ctx, options.dataDir)
	cleanup.backend = backend.Close
	if err != nil {
		return err
	}
	runtime, err := core.OpenRuntime(ctx, backend.Store)
	if err != nil {
		var pending *core.RuntimeOpenError
		if errors.As(err, &pending) {
			cleanup.runtime = pending
		}
		return err
	}
	cleanup.runtime = runtime
	cleanup.binding, err = bindPersistent(ctx, runtime, options)
	if err != nil {
		return err
	}
	agent, err := selectConversationAgent(ctx, runtime, options.request.AgentID, create)
	if err != nil {
		return err
	}
	preparer, ok := cleanup.binding.(interface{ PrepareModel(context.Context) error })
	if !ok {
		return errors.New("对话绑定不支持模型配置")
	}
	if options.request.Command == "chat" || options.request.Command == "resume" {
		needed, err := modelWorkPendingForAgent(ctx, runtime, agent.ID)
		if err != nil {
			return err
		}
		if needed {
			if err := preparer.PrepareModel(ctx); err != nil {
				return err
			}
		}
	}
	return fn(runtime, agent, preparer.PrepareModel)
}

func selectConversationAgent(ctx context.Context, runtime *core.Runtime, id domain.ID, create bool) (core.AgentSnapshot, error) {
	mainDefinition := domain.DefinitionRef{ID: "main", Version: "1"}
	if id != "" {
		agent, err := runtime.AgentContext(ctx, id)
		if err != nil {
			return core.AgentSnapshot{}, fmt.Errorf("选择MainAgent: %w", err)
		}
		if agent.Definition != mainDefinition || agent.Status != domain.AgentStatusActive {
			return core.AgentSnapshot{}, &cli.UsageError{Message: fmt.Sprintf("--agent需要指定active状态的main@1，当前%s为%s@%s(%s)", id, agent.Definition.ID, agent.Definition.Version, agent.Status)}
		}
		return agent, nil
	}
	agents, err := runtime.AgentsContext(ctx)
	if err != nil {
		return core.AgentSnapshot{}, err
	}
	candidates := make([]core.AgentSnapshot, 0)
	for _, agent := range agents {
		if agent.Status == domain.AgentStatusActive && agent.Definition == mainDefinition {
			candidates = append(candidates, agent)
		}
	}
	switch len(candidates) {
	case 0:
		if !create {
			return core.AgentSnapshot{}, errNoMainAgent
		}
		return runtime.CreateAgentContext(ctx, "main", mainDefinition, nil)
	case 1:
		return candidates[0], nil
	default:
		sort.Slice(candidates, func(i, j int) bool {
			if candidates[i].Name == candidates[j].Name {
				return candidates[i].ID < candidates[j].ID
			}
			return candidates[i].Name < candidates[j].Name
		})
		names := make([]string, len(candidates))
		for i, candidate := range candidates {
			names[i] = fmt.Sprintf("%s(%s)", candidate.Name, candidate.ID)
		}
		return core.AgentSnapshot{}, &cli.UsageError{Message: "存在多个MainAgent：" + strings.Join(names, "、") + "；请使用--agent指定"}
	}
}

type conversationCleanupError struct {
	cause   error
	cleanup *conversationCleanup
}

func (e *conversationCleanupError) Error() string { return e.cause.Error() }
func (e *conversationCleanupError) Unwrap() error { return e.cause }
func (e *conversationCleanupError) Close(ctx context.Context) error {
	return e.cleanup.close(ctx)
}

type conversationCleanup struct {
	mu      sync.Mutex
	runtime interface{ Close(context.Context) error }
	binding io.Closer
	backend func() error
}

func (c *conversationCleanup) close(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.runtime != nil {
		if err := c.runtime.Close(ctx); err != nil {
			return fmt.Errorf("关闭对话runtime: %w", err)
		}
		c.runtime = nil
	}
	var result error
	if c.binding != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := c.binding.Close(); err != nil {
			result = fmt.Errorf("关闭对话绑定: %w", err)
		} else {
			c.binding = nil
		}
	}
	if c.backend != nil {
		if err := ctx.Err(); err != nil {
			return errors.Join(result, err)
		}
		if err := c.backend(); err != nil {
			result = errors.Join(result, fmt.Errorf("关闭对话后端: %w", err))
		} else {
			c.backend = nil
		}
	}
	return result
}
