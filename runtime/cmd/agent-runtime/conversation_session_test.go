package main

import (
	"agent-runtime/cli"
	"agent-runtime/core"
	"agent-runtime/domain"
	pythonrunner "agent-runtime/python"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func conversationMemoryOpener(store core.RecoveryStore) backendOpener {
	return func(context.Context, string) (backendHandle, error) {
		return backendHandle{Store: store}, nil
	}
}

func seedConversationAgent(t *testing.T, store core.RecoveryStore, name, definition string, status domain.AgentStatus) domain.ID {
	t.Helper()
	ctx := context.Background()
	session, err := store.OpenSession(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := session.Close(ctx); err != nil {
			t.Error(err)
		}
	}()
	if _, err := session.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	agent := domain.NewAgentInstance(name)
	agent.Definition = domain.DefinitionRef{ID: definition, Version: "1"}
	agent.Status = status
	if err := session.CreateAgent(ctx, agent); err != nil {
		t.Fatal(err)
	}
	return agent.ID
}

func TestConversationCreatesAndReusesMainAcrossShortSessions(t *testing.T) {
	ctx := context.Background()
	store := core.NewMemoryRecoveryStore()
	open := conversationMemoryOpener(store)
	options := commandOptions{request: cli.Request{Command: "status"}}
	var id domain.ID
	for round := range 2 {
		err := withConversation(ctx, options, open, true, func(runtime *core.Runtime, agent core.AgentSnapshot, prepare func(context.Context) error) error {
			if round == 0 {
				id = agent.ID
			}
			if agent.ID != id || agent.Name != "main" || agent.Definition != (domain.DefinitionRef{ID: "main", Version: "1"}) || agent.Status != domain.AgentStatusActive {
				t.Fatalf("未恢复同一MainAgent: %+v", agent)
			}
			if prepare == nil {
				t.Fatal("callback缺少模型准备入口")
			}
			if _, err := store.OpenSession(ctx); !errors.Is(err, core.ErrStoreOwned) {
				t.Fatalf("callback期间未保持存储所有权: %v", err)
			}
			if round == 0 {
				_, err := runtime.SubmitContext(ctx, agent.ID, domain.NewEvent("main.request", map[string]any{"message": "保留这条输入"}))
				return err
			}
			query, err := runtime.QueryAgentContext(ctx, agent.ID)
			if err == nil && (len(query.Deliveries) != 1 || query.Deliveries[0].Event.Payload["message"] != "保留这条输入") {
				t.Fatalf("短会话丢失持久待办: %+v", query)
			}
			return err
		})
		if err != nil {
			t.Fatal(err)
		}
		// 两轮用户输入之间必须释放owner锁。
		session, err := store.OpenSession(ctx)
		if err != nil {
			t.Fatalf("回合结束未释放所有权: %v", err)
		}
		if _, err := session.Recover(ctx); err != nil {
			t.Fatal(err)
		}
		agents, err := session.ListAgents(ctx)
		if err != nil || len(agents) != 1 {
			t.Fatalf("恢复创建了多余Agent: %+v %v", agents, err)
		}
		if err := session.Close(ctx); err != nil {
			t.Fatal(err)
		}
	}
}

func TestConversationAgentSelection(t *testing.T) {
	t.Run("status_does_not_create", func(t *testing.T) {
		store := core.NewMemoryRecoveryStore()
		err := withConversation(context.Background(), commandOptions{request: cli.Request{Command: "status"}}, conversationMemoryOpener(store), false,
			func(*core.Runtime, core.AgentSnapshot, func(context.Context) error) error {
				t.Fatal("空状态进入callback")
				return nil
			})
		if !errors.Is(err, errNoMainAgent) {
			t.Fatalf("空状态错误不符: %v", err)
		}
		session, err := store.OpenSession(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer session.Close(context.Background())
		agents, err := session.ListAgents(context.Background())
		if err != nil || len(agents) != 0 {
			t.Fatalf("状态查询创建了Agent: %+v %v", agents, err)
		}
	})
	t.Run("ignores_echo_and_inactive_main", func(t *testing.T) {
		store := core.NewMemoryRecoveryStore()
		seedConversationAgent(t, store, "echo", "echo", domain.AgentStatusActive)
		seedConversationAgent(t, store, "暂停的main", "main", domain.AgentStatusPaused)
		want := seedConversationAgent(t, store, "可用main", "main", domain.AgentStatusActive)
		err := withConversation(context.Background(), commandOptions{request: cli.Request{Command: "status"}}, conversationMemoryOpener(store), true,
			func(_ *core.Runtime, agent core.AgentSnapshot, _ func(context.Context) error) error {
				if agent.ID != want {
					t.Fatalf("选择了非active MainAgent: %+v", agent)
				}
				return nil
			})
		if err != nil {
			t.Fatal(err)
		}
	})
	t.Run("multiple_requires_explicit_selection", func(t *testing.T) {
		store := core.NewMemoryRecoveryStore()
		first := seedConversationAgent(t, store, "甲", "main", domain.AgentStatusActive)
		second := seedConversationAgent(t, store, "乙", "main", domain.AgentStatusActive)
		options := commandOptions{request: cli.Request{Command: "status"}}
		err := withConversation(context.Background(), options, conversationMemoryOpener(store), true,
			func(*core.Runtime, core.AgentSnapshot, func(context.Context) error) error {
				t.Fatal("多Agent时自动选择")
				return nil
			})
		var usage *cli.UsageError
		if !errors.As(err, &usage) || !strings.Contains(err.Error(), string(first)) || !strings.Contains(err.Error(), string(second)) || !strings.Contains(err.Error(), "--agent") {
			t.Fatalf("多个MainAgent提示不完整: %v", err)
		}
		options.request.AgentID = second
		if err := withConversation(context.Background(), options, conversationMemoryOpener(store), false,
			func(_ *core.Runtime, agent core.AgentSnapshot, _ func(context.Context) error) error {
				if agent.ID != second {
					t.Fatalf("显式选择被覆盖: %+v", agent)
				}
				return nil
			}); err != nil {
			t.Fatal(err)
		}
	})
	for _, fixture := range []struct {
		name       string
		definition string
		status     domain.AgentStatus
	}{{"echo", "echo", domain.AgentStatusActive}, {"paused", "main", domain.AgentStatusPaused}} {
		t.Run("explicit_rejects_"+fixture.name, func(t *testing.T) {
			store := core.NewMemoryRecoveryStore()
			id := seedConversationAgent(t, store, fixture.name, fixture.definition, fixture.status)
			err := withConversation(context.Background(), commandOptions{request: cli.Request{Command: "status", AgentID: id}}, conversationMemoryOpener(store), true,
				func(*core.Runtime, core.AgentSnapshot, func(context.Context) error) error {
					t.Fatal("接受非active MainAgent")
					return nil
				})
			var usage *cli.UsageError
			if !errors.As(err, &usage) {
				t.Fatalf("无效Agent未返回用法错误: %v", err)
			}
		})
	}
}

func TestConversationSQLiteReleasesOwnerOnBindingAndCallbackFailure(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	options := commandOptions{request: cli.Request{Command: "status"}, dataDir: directory,
		python: pythonrunner.Options{SourceDir: filepath.Join(directory, "missing")}}
	if err := withConversation(ctx, options, openCommandBackend, true,
		func(*core.Runtime, core.AgentSnapshot, func(context.Context) error) error {
			t.Fatal("绑定失败进入callback")
			return nil
		}); err == nil {
		t.Fatal("无效python源码目录未报错")
	}
	callbackErr := errors.New("callback失败")
	options.python.SourceDir = ""
	if err := withConversation(ctx, options, openCommandBackend, true,
		func(*core.Runtime, core.AgentSnapshot, func(context.Context) error) error { return callbackErr }); !errors.Is(err, callbackErr) {
		t.Fatalf("callback错误丢失: %v", err)
	}
	if err := withConversation(ctx, options, openCommandBackend, false,
		func(*core.Runtime, core.AgentSnapshot, func(context.Context) error) error { return nil }); err != nil {
		t.Fatalf("失败路径未释放sqlite owner: %v", err)
	}
}

func TestConversationCleanupUsesFreshContextAfterCancellation(t *testing.T) {
	store := core.NewMemoryRecoveryStore()
	ctx, cancel := context.WithCancel(context.Background())
	err := withConversation(ctx, commandOptions{request: cli.Request{Command: "status"}}, conversationMemoryOpener(store), true,
		func(*core.Runtime, core.AgentSnapshot, func(context.Context) error) error { cancel(); return ctx.Err() })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("取消错误丢失: %v", err)
	}
	session, err := store.OpenSession(context.Background())
	if err != nil {
		t.Fatalf("取消后cleanup未释放owner: %v", err)
	}
	if err := session.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

type conversationContextCloser func(context.Context) error

func (f conversationContextCloser) Close(ctx context.Context) error { return f(ctx) }

type conversationBindingCloser func() error

func (f conversationBindingCloser) Close() error { return f() }

func TestConversationCleanupRetriesWithoutClosingOwnedResources(t *testing.T) {
	closeErr := errors.New("session仍在使用")
	var calls []string
	fail := true
	cleanup := &conversationCleanup{
		runtime: conversationContextCloser(func(context.Context) error {
			calls = append(calls, "runtime")
			if fail {
				return closeErr
			}
			return nil
		}),
		binding: conversationBindingCloser(func() error { calls = append(calls, "binding"); return nil }),
		backend: func() error { calls = append(calls, "backend"); return nil },
	}
	if err := cleanup.close(context.Background()); !errors.Is(err, closeErr) || !reflect.DeepEqual(calls, []string{"runtime"}) {
		t.Fatalf("runtime关闭失败后提前关资源: calls=%v err=%v", calls, err)
	}
	fail = false
	pending := &conversationCleanupError{cause: closeErr, cleanup: cleanup}
	if err := pending.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(calls, []string{"runtime", "runtime", "binding", "backend"}) {
		t.Fatalf("cleanup未按顺序完成: %v", calls)
	}
	if err := pending.Close(context.Background()); err != nil || len(calls) != 4 {
		t.Fatalf("完成cleanup重复关闭资源: %v %v", calls, err)
	}
}

func TestConversationCleanupReportsAndRetriesCloseFailures(t *testing.T) {
	backendErr := errors.New("backend关闭失败")
	callbackErr := errors.New("业务失败")
	store := core.NewMemoryRecoveryStore()
	calls := 0
	open := func(context.Context, string) (backendHandle, error) {
		return backendHandle{Store: store, Close: func() error {
			calls++
			if calls == 1 {
				return backendErr
			}
			return nil
		}}, nil
	}
	err := withConversation(context.Background(), commandOptions{request: cli.Request{Command: "status"}}, open, true,
		func(*core.Runtime, core.AgentSnapshot, func(context.Context) error) error { return callbackErr })
	var pending *conversationCleanupError
	if !errors.As(err, &pending) || !errors.Is(err, callbackErr) || !errors.Is(err, backendErr) {
		t.Fatalf("cleanup吞掉原始或关闭错误: %v", err)
	}
	if err := pending.Close(context.Background()); err != nil || calls != 2 {
		t.Fatalf("无法重试未完成cleanup: calls=%d err=%v", calls, err)
	}
}

func TestDefaultConversationDataDir(t *testing.T) {
	project := t.TempDir()
	source := filepath.Join(project, "python", "src", "agent_runtime")
	if err := os.MkdirAll(source, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(source, "worker.py"), nil, 0600); err != nil {
		t.Fatal(err)
	}
	for _, directory := range []string{project, filepath.Join(project, "runtime")} {
		if err := os.MkdirAll(directory, 0700); err != nil {
			t.Fatal(err)
		}
		t.Run(filepath.Base(directory), func(t *testing.T) {
			t.Chdir(directory)
			if got := defaultDataDir(); got != filepath.Join(project, ".agent-runtime") {
				t.Fatalf("项目目录默认位置不符: %s", got)
			}
		})
	}
	t.Run("installed_binary", func(t *testing.T) {
		t.Chdir(t.TempDir())
		if got := defaultDataDir(); got != ".agent-runtime" {
			t.Fatalf("独立目录默认位置不符: %s", got)
		}
	})
}
