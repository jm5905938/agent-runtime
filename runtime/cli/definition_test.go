package cli

import (
	"agent-runtime/core"
	"agent-runtime/domain"
	"agent-runtime/sqlite"
	"context"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

type sqliteApplicationStore struct {
	backend *sqlite.Backend
}

func (s sqliteApplicationStore) OpenSession(ctx context.Context) (core.RecoverySession, error) {
	session, err := s.backend.OpenSession(ctx)
	if err != nil {
		return nil, err
	}
	return session, nil
}

func TestInitSelectsDefinitionAndDefaultName(t *testing.T) {
	for _, test := range []struct {
		definition string
		name       string
		wantID     string
		wantName   string
	}{
		{wantID: "echo", wantName: "echo"},
		{definition: "echo", wantID: "echo", wantName: "echo"},
		{definition: "main", wantID: "main", wantName: "main"},
		{definition: "main", name: "持久主agent", wantID: "main", wantName: "持久主agent"},
	} {
		t.Run(test.definition+"/"+test.name, func(t *testing.T) {
			application := &Application{Backend: core.NewMemoryRecoveryStore(), Bind: func(runtime *core.Runtime) (io.Closer, error) {
				for _, id := range []string{"echo", "main"} {
					if err := runtime.RegisterDefinition(domain.DefinitionRef{ID: id, Version: "1"}, runnerFunc(echoRunner)); err != nil {
						return nil, err
					}
				}
				return nil, nil
			}}
			created := execute(t, application, Request{Command: "init", Definition: test.definition, Name: test.name})
			if created.Agent == nil || created.Agent.Name != test.wantName || created.Agent.Definition != (domain.DefinitionRef{ID: test.wantID, Version: "1"}) {
				t.Fatalf("init未创建指定definition和名称: %+v", created)
			}
		})
	}
}

func TestMainCommandsPersistStateAndDispatchAcrossSQLiteReopens(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runtime.db")
	modelCalls := 0
	open := func() *Application {
		t.Helper()
		backend, err := sqlite.Open(path)
		if err != nil {
			t.Fatal(err)
		}
		return &Application{Backend: sqliteApplicationStore{backend: backend}, CloseBackend: backend.Close, Bind: func(runtime *core.Runtime) (io.Closer, error) {
			err := runtime.RegisterDefinition(domain.DefinitionRef{ID: "main", Version: "1"}, runnerFunc(func(input core.ExecutionContext) (core.ExecutionResult, error) {
				switch input.Event.Type {
				case "main.request":
					return core.ExecutionResult{
						StateUpdate: map[string]any{"request_status": "waiting", "request_event_id": string(input.Event.ID)},
						Actions:     []domain.Action{domain.NewAction("model.generate", input.Event.Payload)},
					}, nil
				case "action.result":
					return core.ExecutionResult{StateUpdate: map[string]any{"request_status": "succeeded", "result": input.Event.Payload["result"].(map[string]any)["message"]}}, nil
				default:
					return core.ExecutionResult{}, fmt.Errorf("main收到错误事件类型%s", input.Event.Type)
				}
			}))
			if err != nil {
				return nil, err
			}
			return nil, runtime.Executor().RegisterWithOptions("model.generate", handlerFunc(func(action domain.Action) (map[string]any, error) {
				modelCalls++
				return map[string]any{"message": "回复: " + action.Payload["message"].(string)}, nil
			}), core.HandlerOptions{Version: "1", RecoveryPolicy: domain.RecoveryPolicyManual, MaxAttempts: 1})
		}}
	}
	created := execute(t, open(), Request{Command: "init", Definition: "main"})
	id := created.Agent.ID
	request := Request{Command: "submit", AgentID: id, EventID: "main-request", Message: "你好"}
	first := execute(t, open(), request)
	duplicate := execute(t, open(), request)
	if first.Submission.Duplicate || !duplicate.Submission.Duplicate || first.Submission.Delivery != duplicate.Submission.Delivery {
		t.Fatalf("main跨后端重开丢失投递身份或去重: %+v, %+v", first.Submission, duplicate.Submission)
	}
	conflicting := request
	conflicting.Message = "不同消息"
	assertExecuteError(t, open(), conflicting, core.ErrStoreConflict)
	pending := execute(t, open(), Request{Command: "status", AgentID: id}).Query
	if pending.Agent.Name != "main" || pending.Agent.Definition != (domain.DefinitionRef{ID: "main", Version: "1"}) || len(pending.Deliveries) != 1 || pending.Deliveries[0].Event.Type != "main.request" || pending.Deliveries[0].Event.Payload["message"] != request.Message || pending.Deliveries[0].Execution != nil {
		t.Fatalf("main待办未持久化或使用错误事件类型: %+v", pending)
	}
	ran := execute(t, open(), Request{Command: "run"}).Run
	if modelCalls != 1 || ran.After.Deliveries[domain.DeliveryStatusCompleted] != 2 || ran.After.Actions[domain.ActionStatusSucceeded] != 1 {
		t.Fatalf("main未完成模型action闭环: %+v, 调用%d次", ran, modelCalls)
	}
	status := execute(t, open(), Request{Command: "status", AgentID: id}).Query
	if status.Agent.StateVersion != 2 || status.Agent.State["request_status"] != "succeeded" || status.Agent.State["result"] != "回复: 你好" || status.Agent.State["request_event_id"] != string(request.EventID) {
		t.Fatalf("main重开后丢失结果或状态: %+v", status.Agent)
	}
	idle := execute(t, open(), Request{Command: "run"}).Run
	if modelCalls != 1 || !reflect.DeepEqual(ran.After, idle.After) || !reflect.DeepEqual(idle.Before, idle.After) {
		t.Fatalf("main重开后重复执行已完成工作: %+v, 调用%d次", idle, modelCalls)
	}
}

func TestSubmitRejectsUnsupportedStoredDefinitionsWithoutQueuing(t *testing.T) {
	for _, definition := range []domain.DefinitionRef{
		{ID: "custom", Version: "1"}, {ID: "main", Version: "2"}, {ID: "echo", Version: "2"},
	} {
		t.Run(definition.ID+"@"+definition.Version, func(t *testing.T) {
			backend := core.NewMemoryRecoveryStore()
			runtime, err := core.OpenRuntime(context.Background(), backend)
			if err != nil {
				t.Fatal(err)
			}
			agent := domain.NewAgentInstance("unsupported")
			agent.Definition = definition
			if err := runtime.RestoreAgent(agent); err != nil {
				t.Fatal(err)
			}
			if err := runtime.Close(context.Background()); err != nil {
				t.Fatal(err)
			}
			application := &Application{Backend: backend}
			result, err := application.Execute(context.Background(), Request{Command: "submit", AgentID: agent.ID, EventID: "unsupported", Message: "message"})
			if err == nil || !strings.Contains(err.Error(), definition.ID+"@"+definition.Version) || !reflect.DeepEqual(result, Result{}) {
				t.Fatalf("未知definition未返回明确错误: %+v, %v", result, err)
			}
			query := execute(t, application, Request{Command: "status", AgentID: agent.ID}).Query
			if len(query.Deliveries) != 0 || query.Agent.StateVersion != 0 {
				t.Fatalf("未知definition提交仍然写入了工作: %+v", query)
			}
			session, err := backend.OpenSession(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer session.Close(context.Background())
			if _, err := session.LoadEvent(context.Background(), "unsupported"); !errors.Is(err, core.ErrStoreNotFound) {
				t.Fatalf("未知definition提交留下了错误event: %v", err)
			}
		})
	}
}

func TestSubmitMissingAgentDoesNotCreateEvent(t *testing.T) {
	application := newApplication()
	assertExecuteError(t, application, Request{Command: "submit", AgentID: "missing", EventID: "missing-agent", Message: "message"}, core.ErrStoreNotFound)
	session, err := application.Backend.OpenSession(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer session.Close(context.Background())
	if _, err := session.LoadEvent(context.Background(), "missing-agent"); !errors.Is(err, core.ErrStoreNotFound) {
		t.Fatalf("不存在agent提交留下了event: %v", err)
	}
}
