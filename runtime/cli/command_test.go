package cli

import (
	"agent-runtime/core"
	"agent-runtime/domain"
	"context"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"sync"
	"testing"
	"time"
)

type runnerFunc func(core.ExecutionContext) (core.ExecutionResult, error)

func (f runnerFunc) Run(input core.ExecutionContext) (core.ExecutionResult, error) { return f(input) }

type handlerFunc func(domain.Action) (map[string]any, error)

func (f handlerFunc) Execute(action domain.Action) (map[string]any, error) { return f(action) }

type closerFunc func() error

func (f closerFunc) Close() error { return f() }

func echoRunner(input core.ExecutionContext) (core.ExecutionResult, error) {
	if input.Event.Type == "action.result" {
		return core.ExecutionResult{StateUpdate: map[string]any{"echoed": input.Event.Payload["result"]}}, nil
	}
	return core.ExecutionResult{Actions: []domain.Action{domain.NewAction("echo", input.Event.Payload)}}, nil
}

func bind(runner core.AgentRunner, handler core.ActionHandler) func(*core.Runtime) (io.Closer, error) {
	return func(runtime *core.Runtime) (io.Closer, error) {
		if err := runtime.RegisterDefinition(domain.DefinitionRef{ID: "echo", Version: "1"}, runner); err != nil {
			return nil, err
		}
		return nil, runtime.Executor().RegisterWithOptions("echo", handler, core.HandlerOptions{
			Version: "1", RecoveryPolicy: domain.RecoveryPolicyManual, MaxAttempts: 3,
		})
	}
}

func newApplication() *Application {
	return &Application{Backend: core.NewMemoryRecoveryStore(), Bind: bind(runnerFunc(echoRunner), core.EchoHandler{})}
}

func execute(t *testing.T, application *Application, request Request) Result {
	t.Helper()
	result, err := application.Execute(context.Background(), request)
	if err != nil {
		t.Fatalf("%s失败: %v", request.Command, err)
	}
	return result
}

func assertExecuteError(t *testing.T, application *Application, request Request, expected error) error {
	t.Helper()
	result, err := application.Execute(context.Background(), request)
	if !errors.Is(err, expected) || !reflect.DeepEqual(result, Result{}) {
		t.Fatalf("%s错误或返回值不符: %+v, %v", request.Command, result, err)
	}
	return err
}

func TestCommandsShareBackendAndReportCompleteEcho(t *testing.T) {
	application := newApplication()
	created := execute(t, application, Request{Command: "init"})
	if created.Agent == nil || created.Agent.Name != "echo" || created.Agent.Definition != (domain.DefinitionRef{ID: "echo", Version: "1"}) {
		t.Fatalf("init结果不完整: %+v", created)
	}
	id := created.Agent.ID
	request := Request{Command: "submit", AgentID: id, EventID: "request-1", Message: "你好"}
	first := execute(t, application, request)
	duplicate := execute(t, application, request)
	if first.Submission == nil || first.Submission.Duplicate || !duplicate.Submission.Duplicate || first.Submission.Delivery != duplicate.Submission.Delivery {
		t.Fatalf("重复事件未保留原投递: %+v, %+v", first.Submission, duplicate.Submission)
	}
	request.Message = "不同消息"
	assertExecuteError(t, application, request, core.ErrStoreConflict)
	pending := execute(t, application, Request{Command: "status", AgentID: id}).Query
	if pending == nil || len(pending.Deliveries) != 1 || pending.Deliveries[0].Execution != nil || !pending.Deliveries[0].Ready {
		t.Fatalf("提交命令提前执行或未显示待办: %+v", pending)
	}
	run := execute(t, application, Request{Command: "run"}).Run
	if run == nil || run.Before.Agents != 1 || run.Before.Deliveries[domain.DeliveryStatusPending] != 1 || run.Before.ExecutionAttempts != 0 ||
		run.After.Agents != 1 || run.After.Deliveries[domain.DeliveryStatusCompleted] != 2 || run.After.Actions[domain.ActionStatusSucceeded] != 1 ||
		run.After.ExecutionAttempts != 2 || run.After.ActionAttempts != 1 || len(run.Agents) != 1 {
		t.Fatalf("run汇总不完整: %+v", run)
	}
	if got := run.Agents[0].Agent.State["echoed"].(map[string]any)["message"]; got != "你好" {
		t.Fatalf("echo闭环结果不符: %v", got)
	}
	list := execute(t, application, Request{Command: "status"})
	if len(list.Agents) != 1 || list.Agents[0].ID != id {
		t.Fatalf("跨命令agent列表不符: %+v", list)
	}
	assertExecuteError(t, application, Request{Command: "retry", AgentID: id, EventID: "request-1"}, core.ErrStoreConflict)
	second := execute(t, application, Request{Command: "run"}).Run
	if !reflect.DeepEqual(second.Before, second.After) || !reflect.DeepEqual(run.After, second.After) {
		t.Fatalf("再次run重复执行已完成工作: %+v", second)
	}
	encoded, err := json.Marshal(execute(t, application, Request{Command: "status", AgentID: id}))
	if err != nil {
		t.Fatal(err)
	}
	var document map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &document); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"command", "agents", "query", "startup_recovery"} {
		if _, ok := document[field]; !ok {
			t.Fatalf("JSON缺少%s: %s", field, encoded)
		}
	}
}

func TestFailedDeliveryRequiresExplicitRetry(t *testing.T) {
	application := newApplication()
	fail := true
	businessErr := errors.New("业务失败")
	application.Bind = bind(runnerFunc(func(input core.ExecutionContext) (core.ExecutionResult, error) {
		if fail {
			return core.ExecutionResult{}, businessErr
		}
		return echoRunner(input)
	}), core.EchoHandler{})
	agent := execute(t, application, Request{Command: "init", Name: "重试样例"}).Agent
	request := Request{Command: "submit", AgentID: agent.ID, EventID: "retry-event", Message: "test"}
	execute(t, application, request)
	failed := execute(t, application, Request{Command: "run"}).Run
	if failed.After.Deliveries[domain.DeliveryStatusFailed] != 1 || failed.After.ExecutionAttempts != 1 || failed.Agents[0].Deliveries[0].Execution.Error != businessErr.Error() {
		t.Fatalf("业务失败未保留在正常run结果: %+v", failed)
	}
	fail = false
	stillFailed := execute(t, application, Request{Command: "run"}).Run
	if !reflect.DeepEqual(failed.After, stillFailed.After) {
		t.Fatalf("未显式retry就重试了失败任务: %+v", stillFailed)
	}
	retried := execute(t, application, Request{Command: "retry", AgentID: agent.ID, EventID: request.EventID})
	if retried.Retry == nil || *retried.Retry != (domain.DeliveryKey{AgentID: agent.ID, EventID: request.EventID}) {
		t.Fatalf("retry没有返回投递身份: %+v", retried)
	}
	assertExecuteError(t, application, Request{Command: "retry", AgentID: agent.ID, EventID: request.EventID}, core.ErrStoreConflict)
	completed := execute(t, application, Request{Command: "run"}).Run
	if completed.After.ExecutionAttempts != 3 || completed.After.ActionAttempts != 1 || completed.After.Deliveries[domain.DeliveryStatusCompleted] != 2 {
		t.Fatalf("显式重试没有保留尝试历史: %+v", completed)
	}
}

func TestUnknownActionCannotBeRetriedByDeliveryCommand(t *testing.T) {
	application := newApplication()
	calls := 0
	application.Bind = bind(runnerFunc(echoRunner), handlerFunc(func(domain.Action) (map[string]any, error) {
		calls++
		panic("外部结果未知")
	}))
	agent := execute(t, application, Request{Command: "init"}).Agent
	execute(t, application, Request{Command: "submit", AgentID: agent.ID, EventID: "unknown"})
	run := execute(t, application, Request{Command: "run"}).Run
	if run.After.Actions[domain.ActionStatusUnknown] != 1 || calls != 1 {
		t.Fatalf("unknown状态未保留: %+v, 调用%d次", run, calls)
	}
	assertExecuteError(t, application, Request{Command: "retry", AgentID: agent.ID, EventID: "unknown"}, core.ErrStoreConflict)
	execute(t, application, Request{Command: "run"})
	if calls != 1 {
		t.Fatalf("manual unknown被自动重试%d次", calls)
	}
}

func TestMissingBindingsRemainQueryableWithoutExecution(t *testing.T) {
	application := newApplication()
	agent := execute(t, application, Request{Command: "init"}).Agent
	execute(t, application, Request{Command: "submit", AgentID: agent.ID, EventID: "pending"})
	application.Bind = nil
	query := execute(t, application, Request{Command: "status", AgentID: agent.ID}).Query
	if query.Agent.BindingError == "" || len(query.Deliveries) != 1 || query.Deliveries[0].Ready {
		t.Fatalf("缺失绑定不可见: %+v", query)
	}
	run := execute(t, application, Request{Command: "run"}).Run
	if !reflect.DeepEqual(run.Before, run.After) || run.After.ExecutionAttempts != 0 {
		t.Fatalf("未绑定runner仍执行了任务: %+v", run)
	}
	application.Bind = bind(runnerFunc(echoRunner), core.EchoHandler{})
	run = execute(t, application, Request{Command: "run"}).Run
	if run.After.Deliveries[domain.DeliveryStatusCompleted] != 2 {
		t.Fatalf("重新绑定后未完成原投递: %+v", run)
	}
}

type wrappedBackend struct {
	core.RecoveryStore
	wrap func(core.RecoverySession) core.RecoverySession
}

func (b wrappedBackend) OpenSession(ctx context.Context) (core.RecoverySession, error) {
	session, err := b.RecoveryStore.OpenSession(ctx)
	if err != nil {
		return nil, err
	}
	return b.wrap(session), nil
}

type faultSession struct {
	core.RecoverySession
	readErr       error
	commitErr     error
	recoverErr    error
	closeErr      error
	closeFailures int
	closeCalls    int
	closeWait     bool
}

func (s *faultSession) Recover(ctx context.Context) (core.RecoveryReport, error) {
	if s.recoverErr != nil {
		return core.RecoveryReport{}, s.recoverErr
	}
	return s.RecoverySession.Recover(ctx)
}

func (s *faultSession) ListAgents(ctx context.Context) ([]domain.AgentInstance, error) {
	if s.readErr != nil {
		return nil, s.readErr
	}
	return s.RecoverySession.ListAgents(ctx)
}

func (s *faultSession) CommitExecution(ctx context.Context, commit core.ExecutionCommit) (domain.ExecutionResult, error) {
	if s.commitErr != nil {
		return domain.ExecutionResult{}, s.commitErr
	}
	return s.RecoverySession.CommitExecution(ctx, commit)
}

func (s *faultSession) Close(ctx context.Context) error {
	s.closeCalls++
	if s.closeWait {
		<-ctx.Done()
		return ctx.Err()
	}
	if s.closeFailures > 0 {
		s.closeFailures--
		return s.closeErr
	}
	return s.RecoverySession.Close(ctx)
}

func assertBackendAvailable(t *testing.T, backend core.RecoveryStore) {
	t.Helper()
	session, err := backend.OpenSession(context.Background())
	if err != nil {
		t.Fatalf("命令未释放会话所有权: %v", err)
	}
	if err := session.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestStorageFailuresReturnErrorsAndReleaseOwnership(t *testing.T) {
	for _, stage := range []string{"read", "commit"} {
		t.Run(stage, func(t *testing.T) {
			application := newApplication()
			agent := execute(t, application, Request{Command: "init"}).Agent
			execute(t, application, Request{Command: "submit", AgentID: agent.ID, EventID: "fault"})
			backend := application.Backend
			fault := errors.New("存储故障")
			application.Backend = wrappedBackend{backend, func(session core.RecoverySession) core.RecoverySession {
				wrapped := &faultSession{RecoverySession: session}
				if stage == "read" {
					wrapped.readErr = fault
				} else {
					wrapped.commitErr = fault
				}
				return wrapped
			}}
			assertExecuteError(t, application, Request{Command: "run"}, fault)
			assertBackendAvailable(t, backend)
			application.Backend = backend
			run := execute(t, application, Request{Command: "run"})
			if run.Run.After.Deliveries[domain.DeliveryStatusCompleted] != 2 {
				t.Fatalf("故障后不能继续工作: %+v", run)
			}
			if stage == "commit" && len(run.StartupRecovery.RequeuedDeliveries) != 1 {
				t.Fatalf("未报告上次中断的execution: %+v", run.StartupRecovery)
			}
		})
	}
}

func TestApplicationRejectsInvalidRequestsBeforeOpeningBackend(t *testing.T) {
	requests := []Request{
		{}, {Command: "other"}, {Command: "init", AgentID: "agent"}, {Command: "init", Name: " "},
		{Command: "submit", AgentID: "agent"}, {Command: "submit", EventID: "event"},
		{Command: "run", AgentID: "agent"}, {Command: "status", EventID: "event"},
		{Command: "retry", AgentID: "agent"}, {Command: "retry", AgentID: "agent", EventID: "event", Message: "message"},
		{Command: "status", AgentID: " "}, {Command: "init", Name: string([]byte{0xff})},
	}
	for _, request := range requests {
		application := &Application{Bind: func(*core.Runtime) (io.Closer, error) {
			t.Fatal("非法请求触发绑定")
			return nil, nil
		}, CloseBackend: func() error {
			t.Fatal("非法请求接管了后端资源")
			return nil
		}}
		result, err := application.Execute(context.Background(), request)
		var usage *UsageError
		if !errors.As(err, &usage) || !reflect.DeepEqual(result, Result{}) {
			t.Fatalf("非法请求未返回UsageError: %+v, %+v, %v", request, result, err)
		}
	}
	closed := 0
	application := &Application{CloseBackend: func() error { closed++; return nil }}
	assertExecuteError(t, application, Request{Command: "status"}, ErrBackendUnavailable)
	if closed != 1 {
		t.Fatalf("空后端的附属资源未关闭: %d", closed)
	}
	var typedNil *wrappedBackend
	application.Backend = typedNil
	assertExecuteError(t, application, Request{Command: "status"}, ErrBackendUnavailable)
}

func TestBindFailureClosesReturnedResourcesAndBackend(t *testing.T) {
	application := newApplication()
	backend := application.Backend
	fault := errors.New("绑定失败")
	var order []string
	application.Bind = func(*core.Runtime) (io.Closer, error) {
		return closerFunc(func() error {
			assertBackendAvailable(t, backend)
			order = append(order, "binding")
			return nil
		}), fault
	}
	application.CloseBackend = func() error {
		assertBackendAvailable(t, backend)
		order = append(order, "backend")
		return nil
	}
	assertExecuteError(t, application, Request{Command: "status"}, fault)
	if !reflect.DeepEqual(order, []string{"binding", "backend"}) {
		t.Fatalf("绑定失败后的清理顺序错误: %v", order)
	}
}

func TestCleanupFailurePreservesOwnershipAndRetryHandle(t *testing.T) {
	for _, stage := range []string{"close failure", "close timeout", "startup failure"} {
		t.Run(stage, func(t *testing.T) {
			backend := core.NewMemoryRecoveryStore()
			closeFault, openFault := errors.New("关闭失败"), errors.New("恢复失败")
			var session *faultSession
			bindingClosed, backendClosed := 0, 0
			application := &Application{Backend: wrappedBackend{backend, func(inner core.RecoverySession) core.RecoverySession {
				session = &faultSession{RecoverySession: inner, closeErr: closeFault, closeFailures: 1}
				if stage == "close timeout" {
					session.closeWait = true
				}
				if stage == "startup failure" {
					session.recoverErr, session.closeFailures = openFault, 2
				}
				return session
			}}, CloseTimeout: time.Millisecond,
				Bind: func(*core.Runtime) (io.Closer, error) {
					return closerFunc(func() error { bindingClosed++; return nil }), nil
				}, CloseBackend: func() error { backendClosed++; return nil },
			}
			result, err := application.Execute(context.Background(), Request{Command: "status"})
			var cleanup *CleanupError
			if !errors.As(err, &cleanup) || !reflect.DeepEqual(result, Result{}) {
				t.Fatalf("关闭失败没有返回清理句柄: %+v, %v", result, err)
			}
			if stage == "close timeout" {
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("关闭超时丢失原因: %v", err)
				}
			} else if !errors.Is(err, closeFault) {
				t.Fatalf("关闭失败丢失原因: %v", err)
			}
			if stage == "startup failure" && !errors.Is(err, openFault) {
				t.Fatalf("清理错误丢失启动错误: %v", err)
			}
			if bindingClosed != 0 || backendClosed != 0 {
				t.Fatalf("runtime未关闭就释放了资源: binding=%d backend=%d", bindingClosed, backendClosed)
			}
			if _, err := backend.OpenSession(context.Background()); !errors.Is(err, core.ErrStoreOwned) {
				t.Fatalf("失败关闭过早释放所有权: %v", err)
			}
			canceled, cancel := context.WithCancel(context.Background())
			cancel()
			if err := cleanup.Close(canceled); !errors.Is(err, context.Canceled) {
				t.Fatalf("清理重试忽略取消: %v", err)
			}
			session.closeWait, session.closeFailures = false, 0
			if err := cleanup.Close(context.Background()); err != nil {
				t.Fatalf("清理重试失败: %v", err)
			}
			if err := cleanup.Close(context.Background()); err != nil || backendClosed != 1 {
				t.Fatalf("重复清理不是幂等的: backend=%d, %v", backendClosed, err)
			}
			if stage != "startup failure" && bindingClosed != 1 {
				t.Fatalf("绑定资源未在重试时释放: %d", bindingClosed)
			}
			assertBackendAvailable(t, backend)
		})
	}
}

func TestCleanupTracksBindingAndBackendFailuresIndependently(t *testing.T) {
	application := newApplication()
	bindFault, backendFault := errors.New("runner关闭失败"), errors.New("backend关闭失败")
	bindingCalls, backendCalls := 0, 0
	application.Bind = func(*core.Runtime) (io.Closer, error) {
		return closerFunc(func() error {
			bindingCalls++
			if bindingCalls == 1 {
				return bindFault
			}
			return nil
		}), nil
	}
	application.CloseBackend = func() error {
		backendCalls++
		if backendCalls <= 2 {
			return backendFault
		}
		return nil
	}
	result, err := application.Execute(context.Background(), Request{Command: "status"})
	var cleanup *CleanupError
	if !errors.As(err, &cleanup) || !errors.Is(err, bindFault) || !errors.Is(err, backendFault) || !reflect.DeepEqual(result, Result{}) {
		t.Fatalf("附属资源清理丢失错误或句柄: %+v, %v", result, err)
	}
	if err := cleanup.Close(context.Background()); !errors.Is(err, backendFault) {
		t.Fatalf("第二次backend关闭错误被吞掉: %v", err)
	}
	if err := cleanup.Close(context.Background()); err != nil || bindingCalls != 2 || backendCalls != 3 {
		t.Fatalf("清理重试重复关闭已释放资源: binding=%d backend=%d, %v", bindingCalls, backendCalls, err)
	}
}

func TestCanceledCommandStillClosesRuntimeAndResources(t *testing.T) {
	application := newApplication()
	ctx, cancel := context.WithCancel(context.Background())
	var captured *core.Runtime
	closed := 0
	application.Bind = func(runtime *core.Runtime) (io.Closer, error) {
		captured = runtime
		cancel()
		return closerFunc(func() error { closed++; return nil }), nil
	}
	application.CloseBackend = func() error { closed++; return nil }
	result, err := application.Execute(ctx, Request{Command: "status"})
	if !errors.Is(err, context.Canceled) || closed != 2 || !reflect.DeepEqual(result, Result{}) {
		t.Fatalf("取消后清理未完成: %+v, %v, closed=%d", result, err, closed)
	}
	if _, err := captured.Agents(); !errors.Is(err, core.ErrStoreClosed) {
		t.Fatalf("取消命令未关闭runtime: %v", err)
	}
	assertBackendAvailable(t, application.Backend)
}

func TestCommandsRespectExclusiveSessionOwnership(t *testing.T) {
	application := newApplication()
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	application.Bind = func(*core.Runtime) (io.Closer, error) {
		close(entered)
		<-release
		return nil, nil
	}
	done := make(chan error, 1)
	go func() {
		_, err := application.Execute(context.Background(), Request{Command: "status"})
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("命令未取得会话")
	}
	second := &Application{Backend: application.Backend}
	assertExecuteError(t, second, Request{Command: "status"}, core.ErrStoreOwned)
	unblock()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("命令未释放会话")
	}
	execute(t, second, Request{Command: "status"})
}
