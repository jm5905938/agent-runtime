package main

import (
	"agent-runtime/codec"
	"agent-runtime/core"
	"agent-runtime/domain"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
	"unicode/utf8"
)

type agentStatusTestRunner func(core.ExecutionContext) (core.ExecutionResult, error)

func (runner agentStatusTestRunner) Run(input core.ExecutionContext) (core.ExecutionResult, error) {
	return runner(input)
}

func TestAgentStatusHandlerReturnsOnlyCompactSnapshot(t *testing.T) {
	store := core.NewMemoryStore()
	runtime, err := core.NewRuntimeWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Close(context.Background()) })
	for _, definition := range []string{"main", "echo"} {
		agent := domain.NewAgentInstance("实例" + definition)
		agent.Status, agent.Definition, agent.StateVersion = domain.AgentStatusPaused, domain.DefinitionRef{ID: definition, Version: "1"}, 17
		agent.State = map[string]any{"request_status": "waiting", "messages": []any{map[string]any{"content": "private-history"}}, "pending_message": "private-pending", "error": "private-error"}
		if err := store.CreateAgent(context.Background(), agent); err != nil {
			t.Fatal(err)
		}
		result, err := (agentStatusHandler{runtime: runtime}).Execute(domain.NewAction(agentStatusActionType, map[string]any{"agent_id": string(agent.ID)}))
		if err != nil || result["id"] != string(agent.ID) || result["name"] != agent.Name || result["status"] != "paused" || result["state_version"] != uint64(17) ||
			result["request_status"] != "waiting" || result["binding_error"] == nil || !reflect.DeepEqual(result["definition"], map[string]any{"id": definition, "version": "1"}) {
			t.Fatalf("状态查询不完整: %+v %v", result, err)
		}
		if err := codec.ValidateData(result); err != nil {
			t.Fatalf("工具返回不支持的业务数据: %v", err)
		}
		encoded, _ := json.Marshal(result)
		if strings.Contains(string(encoded), "private-") || len(result) != 7 {
			t.Fatalf("工具透传了状态或审计数据: %s", encoded)
		}
		result["definition"].(map[string]any)["id"] = "changed"
		unchanged, err := runtime.Agent(agent.ID)
		if err != nil || unchanged.Definition.ID != definition || unchanged.StateVersion != 17 {
			t.Fatalf("查询修改了实例: %+v %v", unchanged, err)
		}
	}
}

func TestAgentStatusHandlerBoundsEncodedText(t *testing.T) {
	store := core.NewMemoryStore()
	runtime, err := core.NewRuntimeWithStore(store)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Close(context.Background()) })
	agent := domain.NewAgentInstance(strings.Repeat("<\n世界\u2028", 1000))
	agent.Status = domain.AgentStatusActive
	agent.Definition = domain.DefinitionRef{ID: strings.Repeat("&世界", 1000), Version: "v" + strings.Repeat("\u2029", 1000)}
	agent.State = map[string]any{"request_status": strings.Repeat("\"\\你好", 1000)}
	if err := store.CreateAgent(context.Background(), agent); err != nil {
		t.Fatal(err)
	}
	result, err := (agentStatusHandler{runtime: runtime}).Execute(domain.NewAction(agentStatusActionType, map[string]any{"agent_id": string(agent.ID)}))
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []any{result["name"], result["request_status"], result["binding_error"], result["definition"].(map[string]any)["id"], result["definition"].(map[string]any)["version"]} {
		text := value.(string)
		encoded, err := json.Marshal(text)
		if err != nil || !utf8.ValidString(text) || len(encoded) > 1024 || text == "" {
			t.Fatalf("文本截断破坏编码或超过预算: bytes=%d err=%v", len(encoded), err)
		}
	}
	encoded, _ := json.Marshal(result)
	if len(encoded) > 8*1024 || !strings.HasPrefix(agent.Name, result["name"].(string)) {
		t.Fatalf("工具结果未控制大小或截断了错误方向: bytes=%d", len(encoded))
	}
}

func TestAgentStatusHandlerRejectsInvalidAndMissingTarget(t *testing.T) {
	runtime := core.NewRuntime()
	t.Cleanup(func() { _ = runtime.Close(context.Background()) })
	handler := agentStatusHandler{runtime: runtime}
	for _, payload := range []map[string]any{
		nil, {}, {"agent_id": nil}, {"agent_id": 1}, {"agent_id": ""}, {"agent_id": " \n"},
		{"agent_id": string([]byte{0xff})}, {"agent_id": strings.Repeat("a", 1025)}, {"agent_id": "id", "extra": true},
	} {
		if output, err := handler.Execute(domain.NewAction(agentStatusActionType, payload)); err == nil || output != nil {
			t.Fatalf("无效参数被当成成功查询: payload=%+v output=%+v err=%v", payload, output, err)
		}
	}
	if _, err := handler.Execute(domain.NewAction(agentStatusActionType, map[string]any{"agent_id": "missing"})); !errors.Is(err, core.ErrStoreNotFound) {
		t.Fatalf("不存在实例的错误丢失: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := (agentStatusHandler{runtime: runtime, ctx: ctx}).Execute(domain.NewAction(agentStatusActionType, map[string]any{"agent_id": "missing"})); !errors.Is(err, context.Canceled) {
		t.Fatalf("查询忽略取消: %v", err)
	}
}

type agentStatusTestBackend struct {
	core.RecoveryStore
	session core.RecoverySession
}

func (backend *agentStatusTestBackend) OpenSession(ctx context.Context) (core.RecoverySession, error) {
	session, err := backend.RecoveryStore.OpenSession(ctx)
	backend.session = session
	return session, err
}

func agentStatusFixture(t *testing.T, actionType string) (*core.Runtime, core.RecoverySession, domain.ID, domain.ActionRecord) {
	t.Helper()
	ctx := context.Background()
	backend := &agentStatusTestBackend{RecoveryStore: core.NewMemoryRecoveryStore()}
	runtime, err := core.OpenRuntime(ctx, backend)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = runtime.Close(context.Background()) })
	if err := registerAgentStatus(ctx, runtime); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Executor().RegisterWithOptions(
		writeFileActionType,
		writeFileHandler{rootDir: t.TempDir()},
		core.HandlerOptions{Version: "1", RecoveryPolicy: domain.RecoveryPolicySafeRetry, MaxAttempts: 3},
	); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Executor().Register("model.generate", core.EchoHandler{}); err != nil {
		t.Fatal(err)
	}
	definition := domain.DefinitionRef{ID: "main", Version: "1"}
	if err := runtime.RegisterDefinition(definition, agentStatusTestRunner(func(input core.ExecutionContext) (core.ExecutionResult, error) {
		action := domain.NewAction(actionType, map[string]any{"agent_id": string(input.Agent.ID)})
		return core.ExecutionResult{StateUpdate: map[string]any{
			"request_status": "waiting", "waiting_action_id": string(action.ID), "waiting_action_type": actionType,
			"request_execution_id": "initial-execution", "waiting_execution_id": string(input.ExecutionID),
		}, Actions: []domain.Action{action}}, nil
	})); err != nil {
		t.Fatal(err)
	}
	agent, err := runtime.CreateAgentContext(ctx, "main", definition, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := runtime.ProcessContext(ctx, agent.ID, domain.NewEvent("fixture.start", nil)); err != nil {
		t.Fatal(err)
	}
	query, err := runtime.QueryAgent(agent.ID)
	if err != nil || len(query.Actions) != 1 {
		t.Fatalf("无法建立持久action fixture: %+v %v", query, err)
	}
	return runtime, backend.session, agent.ID, query.Actions[0].Action
}

func agentStatusCompleteFixture(t *testing.T, session core.RecoverySession, record domain.ActionRecord, failed bool, output map[string]any) {
	t.Helper()
	ctx := context.Background()
	claim, err := session.ClaimAction(ctx, record.Request.ID)
	if err != nil {
		t.Fatal(err)
	}
	result := domain.ActionResult{ActionID: record.Request.ID, EventID: record.ResultEventID, Status: domain.ActionStatusSucceeded, Output: output}
	payload := map[string]any{"action_id": string(record.Request.ID), "action_type": record.Request.Type, "execution_id": string(*record.Request.ExecutionID), "status": "succeeded", "result": output}
	if failed {
		result.Status, result.Output, result.Error = domain.ActionStatusFailed, nil, &domain.Failure{Kind: domain.ErrorKindBusiness, Message: "查询失败"}
		payload["status"], payload["error"] = "failed", result.Error.Message
		delete(payload, "result")
	}
	event := domain.Event{ID: record.ResultEventID, Type: "action.result", Payload: payload, CreatedAt: time.Now().UTC()}
	if _, err := session.CompleteAction(ctx, core.ActionCompletion{Token: claim.Token, Result: result, Event: event}); err != nil {
		t.Fatal(err)
	}
}

func TestModelPreflightIncludesToolContinuation(t *testing.T) {
	for _, test := range []struct {
		name, actionType string
		complete, failed bool
		output           map[string]any
		want             bool
	}{
		{name: "pending_tool", actionType: agentStatusActionType, want: true},
		{name: "tool_success", actionType: agentStatusActionType, complete: true, output: map[string]any{"status": "active"}, want: true},
		{name: "tool_failure", actionType: agentStatusActionType, complete: true, failed: true, want: true},
		{name: "model_tools", actionType: "model.generate", complete: true, output: map[string]any{"tool_calls": []any{map[string]any{"id": "call-1"}}}, want: true},
		{name: "final_text", actionType: "model.generate", complete: true, output: map[string]any{"message": "完成"}},
		{name: "model_failure", actionType: "model.generate", complete: true, failed: true},
		{name: "pending_write_file", actionType: writeFileActionType, want: true},
		{name: "write_file_success", actionType: writeFileActionType, complete: true, output: map[string]any{"path": "a.txt"}, want: true},
		{name: "write_file_failure", actionType: writeFileActionType, complete: true, failed: true, want: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			runtime, session, _, record := agentStatusFixture(t, test.actionType)
			if test.complete {
				agentStatusCompleteFixture(t, session, record, test.failed, test.output)
			}
			needed, err := modelWorkPending(context.Background(), runtime)
			if err != nil || needed != test.want {
				t.Fatalf("模型配置预检错误: got=%t want=%t err=%v", needed, test.want, err)
			}
		})
	}
}

func TestToolActionRecoveryMetadataAndPreflightLimits(t *testing.T) {
	runtime, session, agentID, record := agentStatusFixture(t, agentStatusActionType)
	if record.HandlerVersion != "1" || record.RecoveryPolicy != domain.RecoveryPolicySafeRetry || record.MaxAttempts != 3 {
		t.Fatalf("只读工具没有保存安全重试策略: %+v", record)
	}
	for attempt := uint64(1); attempt <= 3; attempt++ {
		claim, err := session.ClaimAction(context.Background(), record.Request.ID)
		if err != nil {
			t.Fatal(err)
		}
		if err := session.RecordActionUnknown(context.Background(), claim.Token, domain.Failure{Kind: domain.ErrorKindInterrupted, Message: "进程中断"}); err != nil {
			t.Fatal(err)
		}
		needed, err := modelWorkPending(context.Background(), runtime)
		if err != nil || needed != (attempt < 3) {
			t.Fatalf("unknown工具的尝试预算未应用于预检: attempt=%d needed=%t err=%v", attempt, needed, err)
		}
	}
	query, err := runtime.QueryAgent(agentID)
	if err != nil || query.Agent.StateVersion != 1 || query.Actions[0].Action.Status != domain.ActionStatusUnknown || query.Actions[0].Ready {
		t.Fatalf("预检推进业务或耗尽action仍ready: %+v %v", query, err)
	}
}

func TestToolResultModelPreflightChecksCorrelations(t *testing.T) {
	agent := core.AgentSnapshot{Definition: domain.DefinitionRef{ID: "main", Version: "1"}, State: map[string]any{
		"request_status": "waiting", "waiting_action_id": "action", "waiting_action_type": agentStatusActionType,
		"request_execution_id": "original", "waiting_execution_id": "tool-execution",
	}}
	delivery := core.DeliveryQuery{Ready: true, Event: domain.Event{Type: "action.result", Payload: map[string]any{
		"action_id": "action", "action_type": agentStatusActionType, "execution_id": "tool-execution", "status": "failed",
	}}}
	if !deliveryNeedsModel(agent, delivery) {
		t.Fatal("工具失败的续轮没有采用waiting_execution_id")
	}
	for _, field := range []string{"action_id", "action_type", "execution_id", "status"} {
		original := delivery.Event.Payload[field]
		delivery.Event.Payload[field] = "mismatched"
		if deliveryNeedsModel(agent, delivery) {
			t.Fatalf("不匹配的%s触发了配置读取", field)
		}
		delivery.Event.Payload[field] = original
	}
	delete(agent.State, "waiting_execution_id")
	delivery.Event.Payload["execution_id"] = "original"
	if !deliveryNeedsModel(agent, delivery) {
		t.Fatal("旧request_execution_id没有作为fallback")
	}
	agent.Definition.ID = "echo"
	if deliveryNeedsModel(agent, delivery) {
		t.Fatal("其他definition的工具结果触发了main模型配置")
	}
}

func TestMissingModelConfigLeavesPendingToolUnchanged(t *testing.T) {
	backend := core.NewMemoryRecoveryStore()
	prepared, err := core.OpenRuntime(context.Background(), backend)
	if err != nil {
		t.Fatal(err)
	}
	if err := registerAgentStatus(context.Background(), prepared); err != nil {
		t.Fatal(err)
	}
	definition := domain.DefinitionRef{ID: "main", Version: "1"}
	if err := prepared.RegisterDefinition(definition, agentStatusTestRunner(func(input core.ExecutionContext) (core.ExecutionResult, error) {
		action := domain.NewAction(agentStatusActionType, map[string]any{"agent_id": string(input.Agent.ID)})
		return core.ExecutionResult{StateUpdate: map[string]any{"request_status": "waiting", "waiting_action_id": string(action.ID), "waiting_action_type": agentStatusActionType,
			"request_execution_id": string(input.ExecutionID), "waiting_execution_id": string(input.ExecutionID)}, Actions: []domain.Action{action}}, nil
	})); err != nil {
		t.Fatal(err)
	}
	agent, err := prepared.CreateAgent("main", definition, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := prepared.Process(agent.ID, domain.NewEvent("fixture.start", nil)); err != nil {
		t.Fatal(err)
	}
	before, err := prepared.QueryAgent(agent.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := prepared.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
	missingConfig := chatTestConfig(t, "https://example.com")
	missingConfig = filepath.Join(filepath.Dir(missingConfig), "missing.env")
	var stdout, stderr bytes.Buffer
	args := []string{"run", "--data-dir", "test-memory", "--env-file", missingConfig, "--json", "--python", "/missing-python"}
	open := func(context.Context, string) (backendHandle, error) { return backendHandle{Store: backend}, nil }
	if code := runCommandWithBackend(context.Background(), args, &stdout, &stderr, open); code != 1 || stdout.Len() != 0 {
		t.Fatalf("工具续轮缺配置没有在执行前失败: code=%d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	assertCommandError(t, stderr.Bytes(), "operation", 1)
	if strings.Contains(stderr.String(), "python") {
		t.Fatalf("配置预检晚于业务执行: %s", stderr.String())
	}
	reopened, err := core.OpenRuntime(context.Background(), backend)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close(context.Background()) })
	after, err := reopened.QueryAgent(agent.ID)
	if err != nil || after.Agent.StateVersion != before.Agent.StateVersion || !reflect.DeepEqual(after.Agent.State, before.Agent.State) ||
		len(after.Actions) != 1 || after.Actions[0].Action.AttemptCount != 0 || after.Actions[0].Action.Status != domain.ActionStatusPending {
		t.Fatalf("缺配置改变了已保存工具或状态: %+v %v", after, err)
	}
}

func TestToolResultPreflightAfterRestartRequiresModel(t *testing.T) {
	ctx := context.Background()
	backend := &agentStatusTestBackend{RecoveryStore: core.NewMemoryRecoveryStore()}
	prepared, err := core.OpenRuntime(ctx, backend)
	if err != nil {
		t.Fatal(err)
	}
	if err := prepared.Executor().RegisterWithOptions(
		writeFileActionType, writeFileHandler{rootDir: t.TempDir()},
		core.HandlerOptions{Version: "1", RecoveryPolicy: domain.RecoveryPolicySafeRetry, MaxAttempts: 3},
	); err != nil {
		t.Fatal(err)
	}
	definition := domain.DefinitionRef{ID: "main", Version: "1"}
	if err := prepared.RegisterDefinition(definition, agentStatusTestRunner(func(input core.ExecutionContext) (core.ExecutionResult, error) {
		action := domain.NewAction(writeFileActionType, map[string]any{"path": "a.txt", "content": "x"})
		return core.ExecutionResult{StateUpdate: map[string]any{
			"request_status": "waiting", "waiting_action_id": string(action.ID), "waiting_action_type": writeFileActionType,
			"request_execution_id": string(input.ExecutionID), "waiting_execution_id": string(input.ExecutionID),
		}, Actions: []domain.Action{action}}, nil
	})); err != nil {
		t.Fatal(err)
	}
	agent, err := prepared.CreateAgent("main", definition, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := prepared.Process(agent.ID, domain.NewEvent("fixture.start", nil)); err != nil {
		t.Fatal(err)
	}
	query, err := prepared.QueryAgent(agent.ID)
	if err != nil || len(query.Actions) != 1 {
		t.Fatalf("无法建立工具action fixture: %+v %v", query, err)
	}
	// 工具结果落库后关闭runtime，模拟进程重启。
	agentStatusCompleteFixture(t, backend.session, query.Actions[0].Action, false, map[string]any{"path": "a.txt"})
	if err := prepared.Close(ctx); err != nil {
		t.Fatal(err)
	}

	missingConfig := chatTestConfig(t, "https://example.com")
	missingConfig = filepath.Join(filepath.Dir(missingConfig), "missing.env")
	var stdout, stderr bytes.Buffer
	args := []string{"run", "--data-dir", "test-memory", "--env-file", missingConfig, "--json", "--python", "/missing-python"}
	open := func(context.Context, string) (backendHandle, error) { return backendHandle{Store: backend}, nil }
	if code := runCommandWithBackend(ctx, args, &stdout, &stderr, open); code != 1 || stdout.Len() != 0 {
		t.Fatalf("工具结果续轮缺配置没有在执行前失败: code=%d stdout=%s stderr=%s", code, stdout.String(), stderr.String())
	}
	assertCommandError(t, stderr.Bytes(), "operation", 1)
	if strings.Contains(stderr.String(), "python") {
		t.Fatalf("配置预检晚于业务执行，工具结果没有触发模型加载: %s", stderr.String())
	}
}
