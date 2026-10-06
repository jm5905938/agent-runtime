package main

import (
	"agent-runtime/core"
	"agent-runtime/domain"
	pythonrunner "agent-runtime/python"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
	"time"
)

func mainResolutionAction(t *testing.T, query *core.AgentQuery, actionID domain.ID) core.ActionQuery {
	t.Helper()
	for _, action := range query.Actions {
		if action.Action.Request.ID == actionID {
			return action
		}
	}
	t.Fatalf("缺少待处理action%s", actionID)
	return core.ActionQuery{}
}

func mainResolutionAssertUnknown(t *testing.T, before, after core.ActionQuery) {
	t.Helper()
	if after.Action.Status != domain.ActionStatusUnknown || after.Action.Result != nil || after.Action.AttemptCount != 1 ||
		len(after.Attempts) != 1 || after.Attempts[0].Status != domain.ActionStatusUnknown || after.Ready ||
		!reflect.DeepEqual(before.Action, after.Action) || !reflect.DeepEqual(before.Attempts, after.Attempts) {
		t.Fatalf("人工决定篡改了旧调用或尝试的未知结果: before=%+v after=%+v", before, after)
	}
}

func mainResolutionAssertDecision(t *testing.T, query *core.AgentQuery, actionID domain.ID, receipt *core.ActionResolutionReceipt, decision core.ResolutionDecision, reason string) core.DeliveryQuery {
	t.Helper()
	if receipt == nil || receipt.Resolution.ActionID != actionID || receipt.Resolution.EventID == "" ||
		receipt.Resolution.Decision != decision || receipt.Resolution.Reason != reason ||
		receipt.Delivery.Key != (domain.DeliveryKey{AgentID: query.Agent.ID, EventID: receipt.Resolution.EventID}) {
		t.Fatalf("人工决定receipt缺少持久化来源: %+v", receipt)
	}
	action := mainResolutionAction(t, query, actionID)
	if action.Resolution == nil || !reflect.DeepEqual(*action.Resolution, receipt.Resolution) {
		t.Fatalf("status没有展示旧调用的人工决定: %+v", action)
	}
	resolved, manual := false, false
	for _, blocked := range action.BlockedBy {
		resolved = resolved || blocked.Code == core.BlockActionResolved
		manual = manual || blocked.Code == core.BlockManualUnknown
	}
	if !resolved || manual {
		t.Fatalf("已保存人工决定的旧unknown仍显示未处理: %+v", action.BlockedBy)
	}
	delivery := mainQueueDelivery(t, query, receipt.Resolution.EventID)
	if delivery.Event.Type != "action.resolution" || delivery.Event.Payload["action_id"] != string(actionID) ||
		delivery.Event.Payload["decision"] != string(decision) || delivery.Event.Payload["reason"] != reason ||
		delivery.Event.Payload["execution_id"] != string(*action.Action.Request.ExecutionID) {
		t.Fatalf("控制事件没有保存人工决定与旧调用关联: %+v", delivery)
	}
	return delivery
}

// 在模型开始后关闭session，后续CLI启动必须把该次尝试恢复为unknown。
func mainResolutionSQLiteInterruptAction(t *testing.T, directory string, actionID domain.ID) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	handle, err := openCommandBackend(ctx, directory)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := handle.Close(); err != nil {
			t.Error(err)
		}
	}()
	session, err := handle.Store.OpenSession(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := session.Close(context.Background()); err != nil {
			t.Error(err)
		}
	}()
	if _, err := session.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := session.ClaimAction(ctx, actionID); err != nil {
		t.Fatal(err)
	}
}

func mainResolutionSQLiteInterruptDecision(t *testing.T, directory string, key domain.DeliveryKey) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	handle, err := openCommandBackend(ctx, directory)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := handle.Close(); err != nil {
			t.Error(err)
		}
	}()
	session, err := handle.Store.OpenSession(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := session.Close(context.Background()); err != nil {
			t.Error(err)
		}
	}()
	if _, err := session.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := session.ClaimExecution(ctx, key); err != nil {
		t.Fatal(err)
	}
}

// 只提交控制事件的Python执行，模拟停在决定已处理、新调用尚未发送的边界。
func mainResolutionSQLiteCommitDecision(t *testing.T, p sqliteCommandProcess, directory string, agentID, eventID domain.ID) []domain.ActionRecord {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	handle, err := openCommandBackend(ctx, directory)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := handle.Close(); err != nil {
			t.Error(err)
		}
	}()
	session, err := handle.Store.OpenSession(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := session.Close(context.Background()); err != nil {
			t.Error(err)
		}
	}()
	if _, err := session.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	claim, err := session.ClaimExecution(ctx, domain.DeliveryKey{AgentID: agentID, EventID: eventID})
	if err != nil {
		t.Fatal(err)
	}
	runner, err := pythonrunner.NewRunner(pythonrunner.Options{Python: p.python, SourceDir: p.source})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := runner.Close(); err != nil {
			t.Error(err)
		}
	}()
	output, err := runner.RunContext(ctx, core.ExecutionContext{
		Agent: core.AgentSnapshot{ID: claim.Agent.ID, Name: claim.Agent.Name, Definition: claim.Agent.Definition,
			Status: claim.Agent.Status, State: claim.Agent.State, StateVersion: claim.Agent.StateVersion},
		Event: claim.Event, ExecutionID: claim.Token.ExecutionID, AttemptID: claim.Token.AttemptID,
	})
	if err != nil {
		t.Fatalf("真实Python未处理人工决定: %+v %v", output, err)
	}
	records := make([]domain.ActionRecord, 0, len(output.Actions))
	for _, action := range output.Actions {
		if action.Type != "model.generate" {
			t.Fatalf("人工决定生成了意外action: %+v", action)
		}
		action.BindExecution(claim.Token.ExecutionID)
		records = append(records, domain.ActionRecord{
			Request: action, AgentID: agentID, HandlerVersion: "1", RecoveryPolicy: domain.RecoveryPolicyManual,
			MaxAttempts: 1, Status: domain.ActionStatusPending,
			ResultEventID: domain.NewEvent("action.result", nil).ID,
		})
	}
	if _, err := session.CommitExecution(ctx, core.ExecutionCommit{Token: claim.Token, StateUpdate: output.StateUpdate, Actions: records}); err != nil {
		t.Fatal(err)
	}
	return records
}

func TestMainResolutionSQLiteCommandsAcrossProcesses(t *testing.T) {
	pythonArgs := chatTestPython(t)
	binary := filepath.Join(t.TempDir(), "agent-runtime")
	if runtime.GOOS == "windows" {
		binary += ".exe"
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if output, err := exec.CommandContext(ctx, "go", "build", "-o", binary, ".").CombinedOutput(); err != nil {
		t.Fatalf("构建人工恢复cli: %v\n%s", err, output)
	}
	p := sqliteCommandProcess{binary: binary, python: pythonArgs[1], source: pythonArgs[3]}

	t.Run("retry_preserves_unknown_and_frozen_prompt_at_each_restart", func(t *testing.T) {
		directory := filepath.Join(t.TempDir(), "重试 # %")
		created, _ := p.call(t, directory, 0, "init", "--definition", "main")
		agentID := string(created.Agent.ID)
		p.call(t, directory, 0, "submit", "--agent", agentID, "--event-id", "interrupted", "--message", "重试时保留原输入")
		t.Setenv("LLM_SYSTEM_PROMPT", "首次调用的系统提示词")
		t.Setenv("LLM_MAX_PROMPT_CHARS", "10000")
		old := mainSQLiteSaveAction(t, p, directory, created.Agent.ID, "interrupted", "interrupted")
		before, _ := p.call(t, directory, 0, "status", "--agent", agentID)
		original := mainResolutionAction(t, before.Query, old.ActionID)
		mainSQLiteAssertMessages(t, original.Action.Request.Payload["messages"], []mainSQLiteMessage{
			{Role: "system", Content: "首次调用的系统提示词"}, {Role: "user", Content: "重试时保留原输入"},
		})
		var captured mainToolsModelCapture
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			captured.append(mainToolsReadRequest(t, r))
			mainToolsReply(t, w, "人工重试成功")
		}))
		defer server.Close()
		config := chatTestConfig(t, server.URL)
		file, err := os.OpenFile(config, os.O_APPEND|os.O_WRONLY, 0600)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := file.WriteString("LLM_SYSTEM_PROMPT=重启后新的系统提示词\nLLM_MAX_PROMPT_CHARS=5\n"); err != nil {
			file.Close()
			t.Fatal(err)
		}
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
		resolved, _ := p.call(t, directory, 0, "resolve", "--action", string(old.ActionID), "--retry")
		if resolved.Resolution == nil || resolved.Resolution.Duplicate {
			t.Fatalf("首次人工处理没有返回新决定: %+v", resolved)
		}
		pending, _ := p.call(t, directory, 0, "status", "--agent", agentID)
		control := mainResolutionAssertDecision(t, pending.Query, old.ActionID, resolved.Resolution, core.ResolutionRetry, "用户选择重试")
		mainResolutionAssertUnknown(t, original, mainResolutionAction(t, pending.Query, old.ActionID))
		if pending.Query.Agent.StateVersion != before.Query.Agent.StateVersion || len(pending.Query.Actions) != 1 ||
			control.Delivery.Status != domain.DeliveryStatusPending || control.Execution != nil || len(control.Attempts) != 0 || !control.Ready || len(captured.all()) != 0 {
			t.Fatalf("resolve执行了模型或消耗了控制事件: %+v", pending.Query)
		}
		duplicate, _ := p.call(t, directory, 0, "resolve", "--action", string(old.ActionID), "--retry")
		if duplicate.Resolution == nil || !duplicate.Resolution.Duplicate || !reflect.DeepEqual(duplicate.Resolution.Resolution, resolved.Resolution.Resolution) {
			t.Fatalf("待处理决定未幂等: %+v", duplicate)
		}
		_, stderr := p.call(t, directory, 1, "resolve", "--action", string(old.ActionID), "--abandon", "--reason", "改为放弃")
		assertCommandError(t, stderr, "conflict", 1)
		missingConfig := filepath.Join(t.TempDir(), "missing.env")
		_, stderr = p.call(t, directory, 1, "run", "--env-file", missingConfig)
		assertCommandError(t, stderr, "operation", 1)
		unchanged, _ := p.call(t, directory, 0, "status", "--agent", agentID)
		mainQueueAssertUnchanged(t, pending.Query, unchanged.Query)
		retries := mainResolutionSQLiteCommitDecision(t, p, directory, created.Agent.ID, control.Event.ID)
		if len(retries) != 1 || retries[0].Request.ID == old.ActionID || retries[0].Request.ExecutionID == nil {
			t.Fatalf("人工重试没有创建新的关联调用: %+v", retries)
		}
		retry := retries[0]
		restarted, _ := p.call(t, directory, 0, "status", "--agent", agentID)
		processed := mainResolutionAssertDecision(t, restarted.Query, old.ActionID, resolved.Resolution, core.ResolutionRetry, "用户选择重试")
		mainResolutionAssertUnknown(t, original, mainResolutionAction(t, restarted.Query, old.ActionID))
		savedRetry := mainResolutionAction(t, restarted.Query, retry.Request.ID)
		if processed.Execution == nil || processed.Delivery.Status != domain.DeliveryStatusCompleted || len(processed.Attempts) != 1 ||
			*retry.Request.ExecutionID != processed.Execution.ID || savedRetry.Action.Status != domain.ActionStatusPending || len(savedRetry.Attempts) != 0 ||
			restarted.Query.Agent.State["waiting_action_id"] != string(retry.Request.ID) || restarted.Query.Agent.State["waiting_execution_id"] != string(processed.Execution.ID) ||
			restarted.Query.Agent.State["request_execution_id"] != string(old.ExecutionID) || len(captured.all()) != 0 {
			t.Fatalf("控制事件提交后重启丢失新调用或原请求来源: %+v", restarted.Query)
		}
		wantPayload := make(map[string]any, len(original.Action.Request.Payload)+1)
		for key, value := range original.Action.Request.Payload {
			wantPayload[key] = value
		}
		wantPayload["retry_of"] = string(old.ActionID)
		mainToolsAssertJSON(t, savedRetry.Action.Request.Payload, wantPayload)
		p.call(t, directory, 0, "run", "--env-file", config)
		after, _ := p.call(t, directory, 0, "status", "--agent", agentID)
		requests := captured.all()
		if len(requests) != 1 || after.Query.Agent.State["request_status"] != "succeeded" || after.Query.Agent.State["result"] != "人工重试成功" ||
			after.Query.Agent.State["waiting_action_id"] != nil || after.Query.Agent.StateVersion != 3 || len(after.Query.Actions) != 2 || len(after.Query.Deliveries) != 3 {
			t.Fatalf("跨进程人工重试没有完成原轮次: %+v requests=%+v", after.Query, requests)
		}
		mainToolsAssertJSON(t, requests[0].Messages, original.Action.Request.Payload["messages"])
		mainToolsAssertJSON(t, requests[0].Tools, original.Action.Request.Payload["tools"])
		mainSQLiteAssertMessages(t, after.Query.Agent.State["messages"], []mainSQLiteMessage{
			{Role: "user", Content: "重试时保留原输入"}, {Role: "assistant", Content: "人工重试成功"},
		})
		mainResolutionAssertUnknown(t, original, mainResolutionAction(t, after.Query, old.ActionID))
		mainResolutionAssertDecision(t, after.Query, old.ActionID, resolved.Resolution, core.ResolutionRetry, "用户选择重试")
		if completed := mainResolutionAction(t, after.Query, retry.Request.ID); completed.Action.Status != domain.ActionStatusSucceeded || completed.Action.Result == nil || completed.Action.AttemptCount != 1 {
			t.Fatalf("新调用结果或尝试没有持久化: %+v", completed)
		}
		duplicate, _ = p.call(t, directory, 0, "resolve", "--action", string(old.ActionID), "--retry")
		if duplicate.Resolution == nil || !duplicate.Resolution.Duplicate || duplicate.Resolution.Delivery.Status != domain.DeliveryStatusCompleted {
			t.Fatalf("已完成人工决定不能重复读取: %+v", duplicate)
		}
		_, stderr = p.call(t, directory, 1, "resolve", "--action", string(old.ActionID), "--retry", "--reason", "修改原决定理由")
		assertCommandError(t, stderr, "conflict", 1)
		_, stderr = p.call(t, directory, 1, "resolve", "--action", string(retry.Request.ID), "--retry")
		assertCommandError(t, stderr, "conflict", 1)
		p.call(t, directory, 0, "run", "--env-file", missingConfig)
		idle, _ := p.call(t, directory, 0, "status", "--agent", agentID)
		mainQueueAssertUnchanged(t, after.Query, idle.Query)
		if len(captured.all()) != 1 {
			t.Fatal("重复决定或idle重启再次发送了模型调用")
		}
	})

	t.Run("interrupted_resolution_execution_requeues_without_sending_old_call", func(t *testing.T) {
		directory := filepath.Join(t.TempDir(), "data")
		created, _ := p.call(t, directory, 0, "init", "--definition", "main")
		agentID := string(created.Agent.ID)
		p.call(t, directory, 0, "submit", "--agent", agentID, "--event-id", "interrupted-control", "--message", "控制事件处理中断")
		old := mainSQLiteSaveAction(t, p, directory, created.Agent.ID, "interrupted-control", "interrupted")
		before, _ := p.call(t, directory, 0, "status", "--agent", agentID)
		original := mainResolutionAction(t, before.Query, old.ActionID)
		resolved, _ := p.call(t, directory, 0, "resolve", "--action", string(old.ActionID), "--retry")
		if resolved.Resolution == nil {
			t.Fatal("人工重试没有保存控制事件")
		}
		mainResolutionSQLiteInterruptDecision(t, directory, resolved.Resolution.Delivery.Key)
		restarted, _ := p.call(t, directory, 0, "status", "--agent", agentID)
		control := mainResolutionAssertDecision(t, restarted.Query, old.ActionID, resolved.Resolution, core.ResolutionRetry, "用户选择重试")
		if control.Delivery.Status != domain.DeliveryStatusPending || control.Execution == nil || control.Execution.Status != domain.ExecutionStatusPending ||
			control.Execution.AttemptCount != 1 || len(control.Attempts) != 1 || control.Attempts[0].Status != domain.AttemptStatusInterrupted ||
			control.Attempts[0].Error == nil || control.Attempts[0].Error.Kind != domain.ErrorKindInterrupted || !control.Ready ||
			len(restarted.Query.Actions) != 1 || restarted.Query.Agent.StateVersion != before.Query.Agent.StateVersion ||
			!reflect.DeepEqual(restarted.StartupRecovery.RequeuedDeliveries, []domain.DeliveryKey{resolved.Resolution.Delivery.Key}) {
			t.Fatalf("控制事件中断恢复丢失决定、消耗状态或没有重新排队: %+v recovery=%+v", restarted.Query, restarted.StartupRecovery)
		}
		mainResolutionAssertUnknown(t, original, mainResolutionAction(t, restarted.Query, old.ActionID))
		duplicate, _ := p.call(t, directory, 0, "resolve", "--action", string(old.ActionID), "--retry")
		if duplicate.Resolution == nil || !duplicate.Resolution.Duplicate || duplicate.Resolution.Delivery.ExecutionID != control.Execution.ID {
			t.Fatalf("中断恢复后重复决定改变了控制事件身份: %+v", duplicate)
		}
		var captured mainToolsModelCapture
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			captured.append(mainToolsReadRequest(t, r))
			mainToolsReply(t, w, "中断后人工重试完成")
		}))
		defer server.Close()
		config := chatTestConfig(t, server.URL)
		p.call(t, directory, 0, "run", "--env-file", config)
		after, _ := p.call(t, directory, 0, "status", "--agent", agentID)
		processed := mainResolutionAssertDecision(t, after.Query, old.ActionID, resolved.Resolution, core.ResolutionRetry, "用户选择重试")
		if processed.Delivery.Status != domain.DeliveryStatusCompleted || processed.Execution == nil || processed.Execution.AttemptCount != 2 ||
			len(processed.Attempts) != 2 || processed.Attempts[0].Status != domain.AttemptStatusInterrupted || processed.Attempts[1].Status != domain.AttemptStatusSucceeded ||
			len(after.Query.Actions) != 2 || len(captured.all()) != 1 || after.Query.Agent.State["result"] != "中断后人工重试完成" ||
			after.Query.Agent.State["request_status"] != "succeeded" {
			t.Fatalf("重排的控制事件没有恰好创建并执行一次新调用: %+v", after.Query)
		}
		mainResolutionAssertUnknown(t, original, mainResolutionAction(t, after.Query, old.ActionID))
		for _, action := range after.Query.Actions {
			if action.Action.Request.ID == old.ActionID {
				continue
			}
			if action.Action.Request.Payload["retry_of"] != string(old.ActionID) || action.Action.Request.ExecutionID == nil ||
				*action.Action.Request.ExecutionID != processed.Execution.ID || action.Action.Status != domain.ActionStatusSucceeded || action.Action.AttemptCount != 1 || len(action.Attempts) != 1 {
				t.Fatalf("中断恢复后的新调用缺少原决定来源或被重复执行: %+v", action)
			}
		}
		p.call(t, directory, 0, "run", "--env-file", filepath.Join(t.TempDir(), "missing.env"))
		idle, _ := p.call(t, directory, 0, "status", "--agent", agentID)
		mainQueueAssertUnchanged(t, after.Query, idle.Query)
		if len(captured.all()) != 1 {
			t.Fatal("控制事件中断恢复后重复发送了模型调用")
		}
	})

	t.Run("successive_manual_retries_keep_direct_links_and_frozen_payload", func(t *testing.T) {
		directory := filepath.Join(t.TempDir(), "data")
		created, _ := p.call(t, directory, 0, "init", "--definition", "main")
		agentID := string(created.Agent.ID)
		p.call(t, directory, 0, "submit", "--agent", agentID, "--event-id", "retry-chain", "--message", "连续人工重试")
		old := mainSQLiteSaveAction(t, p, directory, created.Agent.ID, "retry-chain", "interrupted")
		before, _ := p.call(t, directory, 0, "status", "--agent", agentID)
		original := mainResolutionAction(t, before.Query, old.ActionID)
		firstDecision, _ := p.call(t, directory, 0, "resolve", "--action", string(old.ActionID), "--retry")
		if firstDecision.Resolution == nil {
			t.Fatal("第一笔人工重试没有保存决定")
		}
		firstRetries := mainResolutionSQLiteCommitDecision(t, p, directory, created.Agent.ID, firstDecision.Resolution.Resolution.EventID)
		if len(firstRetries) != 1 {
			t.Fatalf("第一笔人工重试未产生单个新调用: %+v", firstRetries)
		}
		firstRetry := firstRetries[0]
		mainResolutionSQLiteInterruptAction(t, directory, firstRetry.Request.ID)
		interrupted, _ := p.call(t, directory, 0, "status", "--agent", agentID)
		firstUnknown := mainResolutionAction(t, interrupted.Query, firstRetry.Request.ID)
		if firstUnknown.Action.Status != domain.ActionStatusUnknown || firstUnknown.Action.Result != nil || firstUnknown.Action.AttemptCount != 1 ||
			len(firstUnknown.Attempts) != 1 || firstUnknown.Attempts[0].Status != domain.ActionStatusUnknown || firstUnknown.Resolution != nil ||
			interrupted.Query.Agent.State["waiting_action_id"] != string(firstRetry.Request.ID) {
			t.Fatalf("第一笔重试中断后不能人工处理: %+v", interrupted.Query)
		}
		firstControl := mainResolutionAssertDecision(t, interrupted.Query, old.ActionID, firstDecision.Resolution, core.ResolutionRetry, "用户选择重试")
		if firstControl.Execution == nil || firstRetry.Request.ExecutionID == nil || *firstRetry.Request.ExecutionID != firstControl.Execution.ID ||
			firstRetry.Request.Payload["retry_of"] != string(old.ActionID) {
			t.Fatalf("第一笔重试丢失直接来源关联: %+v", firstRetry)
		}
		secondDecision, _ := p.call(t, directory, 0, "resolve", "--action", string(firstRetry.Request.ID), "--retry", "--reason", "再重试当前调用")
		pending, _ := p.call(t, directory, 0, "status", "--agent", agentID)
		secondControl := mainResolutionAssertDecision(t, pending.Query, firstRetry.Request.ID, secondDecision.Resolution, core.ResolutionRetry, "再重试当前调用")
		if secondControl.Event.ID == firstControl.Event.ID || secondControl.Event.Payload["execution_id"] != string(firstControl.Execution.ID) {
			t.Fatalf("连续决定复用了旧控制事件或指向初始execution: %+v", secondControl)
		}
		secondRetries := mainResolutionSQLiteCommitDecision(t, p, directory, created.Agent.ID, secondControl.Event.ID)
		if len(secondRetries) != 1 {
			t.Fatalf("第二笔人工重试未产生单个新调用: %+v", secondRetries)
		}
		secondRetry := secondRetries[0]
		restarted, _ := p.call(t, directory, 0, "status", "--agent", agentID)
		secondProcessed := mainResolutionAssertDecision(t, restarted.Query, firstRetry.Request.ID, secondDecision.Resolution, core.ResolutionRetry, "再重试当前调用")
		if secondRetry.Request.ID == old.ActionID || secondRetry.Request.ID == firstRetry.Request.ID || secondRetry.Request.ExecutionID == nil ||
			secondProcessed.Execution == nil || *secondRetry.Request.ExecutionID != secondProcessed.Execution.ID ||
			secondRetry.Request.Payload["retry_of"] != string(firstRetry.Request.ID) ||
			restarted.Query.Agent.State["request_execution_id"] != string(old.ExecutionID) || restarted.Query.Agent.State["waiting_action_id"] != string(secondRetry.Request.ID) {
			t.Fatalf("第二笔重试没有保留原轮次与直接调用链: %+v", restarted.Query)
		}
		mainToolsAssertJSON(t, secondRetry.Request.Payload["messages"], original.Action.Request.Payload["messages"])
		mainToolsAssertJSON(t, secondRetry.Request.Payload["tools"], original.Action.Request.Payload["tools"])
		mainResolutionAssertUnknown(t, original, mainResolutionAction(t, restarted.Query, old.ActionID))
		mainResolutionAssertUnknown(t, firstUnknown, mainResolutionAction(t, restarted.Query, firstRetry.Request.ID))
		var captured mainToolsModelCapture
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			captured.append(mainToolsReadRequest(t, r))
			mainToolsReply(t, w, "连续重试最终成功")
		}))
		defer server.Close()
		config := chatTestConfig(t, server.URL)
		p.call(t, directory, 0, "run", "--env-file", config)
		after, _ := p.call(t, directory, 0, "status", "--agent", agentID)
		requests := captured.all()
		if len(requests) != 1 || len(after.Query.Actions) != 3 || len(after.Query.Deliveries) != 4 || after.Query.Agent.StateVersion != 4 ||
			after.Query.Agent.State["request_status"] != "succeeded" || after.Query.Agent.State["result"] != "连续重试最终成功" || after.Query.Agent.State["waiting_action_id"] != nil {
			t.Fatalf("连续重试未恰好执行最后的新调用: %+v requests=%+v", after.Query, requests)
		}
		mainToolsAssertJSON(t, requests[0].Messages, original.Action.Request.Payload["messages"])
		mainToolsAssertJSON(t, requests[0].Tools, original.Action.Request.Payload["tools"])
		mainResolutionAssertUnknown(t, original, mainResolutionAction(t, after.Query, old.ActionID))
		mainResolutionAssertUnknown(t, firstUnknown, mainResolutionAction(t, after.Query, firstRetry.Request.ID))
		mainResolutionAssertDecision(t, after.Query, old.ActionID, firstDecision.Resolution, core.ResolutionRetry, "用户选择重试")
		mainResolutionAssertDecision(t, after.Query, firstRetry.Request.ID, secondDecision.Resolution, core.ResolutionRetry, "再重试当前调用")
		mainSQLiteAssertMessages(t, after.Query.Agent.State["messages"], []mainSQLiteMessage{
			{Role: "user", Content: "连续人工重试"}, {Role: "assistant", Content: "连续重试最终成功"},
		})
		if final := mainResolutionAction(t, after.Query, secondRetry.Request.ID); final.Action.Status != domain.ActionStatusSucceeded || final.Action.Result == nil || final.Action.AttemptCount != 1 || len(final.Attempts) != 1 {
			t.Fatalf("最终新调用的结果或唯一尝试没有保存: %+v", final)
		}
	})

	t.Run("abandon_keeps_successful_history_and_advances_queued_users", func(t *testing.T) {
		directory := filepath.Join(t.TempDir(), "data")
		created, _ := p.call(t, directory, 0, "init", "--definition", "main")
		agentID := string(created.Agent.ID)
		var captured mainToolsModelCapture
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			request := mainToolsReadRequest(t, r)
			captured.append(request)
			mainToolsReply(t, w, "回复："+mainQueueUser(t, request))
		}))
		defer server.Close()
		config := chatTestConfig(t, server.URL)
		p.call(t, directory, 0, "submit", "--agent", agentID, "--event-id", "success", "--message", "已完成的第一轮")
		p.call(t, directory, 0, "run", "--env-file", config)
		history := []mainSQLiteMessage{{Role: "user", Content: "已完成的第一轮"}, {Role: "assistant", Content: "回复：已完成的第一轮"}}
		p.call(t, directory, 0, "submit", "--agent", agentID, "--event-id", "abandoned", "--message", "应放弃的轮次")
		old := mainSQLiteSaveAction(t, p, directory, created.Agent.ID, "abandoned", "interrupted")
		p.call(t, directory, 0, "submit", "--agent", agentID, "--event-id", "queued-second", "--message", "放弃后的第二轮")
		p.call(t, directory, 0, "submit", "--agent", agentID, "--event-id", "queued-third", "--message", "放弃后的第三轮")
		before, _ := p.call(t, directory, 0, "status", "--agent", agentID)
		original := mainResolutionAction(t, before.Query, old.ActionID)
		mainQueueAssertWaiting(t, before.Query, "queued-second", "queued-third")
		resolved, _ := p.call(t, directory, 0, "resolve", "--action", string(old.ActionID), "--abandon", "--reason", "这轮不再需要")
		pending, _ := p.call(t, directory, 0, "status", "--agent", agentID)
		control := mainResolutionAssertDecision(t, pending.Query, old.ActionID, resolved.Resolution, core.ResolutionAbandon, "这轮不再需要")
		mainQueueAssertWaiting(t, pending.Query, "queued-second", "queued-third")
		missingConfig := filepath.Join(t.TempDir(), "missing.env")
		_, stderr := p.call(t, directory, 1, "run", "--env-file", missingConfig)
		assertCommandError(t, stderr, "operation", 1)
		unchanged, _ := p.call(t, directory, 0, "status", "--agent", agentID)
		mainQueueAssertUnchanged(t, pending.Query, unchanged.Query)
		if actions := mainResolutionSQLiteCommitDecision(t, p, directory, created.Agent.ID, control.Event.ID); len(actions) != 0 {
			t.Fatalf("放弃产生了外部调用: %+v", actions)
		}
		abandoned, _ := p.call(t, directory, 0, "status", "--agent", agentID)
		state := abandoned.Query.Agent.State
		if state["request_status"] != "failed" || state["error"] != "用户放弃本轮: 这轮不再需要" || state["result"] != nil ||
			state["waiting_action_id"] != nil || state["waiting_execution_id"] != nil || state["pending_message"] != nil ||
			len(captured.all()) != 1 || len(abandoned.Query.Actions) != 2 {
			t.Fatalf("放弃没有清理当前轮或误发出模型请求: %+v", abandoned.Query)
		}
		mainSQLiteAssertMessages(t, state["messages"], history)
		mainToolsAssertJSON(t, state["pending_messages"], []any{})
		mainToolsAssertJSON(t, state["pending_tool_calls"], []any{})
		mainResolutionAssertUnknown(t, original, mainResolutionAction(t, abandoned.Query, old.ActionID))
		if second := mainQueueDelivery(t, abandoned.Query, "queued-second"); !second.Ready || second.Execution != nil || len(second.Attempts) != 0 {
			t.Fatalf("放弃后最早输入没有就绪: %+v", second)
		}
		if third := mainQueueDelivery(t, abandoned.Query, "queued-third"); third.Ready || third.Execution != nil || len(third.Attempts) != 0 {
			t.Fatalf("第三轮没有继续等待更早输入: %+v", third)
		}
		p.call(t, directory, 0, "run", "--env-file", config)
		after, _ := p.call(t, directory, 0, "status", "--agent", agentID)
		requests := captured.all()
		if len(requests) != 3 || mainQueueUser(t, requests[1]) != "放弃后的第二轮" || mainQueueUser(t, requests[2]) != "放弃后的第三轮" ||
			after.Query.Agent.State["request_status"] != "succeeded" || after.Query.Agent.State["result"] != "回复：放弃后的第三轮" || len(after.Query.Actions) != 4 {
			t.Fatalf("放弃后没有按FIFO完成队列: %+v requests=%+v", after.Query, requests)
		}
		secondInput := append(append([]mainSQLiteMessage(nil), history...), mainSQLiteMessage{Role: "user", Content: "放弃后的第二轮"})
		secondHistory := append(secondInput, mainSQLiteMessage{Role: "assistant", Content: "回复：放弃后的第二轮"})
		thirdInput := append(append([]mainSQLiteMessage(nil), secondHistory...), mainSQLiteMessage{Role: "user", Content: "放弃后的第三轮"})
		mainSQLiteAssertMessages(t, requests[1].Messages, secondInput)
		mainSQLiteAssertMessages(t, requests[2].Messages, thirdInput)
		mainSQLiteAssertMessages(t, after.Query.Agent.State["messages"], append(thirdInput, mainSQLiteMessage{Role: "assistant", Content: "回复：放弃后的第三轮"}))
		mainResolutionAssertUnknown(t, original, mainResolutionAction(t, after.Query, old.ActionID))
		mainResolutionAssertDecision(t, after.Query, old.ActionID, resolved.Resolution, core.ResolutionAbandon, "这轮不再需要")
		duplicate, _ := p.call(t, directory, 0, "resolve", "--action", string(old.ActionID), "--abandon", "--reason", "这轮不再需要")
		if duplicate.Resolution == nil || !duplicate.Resolution.Duplicate {
			t.Fatalf("轮次推进后丢失已保存放弃决定: %+v", duplicate)
		}
		_, stderr = p.call(t, directory, 1, "resolve", "--action", string(old.ActionID), "--retry")
		assertCommandError(t, stderr, "conflict", 1)
		p.call(t, directory, 0, "run", "--env-file", missingConfig)
		idle, _ := p.call(t, directory, 0, "status", "--agent", agentID)
		mainQueueAssertUnchanged(t, after.Query, idle.Query)
		if len(captured.all()) != 3 {
			t.Fatal("重复放弃或重启重复发送了队列请求")
		}
	})

	t.Run("abandon_without_queued_input_needs_no_model_config", func(t *testing.T) {
		directory := filepath.Join(t.TempDir(), "data")
		created, _ := p.call(t, directory, 0, "init", "--definition", "main")
		agentID := string(created.Agent.ID)
		p.call(t, directory, 0, "submit", "--agent", agentID, "--event-id", "abandon-only", "--message", "只放弃当前轮")
		old := mainSQLiteSaveAction(t, p, directory, created.Agent.ID, "abandon-only", "interrupted")
		before, _ := p.call(t, directory, 0, "status", "--agent", agentID)
		original := mainResolutionAction(t, before.Query, old.ActionID)
		resolved, _ := p.call(t, directory, 0, "resolve", "--action", string(old.ActionID), "--abandon", "--reason", "手动结束")
		p.call(t, directory, 0, "run", "--env-file", filepath.Join(t.TempDir(), "missing.env"))
		after, _ := p.call(t, directory, 0, "status", "--agent", agentID)
		if after.Query.Agent.State["request_status"] != "failed" || after.Query.Agent.State["error"] != "用户放弃本轮: 手动结束" ||
			after.Query.Agent.State["waiting_action_id"] != nil || len(after.Query.Actions) != 1 || len(after.Query.Deliveries) != 2 {
			t.Fatalf("无模型配置不能独立处理放弃决定: %+v", after.Query)
		}
		mainResolutionAssertUnknown(t, original, mainResolutionAction(t, after.Query, old.ActionID))
		control := mainResolutionAssertDecision(t, after.Query, old.ActionID, resolved.Resolution, core.ResolutionAbandon, "手动结束")
		if control.Delivery.Status != domain.DeliveryStatusCompleted || control.Execution == nil || len(control.Attempts) != 1 {
			t.Fatalf("放弃决定未完成持久化: %+v", control)
		}
	})

	t.Run("retry_of_tool_continuation_keeps_saved_trajectory", func(t *testing.T) {
		directory := filepath.Join(t.TempDir(), "data")
		created, _ := p.call(t, directory, 0, "init", "--definition", "main")
		agentID := string(created.Agent.ID)
		p.call(t, directory, 0, "submit", "--agent", agentID, "--event-id", "tools", "--message", "查询后恢复模型")
		firstModel := mainToolsSQLiteCommitExecution(t, p, directory, created.Agent.ID, "tools")
		calls := []map[string]any{mainToolsCall("saved-tool", "{}")}
		mainToolsSQLiteCompleteAction(t, directory, firstModel, map[string]any{"message": "", "tool_calls": calls})
		tool := mainToolsSQLiteCommitExecution(t, p, directory, created.Agent.ID, firstModel.ResultEventID)
		mainToolsSQLiteCompleteAction(t, directory, tool, map[string]any{"id": agentID, "name": "main", "request_status": "waiting"})
		continuation := mainToolsSQLiteCommitExecution(t, p, directory, created.Agent.ID, tool.ResultEventID)
		mainResolutionSQLiteInterruptAction(t, directory, continuation.Request.ID)
		before, _ := p.call(t, directory, 0, "status", "--agent", agentID)
		original := mainResolutionAction(t, before.Query, continuation.Request.ID)
		if before.Query.Agent.State["tool_rounds"] != float64(1) || len(before.Query.Actions) != 3 {
			t.Fatalf("工具续轮fixture不完整: %+v", before.Query)
		}
		var captured mainToolsModelCapture
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			captured.append(mainToolsReadRequest(t, r))
			mainToolsReply(t, w, "工具续轮恢复成功")
		}))
		defer server.Close()
		config := chatTestConfig(t, server.URL)
		resolved, _ := p.call(t, directory, 0, "resolve", "--action", string(continuation.Request.ID), "--retry", "--reason", "继续已有工具结果")
		pending, _ := p.call(t, directory, 0, "status", "--agent", agentID)
		control := mainResolutionAssertDecision(t, pending.Query, continuation.Request.ID, resolved.Resolution, core.ResolutionRetry, "继续已有工具结果")
		retries := mainResolutionSQLiteCommitDecision(t, p, directory, created.Agent.ID, control.Event.ID)
		if len(retries) != 1 {
			t.Fatalf("工具续轮人工重试未生成单个模型调用: %+v", retries)
		}
		restarted, _ := p.call(t, directory, 0, "status", "--agent", agentID)
		for _, field := range []string{"messages", "pending_messages", "pending_tool_calls", "tool_rounds", "request_event_id", "request_execution_id"} {
			mainToolsAssertJSON(t, restarted.Query.Agent.State[field], before.Query.Agent.State[field])
		}
		mainToolsAssertJSON(t, retries[0].Request.Payload["messages"], continuation.Request.Payload["messages"])
		mainToolsAssertJSON(t, retries[0].Request.Payload["tools"], continuation.Request.Payload["tools"])
		if retries[0].Request.Payload["retry_of"] != string(continuation.Request.ID) {
			t.Fatalf("工具续轮重试缺少旧调用关联: %+v", retries[0])
		}
		p.call(t, directory, 0, "run", "--env-file", config)
		after, _ := p.call(t, directory, 0, "status", "--agent", agentID)
		requests := captured.all()
		if len(requests) != 1 || after.Query.Agent.State["request_status"] != "succeeded" || after.Query.Agent.State["result"] != "工具续轮恢复成功" ||
			len(after.Query.Actions) != 4 || len(after.Query.Deliveries) != 5 {
			t.Fatalf("工具续轮重试重复执行工具或丢失轨迹: %+v requests=%+v", after.Query, requests)
		}
		mainToolsAssertJSON(t, requests[0].Messages, continuation.Request.Payload["messages"])
		mainToolsAssertJSON(t, requests[0].Tools, continuation.Request.Payload["tools"])
		if len(requests[0].Messages) != 3 {
			t.Fatalf("工具续轮没有携带已保存工具调用与结果: %+v", requests[0].Messages)
		}
		mainToolsOutput(t, requests[0].Messages[2], "saved-tool")
		history := append(append([]map[string]any(nil), requests[0].Messages...), map[string]any{"role": "assistant", "content": "工具续轮恢复成功"})
		mainToolsAssertJSON(t, after.Query.Agent.State["messages"], history)
		mainResolutionAssertUnknown(t, original, mainResolutionAction(t, after.Query, continuation.Request.ID))
		if saved := mainResolutionAction(t, after.Query, tool.Request.ID); saved.Action.AttemptCount != 1 || len(saved.Attempts) != 1 || saved.Action.Status != domain.ActionStatusSucceeded {
			t.Fatalf("人工恢复重复执行了已有工具: %+v", saved)
		}
	})
}
