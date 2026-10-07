package main

import (
	"agent-runtime/core"
	"agent-runtime/domain"
	"context"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
	"time"
)

func mainQueueUser(t *testing.T, request mainToolsModelRequest) string {
	t.Helper()
	for i := len(request.Messages) - 1; i >= 0; i-- {
		if request.Messages[i]["role"] == "user" {
			message, ok := request.Messages[i]["content"].(string)
			if !ok {
				t.Error("模型请求的用户消息不是字符串")
			}
			return message
		}
	}
	t.Error("模型请求缺少用户消息")
	return ""
}

func mainQueueDelivery(t *testing.T, query *core.AgentQuery, eventID domain.ID) core.DeliveryQuery {
	t.Helper()
	for _, delivery := range query.Deliveries {
		if delivery.Event.ID == eventID {
			return delivery
		}
	}
	t.Fatalf("缺少排队事件%s", eventID)
	return core.DeliveryQuery{}
}

func mainQueueAssertWaiting(t *testing.T, query *core.AgentQuery, eventIDs ...domain.ID) {
	t.Helper()
	if query.Agent.State["request_status"] != "waiting" {
		t.Fatalf("fixture未保存等待状态: %+v", query.Agent)
	}
	for _, eventID := range eventIDs {
		delivery := mainQueueDelivery(t, query, eventID)
		waiting := false
		for _, reason := range delivery.BlockedBy {
			waiting = waiting || reason.Code == core.BlockAgentWaiting
		}
		if delivery.Delivery.Status != domain.DeliveryStatusPending || delivery.Execution != nil || len(delivery.Attempts) != 0 ||
			delivery.Ready || !waiting {
			t.Fatalf("等待中的输入被执行、消耗尝试或未显示业务阻塞: %+v", delivery)
		}
	}
}

// 恢复报告只描述当前进程的启动；比较持久化业务记录时不包含它。
func mainQueueAssertUnchanged(t *testing.T, before, after *core.AgentQuery) {
	t.Helper()
	if !reflect.DeepEqual(before.Agent, after.Agent) || !reflect.DeepEqual(before.Deliveries, after.Deliveries) ||
		!reflect.DeepEqual(before.Actions, after.Actions) {
		t.Fatalf("排队或恢复改变了未执行的业务记录: before=%+v after=%+v", before, after)
	}
}

func mainQueueSQLiteFailAction(t *testing.T, directory string, record domain.ActionRecord) {
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
	claim, err := session.ClaimAction(ctx, record.Request.ID)
	if err != nil {
		t.Fatal(err)
	}
	failure := &domain.Failure{Kind: domain.ErrorKindBusiness, Message: "已保存的模型失败"}
	result := domain.ActionResult{ActionID: record.Request.ID, EventID: record.ResultEventID, Status: domain.ActionStatusFailed, Error: failure}
	event := domain.NewEvent("action.result", map[string]any{
		"action_id": string(record.Request.ID), "execution_id": string(*record.Request.ExecutionID),
		"action_type": record.Request.Type, "status": "failed", "error": failure.Message,
	})
	event.ID = record.ResultEventID
	if _, err := session.CompleteAction(ctx, core.ActionCompletion{Token: claim.Token, Result: result, Event: event}); err != nil {
		t.Fatal(err)
	}
}

func TestMainQueueSQLiteCommandsAcrossProcesses(t *testing.T) {
	pythonArgs := chatTestPython(t)
	binary := filepath.Join(t.TempDir(), "agent-runtime")
	if runtime.GOOS == "windows" {
		binary += ".exe"
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if output, err := exec.CommandContext(ctx, "go", "build", "-o", binary, ".").CombinedOutput(); err != nil {
		t.Fatalf("构建main输入排队cli: %v\n%s", err, output)
	}
	p := sqliteCommandProcess{binary: binary, python: pythonArgs[1], source: pythonArgs[3]}

	t.Run("queued_requests_finish_fifo_with_history", func(t *testing.T) {
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
		messages := []string{"第一轮", "第二轮", "第三轮"}
		eventIDs := []string{"first", "second", "third"}
		for i, message := range messages {
			p.call(t, directory, 0, "submit", "--agent", agentID, "--event-id", eventIDs[i], "--message", message)
		}
		before, _ := p.call(t, directory, 0, "status", "--agent", agentID)
		for i, eventID := range eventIDs {
			delivery := mainQueueDelivery(t, before.Query, domain.ID(eventID))
			if delivery.Delivery.Status != domain.DeliveryStatusPending || delivery.Execution != nil || len(delivery.Attempts) != 0 {
				t.Fatalf("submit执行了排队请求: %+v", delivery)
			}
			if i == 0 && !delivery.Ready {
				t.Fatalf("第一条输入没有就绪: %+v", delivery)
			}
			if i > 0 {
				earlier := false
				for _, reason := range delivery.BlockedBy {
					earlier = earlier || reason.Code == core.BlockEarlierInput
				}
				if delivery.Ready || !earlier {
					t.Fatalf("后续输入没有等待更早请求: %+v", delivery)
				}
			}
		}
		ran, _ := p.call(t, directory, 0, "run", "--env-file", config)
		after, _ := p.call(t, directory, 0, "status", "--agent", agentID)
		mainToolsAssertFinished(t, after.Query, "回复：第三轮", 3)
		if len(after.Query.Deliveries) != 6 || after.Query.Agent.StateVersion != 6 || ran.Run.After.ExecutionAttempts != 6 || ran.Run.After.ActionAttempts != 3 {
			t.Fatalf("单次run未恰好完成所有排队输入: %+v %+v", after.Query, ran.Run)
		}
		requests := captured.all()
		if len(requests) != 3 {
			t.Fatalf("排队请求的模型调用次数错误: %d", len(requests))
		}
		var history []mainSQLiteMessage
		for i, message := range messages {
			input := append(append([]mainSQLiteMessage(nil), history...), mainSQLiteMessage{Role: "user", Content: message})
			mainSQLiteAssertMessages(t, requests[i].Messages, input)
			history = append(input, mainSQLiteMessage{Role: "assistant", Content: "回复：" + message})
		}
		mainSQLiteAssertMessages(t, after.Query.Agent.State["messages"], history)
		p.call(t, directory, 0, "run", "--env-file", filepath.Join(t.TempDir(), "missing.env"))
		unchanged, _ := p.call(t, directory, 0, "status", "--agent", agentID)
		mainQueueAssertUnchanged(t, after.Query, unchanged.Query)
		if len(captured.all()) != 3 {
			t.Fatal("完成后run重复调用了排队请求的模型")
		}
	})

	t.Run("failed_model_unblocks_next_request", func(t *testing.T) {
		directory := filepath.Join(t.TempDir(), "data")
		created, _ := p.call(t, directory, 0, "init", "--definition", "main")
		agentID := string(created.Agent.ID)
		var captured mainToolsModelCapture
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			request := mainToolsReadRequest(t, r)
			captured.append(request)
			if mainQueueUser(t, request) == "失败轮次" {
				w.WriteHeader(http.StatusBadGateway)
				return
			}
			mainToolsReply(t, w, "后续成功")
		}))
		defer server.Close()
		config := chatTestConfig(t, server.URL)
		p.call(t, directory, 0, "submit", "--agent", agentID, "--event-id", "failed", "--message", "失败轮次")
		p.call(t, directory, 0, "submit", "--agent", agentID, "--event-id", "next", "--message", "后续轮次")
		p.call(t, directory, 0, "run", "--env-file", config)
		after, _ := p.call(t, directory, 0, "status", "--agent", agentID)
		mainToolsAssertFinished(t, after.Query, "后续成功", 2)
		requests := captured.all()
		if len(requests) != 2 || len(requests[1].Messages) != 1 || mainQueueUser(t, requests[0]) != "失败轮次" || mainQueueUser(t, requests[1]) != "后续轮次" {
			t.Fatalf("模型失败阻塞后续请求或把失败轮次写入历史: %+v", requests)
		}
		failedActions := 0
		for _, action := range after.Query.Actions {
			if action.Action.Status == domain.ActionStatusFailed {
				failedActions++
			}
		}
		if failedActions != 1 || after.Query.Agent.State["request_event_id"] != "next" {
			t.Fatalf("模型失败记录丢失或最终请求错误: %+v", after.Query)
		}
		mainSQLiteAssertMessages(t, after.Query.Agent.State["messages"], []mainSQLiteMessage{
			{Role: "user", Content: "后续轮次"}, {Role: "assistant", Content: "后续成功"},
		})
	})

	t.Run("saved_model_failure_requires_config_for_queued_input", func(t *testing.T) {
		directory := filepath.Join(t.TempDir(), "data")
		created, _ := p.call(t, directory, 0, "init", "--definition", "main")
		agentID := string(created.Agent.ID)
		p.call(t, directory, 0, "submit", "--agent", agentID, "--event-id", "saved-failure", "--message", "已保存失败轮次")
		firstModel := mainToolsSQLiteCommitExecution(t, p, directory, created.Agent.ID, "saved-failure")
		p.call(t, directory, 0, "submit", "--agent", agentID, "--event-id", "queued-after-failure", "--message", "失败后继续")
		mainQueueSQLiteFailAction(t, directory, firstModel)
		before, _ := p.call(t, directory, 0, "status", "--agent", agentID)
		mainQueueAssertWaiting(t, before.Query, "queued-after-failure")
		if result := mainQueueDelivery(t, before.Query, firstModel.ResultEventID); !result.Ready {
			t.Fatalf("已保存模型失败结果不能越过排队输入: %+v", result)
		}
		var captured mainToolsModelCapture
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			request := mainToolsReadRequest(t, r)
			captured.append(request)
			mainToolsReply(t, w, "失败后完成")
		}))
		defer server.Close()
		config := chatTestConfig(t, server.URL)
		missingConfig := filepath.Join(t.TempDir(), "missing.env")
		_, stderr := p.call(t, directory, 1, "run", "--env-file", missingConfig)
		assertCommandError(t, stderr, "operation", 1)
		unchanged, _ := p.call(t, directory, 0, "status", "--agent", agentID)
		mainQueueAssertUnchanged(t, before.Query, unchanged.Query)
		if len(captured.all()) != 0 {
			t.Fatal("缺配置时调用了模型")
		}
		p.call(t, directory, 0, "run", "--env-file", config)
		after, _ := p.call(t, directory, 0, "status", "--agent", agentID)
		mainToolsAssertFinished(t, after.Query, "失败后完成", 2)
		if len(after.Query.Deliveries) != 4 || after.Query.Agent.StateVersion != 4 {
			t.Fatalf("已保存失败后的排队输入未恰好完成: %+v", after.Query)
		}
		requests := captured.all()
		if len(requests) != 1 || mainQueueUser(t, requests[0]) != "失败后继续" || len(requests[0].Messages) != 1 {
			t.Fatalf("恢复重复执行已失败模型或污染后续历史: %+v", requests)
		}
		mainSQLiteAssertMessages(t, after.Query.Agent.State["messages"], []mainSQLiteMessage{
			{Role: "user", Content: "失败后继续"}, {Role: "assistant", Content: "失败后完成"},
		})
		found := false
		for _, saved := range after.Query.Actions {
			if saved.Action.Request.ID == firstModel.Request.ID {
				found = true
				if saved.Action.Status != domain.ActionStatusFailed || saved.Action.AttemptCount != 1 ||
					saved.Action.ResultEventID != firstModel.ResultEventID ||
					saved.Action.Result == nil || saved.Action.Result.Error == nil || saved.Action.Result.Error.Message != "已保存的模型失败" {
					t.Fatalf("恢复改变或丢失已保存模型失败: %+v", saved.Action)
				}
				mainToolsAssertJSON(t, saved.Action.Request, firstModel.Request)
			}
		}
		if !found {
			t.Fatal("恢复丢失已保存失败action")
		}
		p.call(t, directory, 0, "run", "--env-file", missingConfig)
		idle, _ := p.call(t, directory, 0, "status", "--agent", agentID)
		mainQueueAssertUnchanged(t, after.Query, idle.Query)
		if len(captured.all()) != 1 {
			t.Fatal("完成后run重复调用了失败模型")
		}
	})

	t.Run("unknown_model_holds_inputs_without_blocking_other_agents", func(t *testing.T) {
		directory := filepath.Join(t.TempDir(), "data")
		created, _ := p.call(t, directory, 0, "init", "--definition", "main")
		agentID := string(created.Agent.ID)
		p.call(t, directory, 0, "submit", "--agent", agentID, "--event-id", "interrupted", "--message", "结果未知")
		receipt := mainSQLiteSaveAction(t, p, directory, created.Agent.ID, "interrupted", "interrupted")
		p.call(t, directory, 0, "submit", "--agent", agentID, "--event-id", "queued-second", "--message", "等待第二轮")
		p.call(t, directory, 0, "submit", "--agent", agentID, "--event-id", "queued-third", "--message", "等待第三轮")
		before, _ := p.call(t, directory, 0, "status", "--agent", agentID)
		mainQueueAssertWaiting(t, before.Query, "queued-second", "queued-third")
		if len(before.Query.Actions) != 1 || before.Query.Actions[0].Action.Request.ID != receipt.ActionID ||
			before.Query.Actions[0].Action.Status != domain.ActionStatusUnknown || before.Query.Actions[0].Action.AttemptCount != 1 || before.Query.Actions[0].Ready {
			t.Fatalf("中断模型未保持unknown: %+v", before.Query.Actions)
		}
		other, _ := p.call(t, directory, 0, "init")
		otherID := string(other.Agent.ID)
		p.call(t, directory, 0, "submit", "--agent", otherID, "--event-id", "other-echo", "--message", "独立推进")
		missingConfig := filepath.Join(t.TempDir(), "missing.env")
		p.call(t, directory, 0, "run", "--env-file", missingConfig)
		echoStatus, _ := p.call(t, directory, 0, "status", "--agent", otherID)
		if len(echoStatus.Query.Deliveries) != 2 || len(echoStatus.Query.Actions) != 1 || echoStatus.Query.Agent.State["result"] != "独立推进" {
			t.Fatalf("等待unknown模型阻塞了独立echo: %+v", echoStatus.Query)
		}
		after, _ := p.call(t, directory, 0, "status", "--agent", agentID)
		mainQueueAssertWaiting(t, after.Query, "queued-second", "queued-third")
		mainQueueAssertUnchanged(t, before.Query, after.Query)
		var captured mainToolsModelCapture
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			request := mainToolsReadRequest(t, r)
			captured.append(request)
			mainToolsReply(t, w, "独立main成功")
		}))
		defer server.Close()
		config := chatTestConfig(t, server.URL)
		independent, _ := p.call(t, directory, 0, "init", "--definition", "main")
		independentID := string(independent.Agent.ID)
		p.call(t, directory, 0, "submit", "--agent", independentID, "--event-id", "independent-main", "--message", "独立main请求")
		p.call(t, directory, 0, "run", "--env-file", config)
		independentStatus, _ := p.call(t, directory, 0, "status", "--agent", independentID)
		mainToolsAssertFinished(t, independentStatus.Query, "独立main成功", 1)
		if requests := captured.all(); len(requests) != 1 || mainQueueUser(t, requests[0]) != "独立main请求" {
			t.Fatalf("模型unknown后误调用了排队请求: %+v", requests)
		}
		for i := 0; i < 2; i++ {
			p.call(t, directory, 0, "run", "--env-file", missingConfig)
		}
		unchanged, _ := p.call(t, directory, 0, "status", "--agent", agentID)
		mainQueueAssertWaiting(t, unchanged.Query, "queued-second", "queued-third")
		mainQueueAssertUnchanged(t, before.Query, unchanged.Query)
		if len(captured.all()) != 1 {
			t.Fatal("重复run重新调用了unknown模型")
		}
	})

	t.Run("resume_saved_results_before_queued_inputs", func(t *testing.T) {
		for _, boundary := range []string{"saved_model_calls", "pending_tool", "saved_tool_result", "saved_final_text"} {
			t.Run(boundary, func(t *testing.T) {
				directory := filepath.Join(t.TempDir(), "data")
				created, _ := p.call(t, directory, 0, "init", "--definition", "main")
				agentID := string(created.Agent.ID)
				p.call(t, directory, 0, "submit", "--agent", agentID, "--event-id", "resume-first", "--message", "恢复首轮")
				firstModel := mainToolsSQLiteCommitExecution(t, p, directory, created.Agent.ID, "resume-first")
				// 后续输入先入队，action.result随后入队，恢复仍应优先完成当前轮。
				p.call(t, directory, 0, "submit", "--agent", agentID, "--event-id", "resume-second", "--message", "恢复第二轮")
				p.call(t, directory, 0, "submit", "--agent", agentID, "--event-id", "resume-third", "--message", "恢复第三轮")
				calls := []map[string]any{mainToolsCall("resume-status", "{}")}
				mainToolsSQLiteCompleteAction(t, directory, firstModel, map[string]any{"message": "", "tool_calls": calls})
				var toolAction, finalModel domain.ActionRecord
				if boundary != "saved_model_calls" {
					toolAction = mainToolsSQLiteCommitExecution(t, p, directory, created.Agent.ID, firstModel.ResultEventID)
				}
				if boundary == "saved_tool_result" || boundary == "saved_final_text" {
					mainToolsSQLiteCompleteAction(t, directory, toolAction, map[string]any{
						"id": agentID, "definition": map[string]any{"id": "main", "version": "1"}, "status": "active", "state_version": 2,
					})
				}
				if boundary == "saved_final_text" {
					finalModel = mainToolsSQLiteCommitExecution(t, p, directory, created.Agent.ID, toolAction.ResultEventID)
					mainToolsSQLiteCompleteAction(t, directory, finalModel, map[string]any{"message": "回复：恢复首轮"})
				}
				before, _ := p.call(t, directory, 0, "status", "--agent", agentID)
				mainQueueAssertWaiting(t, before.Query, "resume-second", "resume-third")
				for _, delivery := range before.Query.Deliveries {
					if delivery.Event.Type == "action.result" && delivery.Delivery.Status == domain.DeliveryStatusPending && !delivery.Ready {
						t.Fatalf("当前轮结果没有越过等待输入: %+v", delivery)
					}
				}
				var captured mainToolsModelCapture
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					request := mainToolsReadRequest(t, r)
					captured.append(request)
					mainToolsReply(t, w, "回复："+mainQueueUser(t, request))
				}))
				defer server.Close()
				config := chatTestConfig(t, server.URL)
				missingConfig := filepath.Join(t.TempDir(), "missing.env")
				// 即使当前只有已保存的最终回复可执行，后续输入也会需要模型配置。
				_, stderr := p.call(t, directory, 1, "run", "--env-file", missingConfig)
				assertCommandError(t, stderr, "operation", 1)
				unchanged, _ := p.call(t, directory, 0, "status", "--agent", agentID)
				mainQueueAssertUnchanged(t, before.Query, unchanged.Query)
				if len(captured.all()) != 0 {
					t.Fatal("缺模型配置时推进了排队或恢复")
				}
				p.call(t, directory, 0, "run", "--env-file", config)
				after, _ := p.call(t, directory, 0, "status", "--agent", agentID)
				mainToolsAssertFinished(t, after.Query, "回复：恢复第三轮", 5)
				if len(after.Query.Deliveries) != 8 || after.Query.Agent.StateVersion != 8 {
					t.Fatalf("恢复未恰好处理当前轮和两条排队输入: %+v", after.Query)
				}
				requests := captured.all()
				wantUsers := []string{"恢复首轮", "恢复第二轮", "恢复第三轮"}
				if boundary == "saved_final_text" {
					wantUsers = wantUsers[1:]
				}
				if len(requests) != len(wantUsers) {
					t.Fatalf("恢复重复调用已保存模型或丢失排队请求: got=%d want=%d", len(requests), len(wantUsers))
				}
				for i, want := range wantUsers {
					if mainQueueUser(t, requests[i]) != want {
						t.Fatalf("恢复期间请求顺序错误: %+v", requests)
					}
				}
				var queuedInput []map[string]any
				for _, request := range requests {
					if mainQueueUser(t, request) == "恢复第二轮" {
						queuedInput = request.Messages
					}
				}
				if len(queuedInput) != 5 || queuedInput[0]["content"] != "恢复首轮" || queuedInput[3]["content"] != "回复：恢复首轮" {
					t.Fatalf("排队输入未带上恢复完成的工具轮次: %+v", queuedInput)
				}
				mainToolsAssertJSON(t, queuedInput[1]["tool_calls"], calls)
				if output := mainToolsOutput(t, queuedInput[2], "resume-status"); output["id"] != agentID {
					t.Fatalf("恢复丢失已保存工具结果: %+v", output)
				}
				for _, saved := range after.Query.Actions {
					for _, original := range []domain.ActionRecord{firstModel, toolAction, finalModel} {
						if original.Request.ID != "" && original.Request.ID == saved.Action.Request.ID {
							if original.ResultEventID != saved.Action.ResultEventID || saved.Action.AttemptCount != 1 {
								t.Fatalf("排队恢复改变了已保存action身份、输入或尝试次数: %+v", saved.Action)
							}
							mainToolsAssertJSON(t, saved.Action.Request, original.Request)
						}
					}
				}
				p.call(t, directory, 0, "run", "--env-file", missingConfig)
				idle, _ := p.call(t, directory, 0, "status", "--agent", agentID)
				mainQueueAssertUnchanged(t, after.Query, idle.Query)
				if len(captured.all()) != len(wantUsers) {
					t.Fatal("完成后的run重复模型调用")
				}
			})
		}
	})
}
