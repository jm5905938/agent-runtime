package main

import (
	"agent-runtime/core"
	"agent-runtime/domain"
	"agent-runtime/model"
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func subagentModelCall(name, id string, arguments map[string]any) map[string]any {
	encoded, _ := json.Marshal(arguments)
	return map[string]any{"id": id, "type": "function", "function": map[string]any{"name": name, "arguments": string(encoded)}}
}

func subagentTestModel(t *testing.T, childCalls ...map[string]any) (*httptest.Server, *mainToolsModelCapture, *atomic.Int32) {
	t.Helper()
	captured := &mainToolsModelCapture{}
	children := &atomic.Int32{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		request := mainToolsReadRequest(t, r)
		captured.append(request)
		if len(request.Messages) > 0 && request.Messages[0]["role"] == "system" && strings.Contains(request.Messages[0]["content"].(string), "你是一次性subagent") {
			if len(request.Tools) != 8 || !reflect.DeepEqual(request.Tools, captured.all()[0].Tools) {
				t.Errorf("child未共用main工具: %+v", request.Tools)
			}
			children.Add(1)
			encoded, _ := json.Marshal(request.Messages)
			if strings.Contains(string(encoded), "parent-private-message") || !strings.Contains(string(encoded), "explicit-child-context") || !strings.Contains(string(encoded), "child-task") {
				t.Errorf("child没有隔离父历史或丢失显式上下文: %s", encoded)
			}
			if len(childCalls) > 0 && request.Messages[len(request.Messages)-1]["role"] == "user" {
				mainToolsReply(t, w, "", childCalls...)
			} else {
				mainToolsReply(t, w, "child-result")
			}
			return
		}
		last := request.Messages[len(request.Messages)-1]
		if last["role"] == "user" {
			mainToolsReply(t, w, "", subagentModelCall("spawn_subagent", "spawn-child", map[string]any{
				"message": "child-task", "context": "explicit-child-context", "name": "测试child",
			}))
			return
		}
		output := mainToolsOutput(t, last, last["tool_call_id"].(string))
		switch last["tool_call_id"] {
		case "spawn-child":
			taskID, ok := output["task_id"].(string)
			childID, childOK := output["child_id"].(string)
			if !ok || taskID == "" || !childOK || childID == "" {
				t.Errorf("spawn结果缺少任务关联: %+v", output)
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			mainToolsReply(t, w, "", subagentModelCall("wait_subagent", "wait-child", map[string]any{"task_id": taskID}))
		case "wait-child":
			if output["task_status"] != "succeeded" && output["task_status"] != "cancelled" && output["task_status"] != "failed" {
				t.Errorf("wait没有返回task终态: %+v", output)
			}
			mainToolsReply(t, w, "main-result")
		default:
			t.Errorf("意外的工具结果: %+v", last)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	return server, captured, children
}

func subagentCLIProcess(t *testing.T) sqliteCommandProcess {
	t.Helper()
	pythonArgs := chatTestPython(t)
	binary := filepath.Join(t.TempDir(), "agent-runtime")
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	if output, err := exec.CommandContext(ctx, "go", "build", "-o", binary, ".").CombinedOutput(); err != nil {
		t.Fatalf("构建subagent测试cli: %v\n%s", err, output)
	}
	return sqliteCommandProcess{binary: binary, python: pythonArgs[1], source: pythonArgs[3]}
}

func TestSubagentSQLiteSharedFileAndClockTools(t *testing.T) {
	p := subagentCLIProcess(t)
	project := t.TempDir()
	source := filepath.Join(project, "python", "src")
	if err := os.MkdirAll(source, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.CopyFS(source, os.DirFS(p.source)); err != nil {
		t.Fatal(err)
	}
	p.source = source
	t.Chdir(project)
	server, captured, _ := subagentTestModel(t,
		subagentModelCall("write_file", "write-child", map[string]any{"path": "child-result.txt", "content": "child文件结果"}),
		subagentModelCall("read_file", "read-child", map[string]any{"path": "python/src/child-result.txt"}),
		subagentModelCall("get_current_time", "time-child", map[string]any{}),
		subagentModelCall("get_current_date", "date-child", map[string]any{}),
	)
	defer server.Close()
	config := chatTestConfig(t, server.URL)
	directory := t.TempDir()
	parent, _ := p.call(t, directory, 0, "init", "--definition", "main")
	subagentCLIChat(t, p, directory, parent.Agent.ID, config)
	subagentAssertFinished(t, p, directory, parent.Agent.ID, domain.SubagentStatusSucceeded)
	content, err := os.ReadFile(filepath.Join(source, "child-result.txt"))
	if err != nil || string(content) != "child文件结果" {
		t.Fatalf("child文件未落盘: %q %v", content, err)
	}
	var messages []map[string]any
	for _, request := range captured.all() {
		if request.Messages[0]["role"] == "system" && strings.Contains(request.Messages[0]["content"].(string), "你是一次性subagent") {
			messages = request.Messages
		}
	}
	if len(messages) != 7 {
		t.Fatalf("child工具轨迹不完整: %+v", messages)
	}
	if output := mainToolsOutput(t, messages[3], "write-child"); output["bytes"] != float64(len(content)) {
		t.Fatalf("child写入结果错误: %+v", output)
	}
	if output := mainToolsOutput(t, messages[4], "read-child"); output["content"] != string(content) {
		t.Fatalf("child读取结果错误: %+v", output)
	}
	if output := mainToolsOutput(t, messages[5], "time-child"); output["datetime"] == nil {
		t.Fatalf("child时间结果错误: %+v", output)
	}
	if output := mainToolsOutput(t, messages[6], "date-child"); output["date"] == nil {
		t.Fatalf("child日期结果错误: %+v", output)
	}
}

func subagentCLIChat(t *testing.T, p sqliteCommandProcess, directory string, agentID domain.ID, config string) chatResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, p.binary, "chat", "--data-dir", directory, "--agent", string(agentID), "--message", "parent-private-message", "--env-file", config,
		"--json", "--python", p.python, "--python-source", p.source)
	var stdout, stderr bytes.Buffer
	command.Stdout, command.Stderr = &stdout, &stderr
	if err := command.Run(); err != nil || stderr.Len() > 0 {
		t.Fatalf("subagent对话失败: %v stdout=%s stderr=%s", err, &stdout, &stderr)
	}
	var result chatResult
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil || result.Status != "succeeded" || result.Result != "main-result" {
		t.Fatalf("subagent对话未完成: %s %v", &stdout, err)
	}
	return result
}

func subagentAssertFinished(t *testing.T, p sqliteCommandProcess, directory string, parentID domain.ID, status domain.SubagentStatus) *core.AgentQuery {
	t.Helper()
	result, _ := p.call(t, directory, 0, "status", "--agent", string(parentID))
	query := result.Query
	if query == nil || query.Agent.State["request_status"] != "succeeded" || query.Agent.State["result"] != "main-result" || len(query.Tasks) != 1 {
		t.Fatalf("parent或任务未完成: %+v", query)
	}
	task := query.Tasks[0]
	if task.ParentAgentID != parentID || task.ChildAgentID == "" || task.InitialEventID == "" || task.Result == nil || task.Result.Status != status {
		t.Fatalf("任务关联或终态未持久化: %+v", task)
	}
	if status == domain.SubagentStatusSucceeded && task.Result.Output["message"] != "child-result" {
		t.Fatalf("child最终文本未保存在task结果中: %+v", task.Result)
	}
	child, _ := p.call(t, directory, 0, "status", "--agent", string(task.ChildAgentID))
	if child.Query.Agent.Definition != (domain.DefinitionRef{ID: "subagent", Version: "1"}) || child.Query.Agent.Status != domain.AgentStatusTerminated {
		t.Fatalf("child完成后未结束: %+v", child.Query.Agent)
	}
	for _, action := range query.Actions {
		if action.Action.Status != domain.ActionStatusSucceeded || action.Action.AttemptCount != 1 {
			t.Fatalf("parent留下重复或未完成action: %+v", action.Action)
		}
	}
	return query
}

func TestSubagentSQLiteChatAcrossProcessesAndScope(t *testing.T) {
	p := subagentCLIProcess(t)
	server, captured, children := subagentTestModel(t)
	defer server.Close()
	config := chatTestConfig(t, server.URL)
	directory := t.TempDir()
	main, _ := p.call(t, directory, 0, "init", "--definition", "main")
	unrelated, _ := p.call(t, directory, 0, "init", "--definition", "main", "--name", "无关main")
	p.call(t, directory, 0, "submit", "--agent", string(unrelated.Agent.ID), "--event-id", "unrelated-event", "--message", "不应执行")
	before, _ := p.call(t, directory, 0, "status", "--agent", string(unrelated.Agent.ID))
	subagentCLIChat(t, p, directory, main.Agent.ID, config)
	query := subagentAssertFinished(t, p, directory, main.Agent.ID, domain.SubagentStatusSucceeded)
	transcript := newTUIModel(nil, *query, directory).history.View()
	if strings.Contains(transcript, "subagent任务") || !strings.Contains(transcript, "tool.spawn_subagent") || !strings.Contains(transcript, "tool.wait_subagent") || !strings.Contains(transcript, "main-result") {
		t.Fatalf("重开后tui保留了已结束任务或丢失历史: %s", transcript)
	}
	if children.Load() != 1 || len(captured.all()) != 4 || query.Tasks[0].CompletionExecutionID == "" {
		t.Fatalf("spawn、child、wait闭环调用次数或完成来源错误: children=%d model=%d task=%+v", children.Load(), len(captured.all()), query.Tasks[0])
	}
	for _, action := range query.Actions {
		if action.Action.Request.Type == "tool.wait_subagent" && action.Action.Result.Output["task_status"] != "succeeded" {
			t.Fatalf("wait未回传持久化终态: %+v", action.Action)
		}
	}
	after, _ := p.call(t, directory, 0, "status", "--agent", string(unrelated.Agent.ID))
	if !reflect.DeepEqual(before.Query, after.Query) {
		t.Fatalf("对话推进了无关agent: before=%+v after=%+v", before.Query, after.Query)
	}
	_, failure := p.call(t, directory, 1, "cancel", "--agent", string(unrelated.Agent.ID), "--task", string(query.Tasks[0].ID))
	assertCommandError(t, failure, "conflict", 1)
	options, err := parseCommand(append([]string{"chat", "--data-dir", directory, "--agent", string(main.Agent.ID), "--env-file", filepath.Join(t.TempDir(), "missing.env")}, "--python", p.python, "--python-source", p.source))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := drainConversation(context.Background(), options, openCommandBackend); err != nil || len(captured.all()) != 4 {
		t.Fatalf("已完成tree重启重复执行或预检了无关main: %v model=%d", err, len(captured.all()))
	}
}

func subagentFreezeParent(t *testing.T, p sqliteCommandProcess, directory, config string) (domain.ID, domain.SubagentTask) {
	t.Helper()
	options, err := parseCommand([]string{"chat", "--data-dir", directory, "--env-file", config, "--python", p.python, "--python-source", p.source})
	if err != nil {
		t.Fatal(err)
	}
	var parentID domain.ID
	var task domain.SubagentTask
	err = withConversation(context.Background(), options, openCommandBackend, true, func(runtime *core.Runtime, agent core.AgentSnapshot, _ func(context.Context) error) error {
		parentID = agent.ID
		if _, err := runtime.SubmitContext(context.Background(), agent.ID, domain.NewEvent("main.request", map[string]any{"message": "parent-private-message"})); err != nil {
			return err
		}
		if err := runtime.RunAgentUntilIdleContext(context.Background(), agent.ID); err != nil {
			return err
		}
		query, err := runtime.QueryAgentContext(context.Background(), agent.ID)
		if err != nil {
			return err
		}
		if len(query.Tasks) != 1 || query.Agent.State["waiting_action_type"] != "tool.wait_subagent" {
			t.Fatalf("parent没有停在未就绪的wait: %+v", query)
		}
		task = query.Tasks[0]
		for _, action := range query.Actions {
			if action.Action.Request.Type == "tool.wait_subagent" && (action.Ready || action.Action.AttemptCount != 0 || action.Action.Status != domain.ActionStatusPending) {
				t.Fatalf("未就绪wait消耗了attempt: %+v", action)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return parentID, task
}

func TestSubagentSQLiteResumeAndCancelAcrossProcesses(t *testing.T) {
	p := subagentCLIProcess(t)
	for _, cancelTask := range []bool{false, true} {
		name := "resume_pending_child"
		if cancelTask {
			name = "cancel_before_child_start"
		}
		t.Run(name, func(t *testing.T) {
			server, captured, children := subagentTestModel(t)
			defer server.Close()
			config := chatTestConfig(t, server.URL)
			directory := t.TempDir()
			parentID, task := subagentFreezeParent(t, p, directory, config)
			if children.Load() != 0 || len(captured.all()) != 2 {
				t.Fatalf("parent专用调度推进了child: children=%d requests=%d", children.Load(), len(captured.all()))
			}
			before, _ := p.call(t, directory, 0, "status", "--agent", string(task.ChildAgentID))
			p.call(t, directory, 1, "resume", "--agent", string(parentID), "--retry", "--env-file", filepath.Join(t.TempDir(), "missing.env"))
			after, _ := p.call(t, directory, 0, "status", "--agent", string(task.ChildAgentID))
			if !reflect.DeepEqual(before.Query, after.Query) || children.Load() != 0 {
				t.Fatal("缺少模型配置时child输入被领取")
			}
			if cancelTask {
				p.call(t, directory, 0, "cancel", "--agent", string(parentID), "--task", string(task.ID))
				status, _ := p.call(t, directory, 0, "status", "--agent", string(parentID))
				if len(status.Query.Tasks) != 1 || !status.Query.Tasks[0].CancelRequested || status.Query.Tasks[0].Result == nil || status.Query.Tasks[0].Result.Status != domain.SubagentStatusCancelled || children.Load() != 0 {
					t.Fatalf("cancel没有持久化取消终态或执行了child模型: %+v", status.Query)
				}
			}
			p.call(t, directory, 0, "resume", "--agent", string(parentID), "--retry", "--env-file", config)
			wantStatus, wantChildren, wantModels := domain.SubagentStatusSucceeded, int32(1), 4
			if cancelTask {
				wantStatus, wantChildren, wantModels = domain.SubagentStatusCancelled, 0, 3
			}
			subagentAssertFinished(t, p, directory, parentID, wantStatus)
			if children.Load() != wantChildren || len(captured.all()) != wantModels {
				t.Fatalf("恢复或取消重做了模型: children=%d requests=%d", children.Load(), len(captured.all()))
			}
		})
	}
}

func TestSubagentSQLiteResumeInterruptedChild(t *testing.T) {
	p := subagentCLIProcess(t)
	for _, mode := range []string{"execution", "cancel_execution", "retry_unknown", "abandon_unknown", "cancel_unknown"} {
		t.Run(mode, func(t *testing.T) {
			server, captured, children := subagentTestModel(t)
			defer server.Close()
			config := chatTestConfig(t, server.URL)
			directory := t.TempDir()
			parentID, task := subagentFreezeParent(t, p, directory, config)
			decision := "--retry"
			var oldActionID domain.ID
			if mode == "execution" || mode == "cancel_execution" {
				mainResolutionSQLiteInterruptDecision(t, directory, domain.DeliveryKey{AgentID: task.ChildAgentID, EventID: task.InitialEventID})
				if mode == "cancel_execution" {
					p.call(t, directory, 0, "cancel", "--agent", string(parentID), "--task", string(task.ID))
				}
			} else {
				receipt := mainSQLiteSaveAction(t, p, directory, task.ChildAgentID, task.InitialEventID, "interrupted")
				oldActionID = receipt.ActionID
				unknown, _ := p.call(t, directory, 0, "status", "--agent", string(task.ChildAgentID))
				if waitingUnknownConversationAction(*unknown.Query) == nil {
					t.Fatalf("child未知模型调用未恢复: %+v", unknown.Query)
				}
				if mode == "abandon_unknown" || mode == "cancel_unknown" {
					decision = "--abandon"
				}
				if mode == "cancel_unknown" {
					p.call(t, directory, 0, "cancel", "--agent", string(parentID), "--task", string(task.ID))
					pending, _ := p.call(t, directory, 0, "status", "--agent", string(parentID))
					if !pending.Query.Tasks[0].CancelRequested || pending.Query.Tasks[0].Result != nil {
						t.Fatalf("未知调用未确认就发布了取消终态: %+v", pending.Query.Tasks[0])
					}
				}
			}
			p.call(t, directory, 0, "resume", "--agent", string(parentID), decision, "--reason", "处理child中断", "--env-file", config)
			wantStatus, wantChildren, wantModels := domain.SubagentStatusSucceeded, int32(1), 4
			if mode == "abandon_unknown" {
				wantStatus, wantChildren, wantModels = domain.SubagentStatusFailed, 0, 3
			} else if mode == "cancel_unknown" || mode == "cancel_execution" {
				wantStatus, wantChildren, wantModels = domain.SubagentStatusCancelled, 0, 3
			}
			subagentAssertFinished(t, p, directory, parentID, wantStatus)
			if children.Load() != wantChildren || len(captured.all()) != wantModels {
				t.Fatalf("child恢复模型调用数错误: children=%d requests=%d", children.Load(), len(captured.all()))
			}
			if oldActionID != "" {
				child, _ := p.call(t, directory, 0, "status", "--agent", string(task.ChildAgentID))
				old := mainResolutionAction(t, child.Query, oldActionID)
				if old.Action.Status != domain.ActionStatusUnknown || old.Action.Result != nil || old.Resolution == nil || old.Resolution.Reason != "处理child中断" {
					t.Fatalf("child人工处理改写了旧unknown尝试或丢失决定: %+v", old)
				}
			}
		})
	}
}

func TestCancelCommandValidation(t *testing.T) {
	for _, args := range [][]string{{"cancel"}, {"cancel", "--task", " "}, {"cancel", "--task", string([]byte{0xff})}, {"cancel", "--task", "task", "--message", "hello"}, {"cancel", "--task", "task", "--agent", " "}} {
		if _, err := parseCommand(args); err == nil {
			t.Fatalf("cancel接受了无效参数: %q", args)
		}
	}
	options, err := parseCommand([]string{"cancel", "--task", "task", "--agent", "main", "--json"})
	if err != nil || options.taskID != "task" || options.request.AgentID != "main" || options.dataDir == "" || !options.asJSON {
		t.Fatalf("cancel参数解析错误: %+v %v", options, err)
	}
	if requestedJSON([]string{"cancel", "--task", "--json"}) {
		t.Fatal("task值被当作json标志")
	}
	var stdout, stderr bytes.Buffer
	if code := runCommandWithBackend(context.Background(), []string{"cancel", "--task", "task", "--json"}, &stdout, &stderr, nil); code != 1 || stdout.Len() != 0 {
		t.Fatalf("cancel没有正确报告不可用后端: %d %s %s", code, &stdout, &stderr)
	}
	assertCommandError(t, stderr.Bytes(), "backend_unavailable", 1)
}

func TestTUISessionSubagentReadinessAndUnknownResolution(t *testing.T) {
	p := subagentCLIProcess(t)
	for _, interrupted := range []bool{false, true} {
		name := "pending_child"
		if interrupted {
			name = "unknown_child"
		}
		t.Run(name, func(t *testing.T) {
			server, captured, children := subagentTestModel(t)
			defer server.Close()
			config := chatTestConfig(t, server.URL)
			directory := t.TempDir()
			parentID, task := subagentFreezeParent(t, p, directory, config)
			if interrupted {
				mainSQLiteSaveAction(t, p, directory, task.ChildAgentID, task.InitialEventID, "interrupted")
			}
			options, err := parseCommand([]string{"tui", "--data-dir", directory, "--agent", string(parentID), "--env-file", config, "--python", p.python, "--python-source", p.source})
			if err != nil {
				t.Fatal(err)
			}
			err = withConversation(context.Background(), options, openCommandBackend, false, func(runtime *core.Runtime, agent core.AgentSnapshot, prepare func(context.Context) error) error {
				session := newTUISession(context.Background(), runtime, agent.ID, prepare)
				defer session.close()
				query, err := session.query()
				if err != nil {
					return err
				}
				ready, unknown, err := session.treeState()
				if err != nil {
					return err
				}
				if tuiReady(query) || interrupted && unknown == nil || !interrupted && (!ready || unknown != nil) {
					t.Fatalf("终端界面未识别child工作或unknown: ready=%t unknown=%+v query=%+v", ready, unknown, query)
				}
				view := newTUIModel(session, query, directory)
				view.treeReady, view.unknown = ready, unknown
				if interrupted {
					if !strings.Contains(view.View(), "调用结果未知") || session.run(context.Background(), true, "") == nil {
						t.Fatal("终端界面没有提示child未知调用或未经决定就继续")
					}
					return session.run(context.Background(), true, core.ResolutionRetry)
				}
				if cmd := view.start(false, ""); cmd == nil {
					t.Fatal("parent等待wait时界面没有启动child工作")
				} else if msg, ok := cmd().(tuiRunMsg); !ok || msg.err != nil {
					t.Fatalf("终端界面tree运行失败: %+v", msg)
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			subagentAssertFinished(t, p, directory, parentID, domain.SubagentStatusSucceeded)
			if children.Load() != 1 || len(captured.all()) != 4 {
				t.Fatalf("终端恢复重复调用模型: children=%d requests=%d", children.Load(), len(captured.all()))
			}
		})
	}
}

func TestSubagentSQLiteCancelExhaustedUnknownTool(t *testing.T) {
	p := subagentCLIProcess(t)
	server, captured, children := subagentTestModel(t, mainToolsCall("child-status", `{}`))
	defer server.Close()
	config := chatTestConfig(t, server.URL)
	directory := t.TempDir()
	parentID, task := subagentFreezeParent(t, p, directory, config)
	childModel := mainToolsSQLiteCommitExecution(t, p, directory, task.ChildAgentID, task.InitialEventID)
	modelConfig, err := model.LoadConfig(config)
	if err != nil {
		t.Fatal(err)
	}
	handler, err := model.NewHandler(modelConfig)
	if err != nil {
		t.Fatal(err)
	}
	modelResult, err := handler.Execute(childModel.Request)
	if err != nil {
		t.Fatal(err)
	}
	mainToolsSQLiteCompleteAction(t, directory, childModel, modelResult)
	tool := mainToolsSQLiteCommitExecution(t, p, directory, task.ChildAgentID, childModel.ResultEventID)
	if tool.Request.Type != agentStatusActionType || tool.RecoveryPolicy != domain.RecoveryPolicySafeRetry || tool.MaxAttempts != 3 {
		t.Fatalf("child工具fixture丢失恢复策略: %+v", tool)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	handle, err := openCommandBackend(ctx, directory)
	if err != nil {
		t.Fatal(err)
	}
	session, err := handle.Store.OpenSession(ctx)
	if err != nil {
		handle.Close()
		t.Fatal(err)
	}
	if _, err := session.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	for i := uint64(0); i < tool.MaxAttempts; i++ {
		claim, err := session.ClaimAction(ctx, tool.Request.ID)
		if err != nil {
			t.Fatal(err)
		}
		if err := session.RecordActionUnknown(ctx, claim.Token, domain.Failure{Kind: domain.ErrorKindUnknown, Message: "测试child工具结果未知"}); err != nil {
			t.Fatal(err)
		}
	}
	if err := session.Close(ctx); err != nil {
		t.Fatal(err)
	}
	if err := handle.Close(); err != nil {
		t.Fatal(err)
	}
	before, _ := p.call(t, directory, 0, "status", "--agent", string(task.ChildAgentID))
	if waitingUnknownConversationAction(*before.Query) != nil {
		t.Fatal("未取消的safe_retry工具被当作人工模型恢复")
	}
	p.call(t, directory, 0, "cancel", "--agent", string(parentID), "--task", string(task.ID))
	parent, _ := p.call(t, directory, 0, "status", "--agent", string(parentID))
	if !parent.Query.Tasks[0].CancelRequested || parent.Query.Tasks[0].Result != nil {
		t.Fatalf("unknown工具未确认就发布取消终态: %+v", parent.Query.Tasks[0])
	}
	child, _ := p.call(t, directory, 0, "status", "--agent", string(task.ChildAgentID))
	unknown := waitingUnknownConversationAction(*child.Query)
	if unknown == nil || unknown.Action.Request.ID != tool.Request.ID || unknown.Action.AttemptCount != tool.MaxAttempts || unknown.Ready {
		t.Fatalf("取消中的耗尽unknown工具没有人工入口: %+v", child.Query)
	}
	p.call(t, directory, 0, "resume", "--agent", string(parentID), "--abandon", "--reason", "放弃child未知工具结果", "--env-file", config)
	subagentAssertFinished(t, p, directory, parentID, domain.SubagentStatusCancelled)
	after, _ := p.call(t, directory, 0, "status", "--agent", string(task.ChildAgentID))
	old := mainResolutionAction(t, after.Query, tool.Request.ID)
	if old.Action.Status != domain.ActionStatusUnknown || old.Action.Result != nil || old.Action.AttemptCount != 3 || old.Resolution == nil || old.Resolution.Decision != core.ResolutionAbandon {
		t.Fatalf("人工放弃改写了工具unknown尝试或丢失决定: %+v", old)
	}
	mainToolsAssertJSON(t, old.Action.Request, tool.Request)
	if children.Load() != 1 || len(captured.all()) != 4 {
		t.Fatalf("取消耗尽工具时重新调用child模型或丢失parent续轮: children=%d requests=%d", children.Load(), len(captured.all()))
	}
}

func TestTUIUnknownChildAllowsReadySibling(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	view := newTUIModel(&tuiSession{ctx: ctx}, core.AgentQuery{}, "项目")
	view.unknown = &core.ActionQuery{Action: domain.ActionRecord{AgentID: "child", Request: domain.Action{Type: "model.generate"}, Status: domain.ActionStatusUnknown}}
	if view.start(false, "") != nil {
		t.Fatal("没有ready工作时启动了unknown调用")
	}
	view.treeReady = true
	if view.start(false, "") == nil || !view.running {
		t.Fatal("unknown child阻止了ready sibling")
	}
	view.cancel()
}

func TestCancelledUnknownActionOnlyOffersAbandon(t *testing.T) {
	query := core.AgentQuery{Agent: core.AgentSnapshot{ID: "child", State: map[string]any{"request_status": "waiting", "waiting_action_id": "tool"}},
		Tasks: []domain.SubagentTask{{ChildAgentID: "child", CancelRequested: true}},
		Actions: []core.ActionQuery{{Action: domain.ActionRecord{AgentID: "child", Request: domain.Action{ID: "tool", Type: agentStatusActionType},
			Status: domain.ActionStatusUnknown, RecoveryPolicy: domain.RecoveryPolicySafeRetry}}}}
	unknown := waitingUnknownConversationAction(query)
	if unknown == nil || !cancelledUnknownAction(query, unknown) {
		t.Fatal("取消中的unknown工具没有人工放弃入口")
	}
	var output bytes.Buffer
	decision, exit, err := chooseConversationResolution(context.Background(), newConversationInput(strings.NewReader("1\n2\n")), &output, true)
	if err != nil || exit || decision != core.ResolutionAbandon || strings.Contains(output.String(), "可能再次计费") {
		t.Fatalf("取消任务允许重试或没有给出放弃选择: %s %s %t %v", &output, decision, exit, err)
	}
	view := newTUIModel(nil, query, "项目")
	if view.resume(core.ResolutionRetry) != nil || !strings.Contains(view.notice, "请放弃未知结果") {
		t.Fatal("终端界面允许重试已经取消的任务")
	}
}
