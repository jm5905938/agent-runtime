package cli

import (
	"agent-runtime/core"
	"agent-runtime/domain"
	"encoding/json"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
)

func TestResolveRequestValidation(t *testing.T) {
	valid := Request{Command: "resolve", ActionID: "model-action", Decision: core.ResolutionRetry, Reason: "用户选择重试"}
	for _, decision := range []core.ResolutionDecision{core.ResolutionRetry, core.ResolutionAbandon} {
		request := valid
		request.Decision = decision
		request.Reason = strings.Repeat("字", 1024)
		if err := request.Validate(); err != nil {
			t.Fatalf("有效resolve参数被拒绝: %+v %v", request, err)
		}
	}
	for name, mutate := range map[string]func(*Request){
		"missing_action":    func(request *Request) { request.ActionID = "" },
		"blank_action":      func(request *Request) { request.ActionID = " \n" },
		"bad_action_utf8":   func(request *Request) { request.ActionID = domain.ID(string([]byte{0xff})) },
		"missing_decision":  func(request *Request) { request.Decision = "" },
		"bad_decision":      func(request *Request) { request.Decision = "reply" },
		"bad_decision_utf8": func(request *Request) { request.Decision = core.ResolutionDecision(string([]byte{0xff})) },
		"missing_reason":    func(request *Request) { request.Reason = "" },
		"blank_reason":      func(request *Request) { request.Reason = " \n" },
		"large_reason":      func(request *Request) { request.Reason = strings.Repeat("字", 1025) },
		"bad_reason_utf8":   func(request *Request) { request.Reason = string([]byte{0xff}) },
		"agent":             func(request *Request) { request.AgentID = "agent" },
		"event":             func(request *Request) { request.EventID = "event" },
		"message":           func(request *Request) { request.Message = "reply" },
		"name":              func(request *Request) { request.Name = "agent" },
		"definition":        func(request *Request) { request.Definition = "main" },
	} {
		t.Run(name, func(t *testing.T) {
			request := valid
			mutate(&request)
			var usage *UsageError
			if err := request.Validate(); !errors.As(err, &usage) {
				t.Fatalf("无效参数未返回UsageError: %+v %v", request, err)
			}
		})
	}
	for _, command := range []string{"init", "submit", "run", "status", "retry"} {
		for _, field := range []string{"action", "decision", "reason"} {
			t.Run(command+"_"+field, func(t *testing.T) {
				request := Request{Command: command}
				if command == "submit" || command == "retry" {
					request.AgentID, request.EventID = "agent", "event"
				}
				switch field {
				case "action":
					request.ActionID = valid.ActionID
				case "decision":
					request.Decision = valid.Decision
				case "reason":
					request.Reason = valid.Reason
				}
				var usage *UsageError
				if err := request.Validate(); !errors.As(err, &usage) {
					t.Fatalf("%s接受了resolve专属参数: %+v %v", command, request, err)
				}
			})
		}
	}
}

func TestResolveApplicationOnlyPersistsDecision(t *testing.T) {
	for _, decision := range []core.ResolutionDecision{core.ResolutionRetry, core.ResolutionAbandon} {
		t.Run(string(decision), func(t *testing.T) {
			backend := core.NewMemoryRecoveryStore()
			runnerCalls, modelCalls := 0, 0
			app := &Application{Backend: backend, Bind: func(runtime *core.Runtime) (io.Closer, error) {
				if err := runtime.RegisterDefinition(domain.DefinitionRef{ID: "main", Version: "1"}, runnerFunc(func(input core.ExecutionContext) (core.ExecutionResult, error) {
					runnerCalls++
					action := domain.NewAction("model.generate", map[string]any{"messages": []any{map[string]any{"role": "user", "content": "hello"}}})
					return core.ExecutionResult{Actions: []domain.Action{action}, StateUpdate: map[string]any{
						"request_status": "waiting", "waiting_action_id": string(action.ID), "waiting_action_type": "model.generate",
						"waiting_execution_id": string(input.ExecutionID), "request_execution_id": string(input.ExecutionID),
					}}, nil
				})); err != nil {
					return nil, err
				}
				return nil, runtime.Executor().Register("model.generate", handlerFunc(func(domain.Action) (map[string]any, error) {
					modelCalls++
					panic("模拟模型调用结果未知")
				}))
			}}
			agent := execute(t, app, Request{Command: "init", Definition: "main"}).Agent
			execute(t, app, Request{Command: "submit", AgentID: agent.ID, EventID: "request", Message: "hello"})
			run := execute(t, app, Request{Command: "run"}).Run
			if len(run.Agents) != 1 || len(run.Agents[0].Actions) != 1 {
				t.Fatalf("未知调用未创建: %+v", run)
			}
			action := run.Agents[0].Actions[0].Action
			request := Request{Command: "resolve", ActionID: action.Request.ID, Decision: decision, Reason: "人工处理"}
			first := execute(t, app, request)
			duplicate := execute(t, app, request)
			if first.Resolution == nil || first.Resolution.Duplicate || duplicate.Resolution == nil || !duplicate.Resolution.Duplicate ||
				first.Resolution.Resolution.ActionID != action.Request.ID || first.Resolution.Resolution.Decision != decision ||
				first.Resolution.Resolution.Reason != request.Reason || first.Resolution.Delivery.Status != domain.DeliveryStatusPending ||
				first.Resolution.Delivery.Key.AgentID != agent.ID || first.Resolution.Resolution.EventID != first.Resolution.Delivery.Key.EventID ||
				first.Resolution.Delivery != duplicate.Resolution.Delivery {
				t.Fatalf("人工处理回执错误: %+v %+v", first, duplicate)
			}
			status := execute(t, app, Request{Command: "status", AgentID: agent.ID}).Query
			if runnerCalls != 1 || modelCalls != 1 || status.Agent.StateVersion != 1 || len(status.Deliveries) != 2 ||
				status.Actions[0].Action.Status != domain.ActionStatusUnknown || len(status.Actions[0].Attempts) != 1 ||
				!reflect.DeepEqual(status.Actions[0].Action, action) {
				t.Fatalf("resolve执行了业务或改变旧调用: runner=%d model=%d query=%+v", runnerCalls, modelCalls, status)
			}
			encoded, err := json.Marshal(first)
			if err != nil {
				t.Fatal(err)
			}
			var document map[string]json.RawMessage
			if err := json.Unmarshal(encoded, &document); err != nil || len(document["resolution"]) == 0 {
				t.Fatalf("JSON没有人工处理回执: %s %v", encoded, err)
			}
			request.Reason = "另一个决定"
			assertExecuteError(t, app, request, core.ErrStoreConflict)
			assertExecuteError(t, app, Request{Command: "resolve", ActionID: "missing", Decision: decision, Reason: "人工处理"}, core.ErrStoreNotFound)
			assertBackendAvailable(t, backend)
		})
	}
}
