package main

import (
	"agent-runtime/cli"
	"agent-runtime/core"
	"agent-runtime/domain"
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
)

func TestParseResolveCommand(t *testing.T) {
	for _, args := range [][]string{
		{"resolve", "--data-dir", "data", "--action", "model-action", "--retry"},
		{"--retry", "--action=model-action", "--data-dir=data", "resolve"},
	} {
		options, err := parseCommand(args)
		if err != nil || options.request.Command != "resolve" || options.request.ActionID != "model-action" ||
			options.request.Decision != core.ResolutionRetry || options.request.Reason != "用户选择重试" ||
			options.request.AgentID != "" || options.request.EventID != "" || options.request.Name != "" || options.request.Message != "" {
			t.Fatalf("重试参数错误: %+v %v", options, err)
		}
	}
	for _, flag := range []string{"--retry", "--abandon"} {
		options, err := parseCommand([]string{"resolve", "--data-dir", "data", "--action", "model-action", flag, "--reason", "原因", "--json"})
		if err != nil || options.request.Reason != "原因" || string(options.request.Decision) != strings.TrimPrefix(flag, "--") || !options.asJSON {
			t.Fatalf("显式人工处理参数错误: %+v %v", options, err)
		}
	}
	if requestedJSON([]string{"resolve", "--action", "--json"}) || requestedJSON([]string{"resolve", "--reason", "--json"}) ||
		!requestedJSON([]string{"resolve", "--action=--json", "--reason=--json", "--json"}) {
		t.Fatal("resolve参数值影响了错误输出格式")
	}
}

func TestResolveCommandValidationPrecedesBackendOpen(t *testing.T) {
	calls := 0
	open := func(context.Context, string) (backendHandle, error) { calls++; return backendHandle{}, nil }
	for _, parameters := range [][]string{
		{}, {"--action", "model-action"}, {"--retry"},
		{"--action", " ", "--retry"},
		{"--action", "model-action", "--retry", "--abandon", "--reason", "原因"},
		{"--action", "model-action", "--retry", "--abandon=false"},
		{"--action", "model-action", "--retry=false"},
		{"--action", "model-action", "--abandon"},
		{"--action", "model-action", "--abandon", "--reason", " \n"},
		{"--action", "model-action", "--retry", "--reason="},
		{"--action", "model-action", "--retry", "--reason", strings.Repeat("字", 1025)},
		{"--action", "model-action", "--retry", "--reason", string([]byte{0xff})},
		{"--action", "model-action", "--retry", "--agent", "agent"},
		{"--action", "model-action", "--retry", "--event-id", "event"},
		{"--action", "model-action", "--retry", "--message", "回复"},
		{"--action", "model-action", "--retry", "--env-file", "config"},
	} {
		args := append([]string{"resolve", "--data-dir", "data", "--json"}, parameters...)
		var stdout, stderr bytes.Buffer
		if code := runCommandWithBackend(context.Background(), args, &stdout, &stderr, open); code != 2 || stdout.Len() != 0 {
			t.Fatalf("无效resolve参数错误: args=%q code=%d stdout=%q stderr=%q", args, code, stdout.String(), stderr.String())
		}
		assertCommandError(t, stderr.Bytes(), "usage", 2)
	}
	for _, command := range []string{"init", "submit", "run", "status", "retry", "chat", ""} {
		for _, parameter := range []string{"--action=action", "--retry", "--abandon", "--reason=原因"} {
			args := []string{command, parameter}
			if command == "" {
				args = args[1:]
			} else if command != "chat" {
				args = append(args, "--data-dir", "data")
			}
			if command == "submit" || command == "chat" {
				args = append(args, "--message", "hello")
			}
			if command == "submit" || command == "retry" {
				args = append(args, "--agent", "agent", "--event-id", "event")
			}
			var usage *cli.UsageError
			if _, err := parseCommand(args); !errors.As(err, &usage) {
				t.Fatalf("其他命令接受了resolve参数: args=%q err=%v", args, err)
			}
		}
	}
	if calls != 0 {
		t.Fatalf("无效参数打开了后端%d次", calls)
	}
}

func TestWriteResolveCommandResult(t *testing.T) {
	for _, decision := range []core.ResolutionDecision{core.ResolutionRetry, core.ResolutionAbandon} {
		var output bytes.Buffer
		err := writeCommandResult(&output, cli.Result{Command: "resolve", Resolution: &core.ActionResolutionReceipt{
			Resolution: core.ActionResolution{ActionID: "model-action", EventID: "decision-event", Decision: decision, Reason: "原因"},
			Delivery:   domain.Delivery{Key: domain.DeliveryKey{AgentID: "agent", EventID: "decision-event"}, Status: domain.DeliveryStatusPending},
			Duplicate:  true,
		}})
		if err != nil {
			t.Fatal(err)
		}
		for _, expected := range []string{"action: model-action", "decision: " + string(decision), "reason: 原因", "event: decision-event", "delivery: pending", "duplicate: true", "用run继续"} {
			if !strings.Contains(output.String(), expected) {
				t.Fatalf("人工处理结果缺少%q: %s", expected, output.String())
			}
		}
	}
}

func TestWriteResolveCommandResultUsesDeliveryStatus(t *testing.T) {
	for _, decision := range []core.ResolutionDecision{core.ResolutionRetry, core.ResolutionAbandon} {
		for _, scenario := range []struct {
			status domain.DeliveryStatus
			want   []string
		}{
			{domain.DeliveryStatusPending, []string{"决定已保存，用run继续"}},
			{domain.DeliveryStatusFailed, []string{"先用retry重排", "再用run继续", "retry --agent agent --event-id decision-event"}},
			{domain.DeliveryStatusCompleted, []string{"决定已处理", "用run继续"}},
		} {
			t.Run(string(decision)+"_"+string(scenario.status), func(t *testing.T) {
				var output bytes.Buffer
				err := writeCommandResult(&output, cli.Result{Command: "resolve", Resolution: &core.ActionResolutionReceipt{
					Resolution: core.ActionResolution{ActionID: "model-action", EventID: "decision-event", Decision: decision, Reason: "原因"},
					Delivery:   domain.Delivery{Key: domain.DeliveryKey{AgentID: "agent", EventID: "decision-event"}, Status: scenario.status},
					Duplicate:  true,
				}})
				if err != nil {
					t.Fatal(err)
				}
				for _, expected := range scenario.want {
					if !strings.Contains(output.String(), expected) {
						t.Fatalf("人工处理结果缺少%q: %s", expected, output.String())
					}
				}
				if scenario.status != domain.DeliveryStatusPending && strings.Contains(output.String(), "决定已保存，用run继续") {
					t.Fatalf("终态回执沿用了pending提示: %s", output.String())
				}
			})
		}
	}
}

func TestWriteStatusIncludesActionResolution(t *testing.T) {
	var output bytes.Buffer
	err := writeCommandResult(&output, cli.Result{Command: "status", Query: &core.AgentQuery{
		Actions: []core.ActionQuery{{
			Action:     domain.ActionRecord{Request: domain.Action{ID: "model-action"}, Status: domain.ActionStatusUnknown},
			Resolution: &core.ActionResolution{ActionID: "model-action", EventID: "decision-event", Decision: core.ResolutionAbandon, Reason: "原因"},
			BlockedBy:  []core.BlockReason{{Code: core.BlockActionResolved, Message: "已登记人工处理决定"}},
		}},
	}})
	if err != nil {
		t.Fatal(err)
	}
	for _, expected := range []string{`resolution: {"action_id":"model-action","event_id":"decision-event","decision":"abandon","reason":"原因"}`, "blocked: action_resolved", `"status":"unknown"`} {
		if !strings.Contains(output.String(), expected) {
			t.Fatalf("查询缺少人工处理记录%q: %s", expected, output.String())
		}
	}
}
