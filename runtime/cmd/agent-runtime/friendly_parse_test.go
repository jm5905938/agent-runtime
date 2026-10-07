package main

import (
	"agent-runtime/cli"
	"agent-runtime/core"
	"errors"
	"strings"
	"testing"
)

func TestParseFriendlyCommandsUseDefaultDataDirectory(t *testing.T) {
	for _, command := range []string{"chat", "status", "resume"} {
		t.Run(command, func(t *testing.T) {
			options, err := parseCommand([]string{command})
			if err != nil || options.dataDir != defaultDataDir() || options.request.Command != command ||
				options.request.Name != "" || options.request.Message != "" || options.hasMessage {
				t.Fatalf("日常命令没有采用默认目录: %+v %v", options, err)
			}
			if options.friendlyStatus != (command == "status") {
				t.Fatalf("默认状态查询模式错误: %+v", options)
			}
			options, err = parseCommand([]string{command, "--data-dir", "自选目录"})
			if err != nil || options.dataDir != "自选目录" || options.friendlyStatus {
				t.Fatalf("显式数据目录被覆盖: %+v %v", options, err)
			}
		})
	}
	for _, args := range [][]string{
		{"status", "--agent", "main-id"},
		{"--agent=main-id", "status"},
	} {
		options, err := parseCommand(args)
		if err != nil || options.request.AgentID != "main-id" || options.friendlyStatus || options.dataDir != defaultDataDir() {
			t.Fatalf("指定agent查询模式错误: %+v %v", options, err)
		}
	}
}

func TestParseChatPreservesExplicitMessage(t *testing.T) {
	for _, message := range []string{"你好\n世界", "", "status"} {
		args := []string{"--message", message, "chat", "--agent", "main-id", "--env-file", "模型.env", "--json"}
		options, err := parseCommand(args)
		if err != nil || !options.hasMessage || !options.asJSON || options.request.Message != message ||
			options.request.AgentID != "main-id" || options.envFile != "模型.env" || options.request.Name != "" {
			t.Fatalf("单条对话参数错误: %+v %v", options, err)
		}
	}
	options, err := parseCommand([]string{"chat", "--json=false", "--env-file", "模型.env"})
	if err != nil || options.hasMessage || options.request.Message != "" || options.asJSON {
		t.Fatalf("交互对话参数错误: %+v %v", options, err)
	}
}

func TestParseResumeDecisionOptions(t *testing.T) {
	for _, scenario := range []struct {
		args     []string
		decision core.ResolutionDecision
		reason   string
	}{
		{[]string{"resume", "--retry"}, core.ResolutionRetry, "用户选择重试"},
		{[]string{"--retry", "resume", "--json"}, core.ResolutionRetry, "用户选择重试"},
		{[]string{"resume", "--abandon"}, core.ResolutionAbandon, "用户选择放弃"},
		{[]string{"resume", "--abandon", "--reason", "这轮不再需要"}, core.ResolutionAbandon, "这轮不再需要"},
		{[]string{"resume", "--retry", "--reason", "重新发送", "--agent", "main-id", "--env-file", "模型.env"}, core.ResolutionRetry, "重新发送"},
		{[]string{"resume", "--retry", "--reason", strings.Repeat("字", 1024)}, core.ResolutionRetry, strings.Repeat("字", 1024)},
		{[]string{"resume", "--json=false"}, "", ""},
	} {
		options, err := parseCommand(scenario.args)
		if err != nil || options.request.Command != "resume" || options.request.Decision != scenario.decision || options.request.Reason != scenario.reason ||
			options.request.ActionID != "" || options.request.EventID != "" || options.request.Name != "" || options.request.Message != "" {
			t.Fatalf("恢复参数错误: args=%q options=%+v err=%v", scenario.args, options, err)
		}
	}
}

func TestParseFriendlyCommandsRejectInvalidOptions(t *testing.T) {
	for _, args := range [][]string{
		{"chat", "--json"},
		{"chat", "--data-dir="},
		{"chat", "--data-dir", " \n"},
		{"chat", "--agent="},
		{"chat", "--agent", " \n"},
		{"chat", "--agent", string([]byte{0xff})},
		{"chat", "--message", string([]byte{0xff})},
		{"chat", "--event-id", "event"},
		{"chat", "--name", "main"},
		{"chat", "--action", "action"},
		{"chat", "--retry"},
		{"status", "--data-dir="},
		{"status", "--env-file", "配置"},
		{"resume", "--json"},
		{"resume", "--data-dir", " "},
		{"resume", "--data-dir", string([]byte{0xff})},
		{"resume", "--agent="},
		{"resume", "--reason", "理由"},
		{"resume", "--reason="},
		{"resume", "--retry=false"},
		{"resume", "--abandon=false"},
		{"resume", "--retry", "--abandon"},
		{"resume", "--retry", "--abandon=false"},
		{"resume", "--retry=false", "--abandon"},
		{"resume", "--retry", "--reason="},
		{"resume", "--abandon", "--reason", " \n"},
		{"resume", "--retry", "--reason", string([]byte{0xff})},
		{"resume", "--retry", "--reason", strings.Repeat("字", 1025)},
		{"resume", "--action", "action"},
		{"resume", "--event-id", "event"},
		{"resume", "--message", "文本"},
		{"resume", "--name", "main"},
		{"resume", "--definition", "main"},
	} {
		var usage *cli.UsageError
		if _, err := parseCommand(args); !errors.As(err, &usage) {
			t.Fatalf("日常命令接受无效参数: args=%q err=%v", args, err)
		}
	}
	for _, command := range []string{"init", "submit", "run", "retry", "resolve"} {
		var usage *cli.UsageError
		if _, err := parseCommand([]string{command}); !errors.As(err, &usage) || !strings.Contains(err.Error(), "data-dir") {
			t.Fatalf("脚本命令自动采用了默认目录: command=%s err=%v", command, err)
		}
	}
}

func TestCommandHelpExplainsDailyAndScriptCommands(t *testing.T) {
	for _, expected := range []string{"agent-runtime chat", "agent-runtime status", "agent-runtime resume", "脚本和调试命令", "--message", "--retry|--abandon", "恢复main agent和历史"} {
		if !strings.Contains(commandHelp, expected) {
			t.Fatalf("帮助缺少日常工作说明%q", expected)
		}
	}
	if strings.Contains(commandHelp, "chat每次处理一条输入，进程退出后数据丢失") {
		t.Fatal("帮助仍声称chat不会持久化")
	}
}
