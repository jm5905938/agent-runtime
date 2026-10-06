package main

import (
	"agent-runtime/cli"
	"agent-runtime/core"
	"agent-runtime/domain"
	"agent-runtime/model"
	pythonrunner "agent-runtime/python"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

type commandOptions struct {
	request        cli.Request
	dataDir        string
	asJSON         bool
	python         pythonrunner.Options
	envFile        string
	hasMessage     bool
	friendlyStatus bool
	taskID         domain.ID
}

func runCommand(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	return runCommandWithStorage(ctx, args, stdout, stderr, openCommandBackend, openCommandIngress)
}

func runCommandWithBackend(ctx context.Context, args []string, stdout, stderr io.Writer, open backendOpener) int {
	return runCommandWithStorage(ctx, args, stdout, stderr, open, nil)
}

func runCommandWithStorage(ctx context.Context, args []string, stdout, stderr io.Writer, open backendOpener, input inputOpener) int {
	return runCommandWithInput(ctx, args, os.Stdin, stdout, stderr, open, input)
}

func runCommandWithInput(ctx context.Context, args []string, stdin io.Reader, stdout, stderr io.Writer, open backendOpener, input inputOpener) int {
	options, err := parseCommand(args)
	if errors.Is(err, flag.ErrHelp) {
		if _, err := io.WriteString(stdout, commandHelp); err != nil {
			return writeCommandError(stderr, requestedJSON(args), err)
		}
		return 0
	}
	if err != nil {
		return writeCommandError(stderr, requestedJSON(args), err)
	}
	options.python.Stderr = stderr
	var diagnostics workerDiagnostics
	if options.asJSON || options.request.Command == "tui" {
		options.python.Stderr = &diagnostics
	}
	reportError := func(err error) int {
		return writeCommandError(stderr, options.asJSON, err, diagnostics.String())
	}
	if options.request.Command == "tui" {
		if err := runTUI(ctx, options, stdin, stdout, open); err != nil {
			return reportError(err)
		}
		return 0
	}
	if options.request.Command == "cancel" {
		if err := runCancelCommand(ctx, options, stdout, open); err != nil {
			return reportError(err)
		}
		return 0
	}
	if options.request.Command == "chat" || options.request.Command == "resume" || options.friendlyStatus {
		if err := runConversationCommand(ctx, options, stdin, stdout, open); err != nil {
			return reportError(err)
		}
		return 0
	}
	if options.request.Command == "" {
		result, err := runEcho(ctx, options.request.Message, options.python)
		if err != nil {
			return reportError(err)
		}
		if options.asJSON {
			err = json.NewEncoder(stdout).Encode(struct {
				echoResult
				WorkerDiagnostics string `json:"worker_diagnostics,omitempty"`
			}{result, diagnostics.String()})
		} else {
			_, err = fmt.Fprintf(stdout, "%s\nstatus: %s\nagent: %s\nrequest: %s\naction: %s\nexecutions: %d\nactions: %d\n",
				result.Result, result.Status, result.AgentID, result.RequestEventID, result.ActionID, result.Executions, result.Actions)
		}
		if err != nil {
			return reportError(fmt.Errorf("输出失败: %w", err))
		}
		return 0
	}
	var result cli.Result
	if options.request.Command == "submit" && input != nil {
		result, err = submitCommand(ctx, options, input)
	} else {
		if open == nil {
			return reportError(cli.ErrBackendUnavailable)
		}
		backend, openErr := open(ctx, options.dataDir)
		if openErr != nil {
			if backend.Close != nil {
				openErr = errors.Join(openErr, backend.Close())
			}
			return reportError(openErr)
		}
		app := cli.Application{
			Backend: backend.Store, CloseBackend: backend.Close,
			Bind: func(runtime *core.Runtime) (io.Closer, error) { return bindPersistent(ctx, runtime, options) },
		}
		result, err = app.Execute(ctx, options.request)
	}
	if err != nil {
		return reportError(err)
	}
	if options.asJSON {
		err = json.NewEncoder(stdout).Encode(struct {
			cli.Result
			WorkerDiagnostics string `json:"worker_diagnostics,omitempty"`
		}{result, diagnostics.String()})
	} else {
		err = writeCommandResult(stdout, result)
	}
	if err != nil {
		return reportError(fmt.Errorf("输出失败: %w", err))
	}
	return 0
}

func bindPersistent(ctx context.Context, runtime *core.Runtime, options commandOptions) (io.Closer, error) {
	runner := &persistentPythonRunner{}
	var modelMu sync.Mutex
	modelReady := false
	ensureModel := func(ctx context.Context) error {
		modelMu.Lock()
		defer modelMu.Unlock()
		if modelReady {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		config, err := model.LoadConfig(options.envFile)
		if err != nil {
			return err
		}
		handler, err := model.NewHandler(config)
		if err != nil {
			return err
		}
		worker, err := pythonrunner.NewRunner(promptRunnerOptions(options.python, config))
		if err != nil {
			return err
		}
		if err := runtime.Executor().Register("model.generate", handler); err != nil {
			return errors.Join(err, worker.Close())
		}
		if err := runner.replace(worker); err != nil {
			return err
		}
		modelReady = true
		return nil
	}
	runner.prepareModel = ensureModel
	if err := registerEcho(runtime, runner); err != nil {
		return runner, err
	}
	modelRunner := mainAgentRunner{
		AgentRunner: runner,
		prepare: func(ctx context.Context, agent core.AgentSnapshot, event domain.Event) error {
			if (options.request.Command == "run" || options.request.Command == "chat" || options.request.Command == "resume" || options.request.Command == "tui") && deliveryNeedsModel(agent, core.DeliveryQuery{Ready: true, Event: event}) {
				return ensureModel(ctx)
			}
			return nil
		},
	}
	for _, id := range []string{"main", "subagent"} {
		if err := runtime.RegisterDefinition(domain.DefinitionRef{ID: id, Version: "1"}, modelRunner); err != nil {
			return runner, err
		}
	}
	if err := registerAgentStatus(ctx, runtime); err != nil {
		return runner, err
	}
	if err := runtime.RegisterSubagentTools(); err != nil {
		return runner, err
	}
	if options.request.Command == "run" {
		needed, err := modelWorkPending(ctx, runtime)
		if err != nil {
			return runner, err
		}
		if needed {
			if err := ensureModel(ctx); err != nil {
				return runner, err
			}
		}
	}
	if !modelReady {
		worker, err := pythonrunner.NewRunner(options.python)
		if err != nil {
			return runner, err
		}
		return runner, runner.replace(worker)
	}
	return runner, nil
}

// Definitions must be registered for preflight queries before the worker is
// configured. No execution can start until binding and preflight succeed.
type persistentPythonRunner struct {
	*pythonrunner.Runner
	mu           sync.RWMutex
	prepareModel func(context.Context) error
}

func (runner *persistentPythonRunner) PrepareModel(ctx context.Context) error {
	if runner.prepareModel == nil {
		return errors.New("对话绑定没有模型配置入口")
	}
	return runner.prepareModel(ctx)
}

func (runner *persistentPythonRunner) Run(input core.ExecutionContext) (core.ExecutionResult, error) {
	runner.mu.RLock()
	defer runner.mu.RUnlock()
	if runner.Runner == nil {
		return core.ExecutionResult{}, errors.New("python runner尚未配置")
	}
	return runner.Runner.Run(input)
}

func (runner *persistentPythonRunner) RunContext(ctx context.Context, input core.ExecutionContext) (core.ExecutionResult, error) {
	runner.mu.RLock()
	defer runner.mu.RUnlock()
	if runner.Runner == nil {
		return core.ExecutionResult{}, errors.New("python runner尚未配置")
	}
	return runner.Runner.RunContext(ctx, input)
}

func (runner *persistentPythonRunner) Close() error {
	runner.mu.RLock()
	defer runner.mu.RUnlock()
	if runner.Runner == nil {
		return nil
	}
	return runner.Runner.Close()
}

func (runner *persistentPythonRunner) replace(worker *pythonrunner.Runner) error {
	runner.mu.Lock()
	defer runner.mu.Unlock()
	if runner.Runner != nil {
		if err := runner.Runner.Close(); err != nil {
			return errors.Join(err, worker.Close())
		}
	}
	runner.Runner = worker
	return nil
}

func registerEcho(runtime *core.Runtime, runner core.AgentRunner) error {
	if err := runtime.RegisterDefinition(domain.DefinitionRef{ID: "echo", Version: "1"}, runner); err != nil {
		return err
	}
	return runtime.Executor().RegisterWithOptions("echo", core.EchoHandler{}, core.HandlerOptions{
		Version: "1", RecoveryPolicy: domain.RecoveryPolicySafeRetry, MaxAttempts: 3,
	})
}

func parseCommand(args []string) (commandOptions, error) {
	options := commandOptions{}
	flags := flag.NewFlagSet("agent-runtime", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	flags.Usage = func() {}
	var agentID, eventID, actionID, taskID string
	var retryAction, abandonAction bool
	flags.StringVar(&options.dataDir, "data-dir", "", "数据目录")
	flags.BoolVar(&options.asJSON, "json", false, "以json格式输出")
	flags.StringVar(&options.python.Python, "python", "python3", "python可执行文件")
	flags.StringVar(&options.python.SourceDir, "python-source", defaultPythonSource(), "python源码目录")
	flags.DurationVar(&options.python.Timeout, "timeout", 30*time.Second, "每次python调用期限")
	flags.StringVar(&options.request.Message, "message", "hello", "echo消息")
	flags.StringVar(&options.envFile, "env-file", defaultEnvFile(), "模型配置文件")
	flags.StringVar(&options.request.Name, "name", "echo", "实例名称")
	flags.StringVar(&options.request.Definition, "definition", "", "实例定义，echo或main")
	flags.StringVar(&agentID, "agent", "", "agent id")
	flags.StringVar(&eventID, "event-id", "", "event id")
	flags.StringVar(&actionID, "action", "", "结果未知的model.generate action id")
	flags.StringVar(&taskID, "task", "", "subagent任务id")
	flags.BoolVar(&retryAction, "retry", false, "人工确认重试结果未知的模型调用")
	flags.BoolVar(&abandonAction, "abandon", false, "放弃结果未知的模型调用所在轮次")
	flags.StringVar(&options.request.Reason, "reason", "", "人工处理原因")
	parse := func(args []string) error {
		if err := flags.Parse(args); err != nil {
			if errors.Is(err, flag.ErrHelp) {
				return err
			}
			return &cli.UsageError{Message: "参数无效: " + err.Error()}
		}
		return nil
	}
	if err := parse(args); err != nil {
		return options, err
	}
	if remaining := flags.Args(); len(remaining) > 0 {
		options.request.Command = remaining[0]
		switch options.request.Command {
		case "init", "submit", "run", "status", "retry", "resolve", "chat", "resume", "tui", "cancel":
		default:
			return options, &cli.UsageError{Message: "未知命令" + options.request.Command}
		}
		if err := parse(remaining[1:]); err != nil {
			return options, err
		}
		if flags.NArg() != 0 {
			return options, &cli.UsageError{Message: "命令不接受位置参数"}
		}
	}
	if options.python.Timeout <= 0 {
		return options, &cli.UsageError{Message: "超时时间必须大于零"}
	}
	if !utf8.ValidString(options.request.Message) {
		return options, &cli.UsageError{Message: "message必须是有效UTF-8文本"}
	}
	seen := make(map[string]bool)
	flags.Visit(func(f *flag.Flag) { seen[f.Name] = true })
	options.hasMessage = seen["message"]
	options.friendlyStatus = options.request.Command == "status" && !seen["data-dir"] && !seen["agent"]
	allowed := map[string]bool{"json": true, "python": true, "python-source": true, "timeout": true}
	switch options.request.Command {
	case "":
		allowed["message"] = true
	case "chat":
		allowed["message"], allowed["env-file"], allowed["agent"] = true, true, true
	case "tui":
		allowed["env-file"], allowed["agent"] = true, true
		allowed["json"] = false
	case "resume":
		allowed["agent"], allowed["env-file"], allowed["retry"], allowed["abandon"], allowed["reason"] = true, true, true, true, true
	case "cancel":
		allowed["agent"], allowed["task"] = true, true
	case "init":
		allowed["name"], allowed["definition"] = true, true
	case "run":
		allowed["env-file"] = true
	case "submit":
		allowed["agent"], allowed["event-id"], allowed["message"] = true, true, true
	case "status":
		allowed["agent"] = true
	case "retry":
		allowed["agent"], allowed["event-id"] = true, true
	case "resolve":
		allowed["action"], allowed["retry"], allowed["abandon"], allowed["reason"] = true, true, true, true
	}
	persistent := options.request.Command != ""
	if persistent {
		allowed["data-dir"] = true
		if !seen["data-dir"] && (options.request.Command == "chat" || options.request.Command == "status" || options.request.Command == "resume" || options.request.Command == "tui" || options.request.Command == "cancel") {
			options.dataDir = defaultDataDir()
		}
		if strings.TrimSpace(options.dataDir) == "" {
			return options, &cli.UsageError{Message: "命令需要--data-dir指定数据目录"}
		}
		if !utf8.ValidString(options.dataDir) {
			return options, &cli.UsageError{Message: "data-dir必须是有效UTF-8文本"}
		}
	}
	var invalid string
	flags.Visit(func(f *flag.Flag) {
		if invalid == "" && !allowed[f.Name] {
			invalid = f.Name
		}
	})
	if invalid != "" {
		return options, &cli.UsageError{Message: "当前命令不接受--" + invalid}
	}
	if options.request.Command == "submit" && !seen["message"] {
		return options, &cli.UsageError{Message: options.request.Command + "需要--message，允许显式传入空字符串"}
	}
	if options.request.Command == "chat" && options.asJSON && !options.hasMessage {
		return options, &cli.UsageError{Message: "chat使用--json时需要--message"}
	}
	options.request.AgentID, options.request.EventID = domain.ID(agentID), domain.ID(eventID)
	options.request.ActionID = domain.ID(actionID)
	options.taskID = domain.ID(taskID)
	if options.request.Command == "cancel" && (!seen["task"] || !utf8.ValidString(taskID) || strings.TrimSpace(taskID) == "" || len(taskID) > 1024) {
		return options, &cli.UsageError{Message: "cancel需要有效的--task任务id，且不超过1024字节"}
	}
	if options.request.Command == "resolve" || options.request.Command == "resume" {
		if options.request.Command == "resume" && !seen["retry"] && !seen["abandon"] {
			if seen["reason"] {
				return options, &cli.UsageError{Message: "resume的--reason需要同时指定--retry或--abandon"}
			}
			if options.asJSON {
				return options, &cli.UsageError{Message: "resume使用--json时需要--retry或--abandon"}
			}
		} else if seen["retry"] == seen["abandon"] || !retryAction && !abandonAction {
			return options, &cli.UsageError{Message: options.request.Command + "需要恰好一个--retry或--abandon，且值必须为true"}
		}
		if retryAction {
			options.request.Decision = core.ResolutionRetry
			if !seen["reason"] {
				options.request.Reason = "用户选择重试"
			}
		} else if abandonAction {
			options.request.Decision = core.ResolutionAbandon
			if options.request.Command == "resume" && !seen["reason"] {
				options.request.Reason = "用户选择放弃"
			}
		}
	}
	if persistent {
		if options.request.Command == "init" {
			if seen["definition"] && options.request.Definition == "" {
				return options, &cli.UsageError{Message: "definition仅支持echo或main"}
			}
			if !seen["name"] && options.request.Definition != "" {
				options.request.Name = options.request.Definition
			}
		}
		if options.request.Command != "init" {
			options.request.Name = ""
		}
		if options.request.Command != "submit" && (options.request.Command != "chat" || !options.hasMessage) {
			options.request.Message = ""
		}
		if options.request.Command == "chat" || options.request.Command == "resume" || options.request.Command == "tui" || options.request.Command == "cancel" {
			if !utf8.ValidString(agentID) || seen["agent"] && strings.TrimSpace(agentID) == "" {
				return options, &cli.UsageError{Message: "agent必须是非空白的有效UTF-8文本"}
			}
			if options.request.Command == "resume" && options.request.Decision != "" && (!utf8.ValidString(options.request.Reason) || strings.TrimSpace(options.request.Reason) == "" || utf8.RuneCountInString(options.request.Reason) > 1024) {
				return options, &cli.UsageError{Message: "resume的reason必须为非空白的有效UTF-8文本，且不超过1024个字符"}
			}
		} else {
			if err := options.request.Validate(); err != nil {
				return options, err
			}
		}
	}
	return options, nil
}

func requestedJSON(args []string) bool {
	asJSON := false
	for i := 0; i < len(args); i++ {
		name, value, hasValue := strings.Cut(strings.TrimLeft(args[i], "-"), "=")
		if !strings.HasPrefix(args[i], "-") {
			continue
		}
		switch name {
		case "json":
			asJSON = true
			if hasValue {
				asJSON, _ = strconv.ParseBool(value)
			}
		case "message", "agent", "event-id", "action", "task", "reason", "name", "definition", "data-dir", "python", "python-source", "timeout", "env-file":
			if !hasValue {
				i++
			}
		}
	}
	return asJSON
}

func writeCommandError(stderr io.Writer, asJSON bool, err error, diagnostics ...string) int {
	code, kind := 1, "operation"
	var usage *cli.UsageError
	var cleanup *cli.CleanupError
	switch {
	case errors.As(err, &usage):
		code, kind = 2, "usage"
	case errors.As(err, &cleanup):
		kind = "cleanup"
	case errors.Is(err, cli.ErrBackendUnavailable):
		kind = "backend_unavailable"
	case errors.Is(err, core.ErrStoreOwned):
		kind = "store_owned"
	case errors.Is(err, core.ErrStoreNotFound):
		kind = "not_found"
	case errors.Is(err, core.ErrStoreConflict), errors.Is(err, core.ErrStoreStaleClaim):
		kind = "conflict"
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		kind = "canceled"
	}
	if asJSON {
		var workerOutput string
		if len(diagnostics) != 0 {
			workerOutput = diagnostics[0]
		}
		_ = json.NewEncoder(stderr).Encode(struct {
			Error struct {
				Kind    string `json:"kind"`
				Message string `json:"message"`
			} `json:"error"`
			ExitCode          int    `json:"exit_code"`
			WorkerDiagnostics string `json:"worker_diagnostics,omitempty"`
		}{Error: struct {
			Kind    string `json:"kind"`
			Message string `json:"message"`
		}{Kind: kind, Message: err.Error()}, ExitCode: code, WorkerDiagnostics: workerOutput})
	} else {
		_, _ = fmt.Fprintf(stderr, "运行失败: %v\n", err)
	}
	return code
}

const commandHelp = `日常用法:
  agent-runtime tui                          进入终端界面，显示历史、工具状态和输入队列
  agent-runtime chat                         进入持久对话，首次自动创建main agent
  agent-runtime status                       查看当前main agent状态
  agent-runtime resume                       选择重试或放弃卡住的模型调用并继续执行

  chat     [--message <文本>] [--agent <id>] [--env-file <文件>]
                                              不传message时交互对话；传入时处理一条输入
  tui      [--agent <id>] [--env-file <文件>]   Enter发送，Alt+Enter换行，Esc停止，Ctrl+C退出
  status   [--agent <id>]                     指定agent时显示完整记录
  resume   [--agent <id>] [--retry|--abandon] [--reason <原因>]
           [--env-file <文件>]               不指定决定时交互选择，不需要action id
  cancel   --task <id> [--agent <id>]         请求取消当前main的subagent任务

脚本和调试命令，需要显式--data-dir <目录>:
  init     [--definition echo|main] [--name <名称>]
                                              创建持久agent，默认echo
  submit   --agent <id> --event-id <id> --message <文本>
                                              接收消息，不执行
  run      [--env-file <文件>]                推进已有工作并报告剩余状态
  retry    --agent <id> --event-id <id>         重新排队失败的delivery，不执行
  resolve  --action <id> --retry [--reason <原因>]
           --action <id> --abandon --reason <原因>
                                              保存人工处理决定，随后使用run继续处理

公共参数可放在命令前后:
  --data-dir <目录>       tui、chat、status、resume和cancel默认使用当前项目的.agent-runtime目录
                         其它持久命令必填；数据保存到store.db
  --json                 成功结果写stdout，结构化错误写stderr
                         chat需同时传入--message，resume需指定--retry或--abandon
  --python <路径>        python可执行文件，默认python3，需要3.12+
  --python-source <目录> 包含agent_runtime的源码目录
  --timeout <时长>       每次python调用期限，默认30s
  --env-file <文件>      tui、chat、resume或run的模型配置文件，默认项目根目录的.env
  --help                 显示帮助

chat退出后再次启动会恢复同一个main agent和历史；输入/exit或/quit退出
chat遇到结果未知的模型调用会提示选择重试或放弃；resume可在重新启动后处理
显式传入status --data-dir且不指定agent时，保留agent列表查询
submit只持久化输入，可在run执行期间提交
持久命令启动时恢复中断记录；status不会运行agent或action
模型调用结果未知时不会自动重试；人工重试保留旧记录，可能重复产生模型费用
模型请求期限由LLM_TIMEOUT配置，默认60s；没有待执行模型工作时不需要模型配置
无子命令时运行一次内存echo，默认消息hello，进程退出后数据丢失
退出码: 0命令正常结束，1操作或存储错误，2参数错误
`
