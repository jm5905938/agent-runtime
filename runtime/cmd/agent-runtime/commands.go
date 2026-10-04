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
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

type commandOptions struct {
	request cli.Request
	dataDir string
	asJSON  bool
	python  pythonrunner.Options
	envFile string
}

func runCommand(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	return runCommandWithBackend(ctx, args, stdout, stderr, openCommandBackend)
}

func runCommandWithBackend(ctx context.Context, args []string, stdout, stderr io.Writer, open backendOpener) int {
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
	if options.asJSON {
		options.python.Stderr = &diagnostics
	}
	reportError := func(err error) int {
		return writeCommandError(stderr, options.asJSON, err, diagnostics.String())
	}
	if options.request.Command == "chat" {
		config, err := model.LoadConfig(options.envFile)
		if err != nil {
			return reportError(err)
		}
		result, err := runMainAgent(ctx, options.request.Message, options.python, config)
		if err != nil {
			return reportError(err)
		}
		if options.asJSON {
			err = json.NewEncoder(stdout).Encode(struct {
				chatResult
				WorkerDiagnostics string `json:"worker_diagnostics,omitempty"`
			}{result, diagnostics.String()})
		} else {
			_, err = fmt.Fprintln(stdout, result.Result)
		}
		if err != nil {
			return reportError(fmt.Errorf("输出失败: %w", err))
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
	if open == nil {
		return reportError(cli.ErrBackendUnavailable)
	}
	backend, err := open(ctx, options.dataDir)
	if err != nil {
		if backend.Close != nil {
			err = errors.Join(err, backend.Close())
		}
		return reportError(err)
	}
	app := cli.Application{
		Backend: backend.Store, CloseBackend: backend.Close,
		Bind: func(runtime *core.Runtime) (io.Closer, error) { return bindPersistent(ctx, runtime, options) },
	}
	result, err := app.Execute(ctx, options.request)
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
	if err := registerEcho(runtime, runner); err != nil {
		return runner, err
	}
	if err := runtime.RegisterDefinition(domain.DefinitionRef{ID: "main", Version: "1"}, runner); err != nil {
		return runner, err
	}
	if err := registerAgentStatus(ctx, runtime); err != nil {
		return runner, err
	}
	if options.request.Command == "run" {
		needed, err := modelWorkPending(ctx, runtime)
		if err != nil {
			return runner, err
		}
		if needed {
			config, err := model.LoadConfig(options.envFile)
			if err != nil {
				return runner, err
			}
			handler, err := model.NewHandler(config)
			if err != nil {
				return runner, err
			}
			if err := runtime.Executor().Register("model.generate", handler); err != nil {
				return runner, err
			}
			options.python = promptRunnerOptions(options.python, config)
		}
	}
	var err error
	runner.Runner, err = pythonrunner.NewRunner(options.python)
	return runner, err
}

// Definitions must be registered for preflight queries before the worker is
// configured. No execution can start until binding and preflight succeed.
type persistentPythonRunner struct {
	*pythonrunner.Runner
}

func (runner *persistentPythonRunner) Run(input core.ExecutionContext) (core.ExecutionResult, error) {
	if runner.Runner == nil {
		return core.ExecutionResult{}, errors.New("python runner尚未配置")
	}
	return runner.Runner.Run(input)
}

func (runner *persistentPythonRunner) RunContext(ctx context.Context, input core.ExecutionContext) (core.ExecutionResult, error) {
	if runner.Runner == nil {
		return core.ExecutionResult{}, errors.New("python runner尚未配置")
	}
	return runner.Runner.RunContext(ctx, input)
}

func (runner *persistentPythonRunner) Close() error {
	if runner.Runner == nil {
		return nil
	}
	return runner.Runner.Close()
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
	var agentID, eventID string
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
		case "init", "submit", "run", "status", "retry", "chat":
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
	allowed := map[string]bool{"json": true, "python": true, "python-source": true, "timeout": true}
	switch options.request.Command {
	case "":
		allowed["message"] = true
	case "chat":
		allowed["message"], allowed["env-file"] = true, true
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
	}
	persistent := options.request.Command != "" && options.request.Command != "chat"
	if persistent {
		allowed["data-dir"] = true
		if strings.TrimSpace(options.dataDir) == "" {
			return options, &cli.UsageError{Message: "命令需要--data-dir指定数据目录"}
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
	if (options.request.Command == "submit" || options.request.Command == "chat") && !seen["message"] {
		return options, &cli.UsageError{Message: options.request.Command + "需要--message，允许显式传入空字符串"}
	}
	options.request.AgentID, options.request.EventID = domain.ID(agentID), domain.ID(eventID)
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
		if options.request.Command != "submit" {
			options.request.Message = ""
		}
		if err := options.request.Validate(); err != nil {
			return options, err
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
		case "message", "agent", "event-id", "name", "definition", "data-dir", "python", "python-source", "timeout", "env-file":
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

const commandHelp = `用法: agent-runtime [--message hello] [--json]
      agent-runtime chat --message <文本> [--env-file <文件>] [--json]
      agent-runtime --data-dir <目录> <命令> [参数] [--json]

命令:
  chat     --message <文本> [--env-file <文件>] 调用llm并显示main agent回复
  init     [--definition echo|main] [--name <名称>]
                                              创建持久agent，默认echo
  submit   --agent <id> --event-id <id> --message <文本>
                                              接收消息，不执行
  run      [--env-file <文件>]                推进已有工作并报告剩余状态
  status   [--agent <id>]                      查询agent列表或完整记录
  retry    --agent <id> --event-id <id>         重新排队失败的delivery，不执行

公共参数可放在命令前后:
  --data-dir <目录>       持久子命令必填，自动创建目录，数据保存到store.db
  --json                 成功结果写stdout，结构化错误写stderr
  --python <路径>        python可执行文件，默认python3，需要3.12+
  --python-source <目录> 包含agent_runtime的源码目录
  --timeout <时长>       每次python调用期限，默认30s
  --env-file <文件>      chat或run的模型配置文件，默认项目根目录的.env
  --help                 显示帮助

无子命令时运行一次内存echo，默认消息hello，进程退出后数据丢失
chat每次处理一条输入，进程退出后数据丢失，模型请求期限由LLM_TIMEOUT配置，默认60s
持久子命令启动时独占数据库并恢复中断记录，只有run执行待办
创建、提交和查询main不需要模型配置；run有待执行模型工作时才加载配置
main的model.generate结果未知时不会自动重试，避免重复调用模型
status也会执行启动恢复，但不会运行agent或action
退出码: 0命令正常结束，1操作或存储错误，2参数错误
`
