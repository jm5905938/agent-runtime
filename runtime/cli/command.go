package cli

import (
	"agent-runtime/core"
	"agent-runtime/domain"
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

var ErrBackendUnavailable = errors.New("当前未配置可用的恢复后端")

type UsageError struct {
	Message string
}

func (e *UsageError) Error() string { return e.Message }

type Request struct {
	Command    string                  `json:"command"`
	AgentID    domain.ID               `json:"agent_id"`
	EventID    domain.ID               `json:"event_id"`
	Message    string                  `json:"message"`
	Name       string                  `json:"name"`
	Definition string                  `json:"definition,omitempty"`
	ActionID   domain.ID               `json:"action_id,omitempty"`
	Decision   core.ResolutionDecision `json:"decision,omitempty"`
	Reason     string                  `json:"reason,omitempty"`
}

type Application struct {
	Backend      core.RecoveryStore
	Bind         func(*core.Runtime) (io.Closer, error)
	CloseBackend func() error
	CloseTimeout time.Duration
}

type Result struct {
	Command         string                        `json:"command"`
	Agent           *core.AgentSnapshot           `json:"agent,omitempty"`
	Submission      *Submission                   `json:"submission,omitempty"`
	Retry           *domain.DeliveryKey           `json:"retry,omitempty"`
	Resolution      *core.ActionResolutionReceipt `json:"resolution,omitempty"`
	Agents          []core.AgentSnapshot          `json:"agents"`
	Query           *core.AgentQuery              `json:"query,omitempty"`
	Run             *RunResult                    `json:"run,omitempty"`
	StartupRecovery core.RecoveryReport           `json:"startup_recovery"`
}

type Submission struct {
	Delivery  domain.Delivery `json:"delivery"`
	Duplicate bool            `json:"duplicate"`
}

type RunResult struct {
	Before Summary           `json:"before"`
	After  Summary           `json:"after"`
	Agents []core.AgentQuery `json:"agents"`
}

type Summary struct {
	Agents            int                           `json:"agents"`
	Deliveries        map[domain.DeliveryStatus]int `json:"deliveries"`
	Actions           map[domain.ActionStatus]int   `json:"actions"`
	ExecutionAttempts uint64                        `json:"execution_attempts"`
	ActionAttempts    uint64                        `json:"action_attempts"`
}

type CleanupError struct {
	cause   error
	cleanup *Cleanup
}

func (e *CleanupError) Error() string { return e.cause.Error() }
func (e *CleanupError) Unwrap() error { return e.cause }
func (e *CleanupError) Close(ctx context.Context) error {
	return e.cleanup.Close(ctx)
}

type Cleanup struct {
	mu      sync.Mutex
	Runtime interface{ Close(context.Context) error }
	Binding io.Closer
	Backend func() error
}

func (c *Cleanup) Close(ctx context.Context) (err error) {
	defer func() {
		if err != nil {
			err = &CleanupError{cause: err, cleanup: c}
		}
	}()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.Runtime != nil {
		if err := c.Runtime.Close(ctx); err != nil {
			return fmt.Errorf("关闭runtime: %w", err)
		}
		c.Runtime = nil
	}
	var result error
	if c.Binding != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := c.Binding.Close(); err != nil {
			result = fmt.Errorf("关闭绑定资源: %w", err)
		} else {
			c.Binding = nil
		}
	}
	if c.Backend != nil {
		if err := ctx.Err(); err != nil {
			return errors.Join(result, err)
		}
		if err := c.Backend(); err != nil {
			result = errors.Join(result, fmt.Errorf("关闭后端: %w", err))
		} else {
			c.Backend = nil
		}
	}
	return result
}

func (a *Application) Execute(ctx context.Context, request Request) (result Result, err error) {
	if err := request.Validate(); err != nil {
		return Result{}, err
	}
	if a == nil {
		return Result{}, ErrBackendUnavailable
	}
	cleanup := &Cleanup{Backend: a.CloseBackend}
	timeout := a.CloseTimeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
		defer cancel()
		err = errors.Join(err, cleanup.Close(closeCtx))
		if err != nil {
			result = Result{}
		}
	}()
	if nilBackend(a.Backend) {
		return Result{}, ErrBackendUnavailable
	}
	runtime, err := core.OpenRuntime(ctx, a.Backend)
	if err != nil {
		var pending *core.RuntimeOpenError
		if errors.As(err, &pending) {
			cleanup.Runtime = pending
		}
		return Result{}, err
	}
	cleanup.Runtime = runtime
	if a.Bind != nil {
		cleanup.Binding, err = a.Bind(runtime)
		if err != nil {
			return Result{}, err
		}
	}
	result = Result{Command: request.Command, Agents: make([]core.AgentSnapshot, 0), StartupRecovery: runtime.RecoveryReport()}
	switch request.Command {
	case "init":
		definition := domain.DefinitionRef{ID: request.Definition, Version: "1"}
		if definition.ID == "" {
			definition.ID = "echo"
		}
		name := request.Name
		if name == "" {
			name = definition.ID
		}
		agent, createErr := runtime.CreateAgentContext(ctx, name, definition, nil)
		if createErr != nil {
			return Result{}, createErr
		}
		result.Agent = &agent
	case "submit":
		agent, loadErr := runtime.AgentContext(ctx, request.AgentID)
		if loadErr != nil {
			return Result{}, loadErr
		}
		var eventType string
		switch agent.Definition {
		case domain.DefinitionRef{ID: "echo", Version: "1"}:
			eventType = "echo.request"
		case domain.DefinitionRef{ID: "main", Version: "1"}:
			eventType = "main.request"
		default:
			return Result{}, fmt.Errorf("不支持向definition %s@%s提交消息", agent.Definition.ID, agent.Definition.Version)
		}
		event := domain.Event{ID: request.EventID, Type: eventType, Payload: map[string]any{"message": request.Message}, CreatedAt: time.Now().UTC()}
		received, submitErr := runtime.SubmitContext(ctx, request.AgentID, event)
		if submitErr != nil {
			return Result{}, submitErr
		}
		result.Submission = &Submission{Delivery: received.Delivery, Duplicate: received.Duplicate}
	case "run":
		before, queryErr := queryAll(ctx, runtime)
		if queryErr != nil {
			return Result{}, queryErr
		}
		if runErr := runtime.RunUntilIdleContext(ctx); runErr != nil {
			return Result{}, runErr
		}
		after, queryErr := queryAll(ctx, runtime)
		if queryErr != nil {
			return Result{}, queryErr
		}
		result.Run = &RunResult{Before: summarize(before), After: summarize(after), Agents: after}
	case "status":
		if request.AgentID == "" {
			result.Agents, err = runtime.AgentsContext(ctx)
		} else {
			query, queryErr := runtime.QueryAgentContext(ctx, request.AgentID)
			err = queryErr
			result.Query = &query
		}
	case "retry":
		key := domain.DeliveryKey{AgentID: request.AgentID, EventID: request.EventID}
		err = runtime.Retry(ctx, key)
		result.Retry = &key
	case "resolve":
		receipt, resolveErr := runtime.ResolveAction(ctx, request.ActionID, request.Decision, request.Reason)
		err = resolveErr
		result.Resolution = &receipt
	}
	return result, err
}

func nilBackend(backend core.RecoveryStore) bool {
	if backend == nil {
		return true
	}
	switch value := reflect.ValueOf(backend); value.Kind() {
	case reflect.Chan, reflect.Func, reflect.Interface, reflect.Map, reflect.Pointer, reflect.Slice:
		return value.IsNil()
	}
	return false
}

func (request Request) Validate() error {
	invalid := func(message string) error { return &UsageError{Message: message} }
	for _, value := range []string{request.Command, string(request.AgentID), string(request.EventID), request.Message, request.Name, request.Definition, string(request.ActionID), string(request.Decision), request.Reason} {
		if !utf8.ValidString(value) {
			return invalid("命令参数必须是有效UTF-8文本")
		}
	}
	if request.AgentID != "" && strings.TrimSpace(string(request.AgentID)) == "" || request.EventID != "" && strings.TrimSpace(string(request.EventID)) == "" {
		return invalid("agent_id和event_id不能仅含空白")
	}
	if request.Command != "init" && request.Definition != "" {
		return invalid("definition仅适用于init")
	}
	if request.Command != "resolve" && (request.ActionID != "" || request.Decision != "" || request.Reason != "") {
		return invalid("action_id、decision和reason仅适用于resolve")
	}
	switch request.Command {
	case "init":
		if request.AgentID != "" || request.EventID != "" || request.Message != "" || request.Name != "" && strings.TrimSpace(request.Name) == "" {
			return invalid("init仅接受definition和非空白name")
		}
		if request.Definition != "" && request.Definition != "echo" && request.Definition != "main" {
			return invalid(fmt.Sprintf("不支持的definition %q，仅支持echo或main", request.Definition))
		}
	case "submit":
		if request.AgentID == "" || request.EventID == "" || request.Name != "" {
			return invalid("submit需要agent_id和event_id，仅接受message")
		}
	case "run":
		if request.AgentID != "" || request.EventID != "" || request.Message != "" || request.Name != "" {
			return invalid("run不接受agent_id、event_id、message或name")
		}
	case "status":
		if request.EventID != "" || request.Message != "" || request.Name != "" {
			return invalid("status仅接受agent_id")
		}
	case "retry":
		if request.AgentID == "" || request.EventID == "" || request.Message != "" || request.Name != "" {
			return invalid("retry仅接受必填的agent_id和event_id")
		}
	case "resolve":
		if strings.TrimSpace(string(request.ActionID)) == "" || request.AgentID != "" || request.EventID != "" || request.Message != "" || request.Name != "" {
			return invalid("resolve需要action_id，仅接受decision和reason")
		}
		if request.Decision != core.ResolutionRetry && request.Decision != core.ResolutionAbandon {
			return invalid("resolve的decision仅支持retry或abandon")
		}
		if strings.TrimSpace(request.Reason) == "" || utf8.RuneCountInString(request.Reason) > 1024 {
			return invalid("resolve的reason必须为非空白文本，且不超过1024个字符")
		}
	default:
		return invalid(fmt.Sprintf("未知命令%q", request.Command))
	}
	return nil
}

func queryAll(ctx context.Context, runtime *core.Runtime) ([]core.AgentQuery, error) {
	agents, err := runtime.AgentsContext(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]core.AgentQuery, 0, len(agents))
	for _, agent := range agents {
		query, err := runtime.QueryAgentContext(ctx, agent.ID)
		if err != nil {
			return nil, err
		}
		result = append(result, query)
	}
	return result, nil
}

func summarize(agents []core.AgentQuery) Summary {
	result := Summary{
		Agents: len(agents),
		Deliveries: map[domain.DeliveryStatus]int{
			domain.DeliveryStatusPending: 0, domain.DeliveryStatusRunning: 0,
			domain.DeliveryStatusCompleted: 0, domain.DeliveryStatusFailed: 0,
		},
		Actions: map[domain.ActionStatus]int{
			domain.ActionStatusPending: 0, domain.ActionStatusRunning: 0, domain.ActionStatusSucceeded: 0,
			domain.ActionStatusFailed: 0, domain.ActionStatusUnknown: 0,
		},
	}
	for _, agent := range agents {
		for _, delivery := range agent.Deliveries {
			result.Deliveries[delivery.Delivery.Status]++
			result.ExecutionAttempts += uint64(len(delivery.Attempts))
		}
		for _, action := range agent.Actions {
			result.Actions[action.Action.Status]++
			result.ActionAttempts += uint64(len(action.Attempts))
		}
	}
	return result
}
