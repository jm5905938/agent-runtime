package main

import (
	"agent-runtime/cli"
	"agent-runtime/core"
	"agent-runtime/domain"
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode/utf8"
)

type conversationTurn struct {
	eventID   domain.ID
	requestID domain.ID
	status    string
	reply     string
	failure   string
}

type conversationTurnError struct{ message string }

func (err *conversationTurnError) Error() string { return err.message }

func noConversationAgent(err error) bool {
	var cleanup *conversationCleanupError
	return errors.Is(err, errNoMainAgent) && !errors.As(err, &cleanup)
}

func conversationTurns(query core.AgentQuery) []conversationTurn {
	var turns []conversationTurn
	for _, delivery := range query.Deliveries {
		if delivery.Execution == nil || delivery.Execution.Result == nil || delivery.Delivery.Status != domain.DeliveryStatusCompleted {
			continue
		}
		state := delivery.Execution.Result.StateUpdate
		status, _ := state["request_status"].(string)
		requestID, _ := state["request_event_id"].(string)
		if requestID == "" || (status != "succeeded" && status != "failed") {
			continue
		}
		reply, _ := state["result"].(string)
		failure, _ := state["error"].(string)
		turns = append(turns, conversationTurn{delivery.Event.ID, domain.ID(requestID), status, reply, failure})
	}
	return turns
}

func waitingUnknownConversationAction(query core.AgentQuery) *core.ActionQuery {
	if query.Agent.State["request_status"] != "waiting" {
		return nil
	}
	for i := range query.Actions {
		action := &query.Actions[i]
		if query.Agent.State["waiting_action_id"] == string(action.Action.Request.ID) && action.Action.Status == domain.ActionStatusUnknown &&
			action.Action.RecoveryPolicy == domain.RecoveryPolicyManual && action.Resolution == nil {
			return action
		}
	}
	return nil
}

func conversationSnapshot(ctx context.Context, options commandOptions, open backendOpener, create bool) (query core.AgentQuery, err error) {
	options.request.Command = "status"
	err = withConversation(ctx, options, open, create, func(runtime *core.Runtime, agent core.AgentSnapshot, _ func(context.Context) error) error {
		query, err = runtime.QueryAgentContext(ctx, agent.ID)
		return err
	})
	return query, err
}

func drainConversation(ctx context.Context, options commandOptions, open backendOpener) (query core.AgentQuery, err error) {
	err = withConversation(ctx, options, open, false, func(runtime *core.Runtime, agent core.AgentSnapshot, prepare func(context.Context) error) error {
		if options.request.Command == "resume" {
			if err := retryConversationDelivery(ctx, runtime, agent.ID, options.request.Decision, prepare); err != nil {
				return err
			}
		}
		if err := runtime.RunAgentUntilIdleContext(ctx, agent.ID); err != nil {
			return err
		}
		query, err = runtime.QueryAgentContext(ctx, agent.ID)
		return err
	})
	return query, err
}

// resume可重新排队当前失败的纯Agent执行，已完成的外部调用不会重做。
func retryConversationDelivery(ctx context.Context, runtime *core.Runtime, agentID domain.ID, decision core.ResolutionDecision, prepare func(context.Context) error) error {
	query, err := runtime.QueryAgentContext(ctx, agentID)
	if err != nil {
		return err
	}
	index := -1
	for i, delivery := range query.Deliveries {
		if delivery.Delivery.Status != domain.DeliveryStatusFailed {
			continue
		}
		candidate := delivery
		candidate.Ready = true
		if waitingResultMatches(query.Agent, candidate) || waitingResolutionMatches(query.Agent, candidate) {
			if delivery.Event.Type == core.ActionResolutionEventType && decision != "" && delivery.Event.Payload["decision"] != string(decision) {
				return fmt.Errorf("当前调用已登记其他决定，使用resume继续原决定: %w", core.ErrStoreConflict)
			}
			index = i
			break
		}
	}
	if index < 0 && query.Agent.State["request_status"] != "waiting" {
		// 后续输入已取代的历史失败轮不应因resume而再次执行。
		for i := len(query.Deliveries) - 1; i >= 0; i-- {
			delivery := query.Deliveries[i]
			if delivery.Event.Type != "main.request" {
				continue
			}
			if delivery.Delivery.Status == domain.DeliveryStatusFailed {
				index = i
			}
			break
		}
	}
	if index < 0 {
		return nil
	}
	query.Deliveries[index].Ready = true
	query.Deliveries[index].BlockedBy = nil
	query.Deliveries[index].Delivery.Status = domain.DeliveryStatusPending
	if queryNeedsModel(query) {
		if err := prepare(ctx); err != nil {
			return err
		}
	}
	return runtime.Retry(ctx, query.Deliveries[index].Delivery.Key)
}

func sendConversationMessage(ctx context.Context, options commandOptions, open backendOpener, message string) (result chatResult, err error) {
	if !utf8.ValidString(message) {
		return result, &cli.UsageError{Message: "输入必须是有效UTF-8文本"}
	}
	request := domain.NewEvent("main.request", map[string]any{"message": message})
	err = withConversation(ctx, options, open, true, func(runtime *core.Runtime, agent core.AgentSnapshot, prepare func(context.Context) error) error {
		before, err := runtime.QueryAgentContext(ctx, agent.ID)
		if err != nil {
			return err
		}
		// 等待人工处理时允许继续保存输入，模型配置在真正执行前准备。
		if waitingUnknownConversationAction(before) == nil {
			if err := prepare(ctx); err != nil {
				return err
			}
		}
		if _, err := runtime.SubmitContext(ctx, agent.ID, request); err != nil {
			return err
		}
		if err := runtime.RunAgentUntilIdleContext(ctx, agent.ID); err != nil {
			return err
		}
		query, err := runtime.QueryAgentContext(ctx, agent.ID)
		if err != nil {
			return err
		}
		result = chatResult{AgentID: agent.ID, Status: "pending", RequestEventID: request.ID, Queued: true, Actions: len(query.Actions)}
		for _, delivery := range query.Deliveries {
			if delivery.Execution != nil {
				result.Executions++
			}
		}
		for _, turn := range conversationTurns(query) {
			if turn.requestID != request.ID {
				continue
			}
			if turn.status == "failed" {
				return &conversationTurnError{message: fmt.Sprintf("MainAgent请求失败: %s", turn.failure)}
			}
			result.Status, result.Result, result.Queued = turn.status, turn.reply, false
			return nil
		}
		if query.Agent.State["request_event_id"] == string(request.ID) && query.Agent.State["request_status"] == "waiting" {
			result.Status, result.Queued = "waiting", false
		}
		waiting, _ := query.Agent.State["waiting_action_id"].(string)
		result.WaitingActionID = domain.ID(waiting)
		for _, delivery := range query.Deliveries {
			if delivery.Event.ID == request.ID && delivery.Delivery.Status == domain.DeliveryStatusFailed {
				return &conversationTurnError{message: fmt.Sprintf("MainAgent输入处理失败: %s", delivery.Execution.Error)}
			}
		}
		return nil
	})
	return result, err
}

func resolveConversation(ctx context.Context, options commandOptions, open backendOpener, actionID domain.ID, decision core.ResolutionDecision, reason string) (query core.AgentQuery, receipt core.ActionResolutionReceipt, err error) {
	err = withConversation(ctx, options, open, false, func(runtime *core.Runtime, agent core.AgentSnapshot, prepare func(context.Context) error) error {
		before, err := runtime.QueryAgentContext(ctx, agent.ID)
		if err != nil {
			return err
		}
		needed := decision == core.ResolutionRetry
		for _, delivery := range before.Deliveries {
			needed = needed || delivery.Event.Type == "main.request" && delivery.Delivery.Status == domain.DeliveryStatusPending
		}
		if needed {
			if err := prepare(ctx); err != nil {
				return err
			}
		}
		receipt, err = runtime.ResolveAction(ctx, actionID, decision, reason)
		if err != nil {
			return err
		}
		if err := runtime.RunAgentUntilIdleContext(ctx, agent.ID); err != nil {
			return err
		}
		query, err = runtime.QueryAgentContext(ctx, agent.ID)
		return err
	})
	return query, receipt, err
}

func writeConversationStatus(output io.Writer, query core.AgentQuery) error {
	status, _ := query.Agent.State["request_status"].(string)
	label := map[string]string{"": "空闲", "idle": "空闲", "succeeded": "已完成", "failed": "本轮失败", "waiting": "等待结果"}[status]
	if label == "" {
		label = status
	}
	if waitingUnknownConversationAction(query) != nil {
		label = "调用结果未知，需要选择重试或放弃"
	}
	queued := 0
	for _, delivery := range query.Deliveries {
		if delivery.Event.Type == "main.request" && delivery.Delivery.Status == domain.DeliveryStatusPending {
			queued++
		}
	}
	_, err := fmt.Fprintf(output, "MainAgent：%s\n状态：%s\n排队输入：%d\n", query.Agent.Name, label, queued)
	return err
}

type conversationInput struct {
	reader  io.Reader
	scanner *bufio.Scanner
}

func newConversationInput(reader io.Reader) *conversationInput {
	if reader == nil {
		reader = strings.NewReader("")
	}
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 4096), 1<<20)
	return &conversationInput{reader: reader, scanner: scanner}
}

func (input *conversationInput) line(ctx context.Context) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	type answer struct {
		line string
		err  error
	}
	done := make(chan answer, 1)
	go func() {
		if input.scanner.Scan() {
			done <- answer{line: input.scanner.Text()}
			return
		}
		err := input.scanner.Err()
		if err == nil {
			err = io.EOF
		}
		done <- answer{err: err}
	}()
	select {
	case result := <-done:
		if err := ctx.Err(); err != nil {
			return "", err
		}
		return result.line, result.err
	case <-ctx.Done():
		if closer, ok := input.reader.(io.Closer); ok {
			_ = closer.Close()
		}
		return "", ctx.Err()
	}
}

func chooseConversationResolution(ctx context.Context, input *conversationInput, output io.Writer) (core.ResolutionDecision, bool, error) {
	for {
		if _, err := fmt.Fprint(output, "调用结果未知，请选择：1重试（可能再次计费），2放弃本轮，3暂不处理\n选择> "); err != nil {
			return "", false, err
		}
		line, err := input.line(ctx)
		if errors.Is(err, io.EOF) {
			return "", true, nil
		}
		if err != nil {
			return "", false, err
		}
		switch strings.TrimSpace(line) {
		case "1", "retry", "重试":
			return core.ResolutionRetry, false, nil
		case "2", "abandon", "放弃":
			return core.ResolutionAbandon, false, nil
		case "3":
			return "", false, nil
		case "/exit", "/quit":
			return "", true, nil
		}
		if _, err := fmt.Fprintln(output, "请输入1、2或3。"); err != nil {
			return "", false, err
		}
	}
}

func runConversationCommand(ctx context.Context, options commandOptions, stdin io.Reader, output io.Writer, open backendOpener) error {
	if options.friendlyStatus {
		query, err := conversationSnapshot(ctx, options, open, false)
		if noConversationAgent(err) {
			if options.asJSON {
				return json.NewEncoder(output).Encode(cli.Result{Command: "status", Agents: []core.AgentSnapshot{}})
			}
			_, err = fmt.Fprintln(output, errNoMainAgent)
			return err
		}
		if err != nil {
			return err
		}
		if options.asJSON {
			return json.NewEncoder(output).Encode(cli.Result{Command: "status", Agents: []core.AgentSnapshot{query.Agent}, Query: &query, StartupRecovery: query.StartupRecovery})
		}
		return writeConversationStatus(output, query)
	}
	if options.request.Command == "chat" && options.hasMessage {
		result, err := sendConversationMessage(ctx, options, open, options.request.Message)
		if err != nil {
			return err
		}
		if options.asJSON {
			return json.NewEncoder(output).Encode(result)
		}
		if result.Status != "succeeded" {
			_, err = fmt.Fprintln(output, "输入已保存，等待当前调用恢复。运行agent-runtime resume继续。")
		} else {
			_, err = fmt.Fprintln(output, result.Result)
		}
		return err
	}
	return runConversationLoop(ctx, options, newConversationInput(stdin), output, open)
}

func runConversationLoop(ctx context.Context, options commandOptions, input *conversationInput, output io.Writer, open backendOpener) error {
	query, err := conversationSnapshot(ctx, options, open, options.request.Command == "chat")
	if noConversationAgent(err) {
		if options.asJSON {
			return json.NewEncoder(output).Encode(struct {
				Command string `json:"command"`
				Status  string `json:"status"`
			}{options.request.Command, "not_initialized"})
		}
		_, err = fmt.Fprintln(output, errNoMainAgent)
		return err
	}
	if err != nil {
		return err
	}
	options.request.AgentID = query.Agent.ID
	displayed := make(map[domain.ID]bool)
	for _, turn := range conversationTurns(query) {
		displayed[turn.eventID] = true
	}
	display := func(query core.AgentQuery) error {
		if options.asJSON {
			return nil
		}
		for _, turn := range conversationTurns(query) {
			if displayed[turn.eventID] {
				continue
			}
			displayed[turn.eventID] = true
			if turn.status == "failed" {
				_, err = fmt.Fprintln(output, "本轮结束："+turn.failure)
			} else {
				_, err = fmt.Fprintln(output, turn.reply)
			}
			if err != nil {
				return err
			}
		}
		return nil
	}
	query, err = drainConversation(ctx, options, open)
	if err != nil {
		return err
	}
	if err := display(query); err != nil {
		return err
	}
	if options.request.Command == "chat" {
		if _, err := fmt.Fprintln(output, "持久对话已就绪。/status查看状态，/resume处理未知调用，/exit退出。"); err != nil {
			return err
		}
	}
	var deferred domain.ID
	var lastReceipt *core.ActionResolutionReceipt
	for {
		unknown := waitingUnknownConversationAction(query)
		if unknown != nil && unknown.Action.Request.ID != deferred {
			decision, reason := options.request.Decision, options.request.Reason
			if decision == "" {
				var exit bool
				decision, exit, err = chooseConversationResolution(ctx, input, output)
				if err != nil {
					return err
				}
				if exit {
					return nil
				}
				if decision == core.ResolutionRetry {
					reason = "用户选择重试"
				} else {
					reason = "用户选择放弃"
				}
			}
			if decision == "" {
				deferred = unknown.Action.Request.ID
			} else {
				var receipt core.ActionResolutionReceipt
				query, receipt, err = resolveConversation(ctx, options, open, unknown.Action.Request.ID, decision, reason)
				if err != nil {
					return err
				}
				lastReceipt = &receipt
				if err := display(query); err != nil {
					return err
				}
				if options.request.Decision == "" {
					continue
				}
			}
		}
		if options.request.Command == "resume" {
			if options.asJSON {
				return json.NewEncoder(output).Encode(struct {
					Command    string                        `json:"command"`
					Agent      core.AgentSnapshot            `json:"agent"`
					Resolution *core.ActionResolutionReceipt `json:"resolution,omitempty"`
				}{"resume", query.Agent, lastReceipt})
			}
			return writeConversationStatus(output, query)
		}
		if _, err := fmt.Fprint(output, "你> "); err != nil {
			return err
		}
		line, err := input.line(ctx)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		switch strings.TrimSpace(line) {
		case "/exit", "/quit":
			return nil
		case "/status":
			query, err = conversationSnapshot(ctx, options, open, false)
			if err == nil {
				err = writeConversationStatus(output, query)
			}
		case "/resume":
			deferred = ""
			resume := options
			resume.request.Command = "resume"
			query, err = drainConversation(ctx, resume, open)
		default:
			if strings.TrimSpace(line) == "" {
				continue
			}
			_, err = sendConversationMessage(ctx, options, open, line)
			var failed *conversationTurnError
			var cleanup *conversationCleanupError
			if err == nil || errors.As(err, &failed) && !errors.As(err, &cleanup) {
				query, err = conversationSnapshot(ctx, options, open, false)
				if err == nil && failed != nil {
					finished := false
					for _, turn := range conversationTurns(query) {
						finished = finished || !displayed[turn.eventID]
					}
					if !finished {
						_, err = fmt.Fprintln(output, failed.Error()+"；输入已保存，可使用/resume继续。")
					}
				}
			}
		}
		if err != nil {
			return err
		}
		if err := display(query); err != nil {
			return err
		}
	}
}
