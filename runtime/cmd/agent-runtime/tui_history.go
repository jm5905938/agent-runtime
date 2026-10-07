package main

import (
	"agent-runtime/core"
	"agent-runtime/domain"
	"encoding/json"
	"fmt"
	"strings"
	"unicode"

	"github.com/charmbracelet/x/ansi"
)

func tuiTranscript(query core.AgentQuery, width int, expanded bool) string {
	turns := make(map[domain.ID]conversationTurn)
	for _, turn := range conversationTurns(query) {
		turns[turn.requestID] = turn
	}
	requests := make(map[domain.ID]domain.ID)
	for _, delivery := range query.Deliveries {
		if delivery.Execution == nil || delivery.Execution.Result == nil {
			continue
		}
		request, _ := delivery.Execution.Result.StateUpdate["request_event_id"].(string)
		if request != "" {
			requests[delivery.Delivery.ExecutionID] = domain.ID(request)
		}
	}
	actions := make(map[domain.ID][]core.ActionQuery)
	for _, action := range query.Actions {
		if action.Action.Request.ExecutionID != nil {
			requestID := requests[*action.Action.Request.ExecutionID]
			actions[requestID] = append(actions[requestID], action)
		}
	}
	var rows []string
	for _, delivery := range query.Deliveries {
		if delivery.Event.Type != "main.request" {
			continue
		}
		message, _ := delivery.Event.Payload["message"].(string)
		rows = append(rows, tuiAccent.Bold(true).Render("› 你"), tuiWrap(message, width), "")
		for _, action := range actions[delivery.Event.ID] {
			if action.Action.Request.Type == "model.generate" {
				continue
			}
			rows = append(rows, tuiTool(action, width, expanded))
		}
		if turn, ok := turns[delivery.Event.ID]; ok {
			if turn.status == "succeeded" {
				rows = append(rows, tuiAccent.Bold(true).Render("• 助手"), tuiWrap(turn.reply, width), "")
			} else {
				rows = append(rows, tuiWarning.Render(tuiWrap("本轮失败："+turn.failure, width)), "")
			}
		} else if delivery.Delivery.Status == domain.DeliveryStatusFailed {
			rows = append(rows, tuiWarning.Render("执行中断 · /resume 继续"), "")
		} else if delivery.Delivery.Status == domain.DeliveryStatusPending {
			rows = append(rows, tuiMuted.Render("等待执行"), "")
		}
	}
	if len(rows) == 0 {
		return tuiAccent.Bold(true).Render("开始对话") + "\n\n" + tuiWrap("输入任务，按Enter发送，执行中可继续补充消息\n\n历史自动保存，输入/help查看命令", width)
	}
	for _, task := range query.Tasks {
		status := "执行中"
		if task.CancelRequested {
			status = "取消中"
		}
		if task.Result != nil {
			status = string(task.Result.Status)
		}
		rows = append(rows, tuiMuted.Render(tuiWrap(fmt.Sprintf("subagent任务%s · %s", task.ID, status), width)))
	}
	return strings.Join(rows, "\n")
}

func tuiTool(action core.ActionQuery, width int, expanded bool) string {
	symbol, status := "○", "等待执行"
	switch action.Action.Status {
	case domain.ActionStatusRunning:
		symbol, status = "◌", "执行中"
	case domain.ActionStatusSucceeded:
		symbol, status = "✓", "完成"
	case domain.ActionStatusFailed:
		symbol, status = "✗", "失败"
	case domain.ActionStatusUnknown:
		symbol, status = "?", "结果未知"
	}
	if len(action.Attempts) != 0 {
		last := action.Attempts[len(action.Attempts)-1]
		if last.FinishedAt != nil {
			status += fmt.Sprintf(" · %.1fs", last.FinishedAt.Sub(last.StartedAt).Seconds())
		}
	}
	header := tuiMuted.Render(tuiWrap("  "+symbol+" "+action.Action.Request.Type+" · "+status, width))
	if !expanded {
		return header
	}
	var detail string
	if action.Action.Result != nil {
		if action.Action.Result.Error != nil {
			detail = action.Action.Result.Error.Message
		} else if output, err := json.MarshalIndent(action.Action.Result.Output, "", "  "); err == nil {
			detail = string(output)
		}
	} else if action.Action.LastError != nil {
		detail = action.Action.LastError.Message
	}
	if len([]rune(detail)) > 2000 {
		detail = string([]rune(detail)[:2000]) + "\n…"
	}
	if detail != "" {
		header += "\n" + tuiMuted.Render(tuiWrap(detail, width))
	}
	return header
}

func tuiWrap(value string, width int) string {
	return ansi.Hardwrap(tuiText(value), max(1, width), true)
}

// 历史与工具输出只作为文本显示，不执行其中的终端控制序列。
func tuiText(value string) string {
	return strings.Map(func(r rune) rune {
		if r == '\t' {
			return ' '
		}
		if r != '\n' && unicode.IsControl(r) {
			return -1
		}
		return r
	}, ansi.Strip(value))
}
