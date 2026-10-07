package main

import (
	"agent-runtime/core"
	"agent-runtime/domain"
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/charmbracelet/bubbles/key"
	"github.com/charmbracelet/bubbles/spinner"
	"github.com/charmbracelet/bubbles/textarea"
	"github.com/charmbracelet/bubbles/viewport"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
	"github.com/charmbracelet/x/ansi"
)

type tuiTickMsg struct{}
type tuiQueryMsg struct {
	query   core.AgentQuery
	err     error
	epoch   uint64
	ready   bool
	unknown *core.ActionQuery
}
type tuiSubmitMsg struct{ err error }
type tuiRunMsg struct{ err error }

type tuiModel struct {
	session   *tuiSession
	query     core.AgentQuery
	input     textarea.Model
	history   viewport.Model
	spinner   spinner.Model
	directory string
	modelName string
	width     int
	height    int
	epoch     uint64
	querying  bool
	running   bool
	paused    bool
	deferred  bool
	expanded  bool
	help      bool
	posting   bool
	quitting  bool
	outbox    []domain.Event
	cancel    context.CancelFunc
	started   time.Time
	notice    string
	treeReady bool
	unknown   *core.ActionQuery

	visibleTasks map[domain.ID]bool
}

var tuiAccent = lipgloss.NewStyle().Foreground(lipgloss.Color("114"))
var tuiMuted = lipgloss.NewStyle().Foreground(lipgloss.Color("244"))
var tuiWarning = lipgloss.NewStyle().Foreground(lipgloss.Color("215"))

func newTUIModel(session *tuiSession, query core.AgentQuery, directory string) *tuiModel {
	input := textarea.New()
	input.Placeholder = "输入消息，/help查看命令"
	input.Prompt = "› "
	input.ShowLineNumbers = false
	input.CharLimit = 0
	input.SetHeight(3)
	input.Focus()
	input.KeyMap.InsertNewline = key.NewBinding(key.WithKeys("alt+enter", "ctrl+j"))
	spin := spinner.New(spinner.WithSpinner(spinner.Dot), spinner.WithStyle(tuiAccent))
	m := &tuiModel{session: session, query: query, input: input, history: viewport.New(76, 15),
		spinner: spin, directory: directory, width: 80, height: 26, visibleTasks: make(map[domain.ID]bool)}
	for _, task := range query.Tasks {
		m.visibleTasks[task.ID] = task.Result == nil
	}
	m.resize()
	m.refreshHistory()
	return m
}

func (m *tuiModel) Init() tea.Cmd {
	return tea.Batch(textarea.Blink, m.spinner.Tick, tuiTick(), m.start(false, ""))
}

func tuiTick() tea.Cmd {
	return tea.Tick(time.Second, func(time.Time) tea.Msg { return tuiTickMsg{} })
}

func (m *tuiModel) snapshot() tea.Cmd {
	if m.querying {
		return nil
	}
	m.querying = true
	epoch := m.epoch
	return func() tea.Msg {
		query, err := m.session.query()
		var ready bool
		var unknown *core.ActionQuery
		if err == nil {
			ready, unknown, err = m.session.treeState()
		}
		return tuiQueryMsg{query: query, err: err, epoch: epoch, ready: ready, unknown: unknown}
	}
}

func (m *tuiModel) start(resume bool, decision core.ResolutionDecision) tea.Cmd {
	if m.running || m.paused || m.posting || m.quitting {
		return nil
	}
	if !resume && !(tuiReady(m.query) || m.treeReady) {
		return nil
	}
	ctx, cancel := context.WithCancel(m.session.ctx)
	m.cancel, m.running = cancel, true
	m.started, m.notice = time.Now(), ""
	m.epoch++
	return func() tea.Msg {
		defer cancel()
		return tuiRunMsg{err: m.session.run(ctx, resume, decision)}
	}
}

func (m *tuiModel) post() tea.Cmd {
	if m.posting || len(m.outbox) == 0 {
		return nil
	}
	m.posting = true
	event := m.outbox[0]
	return func() tea.Msg { return tuiSubmitMsg{err: m.session.submit(event)} }
}

func (m *tuiModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
		m.resize()
		m.refreshHistory()
	case tuiTickMsg:
		return m, tea.Batch(tuiTick(), m.snapshot())
	case tuiQueryMsg:
		m.querying = false
		if msg.epoch != m.epoch {
			return m, m.snapshot()
		}
		if msg.err != nil {
			m.notice = msg.err.Error()
			return m, nil
		}
		m.query = msg.query
		for _, task := range msg.query.Tasks {
			if _, known := m.visibleTasks[task.ID]; !known || task.Result == nil {
				m.visibleTasks[task.ID] = true
			}
		}
		m.treeReady, m.unknown = msg.ready, msg.unknown
		m.refreshHistory()
		return m, m.start(false, "")
	case tuiSubmitMsg:
		m.posting = false
		m.epoch++
		if msg.err != nil {
			m.quitting = false
			// 保留原event，重试保存不会生成第二条输入。
			m.notice = "保存输入失败：" + msg.err.Error() + "，/send重试"
			m.refreshHistory()
			return m, m.snapshot()
		}
		m.outbox = m.outbox[1:]
		if m.quitting && len(m.outbox) == 0 {
			return m, tea.Quit
		}
		return m, tea.Batch(m.post(), m.snapshot())
	case tuiRunMsg:
		m.running, m.cancel = false, nil
		m.epoch++
		if errors.Is(msg.err, context.Canceled) {
			m.notice = "已停止，/resume继续，结果未知时需先确认"
		}
		if msg.err != nil && !errors.Is(msg.err, context.Canceled) {
			m.notice, m.paused = msg.err.Error(), true
		}
		// 完成后重新读取，覆盖最后一次扫描与新输入提交之间的空隙。
		return m, m.snapshot()
	case tea.KeyMsg:
		switch msg.String() {
		case "ctrl+c":
			return m, m.quit()
		case "esc":
			if m.running {
				m.paused = true
				m.cancel()
				m.notice = "正在停止，已保存的输入会保留"
			} else {
				m.deferred = true
			}
			return m, nil
		case "alt+r":
			return m, m.resume(core.ResolutionRetry)
		case "alt+a":
			return m, m.resume(core.ResolutionAbandon)
		case "ctrl+t":
			m.expanded = !m.expanded
			m.refreshHistory()
			return m, nil
		case "pgup", "pgdown", "ctrl+home", "ctrl+end":
			if msg.String() == "ctrl+home" {
				m.history.GotoTop()
			} else if msg.String() == "ctrl+end" {
				m.history.GotoBottom()
			} else {
				m.history, _ = m.history.Update(msg)
			}
			return m, nil
		case "enter":
			value := strings.TrimSpace(m.input.Value())
			if value == "" {
				return m, nil
			}
			if strings.HasPrefix(value, "/") {
				cmd, accepted := m.command(value)
				if accepted {
					m.input.Reset()
				}
				return m, cmd
			}
			m.outbox = append(m.outbox, domain.NewEvent("main.request", map[string]any{"message": value}))
			for _, task := range m.query.Tasks {
				if task.Result != nil {
					m.visibleTasks[task.ID] = false
				}
			}
			m.help = false
			m.input.Reset()
			m.history.GotoBottom()
			m.refreshHistory()
			return m, m.post()
		}
	}
	var inputCmd, spinCmd tea.Cmd
	m.input, inputCmd = m.input.Update(msg)
	m.spinner, spinCmd = m.spinner.Update(msg)
	return m, tea.Batch(inputCmd, spinCmd)
}

func (m *tuiModel) resume(decision core.ResolutionDecision) tea.Cmd {
	if m.running || m.posting {
		m.notice = "请等待当前操作完成"
		return nil
	}
	if decision == core.ResolutionRetry && cancelledUnknownAction(m.query, m.waitingUnknown()) {
		m.notice = "任务已取消，请放弃未知结果"
		return nil
	}
	if m.waitingUnknown() != nil && decision == "" {
		m.deferred = false
		return nil
	}
	m.paused, m.deferred = false, false
	return m.start(true, decision)
}

func (m *tuiModel) command(value string) (tea.Cmd, bool) {
	switch value {
	case "/exit", "/quit":
		return m.quit(), true
	case "/resume":
		return m.resume(""), true
	case "/retry":
		return m.resume(core.ResolutionRetry), true
	case "/abandon":
		return m.resume(core.ResolutionAbandon), true
	case "/later":
		m.deferred = true
	case "/send":
		return m.post(), true
	case "/status":
		return m.snapshot(), true
	case "/help":
		m.help = !m.help
		m.refreshHistory()
		m.history.GotoBottom()
	default:
		m.notice = "未知命令，/help查看帮助"
		return nil, false
	}
	return nil, true
}

func (m *tuiModel) quit() tea.Cmd {
	if m.quitting || len(m.outbox) == 0 {
		return tea.Quit
	}
	m.quitting, m.paused = true, true
	if m.cancel != nil {
		m.cancel()
	}
	m.notice = "保存后退出，再按Ctrl+C立即退出"
	return m.post()
}

func (m *tuiModel) resize() {
	w := max(1, m.width-4)
	m.input.SetWidth(w)
	m.input.SetHeight(3)
	m.history.Width = w
	m.history.Height = max(1, m.height-12)
}

func (m *tuiModel) refreshHistory() {
	bottom := m.history.AtBottom()
	query := m.query
	query.Tasks = nil
	for _, task := range m.query.Tasks {
		if task.Result == nil || m.visibleTasks[task.ID] {
			query.Tasks = append(query.Tasks, task)
		}
	}
	content := tuiTranscript(query, m.history.Width, m.expanded)
	saved := make(map[domain.ID]bool)
	for _, delivery := range m.query.Deliveries {
		saved[delivery.Event.ID] = true
	}
	for _, event := range m.outbox {
		if !saved[event.ID] {
			message, _ := event.Payload["message"].(string)
			content += "\n\n" + tuiAccent.Render("› 你 · 待保存") + "\n" + tuiWrap(message, m.history.Width)
		}
	}
	if m.help {
		content += "\n\n" + tuiWrap("命令与快捷键\n/resume 继续执行\n/retry 或 Alt+R 重试未知调用\n/abandon 或 Alt+A 放弃未知调用\n/later 或 Esc 稍后处理\n/send 重试保存失败的输入\n/status 刷新状态\n/exit 或 Ctrl+C 退出\nCtrl+T 展开工具详情\nPgUp/PgDn 滚动历史，Ctrl+Home/End 跳到首尾\n/help 关闭帮助", m.history.Width)
	}
	m.history.SetContent(content)
	if bottom {
		m.history.GotoBottom()
	}
}

func (m *tuiModel) View() string {
	if m.width < 32 || m.height < 14 {
		return ansi.Truncate("窗口至少需要32列、14行", max(1, m.width), "…")
	}
	w := max(1, m.width-4)
	line := tuiMuted.Render(strings.Repeat("─", w))
	header := tuiAccent.Bold(true).Render("agent-runtime") + "  " + tuiMuted.Render("main · "+tuiText(m.directory))
	if m.modelName != "" {
		header = tuiAccent.Bold(true).Render("agent-runtime") + "  " + tuiMuted.Render(tuiText(m.modelName+" · "+m.directory))
	}
	status := "就绪"
	if m.running {
		status = m.spinner.View() + fmt.Sprintf(" 执行中 · %.0fs", time.Since(m.started).Seconds())
		if m.query.Agent.State["waiting_action_type"] == "model.generate" {
			status += " · 等待模型回复"
		}
	} else if m.waitingUnknown() != nil {
		status = "调用结果未知 · 请重试或放弃"
	} else if m.paused {
		status = "已暂停 · /resume 继续"
	}
	queued := 0
	for _, delivery := range m.query.Deliveries {
		if delivery.Event.Type == "main.request" && delivery.Delivery.Status == domain.DeliveryStatusPending {
			queued++
		}
	}
	status += fmt.Sprintf("  · 排队 %d", queued)
	if len(m.outbox) != 0 {
		status += fmt.Sprintf(" · 待保存 %d", len(m.outbox))
	}
	notice := tuiText(m.notice)
	if m.waitingUnknown() != nil && !m.deferred && !m.running {
		notice = "Alt+R 重试 · Alt+A 放弃本轮 · Esc 稍后"
		if cancelledUnknownAction(m.query, m.waitingUnknown()) {
			notice = "任务已取消 · Alt+A 放弃未知结果 · Esc 稍后"
		}
	}
	rows := []string{ansi.Truncate(strings.ReplaceAll(header, "\n", " "), w, "…"), line, m.history.View(), "", ansi.Truncate(status, w, "…"),
		tuiWarning.Render(ansi.Truncate(notice, w, "…")), line, m.input.View(), line,
		tuiMuted.Render(ansi.Truncate("Enter 发送 · Alt+Enter 换行 · Esc 停止 · Ctrl+C 退出 · PgUp/PgDn 历史 · Ctrl+T 工具详情", w, "…"))}
	return lipgloss.NewStyle().Padding(0, 2).Render(strings.Join(rows, "\n"))
}

func (m *tuiModel) waitingUnknown() *core.ActionQuery {
	if m.unknown != nil {
		return m.unknown
	}
	return waitingUnknownConversationAction(m.query)
}

func tuiReady(query core.AgentQuery) bool {
	for _, delivery := range query.Deliveries {
		if delivery.Ready {
			return true
		}
	}
	for _, action := range query.Actions {
		if action.Ready {
			return true
		}
	}
	return false
}
