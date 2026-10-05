package main

import (
	"agent-runtime/core"
	"agent-runtime/domain"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/x/ansi"
)

func TestTUICommandRequiresTerminal(t *testing.T) {
	options, err := parseCommand([]string{"tui"})
	if err != nil || options.dataDir == "" {
		t.Fatalf("tui没有默认数据目录: %+v %v", options, err)
	}
	for _, args := range [][]string{{"tui", "--json"}, {"tui", "--message", "hello"}, {"tui", "--agent", " "}} {
		if _, err := parseCommand(args); err == nil {
			t.Fatalf("接受了无效参数: %v", args)
		}
	}
	var stdout, stderr bytes.Buffer
	code := runCommandWithInput(context.Background(), []string{"tui"}, strings.NewReader(""), &stdout, &stderr, nil, nil)
	if code != 1 || !strings.Contains(stderr.String(), "交互终端") || stdout.Len() != 0 {
		t.Fatalf("非终端没有清晰报错: %d %s %s", code, &stdout, &stderr)
	}
}

func TestTUISessionCancelQueueAndResume(t *testing.T) {
	started := make(chan struct{})
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		io.Copy(io.Discard, r.Body)
		call := calls.Add(1)
		if call == 1 {
			close(started)
			<-r.Context().Done()
			return
		}
		if call == 2 {
			mainToolsReply(t, w, "", mainToolsCall("tui-status", `{}`))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"choices":[{"message":{"role":"assistant","content":"收到"},"finish_reason":"stop"}]}`)
	}))
	defer server.Close()
	directory := t.TempDir()
	args := append([]string{"tui", "--data-dir", directory, "--env-file", chatTestConfig(t, server.URL)}, chatTestPython(t)...)
	options, err := parseCommand(args)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var agentID domain.ID
	err = withConversation(ctx, options, openCommandBackend, true, func(runtime *core.Runtime, agent core.AgentSnapshot, prepare func(context.Context) error) error {
		agentID = agent.ID
		session := newTUISession(ctx, runtime, agent.ID, prepare)
		defer session.close()
		if err := session.submit(domain.NewEvent("main.request", map[string]any{"message": "第一条"})); err != nil {
			return err
		}
		runCtx, stop := context.WithCancel(ctx)
		defer stop()
		done := make(chan error, 1)
		go func() { done <- session.run(runCtx, false, "") }()
		select {
		case <-started:
		case <-ctx.Done():
			return ctx.Err()
		}
		// 模型等待期间仍能查询和保存下一条输入。
		if _, err := session.query(); err != nil {
			return err
		}
		if err := session.submit(domain.NewEvent("main.request", map[string]any{"message": "第二条"})); err != nil {
			return err
		}
		stop()
		if err := <-done; !errors.Is(err, context.Canceled) {
			return fmt.Errorf("停止没有返回取消状态: %v", err)
		}
		query, err := session.query()
		if err != nil {
			return err
		}
		if waitingUnknownConversationAction(query) == nil {
			return errors.New("取消后没有保留未知调用")
		}
		if err := session.run(ctx, true, ""); err == nil || calls.Load() != 1 {
			return errors.New("没有人工决定就重试了未知调用")
		}
		if err := session.run(ctx, true, core.ResolutionRetry); err != nil {
			return err
		}
		query, err = session.query()
		if err != nil {
			return err
		}
		if len(conversationTurns(query)) != 2 || calls.Load() != 4 || waitingUnknownConversationAction(query) != nil {
			return fmt.Errorf("恢复没有处理完两条消息: calls=%d turns=%d", calls.Load(), len(conversationTurns(query)))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	query := conversationQuery(t, directory, agentID)
	transcript := tuiTranscript(*query, 60, false)
	if strings.Count(transcript, "第一条") != 1 || strings.Count(transcript, "第二条") != 1 || strings.Count(transcript, "收到") != 2 {
		t.Fatalf("重开后历史丢失或重复: %s", transcript)
	}
	if !strings.Contains(transcript, "✓ tool.agent_status") || strings.Contains(transcript, `"status"`) {
		t.Fatalf("工具没有显示折叠摘要: %s", transcript)
	}
	expanded := tuiTranscript(*query, 60, true)
	if !strings.Contains(expanded, `"status": "active"`) || strings.Index(expanded, "tool.agent_status") > strings.Index(expanded, "第二条") {
		t.Fatalf("工具详情没有归入对应轮次: %s", expanded)
	}
}

func TestTUICompletionRefreshAndPause(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	session := &tuiSession{ctx: ctx}
	m := newTUIModel(session, core.AgentQuery{}, "项目")
	m.running = true
	_, cmd := m.Update(tuiRunMsg{})
	if cmd == nil || !m.querying || m.running {
		t.Fatal("执行结束后没有请求新快照")
	}
	ready := core.AgentQuery{Deliveries: []core.DeliveryQuery{{Ready: true}}}
	_, cmd = m.Update(tuiQueryMsg{query: ready, epoch: m.epoch})
	if cmd == nil || !m.running {
		t.Fatal("最后一次扫描后到达的消息没有启动执行")
	}
	m.Update(tea.KeyMsg{Type: tea.KeyEsc})
	if !m.paused {
		t.Fatal("Esc没有暂停队列")
	}
	m.Update(tuiRunMsg{err: context.Canceled})
	_, cmd = m.Update(tuiQueryMsg{query: ready, epoch: m.epoch})
	if cmd != nil || m.running {
		t.Fatal("取消后快照自动重启了执行")
	}
	_, cmd = m.Update(tuiQueryMsg{query: core.AgentQuery{}, epoch: m.epoch - 1})
	if cmd == nil || !tuiReady(m.query) {
		t.Fatal("过期快照覆盖了当前状态")
	}
}

func TestTUIInputAndResize(t *testing.T) {
	m := newTUIModel(nil, core.AgentQuery{}, "/一个很长的项目目录/终端界面")
	m.input.SetValue("第一行")
	m.Update(tea.KeyMsg{Type: tea.KeyEnter, Alt: true})
	m.Update(tea.KeyMsg{Type: tea.KeyRunes, Runes: []rune("第二行")})
	if m.input.Value() != "第一行\n第二行" {
		t.Fatalf("多行输入失败: %q", m.input.Value())
	}
	_, cmd := m.Update(tea.KeyMsg{Type: tea.KeyEnter})
	if cmd == nil || len(m.outbox) != 1 || m.input.Value() != "" {
		t.Fatal("输入没有加入保存队列")
	}
	id := m.outbox[0].ID
	m.Update(tuiSubmitMsg{err: errors.New("存储暂不可用")})
	if len(m.outbox) != 1 || m.outbox[0].ID != id || !strings.Contains(m.history.View(), "第一行") {
		t.Fatal("保存失败丢失了输入")
	}
	for _, size := range []tea.WindowSizeMsg{{Width: 80, Height: 24}, {Width: 40, Height: 18}, {Width: 12, Height: 5}} {
		m.Update(size)
		view := m.View()
		if len(strings.Split(view, "\n")) > size.Height {
			t.Fatalf("界面超出高度: %dx%d", size.Width, size.Height)
		}
		for _, row := range strings.Split(view, "\n") {
			if ansi.StringWidth(row) > size.Width {
				t.Fatalf("中文界面超出宽度: %dx%d %q", size.Width, size.Height, row)
			}
		}
	}
	if got := tuiText("文字\x1b[2J\x1b]0;标题\a\r\n下一行"); got != "文字\n下一行" {
		t.Fatalf("输出保留了终端控制序列: %q", got)
	}
}
