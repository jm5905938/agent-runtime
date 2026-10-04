package sqlite

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"time"

	"agent-runtime/core"
	"agent-runtime/domain"
	sqliteDriver "modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

// Ingress只接收用户消息，不取得执行所有权，也不改变Agent、Execution或Action。
type Ingress struct {
	db     *sql.DB
	gate   chan struct{}
	closed bool
}

// OpenIngress仅打开现有数据库，执行会话可以同时持有独占所有权。
func OpenIngress(path string) (*Ingress, error) {
	absolute, err := databasePath(path)
	if err != nil {
		return nil, fmt.Errorf("解析sqlite路径: %w", err)
	}
	if err := checkDatabasePath(absolute); err != nil {
		return nil, err
	}
	db, err := openDatabase(context.Background(), absolute, "rw")
	if err != nil {
		return nil, err
	}
	ingress := &Ingress{db: db, gate: make(chan struct{}, 1)}
	ingress.gate <- struct{}{}
	return ingress, nil
}

func (i *Ingress) lock(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-i.gate:
	}
	if err := ctx.Err(); err != nil {
		i.gate <- struct{}{}
		return err
	}
	return nil
}

func (i *Ingress) Close() error {
	if err := i.lock(context.Background()); err != nil {
		return err
	}
	defer func() { i.gate <- struct{}{} }()
	if i.closed {
		return nil
	}
	if err := i.db.Close(); err != nil {
		return fmt.Errorf("关闭sqlite输入通道: %w", err)
	}
	i.closed = true
	return nil
}

// SubmitMessage的定义检查、ID去重与receive_seq分配共享同一IMMEDIATE事务。
func (i *Ingress) SubmitMessage(ctx context.Context, agentID, eventID domain.ID, message string) (core.ReceivedEvent, error) {
	if err := i.lock(ctx); err != nil {
		return core.ReceivedEvent{}, err
	}
	defer func() { i.gate <- struct{}{} }()
	if i.closed {
		return core.ReceivedEvent{}, core.ErrStoreClosed
	}
	conn, err := i.db.Conn(ctx)
	if err != nil {
		return core.ReceivedEvent{}, err
	}
	defer conn.Close()
	// SQLite默认busy handler等待期间不会及时响应取消，分成短等待再检查ctx。
	if _, err := conn.ExecContext(ctx, `PRAGMA busy_timeout = 50`); err != nil {
		return core.ReceivedEvent{}, err
	}
	defer restoreIngressTimeout(conn)
	deadline := time.Now().Add(5 * time.Second)
	for {
		received, err := submitMessageTransaction(ctx, conn, agentID, eventID, message)
		if err == nil {
			return received, nil
		}
		if err := ctx.Err(); err != nil {
			return core.ReceivedEvent{}, err
		}
		var sqliteErr *sqliteDriver.Error
		if !errors.As(err, &sqliteErr) || time.Now().After(deadline) {
			return core.ReceivedEvent{}, err
		}
		switch sqliteErr.Code() & 0xff {
		case sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED:
		default:
			return core.ReceivedEvent{}, err
		}
		timer := time.NewTimer(10 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return core.ReceivedEvent{}, ctx.Err()
		case <-timer.C:
		}
	}
}

// 重试整个事务，BEGIN和COMMIT遇到竞争都保留同一个等待预算。
func submitMessageTransaction(ctx context.Context, conn *sql.Conn, agentID, eventID domain.ID, message string) (core.ReceivedEvent, error) {
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return core.ReceivedEvent{}, err
	}
	defer tx.Rollback()
	if err := verifySchema(ctx, tx); err != nil {
		return core.ReceivedEvent{}, err
	}
	agent, err := loadAgent(ctx, tx, agentID)
	if err != nil {
		return core.ReceivedEvent{}, err
	}
	var eventType string
	switch agent.Definition {
	case domain.DefinitionRef{ID: "echo", Version: "1"}:
		eventType = "echo.request"
	case domain.DefinitionRef{ID: "main", Version: "1"}:
		eventType = "main.request"
	default:
		return core.ReceivedEvent{}, fmt.Errorf("不支持向definition %s@%s提交消息", agent.Definition.ID, agent.Definition.Version)
	}
	event := domain.Event{ID: eventID, Type: eventType, Payload: map[string]any{"message": message}, CreatedAt: time.Now().UTC()}
	if err := core.ValidateEvent(event); err != nil {
		return core.ReceivedEvent{}, err
	}
	received, err := receiveEvent(ctx, tx, agentID, event)
	if err != nil {
		return core.ReceivedEvent{}, err
	}
	if err := tx.Commit(); err != nil {
		return core.ReceivedEvent{}, err
	}
	return received, nil
}

func restoreIngressTimeout(conn *sql.Conn) {
	if _, err := conn.ExecContext(context.Background(), `PRAGMA busy_timeout = 5000`); err != nil {
		// 恢复失败就丢弃连接；避免影响后续提交，也不掩盖已保存的结果或原始错误。
		_ = conn.Raw(func(any) error { return driver.ErrBadConn })
	}
}
