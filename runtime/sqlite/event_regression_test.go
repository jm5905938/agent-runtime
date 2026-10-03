package sqlite

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"agent-runtime/core"
	"agent-runtime/domain"
)

func TestReceiveEventRollsBackAfterSQLWrite(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestSession(t)
	agent := testAgent()
	if err := s.CreateAgent(ctx, agent); err != nil {
		t.Fatal(err)
	}
	event := domain.NewEvent("test", nil)
	if _, err := s.backend.db.ExecContext(ctx, `CREATE TEMP TRIGGER fail_event_delivery BEFORE INSERT ON deliveries
		BEGIN
			SELECT CASE WHEN EXISTS (SELECT 1 FROM events WHERE id = NEW.event_id)
				THEN RAISE(ABORT, 'event写入已发生')
				ELSE RAISE(ABORT, 'event写入未发生') END;
		END`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReceiveEvent(ctx, agent.ID, event); err == nil || !strings.Contains(err.Error(), "event写入已发生") {
		t.Fatalf("未命中event SQL写入后的故障: %v", err)
	}
	if _, err := s.LoadEvent(ctx, event.ID); !errors.Is(err, core.ErrStoreNotFound) {
		t.Fatalf("回滚后留下event: %v", err)
	}
	if _, err := s.LoadDelivery(ctx, domain.DeliveryKey{AgentID: agent.ID, EventID: event.ID}); !errors.Is(err, core.ErrStoreNotFound) {
		t.Fatalf("回滚后留下delivery: %v", err)
	}
	if _, err := s.backend.db.ExecContext(ctx, `DROP TRIGGER fail_event_delivery`); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReceiveEvent(ctx, agent.ID, event); err != nil {
		t.Fatalf("回滚后不能重新接收event: %v", err)
	}
}

func TestReceiveEventRejectsStoredInvalidTime(t *testing.T) {
	ctx := context.Background()
	s, _ := newTestSession(t)
	agent := testAgent()
	if err := s.CreateAgent(ctx, agent); err != nil {
		t.Fatal(err)
	}
	other := agent
	other.ID = "other-agent"
	if err := s.CreateAgent(ctx, other); err != nil {
		t.Fatal(err)
	}
	event := domain.NewEvent("test", nil)
	if _, err := s.ReceiveEvent(ctx, agent.ID, event); err != nil {
		t.Fatal(err)
	}
	if _, err := s.backend.db.ExecContext(ctx, `UPDATE events SET created_at = '10000-01-01T00:00:00Z' WHERE id = ?`, string(event.ID)); err != nil {
		t.Fatal(err)
	}
	before := recoveryDatabaseSnapshot(t, s.backend)
	if _, err := s.ReceiveEvent(ctx, other.ID, event); err == nil {
		t.Fatal("接收重复event忽略已有时间解析错误")
	}
	if _, err := s.LoadDelivery(ctx, domain.DeliveryKey{AgentID: other.ID, EventID: event.ID}); !errors.Is(err, core.ErrStoreNotFound) {
		t.Fatalf("损坏event留下新delivery: %v", err)
	}
	after := recoveryDatabaseSnapshot(t, s.backend)
	if !reflect.DeepEqual(after, before) {
		t.Fatal("拒绝损坏event后数据库发生变化")
	}
}
