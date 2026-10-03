package storetest

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"agent-runtime/core"
	"agent-runtime/domain"
)

func testEventValidation(t *testing.T, backend core.RecoveryStore) {
	ctx := context.Background()
	s := readySession(t, backend)
	agent := createAgent(t, s, "agent")
	badText := string([]byte{0xff})
	for _, test := range []struct {
		name   string
		change func(*domain.Event)
	}{
		{"empty_id", func(e *domain.Event) { e.ID = "" }},
		{"blank_id", func(e *domain.Event) { e.ID = " \t" }},
		{"empty_type", func(e *domain.Event) { e.Type = "" }},
		{"blank_type", func(e *domain.Event) { e.Type = " \n" }},
		{"invalid_id_text", func(e *domain.Event) { e.ID = domain.ID(badText) }},
		{"invalid_type_text", func(e *domain.Event) { e.Type = badText }},
		{"invalid_payload_text", func(e *domain.Event) { e.Payload = map[string]any{"text": badText} }},
		{"invalid_payload_key", func(e *domain.Event) { e.Payload = map[string]any{badText: true} }},
		{"invalid_payload_number", func(e *domain.Event) { e.Payload = map[string]any{"number": math.NaN()} }},
		{"year_10000", func(e *domain.Event) { e.CreatedAt = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC) }},
		{"negative_year", func(e *domain.Event) { e.CreatedAt = time.Date(-1, 1, 1, 0, 0, 0, 0, time.UTC) }},
		{"utc_year_overflow", func(e *domain.Event) {
			e.CreatedAt = time.Date(9999, 12, 31, 23, 0, 0, 0, time.FixedZone("offset", -3600))
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			event := domain.NewEvent("test", nil)
			test.change(&event)
			before, err := s.ListDeliveries(ctx)
			must(t, err)
			if _, err := s.ReceiveEvent(ctx, agent.ID, event); err == nil {
				t.Error("非法event接收成功")
			}
			if _, err := s.LoadEvent(ctx, event.ID); !errors.Is(err, core.ErrStoreNotFound) {
				t.Errorf("非法event留下记录: %v", err)
			}
			if _, err := s.LoadDelivery(ctx, domain.DeliveryKey{AgentID: agent.ID, EventID: event.ID}); !errors.Is(err, core.ErrStoreNotFound) {
				t.Errorf("非法event留下delivery: %v", err)
			}
			after, err := s.ListDeliveries(ctx)
			must(t, err)
			sameJSON(t, after, before)
		})
	}
	valid := domain.NewEvent("test", map[string]any{"value": "原值"})
	received := receive(t, s, agent.ID, valid)
	duplicate := valid
	duplicate.CreatedAt = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
	if _, err := s.ReceiveEvent(ctx, agent.ID, duplicate); err == nil {
		t.Fatal("重复event绕过时间校验")
	}
	saved, err := s.LoadEvent(ctx, valid.ID)
	must(t, err)
	sameJSON(t, *saved, valid)
	delivery, err := s.LoadDelivery(ctx, received.Delivery.Key)
	must(t, err)
	sameJSON(t, *delivery, received.Delivery)
}
