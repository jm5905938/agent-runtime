package main

import (
	"agent-runtime/domain"
	"testing"
	"time"
)

func TestGetCurrentTimeHandlerReturnsUTC(t *testing.T) {
	result, err := (getCurrentTimeHandler{}).Execute(
		domain.NewAction(getCurrentTimeActionType, map[string]any{}),
	)
	if err != nil {
		t.Fatal(err)
	}

	value, ok := result["datetime"].(string)
	if !ok || value == "" {
		t.Fatalf("datetime无效: %#v", result["datetime"])
	}

	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		t.Fatalf("datetime不是有效RFC3339时间: %v", err)
	}
	if parsed.Location() != time.UTC {
		t.Fatalf("时间不是UTC: %v", parsed.Location())
	}
}

func TestGetCurrentTimeHandlerRejectsPayload(t *testing.T) {
	_, err := (getCurrentTimeHandler{}).Execute(
		domain.NewAction(
			getCurrentTimeActionType,
			map[string]any{"timezone": "UTC"},
		),
	)
	if err == nil {
		t.Fatal("带参数的get_current_time不应执行成功")
	}
}
