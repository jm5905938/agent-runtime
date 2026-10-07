package main

import (
	"agent-runtime/domain"
	"testing"
	"time"
)

func TestGetCurrentDateHandlerReturnsUTCDate(t *testing.T) {
	result, err := (getCurrentDateHandler{}).Execute(
		domain.NewAction(getCurrentDateActionType, map[string]any{}),
	)
	if err != nil {
		t.Fatal(err)
	}

	value, ok := result["date"].(string)
	if !ok || value == "" {
		t.Fatalf("date无效: %#v", result["date"])
	}

	if _, err := time.Parse("2006-01-02", value); err != nil {
		t.Fatalf("date不是有效日期: %v", err)
	}
}

func TestGetCurrentDateHandlerRejectsPayload(t *testing.T) {
	_, err := (getCurrentDateHandler{}).Execute(
		domain.NewAction(
			getCurrentDateActionType,
			map[string]any{"timezone": "UTC"},
		),
	)
	if err == nil {
		t.Fatal("带参数的get_current_date不应执行成功")
	}
}
