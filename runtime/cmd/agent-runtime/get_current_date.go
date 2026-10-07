package main

import (
	"errors"
	"time"

	"agent-runtime/domain"
)

const getCurrentDateActionType = "tool.get_current_date"

type getCurrentDateHandler struct{}

func (getCurrentDateHandler) Execute(action domain.Action) (map[string]any, error) {
	if len(action.Payload) != 0 {
		return nil, errors.New("get_current_date不接受参数")
	}

	return map[string]any{
		"date": time.Now().UTC().Format("2006-01-02"),
	}, nil
}
