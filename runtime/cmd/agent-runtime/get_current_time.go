package main

import (
	"errors"
	"time"

	"agent-runtime/domain"
)

const getCurrentTimeActionType = "tool.get_current_time"

type getCurrentTimeHandler struct{}

func (getCurrentTimeHandler) Execute(action domain.Action) (map[string]any, error) {
	if len(action.Payload) != 0 {
		return nil, errors.New("get_current_time不接受参数")
	}

	return map[string]any{
		"datetime": time.Now().UTC().Format(time.RFC3339Nano),
	}, nil
}
