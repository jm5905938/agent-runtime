package domain

import "time"

// Event 是发给 Agent 的一条消息。
// 消息可以来自用户、定时器或其他程序。
// Payload 是消息里带的数据，CreatedAt 是消息到达的时间。
type Event struct {
	ID        ID             `json:"id"`
	Type      string         `json:"type"`
	Payload   map[string]any `json:"payload"`
	CreatedAt time.Time      `json:"created_at"`
}

// NewEvent 创建一条新消息，并自动生成编号和创建时间。
func NewEvent(eventType string, payload map[string]any) Event {
	return Event{
		ID:        mustNewID(),
		Type:      eventType,
		Payload:   payload,
		CreatedAt: time.Now().UTC(),
	}
}
