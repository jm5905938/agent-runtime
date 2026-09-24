package domain

import "time"

//发给agent的消息
type Event struct {
	ID        ID             `json:"id"`
	Type      string         `json:"type"`
	Payload   map[string]any `json:"payload"`
	CreatedAt time.Time      `json:"created_at"`
}

//创建消息，生成id和时间
func NewEvent(eventType string, payload map[string]any) Event {
	return Event{
		ID:        mustNewID(),
		Type:      eventType,
		Payload:   payload,
		CreatedAt: time.Now().UTC(),
	}
}
