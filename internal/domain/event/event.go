package event

import (
	"encoding/json"
	"time"
)

type Event struct {
	ID        string          `json:"id"`
	TenantID  string          `json:"tenant_id"`
	EventType string          `json:"event_type"`
	Timestamp time.Time       `json:"timestamp"`
	Payload   json.RawMessage `json:"payload"`
}
