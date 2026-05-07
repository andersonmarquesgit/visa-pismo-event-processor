package model

import "time"

type ProcessedEvent struct {
	ID          string
	EventID     string
	EventType   string
	TenantID    string
	Payload     []byte
	Status      string
	ProcessedAt time.Time
	CreatedAt   time.Time
}
