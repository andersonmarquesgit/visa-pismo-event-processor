package repository

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/andersonmarquesgit/visa-pismo-event-processor/internal/infrastructure/persistence/postgres/model"
)

const processedEventStatusProcessed = "processed"

const insertProcessedEventSQL = `
INSERT INTO processed_events (id, event_id, event_type, tenant_id, payload, status, processed_at, created_at)
VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7, $8)
ON CONFLICT (event_id) DO NOTHING
`

type ProcessedEventRepository struct {
	db *sql.DB
}

func NewProcessedEventRepository(db *sql.DB) *ProcessedEventRepository {
	return &ProcessedEventRepository{db: db}
}

func (r *ProcessedEventRepository) Save(ctx context.Context, e *model.ProcessedEvent) error {
	if e == nil {
		return fmt.Errorf("processed event is nil")
	}
	processedAt := e.ProcessedAt
	if processedAt.IsZero() {
		processedAt = time.Now().UTC()
	}
	createdAt := e.CreatedAt
	if createdAt.IsZero() {
		createdAt = processedAt
	}
	status := e.Status
	if status == "" {
		status = processedEventStatusProcessed
	}

	_, err := r.db.ExecContext(
		ctx,
		insertProcessedEventSQL,
		e.ID,
		e.EventID,
		e.EventType,
		e.TenantID,
		e.Payload,
		status,
		processedAt,
		createdAt,
	)
	if err != nil {
		return fmt.Errorf("save processed event: %w", err)
	}
	return nil
}
