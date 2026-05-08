package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/andersonmarquesgit/visa-pismo-event-processor/internal/domain/event"
	"github.com/andersonmarquesgit/visa-pismo-event-processor/internal/infrastructure/persistence/postgres/model"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

type ProcessedEventsRepository interface {
	Save(ctx context.Context, e *model.ProcessedEvent) error
}

type EventHandler struct {
	processedEvents ProcessedEventsRepository
	workerID        string
}

func NewEventHandler(processedEvents ProcessedEventsRepository, workerID string) *EventHandler {
	return &EventHandler{
		processedEvents: processedEvents,
		workerID:        workerID,
	}
}

func (h *EventHandler) HandleEvent(msg amqp.Delivery) error {
	start := time.Now()

	// Small randomized delay to make competing-consumers distribution visible during demos.
	time.Sleep(time.Duration(100+rand.Intn(401)) * time.Millisecond)

	var ev event.Event
	if err := json.Unmarshal(msg.Body, &ev); err != nil {
		log.Printf("worker=%s | event_id=%s | type=%s | status=failed | sent_to_dlq=true | duration_ms=%d | err=%v",
			h.workerID, "", "unknown", time.Since(start).Milliseconds(), err,
		)
		return fmt.Errorf("%w: invalid json payload: %w", ErrInvalidEvent, err)
	}
	if ev.ID == "" {
		log.Printf("worker=%s | event_id=%s | type=%s | status=failed | sent_to_dlq=true | duration_ms=%d | err=%s",
			h.workerID, "", ev.EventType, time.Since(start).Milliseconds(), "event without id",
		)
		return fmt.Errorf("%w: missing id", ErrInvalidEvent)
	}
	if ev.TenantID == "" {
		log.Printf("worker=%s | event_id=%s | type=%s | status=failed | sent_to_dlq=true | duration_ms=%d | err=%s",
			h.workerID, ev.ID, ev.EventType, time.Since(start).Milliseconds(), "event without tenant_id",
		)
		return fmt.Errorf("%w: missing tenant_id", ErrInvalidEvent)
	}
	if ev.EventType == "" {
		log.Printf("worker=%s | event_id=%s | type=%s | status=failed | sent_to_dlq=true | duration_ms=%d | err=%s",
			h.workerID, ev.ID, "unknown", time.Since(start).Milliseconds(), "event without event_type",
		)
		return fmt.Errorf("%w: missing event_type", ErrInvalidEvent)
	}
	if err := validatePayload(ev.EventType, ev.Payload); err != nil {
		log.Printf("worker=%s | event_id=%s | type=%s | status=failed | sent_to_dlq=true | duration_ms=%d | err=%v",
			h.workerID, ev.ID, ev.EventType, time.Since(start).Milliseconds(), err,
		)
		return fmt.Errorf("%w: invalid contract: %w", ErrInvalidEvent, err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pe := model.ProcessedEvent{
		ID:          uuid.NewString(),
		EventID:     ev.ID,
		EventType:   ev.EventType,
		TenantID:    ev.TenantID,
		Payload:     msg.Body,
		ProcessedAt: time.Now().UTC(),
	}

	if err := h.processedEvents.Save(ctx, &pe); err != nil {
		log.Printf("worker=%s | event_id=%s | type=%s | status=failed | sent_to_dlq=true | duration_ms=%d | err=%v",
			h.workerID, ev.ID, ev.EventType, time.Since(start).Milliseconds(), err,
		)
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
			return fmt.Errorf("%w: persist timed out: %w", ErrTransient, err)
		}
		return fmt.Errorf("%w: persist processed event: %w", ErrTransient, err)
	}

	log.Printf("worker=%s | event_id=%s | type=%s | status=processed | duration_ms=%d",
		h.workerID, ev.ID, ev.EventType, time.Since(start).Milliseconds(),
	)
	return nil
}
