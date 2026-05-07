package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/andersonmarquesgit/visa-pismo-event-processor/internal/domain/event"
	"github.com/andersonmarquesgit/visa-pismo-event-processor/internal/infrastructure/persistence/postgres/model"
	"github.com/andersonmarquesgit/visa-pismo-event-processor/internal/infrastructure/persistence/postgres/repository"
	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

type EventHandler struct {
	processedEvents *repository.ProcessedEventRepository
}

func NewEventHandler(processedEvents *repository.ProcessedEventRepository) *EventHandler {
	return &EventHandler{
		processedEvents: processedEvents,
	}
}

func (h *EventHandler) HandleEvent(msg amqp.Delivery) error {
	var ev event.Event
	if err := json.Unmarshal(msg.Body, &ev); err != nil {
		log.Printf("invalid event payload (ack to drop): %v", err)
		return nil
	}
	if ev.ID == "" {
		log.Printf("event without id (ack to drop)")
		return nil
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
		return fmt.Errorf("persist processed event: %w", err)
	}

	log.Printf("consumer finished | event_id=%s | type=%s", ev.ID, ev.EventType)
	return nil
}
