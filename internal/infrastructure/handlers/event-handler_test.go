package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/andersonmarquesgit/visa-pismo-event-processor/internal/domain/event"
	"github.com/andersonmarquesgit/visa-pismo-event-processor/internal/infrastructure/persistence/postgres/model"
	amqp "github.com/rabbitmq/amqp091-go"
)

type fakeProcessedEventsRepo struct {
	lastSaved *model.ProcessedEvent
	err       error
}

func (f *fakeProcessedEventsRepo) Save(_ctx context.Context, e *model.ProcessedEvent) error {
	f.lastSaved = e
	return f.err
}

func TestEventHandler_HandleEvent(t *testing.T) {
	t.Parallel()

	t.Run("invalid json returns error", func(t *testing.T) {
		t.Parallel()

		repo := &fakeProcessedEventsRepo{}
		h := NewEventHandler(repo, "w1")

		err := h.HandleEvent(amqp.Delivery{Body: []byte(`{"oops":`)})
		if err == nil {
			t.Fatalf("expected error")
		}
		if repo.lastSaved != nil {
			t.Fatalf("did not expect Save to be called")
		}
	})

	t.Run("missing id returns error", func(t *testing.T) {
		t.Parallel()

		repo := &fakeProcessedEventsRepo{}
		h := NewEventHandler(repo, "w1")

		ev := event.Event{
			TenantID:  "t1",
			EventType: "user.created",
			Timestamp: time.Now(),
			Payload:   map[string]any{"x": "y"},
		}
		body, _ := json.Marshal(ev)

		err := h.HandleEvent(amqp.Delivery{Body: body})
		if err == nil {
			t.Fatalf("expected error")
		}
		if repo.lastSaved != nil {
			t.Fatalf("did not expect Save to be called")
		}
	})

	t.Run("missing tenant_id returns error", func(t *testing.T) {
		t.Parallel()

		repo := &fakeProcessedEventsRepo{}
		h := NewEventHandler(repo, "w1")

		ev := event.Event{
			ID:        "e1",
			EventType: "user.created",
			Timestamp: time.Now(),
			Payload:   map[string]any{"x": "y"},
		}
		body, _ := json.Marshal(ev)

		err := h.HandleEvent(amqp.Delivery{Body: body})
		if err == nil {
			t.Fatalf("expected error")
		}
		if repo.lastSaved != nil {
			t.Fatalf("did not expect Save to be called")
		}
	})

	t.Run("repo error returns error", func(t *testing.T) {
		t.Parallel()

		repo := &fakeProcessedEventsRepo{err: errors.New("db down")}
		h := NewEventHandler(repo, "w1")

		ev := event.Event{
			ID:        "e1",
			TenantID:  "t1",
			EventType: "user.created",
			Timestamp: time.Now(),
			Payload:   map[string]any{"x": "y"},
		}
		body, _ := json.Marshal(ev)

		err := h.HandleEvent(amqp.Delivery{Body: body})
		if err == nil {
			t.Fatalf("expected error")
		}
	})

	t.Run("valid event persists and returns nil", func(t *testing.T) {
		t.Parallel()

		repo := &fakeProcessedEventsRepo{}
		h := NewEventHandler(repo, "w1")

		ev := event.Event{
			ID:        "e1",
			TenantID:  "t1",
			EventType: "user.created",
			Timestamp: time.Now(),
			Payload:   map[string]any{"x": "y"},
		}
		body, _ := json.Marshal(ev)

		err := h.HandleEvent(amqp.Delivery{Body: body})
		if err != nil {
			t.Fatalf("expected nil error, got %v", err)
		}
		if repo.lastSaved == nil {
			t.Fatalf("expected Save to be called")
		}
		if repo.lastSaved.EventID != "e1" {
			t.Fatalf("expected EventID=e1, got %q", repo.lastSaved.EventID)
		}
		if repo.lastSaved.TenantID != "t1" {
			t.Fatalf("expected TenantID=t1, got %q", repo.lastSaved.TenantID)
		}
		if repo.lastSaved.EventType != "user.created" {
			t.Fatalf("expected EventType=user.created, got %q", repo.lastSaved.EventType)
		}
		if len(repo.lastSaved.Payload) == 0 {
			t.Fatalf("expected Payload to be set")
		}
	})
}

