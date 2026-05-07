package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"time"

	"github.com/andersonmarquesgit/visa-pismo-event-processor/internal/domain/event"
	"github.com/andersonmarquesgit/visa-pismo-event-processor/internal/infrastructure/config"
	"github.com/andersonmarquesgit/visa-pismo-event-processor/internal/infrastructure/events/rabbitmq"
	"github.com/andersonmarquesgit/visa-pismo-event-processor/internal/infrastructure/events/rabbitmq/producers"
	"github.com/google/uuid"
)

func main() {

	cfg := config.LoadConfig()

	rabbitConn, err := rabbitmq.NewConnection(cfg.RabbitMQ.URL)
	if err != nil {
		log.Fatalf("Could not connect to RabbitMQ: %v", err)
	}
	defer rabbitConn.Close()

	producers, err := producers.NewProducers(rabbitConn)
	if err != nil {
		log.Fatalf("Could not create RabbitMQ producers: %v", err)
	}

	eventTypes := []string{
		"transaction.authorized",
		"monitoring.alert",
		"user.created",
	}

	for {
		eventType := eventTypes[rand.Intn(len(eventTypes))]

		ev := event.Event{
			ID:        uuid.NewString(),
			TenantID:  "visa",
			EventType: eventType,
			Timestamp: time.Now(),
			Payload: map[string]interface{}{
				"random": rand.Intn(1000),
			},
		}

		body, err := json.Marshal(ev)
		if err != nil {
			log.Printf("marshal error: %v", err)
			continue
		}

		err = producers.EventProducer.Publish(body)
		if err != nil {
			log.Printf("publish error: %v", err)
			continue
		}

		log.Printf(
			"event published | type=%s | id=%s",
			ev.EventType,
			ev.ID,
		)

		time.Sleep(2 * time.Second)
	}

}
