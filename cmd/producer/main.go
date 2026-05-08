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

		isInvalid := rand.Intn(100) < 10

		var body []byte
		var routingKey string

		if isInvalid {
			if rand.Intn(2) == 0 {
				body = []byte(`{"id":`)
				routingKey = "malformed.json"

				log.Printf("malformed event published")
			} else {
				ev := event.Event{
					ID:        uuid.NewString(),
					TenantID:  "visa",
					EventType: "demo.fail_processing",
					Timestamp: time.Now(),
					Payload: map[string]interface{}{
						"reason": "simulated failure",
					},
				}

				body, _ = json.Marshal(ev)
				routingKey = ev.EventType
				log.Printf(
					"poison event published | type=%s | id=%s",
					ev.EventType,
					ev.ID,
				)
			}
		} else {
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

			body, _ = json.Marshal(ev)
			routingKey = ev.EventType
			log.Printf(
				"valid event published | type=%s | id=%s",
				ev.EventType,
				ev.ID,
			)
		}

		err = producers.EventProducer.Publish(
			routingKey,
			body,
		)

		if err != nil {
			log.Printf("publish error: %v", err)
		}

		time.Sleep(2 * time.Second)
	}
}
