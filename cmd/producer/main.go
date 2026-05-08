package main

import (
	"encoding/json"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/andersonmarquesgit/visa-pismo-event-processor/internal/domain/event"
	"github.com/andersonmarquesgit/visa-pismo-event-processor/internal/infrastructure/config"
	"github.com/andersonmarquesgit/visa-pismo-event-processor/internal/infrastructure/events/rabbitmq"
	"github.com/andersonmarquesgit/visa-pismo-event-processor/internal/infrastructure/events/rabbitmq/producers"
	"github.com/google/uuid"
)

func main() {
	rand.Seed(time.Now().UnixNano())

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

	validEventFiles := []string{
		"fake-events/valid/transaction-authorized.json",
		"fake-events/valid/monitoring-alert.json",
		"fake-events/valid/user-created.json",
	}

	invalidEventFiles := []string{
		"fake-events/invalid/malformed.json",
	}

	for {
		if shouldPublishInvalidEvent() {
			publishInvalidEvent(producers, invalidEventFiles)
		} else {
			publishValidEvent(producers, validEventFiles)
		}

		time.Sleep(2 * time.Second)
	}
}

func shouldPublishInvalidEvent() bool {
	return rand.Intn(100) < 10
}

func loadEventJSON(path string) (event.Event, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return event.Event{}, err
	}

	var ev event.Event
	if err := json.Unmarshal(b, &ev); err != nil {
		return event.Event{}, err
	}

	return ev, nil
}

func publishInvalidEvent(p *producers.Producers, invalidEventFiles []string) {
	path := invalidEventFiles[rand.Intn(len(invalidEventFiles))]

	var (
		body       []byte
		routingKey string
	)

	body, err := os.ReadFile(path)
	if err != nil {
		log.Printf("could not read malformed event file: %v", err)
		return
	}

	routingKey = "malformed.json"
	log.Printf("malformed event published")

	if err := p.EventProducer.Publish(routingKey, body); err != nil {
		log.Printf("publish error: %v", err)
	}
}

func publishValidEvent(p *producers.Producers, validEventFiles []string) {
	path := validEventFiles[rand.Intn(len(validEventFiles))]
	ev, err := loadEventJSON(path)
	if err != nil {
		log.Printf("could not load valid event file: %v", err)
		return
	}

	ev.ID = uuid.NewString()
	ev.Timestamp = time.Now()

	body, err := json.Marshal(ev)
	if err != nil {
		log.Printf("could not marshal event: %v", err)
		return
	}

	routingKey := ev.EventType
	log.Printf("valid event published | type=%s | id=%s", ev.EventType, ev.ID)

	if err := p.EventProducer.Publish(routingKey, body); err != nil {
		log.Printf("publish error: %v", err)
	}
}
