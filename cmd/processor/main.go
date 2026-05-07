package main

import (
	"context"
	"log"
	"os/signal"
	"syscall"

	"github.com/andersonmarquesgit/visa-pismo-event-processor/internal/infrastructure/config"
	"github.com/andersonmarquesgit/visa-pismo-event-processor/internal/infrastructure/events/rabbitmq"
	"github.com/andersonmarquesgit/visa-pismo-event-processor/internal/infrastructure/events/rabbitmq/consumers"
	"github.com/andersonmarquesgit/visa-pismo-event-processor/internal/infrastructure/handlers"
)

func main() {

	cfg := config.LoadConfig()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	rabbitConn, err := rabbitmq.NewConnection(cfg.RabbitMQ.URL)
	if err != nil {
		log.Fatalf("Could not connect to RabbitMQ: %v", err)
	}
	defer rabbitConn.Close()

	eventHandler := handlers.NewEventHandler()
	eventHandlers := handlers.NewEventHandlers(eventHandler)

	if err := consumers.NewConsumers(ctx, rabbitConn, eventHandlers); err != nil {
		log.Fatalf("Could not create RabbitMQ consumers: %v", err)
	}

	<-ctx.Done()
	log.Println("shutting down")
}
