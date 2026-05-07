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
	"github.com/andersonmarquesgit/visa-pismo-event-processor/internal/infrastructure/persistence/postgres"
	"github.com/andersonmarquesgit/visa-pismo-event-processor/internal/infrastructure/persistence/postgres/repository"
)

func main() {

	cfg := config.LoadConfig()

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	db, err := postgres.NewDB(ctx, cfg.Postgres.URL)
	if err != nil {
		log.Fatalf("Could not connect to Postgres: %v", err)
	}
	if err := postgres.EnsureSchema(ctx, db); err != nil {
		log.Fatalf("Could not ensure postgres schema: %v", err)
	}
	defer func() {
		if err := db.Close(); err != nil {
			log.Printf("db close: %v", err)
		}
	}()

	rabbitConn, err := rabbitmq.NewConnection(cfg.RabbitMQ.URL)
	if err != nil {
		log.Fatalf("Could not connect to RabbitMQ: %v", err)
	}
	defer rabbitConn.Close()

	processedEventRepo := repository.NewProcessedEventRepository(db)
	eventHandler := handlers.NewEventHandler(processedEventRepo)
	eventHandlers := handlers.NewEventHandlers(eventHandler)

	if err := consumers.NewConsumers(ctx, rabbitConn, eventHandlers); err != nil {
		log.Fatalf("Could not create RabbitMQ consumers: %v", err)
	}

	<-ctx.Done()
	log.Println("shutting down")
}
