package main

import (
	"log"

	"github.com/andersonmarquesgit/visa-pismo-event-processor/internal/infrastructure/config"
	"github.com/andersonmarquesgit/visa-pismo-event-processor/internal/infrastructure/events/rabbitmq"
)

func main() {

	cfg := config.LoadConfig()

	rabbitConn, err := rabbitmq.NewConnection(cfg.RabbitMQ.URL)
	if err != nil {
		log.Fatalf("Could not connect to RabbitMQ: %v", err)
	}
	defer rabbitConn.Close()

	//TODO: Make consumer
}
