package handlers

import (
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type EventHandler struct {
}

func NewEventHandler() *EventHandler {
	return &EventHandler{}
}

func (h *EventHandler) HandleEvent(msg amqp.Delivery) error {
	log.Printf("Processing events: %s", string(msg.Body))

	// Process the message
	//err := h.EventProcessorUseCase.Process(msg)
	//if err != nil {
	//	log.Printf("Failed to process events: %v", err)
	//	return err
	//}
	return nil
}
