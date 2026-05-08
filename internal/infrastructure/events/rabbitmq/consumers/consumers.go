package consumers

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"

	"github.com/andersonmarquesgit/visa-pismo-event-processor/internal/infrastructure/events/rabbitmq"
	"github.com/andersonmarquesgit/visa-pismo-event-processor/internal/infrastructure/handlers"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Topic exchange: "#" binds the processing queue to every message published to the exchange.
const eventsQueueBindingKey = "#"

type Consumer struct {
	Connection   *amqp.Connection
	Exchange     string
	ExchangeType string
	QueueName    string
}

type Consumers struct {
	EventConsumer *Consumer
}

func NewConsumers(ctx context.Context, conn *amqp.Connection, eventHandlers *handlers.EventHandlers) error {
	eventConsumer, err := NewEventConsumer(conn)
	if err != nil {
		return fmt.Errorf("create event consumer: %w", err)
	}

	go func() {
		if err := eventConsumer.Listen(ctx, eventHandlers.EventHandler.HandleEvent); err != nil {
			log.Printf("consumer listen ended: %v", err)
		}
	}()

	return nil
}

func (consumer *Consumer) setup() error {
	channel, err := consumer.Connection.Channel()
	if err != nil {
		return err
	}
	defer channel.Close()

	err = rabbitmq.DeclareExchange(channel, consumer.Exchange, consumer.ExchangeType)
	if err != nil {
		return err
	}

	_, err = rabbitmq.DeclareEventsProcessingQueue(channel)
	if err != nil {
		return err
	}
	err = channel.QueueBind(consumer.QueueName, eventsQueueBindingKey, consumer.Exchange, false, nil)
	if err != nil {
		return err
	}
	log.Printf("queue %s bound to %q on exchange %s", consumer.QueueName, eventsQueueBindingKey, consumer.Exchange)

	return nil
}

func (consumer *Consumer) Listen(ctx context.Context, handler func(msg amqp.Delivery) error) error {
	ch, err := consumer.Connection.Channel()
	if err != nil {
		return err
	}

	var wg sync.WaitGroup

	msgs, err := ch.Consume(consumer.QueueName, "", false, false, false, false, nil)
	if err != nil {
		_ = ch.Close()
		return err
	}

	wg.Add(1)
	go func(msgs <-chan amqp.Delivery) {
		defer wg.Done()
		for msg := range msgs {
			if err := handler(msg); err != nil {
				class := "unknown"
				switch {
				case errors.Is(err, handlers.ErrInvalidEvent):
					class = "invalid_event"
				case errors.Is(err, handlers.ErrTransient):
					class = "transient"
				}
				log.Printf("handler failed | class=%s | action=dlq | dead_letter_queue=%q | err=%v", class, rabbitmq.EventsDeadLetterQueue, err)
				if nackErr := msg.Nack(false, false); nackErr != nil {
					log.Printf("nack: %v", nackErr)
				}
				continue
			}
			if ackErr := msg.Ack(false); ackErr != nil {
				log.Printf("ack: %v", ackErr)
			}
		}
	}(msgs)

	<-ctx.Done()
	if err := ch.Close(); err != nil {
		log.Printf("consumer channel close: %v", err)
	}
	wg.Wait()
	return nil
}
