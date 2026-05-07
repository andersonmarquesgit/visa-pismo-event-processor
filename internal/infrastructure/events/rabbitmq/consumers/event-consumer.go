package consumers

import (
	"github.com/andersonmarquesgit/visa-pismo-event-processor/internal/infrastructure/events/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
)

func NewEventConsumer(conn *amqp.Connection) (*Consumer, error) {
	consumer := Consumer{
		Connection:   conn,
		Exchange:     rabbitmq.EventsExchange,
		ExchangeType: rabbitmq.EventsExchangeType,
		QueueName:    rabbitmq.EventsProcessingQueue,
	}

	err := consumer.setup()
	if err != nil {
		return nil, err
	}

	return &consumer, nil
}
