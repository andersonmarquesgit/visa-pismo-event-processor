package producers

import (
	"context"

	"github.com/andersonmarquesgit/visa-pismo-event-processor/internal/infrastructure/events/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
)

func NewEventProducer(conn *amqp.Connection) (*Producer, error) {
	producer := Producer{
		Connection:   conn,
		Exchange:     rabbitmq.EventsExchange,
		ExchangeType: rabbitmq.EventsExchangeType,
	}

	err := producer.setup()
	if err != nil {
		return nil, err
	}

	return &producer, nil
}

func (p *Producer) Publish(routingKey string, body []byte) error {
	channel, err := p.Connection.Channel()
	if err != nil {
		return err
	}
	defer channel.Close()

	return channel.PublishWithContext(
		context.Background(),
		p.Exchange,
		routingKey,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		},
	)
}
