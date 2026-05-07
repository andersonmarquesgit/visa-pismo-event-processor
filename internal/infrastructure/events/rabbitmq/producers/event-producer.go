package producers

import (
	"context"

	amqp "github.com/rabbitmq/amqp091-go"
)

func NewEventProducer(conn *amqp.Connection) (*Producer, error) {
	producer := Producer{
		Connection:   conn,
		Exchange:     "events-exchange",
		QueueName:    "events-queue",
		ExchangeType: "topic",
	}

	err := producer.setup()
	if err != nil {
		return nil, err
	}

	return &producer, nil
}

func (p *Producer) Publish(body []byte) error {
	channel, err := p.Connection.Channel()
	if err != nil {
		return err
	}
	defer channel.Close()

	return channel.PublishWithContext(
		context.Background(),
		p.Exchange,
		p.RoutingKey,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		},
	)
}
