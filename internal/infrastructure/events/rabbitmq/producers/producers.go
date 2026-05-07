package producers

import (
	"github.com/andersonmarquesgit/visa-pismo-event-processor/internal/infrastructure/events/rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
)

type Producer struct {
	Connection   *amqp.Connection
	Exchange     string
	ExchangeType string
}

type Producers struct {
	EventProducer *Producer
}

func NewProducers(conn *amqp.Connection) (*Producers, error) {
	eventProducer, err := NewEventProducer(conn)
	if err != nil {
		return nil, err
	}

	return &Producers{
		EventProducer: eventProducer,
	}, nil
}

func (producer *Producer) setup() error {
	channel, err := producer.Connection.Channel()
	if err != nil {
		return err
	}
	defer channel.Close()

	return rabbitmq.DeclareExchange(channel, producer.Exchange, producer.ExchangeType)
}
