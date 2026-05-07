package rabbitmq

import amqp "github.com/rabbitmq/amqp091-go"

const (
	EventsExchange = "events-exchange"

	EventsExchangeType = "topic"

	EventsProcessingQueue = "events-processing-queue"
)

func DeclareExchange(ch *amqp.Channel, exchangeName, exchangeType string) error {
	return ch.ExchangeDeclare(
		exchangeName,
		exchangeType,
		true,
		false,
		false,
		false,
		nil,
	)
}

func DeclareQueue(ch *amqp.Channel, queueName string) (amqp.Queue, error) {
	return ch.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		nil,
	)
}
