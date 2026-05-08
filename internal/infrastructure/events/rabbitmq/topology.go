package rabbitmq

import amqp "github.com/rabbitmq/amqp091-go"

const (
	EventsExchange = "events-exchange"

	EventsExchangeType = "topic"

	EventsProcessingQueue = "events-processing-queue"

	// EventsDeadLetterQueue receives messages rejected from EventsProcessingQueue (nack requeue=false)
	// or that expire, for inspection and replay.
	EventsDeadLetterQueue = "events-dead-letter-queue"
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

func DeclareDeadLetterQueue(ch *amqp.Channel) (amqp.Queue, error) {
	return ch.QueueDeclare(
		EventsDeadLetterQueue,
		true,
		false,
		false,
		false,
		nil,
	)
}

// DeclareEventsProcessingQueue declares the DLQ then the processing queue wired to dead-letter
// failed messages via the default exchange (x-dead-letter-routing-key -> EventsDeadLetterQueue).
func DeclareEventsProcessingQueue(ch *amqp.Channel) (amqp.Queue, error) {
	if _, err := DeclareDeadLetterQueue(ch); err != nil {
		return amqp.Queue{}, err
	}
	args := amqp.Table{
		"x-dead-letter-exchange":    "",
		"x-dead-letter-routing-key": EventsDeadLetterQueue,
	}
	return ch.QueueDeclare(
		EventsProcessingQueue,
		true,
		false,
		false,
		false,
		args,
	)
}
