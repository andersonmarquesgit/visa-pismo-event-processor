package rabbitmq

import (
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

const maxRetryConnection = 5

func NewConnection(url string) (*amqp.Connection, error) {
	var retries int

	for {
		conn, err := amqp.Dial(url)
		if err == nil {
			return conn, nil
		}

		retries++

		if retries >= maxRetryConnection {
			return nil, fmt.Errorf("rabbitmq connection failed: %w", err)
		}

		backoff := time.Duration(retries*retries) * time.Second

		time.Sleep(backoff)
	}
}
