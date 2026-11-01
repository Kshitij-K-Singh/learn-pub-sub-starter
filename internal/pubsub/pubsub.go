// Package pubsub provides reusable RabbitMQ publishing and subscription helpers.
package pubsub

import (
	"context"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// SimpleQueueType describes whether a queue persists or is transient.
type SimpleQueueType string

const (
	Durable   SimpleQueueType = "durable"
	Transient SimpleQueueType = "transient"
)

// DeclareAndBind declares a queue, binds it to an exchange and routing key,
// and returns the channel that owns the queue declaration.
func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
) (*amqp.Channel, amqp.Queue, error) {
	var durable, autoDelete, exclusive bool
	switch queueType {
	case Durable:
		durable = true
	case Transient:
		autoDelete = true
		exclusive = true
	default:
		return nil, amqp.Queue{}, fmt.Errorf("unsupported queue type %q", queueType)
	}

	channel, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	queue, err := channel.QueueDeclare(queueName, durable, autoDelete, exclusive, false, nil)
	if err != nil {
		channel.Close()
		return nil, amqp.Queue{}, err
	}

	if err := channel.QueueBind(queue.Name, key, exchange, false, nil); err != nil {
		channel.Close()
		return nil, amqp.Queue{}, err
	}

	return channel, queue, nil
}

// PublishJSON marshals val as JSON and publishes it to the given exchange and key.
func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	body, err := json.Marshal(val)
	if err != nil {
		return err
	}

	return ch.PublishWithContext(
		context.Background(),
		exchange,
		key,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		},
	)
}
