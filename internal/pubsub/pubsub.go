// Package pubsub provides reusable RabbitMQ publishing and subscription helpers.
package pubsub

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

// SimpleQueueType describes whether a queue persists or is transient.
type SimpleQueueType string

const (
	Durable   SimpleQueueType = "durable"
	Transient SimpleQueueType = "transient"
)

// AckType determines how a consumed RabbitMQ message is acknowledged.
type AckType uint8

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
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

	queue, err := channel.QueueDeclare(queueName, durable, autoDelete, exclusive, false, amqp.Table{
		"x-dead-letter-exchange": routing.ExchangePerilDLX,
	})
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

// SubscribeJSON declares and binds a queue, then decodes and handles messages
// delivered to it. Messages are acknowledged after handling (or after a decode
// failure) so malformed messages do not remain in the queue indefinitely.
func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType,
	handler func(T) AckType,
) error {
	channel, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}

	deliveries, err := channel.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		channel.Close()
		return err
	}

	go func() {
		defer channel.Close()
		for delivery := range deliveries {
			var message T
			ackType := Ack
			if err := json.Unmarshal(delivery.Body, &message); err != nil {
				log.Printf("Failed to decode RabbitMQ message: %v", err)
			} else {
				ackType = handler(message)
			}

			switch ackType {
			case Ack:
				log.Printf("Acknowledging RabbitMQ message")
				if err := delivery.Ack(false); err != nil {
					log.Printf("Failed to acknowledge RabbitMQ message: %v", err)
				}
			case NackRequeue:
				log.Printf("Rejecting RabbitMQ message and requeueing it")
				if err := delivery.Nack(false, true); err != nil {
					log.Printf("Failed to reject and requeue RabbitMQ message: %v", err)
				}
			case NackDiscard:
				log.Printf("Rejecting RabbitMQ message without requeueing it")
				if err := delivery.Nack(false, false); err != nil {
					log.Printf("Failed to reject RabbitMQ message: %v", err)
				}
			default:
				log.Printf("Unknown RabbitMQ acknowledgement type %d; discarding message", ackType)
				if err := delivery.Nack(false, false); err != nil {
					log.Printf("Failed to reject RabbitMQ message: %v", err)
				}
			}
		}
	}()

	return nil
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
