package pubsub

import (
	"context"
	"encoding/json"

	amqp "github.com/rabbitmq/amqp091-go"
)

type simpleQueueType string

const (
	Durable   simpleQueueType = "durable"
	Transient simpleQueueType = "transient"
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	body, err := json.Marshal(val)
	if err != nil {
		return err
	}
	msg := amqp.Publishing{
		ContentType: "application/json",
		Body:        body,
	}
	// func (ch *Channel) PublishWithContext(_ context.Context, exchange, key string, mandatory, immediate bool, msg Publishing) error
	return ch.PublishWithContext(context.Background(), exchange, key, false, false, msg)
}

func DeclareAndBind(
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType simpleQueueType, // an enum to represent "durable" or "transient"
) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	durable := (queueType == "durable")
	autoDelete := (queueType == "transient")
	exclusive := (queueType == "transient")
	noWait := false

	queue, err := ch.QueueDeclare(queueName, durable, autoDelete, exclusive, noWait, nil)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	err = ch.QueueBind(queueName, key, exchange, noWait, nil)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	return ch, queue, nil
}
