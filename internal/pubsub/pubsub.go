package pubsub

import (
	"context"
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type SimpleQueueType string

const (
	Durable   SimpleQueueType = "durable"
	Transient SimpleQueueType = "transient"
)

type AckType string

const (
	Ack         AckType = "ack"
	NackRequeue AckType = "nack_requeue"
	NackDiscard AckType = "nack_discard"
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
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
) (*amqp.Channel, amqp.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	durable := (queueType == "durable")
	autoDelete := (queueType == "transient")
	exclusive := (queueType == "transient")
	noWait := false

	deadLetterExchange := "peril_dlx"
	table := map[string]any{"x-dead-letter-exchange": deadLetterExchange}

	queue, err := ch.QueueDeclare(queueName, durable, autoDelete, exclusive, noWait, table)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	err = ch.QueueBind(queueName, key, exchange, noWait, nil)
	if err != nil {
		return nil, amqp.Queue{}, err
	}

	return ch, queue, nil
}

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T) AckType, // handler function that processes the message and returns an AckType
) error {
	ch, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		fmt.Printf("Error declaring and binding queue: %s\n", err)
		return err
	}

	deliverCh, err := ch.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		fmt.Printf("Error starting consumer: %s\n", err)
		return err
	}

	go func() {
		for d := range deliverCh {
			var body T
			if err := json.Unmarshal(d.Body, &body); err != nil {
				fmt.Printf("Error unmarshalling message: %s\n", err)
				continue
			}
			ackType := handler(body)
			switch ackType {
			case Ack:
				fmt.Printf("Acknowledging message: %s\n", d.Body)
				err = d.Ack(false)
				if err != nil {
					fmt.Printf("Error acknowledging message: %s\n", err)
				}
			case NackRequeue:
				fmt.Printf("Nacking and requeuing message: %s\n", d.Body)
				err = d.Nack(false, true)
				if err != nil {
					fmt.Printf("Error nacking and requeuing message: %s\n", err)
				}
			case NackDiscard:
				fmt.Printf("Nacking and discarding message: %s\n", d.Body)
				err = d.Nack(false, false)
				if err != nil {
					fmt.Printf("Error nacking and discarding message: %s\n", err)
				}
			}
		}
	}()
	return nil
}
