package main

import (
	"fmt"
	"os"
	"os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")
	connectionString := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(connectionString)
	if err != nil {
		fmt.Printf("Failed to connect to RabbitMQ: %s\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	fmt.Println("Connected to RabbitMQ successfully!")

	rabbitChan, err := conn.Channel()
	if err != nil {
		fmt.Printf("Failed to open a channel: %s\n", err)
		os.Exit(1)
	}

	msg := routing.PlayingState{
		IsPaused: true,
	}
	err = pubsub.PublishJSON(rabbitChan, routing.ExchangePerilDirect, routing.PauseKey, msg)
	if err != nil {
		fmt.Printf("Failed to publish message: %s\n", err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan)

	<-sigChan
	fmt.Println("Signal received, shutting down...")
	conn.Close()
	os.Exit(0)
}
