package main

import (
	"fmt"
	"os"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func handlerGameLog() func(routing.GameLog) pubsub.AckType {
	return func(gamelog routing.GameLog) pubsub.AckType {
		defer fmt.Print("> ")
		err := gamelogic.WriteLog(gamelog)
		if err != nil {
			fmt.Printf("Failed to write game log: %s\n", err)
		}
		return pubsub.Ack
	}
}

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

	_, _, err = pubsub.DeclareAndBind(conn, routing.ExchangePerilTopic, routing.GameLogSlug, "game_logs.*", pubsub.Durable)
	if err != nil {
		fmt.Printf("Failed to declare and bind game logs queue: %s\n", err)
		os.Exit(1)
	}

	pubsub.SubscribeGob(conn, routing.ExchangePerilTopic, routing.GameLogSlug, "game_logs.*", pubsub.Durable, handlerGameLog())

	gamelogic.PrintServerHelp()
outerloop:
	for {
		userWords := gamelogic.GetInput()
		if len(userWords) == 0 {
			continue
		}

		switch userWords[0] {
		case "pause":
			fmt.Println("Sending pause message to RabbitMQ...")
			msg := routing.PlayingState{
				IsPaused: true,
			}
			err = pubsub.PublishJSON(rabbitChan, routing.ExchangePerilDirect, routing.PauseKey, msg)
			if err != nil {
				fmt.Printf("Failed to publish message: %s\n", err)
			}
		case "resume":
			fmt.Println("Sending pause message to RabbitMQ...")
			msg := routing.PlayingState{
				IsPaused: false,
			}
			err = pubsub.PublishJSON(rabbitChan, routing.ExchangePerilDirect, routing.PauseKey, msg)
			if err != nil {
				fmt.Printf("Failed to publish message: %s\n", err)
			}
		case "quit":
			fmt.Println("Exiting...")
			break outerloop
		default:
			fmt.Println("I don't understand the command")
			continue
		}
	}

	conn.Close()
	os.Exit(0)
}
