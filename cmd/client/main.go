package main

import (
	"fmt"
	"os"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"

	amqp "github.com/rabbitmq/amqp091-go"
)

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	return func(ps routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return pubsub.Ack
	}
}

func handlerMove(gs *gamelogic.GameState) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(am gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")
		outcome := gs.HandleMove(am)
		if outcome == gamelogic.MoveOutComeSafe || outcome == gamelogic.MoveOutcomeMakeWar {
			return pubsub.Ack
		}
		return pubsub.NackDiscard
	}
}

func main() {
	fmt.Println("Starting Peril client...")
	connectionString := "amqp://guest:guest@localhost:5672/"
	conn, err := amqp.Dial(connectionString)
	if err != nil {
		fmt.Printf("Failed to connect to RabbitMQ: %s\n", err)
		os.Exit(1)
	}
	defer conn.Close()

	fmt.Println("Connected to RabbitMQ successfully!")

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		fmt.Printf("Error during client welcome: %s\n", err)
		os.Exit(1)
	}

	rabbitChan, err := conn.Channel()
	if err != nil {
		fmt.Printf("Failed to open a channel: %s\n", err)
		os.Exit(1)
	}

	pauseQueueName := fmt.Sprintf("%s.%s", routing.PauseKey, username)
	_, _, err = pubsub.DeclareAndBind(conn, routing.ExchangePerilDirect, pauseQueueName, routing.PauseKey, pubsub.Transient)
	if err != nil {
		fmt.Printf("Failed to declare and bind queue: %s\n", err)
		os.Exit(1)
	}

	gameState := gamelogic.NewGameState(username)
	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilDirect, pauseQueueName, routing.PauseKey, pubsub.Transient, handlerPause(gameState))
	if err != nil {
		fmt.Printf("Failed to subscribe to pause messages: %s\n", err)
		os.Exit(1)
	}

	armyMovesQueueName := fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, username)
	armyMovesRoutingKey := fmt.Sprintf("%s.*", routing.ArmyMovesPrefix)
	_, _, err = pubsub.DeclareAndBind(conn, routing.ExchangePerilTopic, armyMovesQueueName, armyMovesRoutingKey, pubsub.Transient)
	if err != nil {
		fmt.Printf("Failed to declare and bind queue: %s\n", err)
		os.Exit(1)
	}

	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic, armyMovesQueueName, armyMovesRoutingKey, pubsub.Transient, handlerMove(gameState))
	if err != nil {
		fmt.Printf("Failed to subscribe to army move messages: %s\n", err)
		os.Exit(1)
	}

outerloop:
	for {
		userWords := gamelogic.GetInput()
		if len(userWords) == 0 {
			continue
		}

		switch userWords[0] {
		case "spawn":
			err := gameState.CommandSpawn(userWords)
			if err != nil {
				fmt.Printf("Error processing spawn command: %s\n", err)
			}
		case "move":
			move, err := gameState.CommandMove(userWords)
			if err != nil {
				fmt.Printf("Error processing move command: %s\n", err)
				continue
			}
			err = pubsub.PublishJSON(rabbitChan, routing.ExchangePerilTopic, fmt.Sprintf("army_moves.%s", username), move)
			if err != nil {
				fmt.Printf("Failed to publish move message: %s\n", err)
				continue
			}
			fmt.Println("Move successful!")
		case "status":
			gameState.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("Spamming not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
			break outerloop
		default:
			fmt.Println("I don't understand the command")
			continue
		}
	}
	conn.Close()
	os.Exit(0)
}
