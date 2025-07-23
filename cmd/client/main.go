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

func handlerMove(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(am gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")
		outcome := gs.HandleMove(am)
		if outcome == gamelogic.MoveOutComeSafe {
			return pubsub.Ack
		}
		if outcome == gamelogic.MoveOutcomeMakeWar {
			msg := gamelogic.RecognitionOfWar{
				Attacker: am.Player,
				Defender: gs.GetPlayerSnap(),
			}
			err := pubsub.PublishJSON(ch, routing.ExchangePerilTopic, fmt.Sprintf("%s.%s", routing.WarRecognitionsPrefix, gs.GetUsername()), msg)
			if err != nil {
				fmt.Printf("Failed to publish war recognition message: %s\n", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		}
		return pubsub.NackDiscard
	}
}

func handlerWar(gs *gamelogic.GameState, ch *amqp.Channel) func(gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(warMsg gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")
		attacker := warMsg.Attacker.Username
		outcome, winner, loser := gs.HandleWar(warMsg)
		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeOpponentWon:
			logMsg := fmt.Sprintf("%s won a war against %s", winner, loser)
			err := pubsub.PublishGameLog(ch, attacker, logMsg)
			if err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		case gamelogic.WarOutcomeYouWon:
			logMsg := fmt.Sprintf("%s won a war against %s", winner, loser)
			err := pubsub.PublishGameLog(ch, attacker, logMsg)
			if err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		case gamelogic.WarOutcomeDraw:
			logMsg := fmt.Sprintf("A war between %s and %s resulted in a draw", winner, loser)
			err := pubsub.PublishGameLog(ch, attacker, logMsg)
			if err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		default:
			fmt.Printf("Error: unexpected war outcome %d\n", outcome)
			return pubsub.NackDiscard
		}
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
	gameState := gamelogic.NewGameState(username)
	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilDirect, pauseQueueName, routing.PauseKey, pubsub.Transient, handlerPause(gameState))
	if err != nil {
		fmt.Printf("Failed to subscribe to pause messages: %s\n", err)
		os.Exit(1)
	}

	armyMovesQueueName := fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, username)
	armyMovesRoutingKey := fmt.Sprintf("%s.*", routing.ArmyMovesPrefix)
	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic, armyMovesQueueName, armyMovesRoutingKey, pubsub.Transient, handlerMove(gameState, rabbitChan))
	if err != nil {
		fmt.Printf("Failed to subscribe to army move messages: %s\n", err)
		os.Exit(1)
	}

	warQueueName := "war"
	warRoutingKey := fmt.Sprintf("%s.*", routing.WarRecognitionsPrefix)
	err = pubsub.SubscribeJSON(conn, routing.ExchangePerilTopic, warQueueName, warRoutingKey, pubsub.Durable, handlerWar(gameState, rabbitChan))
	if err != nil {
		fmt.Printf("Failed to subscribe to war recognition messages: %s\n", err)
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
