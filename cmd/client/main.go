package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")
	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatal(err)
	}

	connectionString := "amqp://guest:guest@localhost:5672/"
	connection, err := amqp.Dial(connectionString)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer connection.Close()

	fmt.Println("Connected to RabbitMQ successfully.")
	gameState := gamelogic.NewGameState(username)
	if err := pubsub.SubscribeJSON(
		connection,
		routing.ExchangePerilTopic,
		routing.ArmyMovesPrefix+"."+username,
		routing.ArmyMovesPrefix+".*",
		pubsub.Transient,
		handlerMove(gameState),
	); err != nil {
		log.Fatalf("Failed to subscribe to army moves: %v", err)
	}

	if err := pubsub.SubscribeJSON(
		connection,
		routing.ExchangePerilDirect,
		routing.PauseKey+"."+username,
		routing.PauseKey,
		pubsub.Transient,
		handlerPause(gameState),
	); err != nil {
		log.Fatalf("Failed to subscribe to pause messages: %v", err)
	}
	publishChannel, err := connection.Channel()
	if err != nil {
		log.Fatalf("Failed to open a RabbitMQ channel for publishing: %v", err)
	}
	defer publishChannel.Close()

commandLoop:
	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}

		switch words[0] {
		case "spawn":
			if err := gameState.CommandSpawn(words); err != nil {
				fmt.Println(err)
			}
		case "move":
			move, err := gameState.CommandMove(words)
			if err != nil {
				fmt.Println(err)
			} else {
				if err := pubsub.PublishJSON(
					publishChannel,
					routing.ExchangePerilTopic,
					routing.ArmyMovesPrefix+"."+username,
					move,
				); err != nil {
					log.Printf("Failed to publish move: %v", err)
				} else {
					fmt.Println("Move published successfully!")
				}
			}
		case "status":
			gameState.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("Spamming not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
			break commandLoop
		default:
			fmt.Printf("Unknown command: %s\n", words[0])
		}
	}

	fmt.Println("Shutting down Peril client...")
}

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	return func(state routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")
		gs.HandlePause(state)
		return pubsub.Ack
	}
}

func handlerMove(gs *gamelogic.GameState) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(move gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")
		switch gs.HandleMove(move) {
		case gamelogic.MoveOutComeSafe, gamelogic.MoveOutcomeMakeWar:
			return pubsub.Ack
		default:
			return pubsub.NackDiscard
		}
	}
}
