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
	fmt.Println("Starting Peril server...")

	connectionString := "amqp://guest:guest@localhost:5672/"
	connection, err := amqp.Dial(connectionString)
	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %v", err)
	}
	defer connection.Close()

	channel, err := connection.Channel()
	if err != nil {
		log.Fatalf("Failed to open a RabbitMQ channel: %v", err)
	}
	defer channel.Close()

	fmt.Println("Connected to RabbitMQ successfully.")
	gamelogic.PrintServerHelp()

commandLoop:
	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}

		switch words[0] {
		case "pause":
			fmt.Println("Sending pause message...")
			if err := pubsub.PublishJSON(
				channel,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{IsPaused: true},
			); err != nil {
				log.Fatalf("Failed to publish paused state: %v", err)
			}
		case "resume":
			fmt.Println("Sending resume message...")
			if err := pubsub.PublishJSON(
				channel,
				routing.ExchangePerilDirect,
				routing.PauseKey,
				routing.PlayingState{IsPaused: false},
			); err != nil {
				log.Fatalf("Failed to publish resumed state: %v", err)
			}
		case "quit":
			fmt.Println("Exiting...")
			break commandLoop
		default:
			fmt.Printf("I don't understand the command %q.\n", words[0])
		}
	}

	fmt.Println("Shutting down Peril server...")
}
