package main

import (
	"fmt"
	"os"
	"os/signal"
	"strings"

	"github.com/jthughes/peril/internal/gamelogic"
	"github.com/jthughes/peril/internal/pubsub"
	"github.com/jthughes/peril/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")
	connection_str := "amqp://guest:guest@localhost:5672/"
	connection, err := amqp.Dial(connection_str)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer connection.Close()
	fmt.Println("Connection established")

	ch, err := connection.Channel()
	if err != nil {
		fmt.Println(err)
		return
	}

	_, _, err = pubsub.DeclareAndBind(connection, routing.ExchangePerilTopic, routing.GameLogSlug, fmt.Sprintf("%s.*", routing.GameLogSlug), pubsub.Durable)

	gamelogic.PrintServerHelp()
	inMenu := true
	for inMenu {
		input := gamelogic.GetInput()
		switch strings.ToLower(input[0]) {
		case "pause":
			pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})
		case "resume":
			pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: false})
		case "quit":
			fmt.Println("Exiting...")
			inMenu = false
		default:
			fmt.Println("Command not recognised")
		}
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
	fmt.Println("Closing connection and shutting down")
}
