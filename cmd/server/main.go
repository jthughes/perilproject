package main

import (
	"fmt"
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

	logHandler := func(gl routing.GameLog) pubsub.AckType {
		defer fmt.Print("> ")
		gamelogic.WriteLog(gl)
		return pubsub.MsgAck
	}

	err = pubsub.SubscribeGob(connection, routing.ExchangePerilTopic, routing.GameLogSlug, fmt.Sprintf("%s.*", routing.GameLogSlug), pubsub.Durable, logHandler)
	if err != nil {
		fmt.Println(err)
		return
	}

	gamelogic.PrintServerHelp()
	for {
		input := gamelogic.GetInput()
		switch strings.ToLower(input[0]) {
		case "pause":
			pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})
		case "resume":
			pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: false})
		case "quit":
			fmt.Println("Exiting...")
			return
		default:
			fmt.Println("Command not recognised")
		}
	}

}
