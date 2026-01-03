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
	fmt.Println("Starting Peril client...")
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

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		fmt.Println(err)
		return
	}

	pubsub.DeclareAndBind(connection, routing.ExchangePerilDirect, fmt.Sprintf("%s.%s", routing.PauseKey, username), routing.PauseKey, pubsub.Transient)

	state := gamelogic.NewGameState(username)
	pubsub.SubscribeJSON(connection, routing.ExchangePerilDirect, fmt.Sprintf("%s.%s", routing.PauseKey, username), routing.PauseKey, pubsub.Transient, handlerPause(state))

	pubsub.SubscribeJSON(connection, routing.ExchangePerilTopic, fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, username), fmt.Sprintf("%s.*", routing.ArmyMovesPrefix), pubsub.Transient, handlerMove(state))

	inMenu := true
	for inMenu {
		input := gamelogic.GetInput()
		switch strings.ToLower(input[0]) {
		case "spawn":
			err = state.CommandSpawn(input)
			if err != nil {
				fmt.Printf("Error: %s", err)
			} else {
				fmt.Println("Success!")
			}
		case "move":
			move, err := state.CommandMove(input)
			if err != nil {
				fmt.Printf("Error: %s", err)
			} else {
				fmt.Println("Success!")
				pubsub.PublishJSON(ch, routing.ExchangePerilTopic, fmt.Sprintf("%s.%s", routing.ArmyMovesPrefix, username), move)
			}
		case "status":
			state.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			fmt.Println("Spamming not allowed yet!")

		case "quit":
			gamelogic.PrintQuit()
			inMenu = false
		default:
			fmt.Println("Command not recognised")
		}
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan
}
