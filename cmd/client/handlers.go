package main

import (
	"fmt"

	"github.com/jthughes/peril/internal/gamelogic"
	"github.com/jthughes/peril/internal/pubsub"
	"github.com/jthughes/peril/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	return func(ps routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return pubsub.MsgAck
	}
}

func handlerMove(gs *gamelogic.GameState, publishCh *amqp.Channel) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(move gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")
		outcome := gs.HandleMove(move)
		switch outcome {
		case gamelogic.MoveOutComeSafe:
			return pubsub.MsgAck
		case gamelogic.MoveOutcomeMakeWar:
			err := pubsub.PublishJSON(publishCh, routing.ExchangePerilTopic, fmt.Sprintf("%s.%s", routing.WarRecognitionsPrefix, gs.GetUsername()), gamelogic.RecognitionOfWar{
				Attacker: move.Player,
				Defender: gs.GetPlayerSnap(),
			})
			if err != nil {
				fmt.Println("Failed to publish war move, retrying...")
				return pubsub.MsgNackRequeue
			}
			return pubsub.MsgAck
		default:
			// outcome = same player or anything else
			return pubsub.MsgNackDiscard
		}
	}
}

func handlerWar(gs *gamelogic.GameState) func(rw gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(rw gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")
		outcome, _, _ := gs.HandleWar(rw)
		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.MsgNackRequeue
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.MsgNackDiscard
		case gamelogic.WarOutcomeOpponentWon:
			return pubsub.MsgAck
		case gamelogic.WarOutcomeYouWon:
			return pubsub.MsgAck
		case gamelogic.WarOutcomeDraw:
			return pubsub.MsgAck
		default:
			fmt.Printf("Unrecognised outcome: %v\n", outcome)
			return pubsub.MsgNackDiscard
		}
	}
}
