package main

import (
	"fmt"

	"github.com/jthughes/peril/internal/gamelogic"
	"github.com/jthughes/peril/internal/pubsub"
	"github.com/jthughes/peril/internal/routing"
)

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	return func(ps routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return pubsub.MsgAck
	}
}

func handlerMove(gs *gamelogic.GameState) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(move gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")
		outcome := gs.HandleMove(move)
		if outcome == gamelogic.MoveOutComeSafe || outcome == gamelogic.MoveOutcomeMakeWar {
			return pubsub.MsgAck
		} else {
			// outcome = same player or anything else
			return pubsub.MsgNackDiscard
		}
	}
}
