package pubsub

import (
	"encoding/json"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type AckType int

const (
	MsgAck AckType = iota
	MsgNackRequeue
	MsgNackDiscard
)

func SubscribeJSON[T any](
	conn *amqp.Connection,
	exchange,
	queueName,
	key string,
	queueType SimpleQueueType, // an enum to represent "durable" or "transient"
	handler func(T) AckType,
) error {
	ch, queue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return fmt.Errorf("could not declare and bind queue: %v", err)
	}
	chDelivery, err := ch.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("could not consume messages: %v", err)
	}

	unmarshaller := func(data []byte) (T, error) {
		var target T
		err := json.Unmarshal(data, &target)
		return target, err
	}

	go func() {
		defer ch.Close()
		for delivery := range chDelivery {
			target, err := unmarshaller(delivery.Body)
			if err != nil {
				fmt.Printf("could not unmarshal message: %v\n", err)
				continue
			}
			resp := handler(target)
			if resp == MsgAck {
				delivery.Ack(false)
				fmt.Println("Ack")
			} else if resp == MsgNackRequeue {
				delivery.Nack(false, true)
				fmt.Println("Nack (Re)")
			} else if resp == MsgNackDiscard {
				delivery.Nack(false, false)
				fmt.Println("Nack (Disc)")
			}

		}
	}()
	return nil
}
