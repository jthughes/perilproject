package pubsub

import (
	"bytes"
	"encoding/gob"
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

const (
	PrefetchCount = 10
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
	ch.Qos(PrefetchCount, 0, false)
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
			switch resp {
			case MsgAck:
				delivery.Ack(false)
			case MsgNackRequeue:
				delivery.Nack(false, true)
			case MsgNackDiscard:
				delivery.Nack(false, false)
			}

		}
	}()
	return nil
}

func SubscribeGob[T any](
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
	ch.Qos(PrefetchCount, 0, false)
	chDelivery, err := ch.Consume(queue.Name, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("could not consume messages: %v", err)
	}

	unmarshaller := func(data []byte) (T, error) {
		buffer := bytes.NewBuffer(data)
		dec := gob.NewDecoder(buffer)

		var result T
		err := dec.Decode(&result)
		return result, err
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
			switch resp {
			case MsgAck:
				delivery.Ack(false)
			case MsgNackRequeue:
				delivery.Nack(false, true)
			case MsgNackDiscard:
				delivery.Nack(false, false)
			}

		}
	}()
	return nil
}
