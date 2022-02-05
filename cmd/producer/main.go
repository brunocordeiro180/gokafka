package main

import (
	"fmt"
	"log"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	deliveryChanel := make(chan kafka.Event)
	var producer *kafka.Producer = NewKafkaProducer()
	Publish("mensagem", "teste", producer, []byte("transferencia"), deliveryChanel)
	go DeliveryReport(deliveryChanel) //async
	e := <-deliveryChanel
	msg := e.(*kafka.Message)

	if msg.TopicPartition.Error != nil {
		fmt.Println("Erro ao enviar")
	} else {
		fmt.Println("Mensagem enviada: ", msg.TopicPartition)
	}
}

func NewKafkaProducer() *kafka.Producer {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": "gokafka_kafka_1:9092",
		// "delivery.timeout.ms": "0",
		// "acks":                "all",
		// "enable.idempotence":  "true", //se deixar como true o acks tem que ser all
	}

	p, err := kafka.NewProducer(configMap)
	if err != nil {
		log.Println(err.Error())
	}

	return p
}

func Publish(msg, topic string, producer *kafka.Producer, key []byte, deliveryChan chan kafka.Event) error {
	message := &kafka.Message{
		Value:          []byte(msg),
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            key,
	}

	err := producer.Produce(message, deliveryChan)

	if err != nil {
		return err
	}

	return nil
}

func DeliveryReport(deliveryChan chan kafka.Event) {
	for e := range deliveryChan {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				fmt.Println("Erro ao enviar")
			} else {
				fmt.Println("Mensagem enviada: ", ev.TopicPartition)
			}
		}
	}
}
