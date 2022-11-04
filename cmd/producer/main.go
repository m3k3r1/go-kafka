package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	deliveryChan := make(chan kafka.Event)

	producer := NewKafkaProducer()
	defer producer.Close()

	err := Publish("Hello World", "test", producer, nil, deliveryChan)
	if err != nil {
		panic(err)
	}

	go DelieveryReport(deliveryChan)
	producer.Flush(10000)
}

func NewKafkaProducer() *kafka.Producer {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": "kafka:9092",
	}

	p, err := kafka.NewProducer(configMap)
	if err != nil {
		panic(err)
	}

	return p
}

func Publish(msg string, topic string, producer *kafka.Producer, key []byte, deliveryChan chan kafka.Event) error {
	message := &kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(msg),
		Key:            key,
	}

	err := producer.Produce(message, deliveryChan)
	if err != nil {
		return err
	}

	return nil
}

func DelieveryReport(deliveryChan chan kafka.Event) {
	for e := range deliveryChan {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				fmt.Println("Delivery failed: ", ev.TopicPartition.Error)
			} else {
				fmt.Println("Delivered message to topic ", ev.TopicPartition)
				// Use case -> Save transaction to database
			}
		}
	}
}
