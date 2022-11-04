package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	deliveryChan := make(chan kafka.Event)

	producer := NewKafkaProducer()
	defer producer.Close()

	// key guaranties that the message will be delivered to the same partition
	err := Publish("Transfer 10", "test", producer, []byte("transference"), deliveryChan)
	if err != nil {
		panic(err)
	}

	go DelieveryReport(deliveryChan)
	producer.Flush(10000)
}

func NewKafkaProducer() *kafka.Producer {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers":   "kafka:9092",
		"delivery.timeout.ms": "0",
		"acks":                "all",  // 0 -> no acknowledge , 1 -> just acknowledge of leader, all -> acknowledge of leader and replicas
		"enable.idempotence":  "true", // guarantee that messages are produced only once (default is false)
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
