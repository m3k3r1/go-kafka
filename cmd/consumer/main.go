package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func main() {
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": "kafka:9092",
		"client.id":         "go-consumer",
		"group.id":          "go-consumer-group",
		"auto.offset.reset": "earliest", // earliest -> read from the beginning of the topic, latest -> read from the end of the topic
	}

	c, err := kafka.NewConsumer(configMap)
	if err != nil {
		panic(err)
	}

	topics := []string{"test"}
	err = c.SubscribeTopics(topics, nil)
	if err != nil {
		panic(err)
	}

	for {
		msg, err := c.ReadMessage(-1)
		if err == nil {
			fmt.Printf("Message on %s: %s\n", msg.TopicPartition, string(msg.Value))
		} else {
			fmt.Printf("Consumer error: %v (%v)\n", err, msg)
		}
	}

}
