package main

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/tokopedia/TKPModellingTechWorkshop/delivery"
)

func main() {
	topicName := "demo_topic"
	bootstrapServers := "127.0.0.1"
	groupId := "sample_group"
	autoOffsetReset := "latest"
	timeout := "10s"
	batchSize := 2

	fmt.Println("Initializing Kafka Consumer... ")
	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": bootstrapServers,
		"group.id":          groupId,
		"auto.offset.reset": autoOffsetReset,
	})
	if err != nil {
		fmt.Println("Error occurred while creating Kafka Consumer Client: ", err)
		return
	}
	fmt.Println("Initialized Kafka Consumer: ", consumer)

	fmt.Println("Subscribing to the topic... ")
	err = consumer.SubscribeTopics([]string{topicName}, nil)
	if err != nil {
		fmt.Println("Error occurred while subscribing to the topic", err)
		return
	}
	fmt.Println("Subscribed to Kafka Topic: ", topicName)

	fmt.Println("Initializing Kafka Delivery Layer... ")
	deliveryLayer, err := delivery.NewKafkaDeliveryLayer(delivery.KafkaDeliveryParams{
		Consumer:  consumer,
		Usecase:   nil, // need to give usecase here
		Timeout:   timeout,
		BatchSize: batchSize,
	})
	if err != nil {
		fmt.Println("Error occurred while initializing Kafka Delivery Layer as: ", err)
		return
	}
	fmt.Println("Initialized Kafka Delivery Layer: ", deliveryLayer)

	deliveryLayer.ConsumeEvents(topicName)

}
