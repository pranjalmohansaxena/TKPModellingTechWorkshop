package main

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/tokopedia/TKPModellingTechWorkshop/delivery"
	"github.com/tokopedia/TKPModellingTechWorkshop/usecase"
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

	fmt.Println("Initializing Usecase Layer... ")
	usecaseLayer, err := usecase.NewPipelineUsecase(usecase.Param{
		Repository: nil, // need to pass the repository layer
	})
	if err != nil {
		fmt.Println("Error occurred while initializing Usecase Layer as: ", err)
		return
	}
	fmt.Println("Initialized Usecase Layer: ", usecaseLayer)

	fmt.Println("Initializing Kafka Delivery Layer... ")
	deliveryLayer, err := delivery.NewKafkaDeliveryLayer(delivery.KafkaDeliveryParams{
		Consumer:  consumer,
		Usecase:   usecaseLayer,
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
