package main

import (
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/gocql/gocql"
	"github.com/pranjalmohansaxena/TKPModellingTechWorkshop/delivery"
	"github.com/pranjalmohansaxena/TKPModellingTechWorkshop/repository"
	"github.com/pranjalmohansaxena/TKPModellingTechWorkshop/usecase"
	"go.uber.org/ratelimit"
)

func main() {

	// kafka connection settings
	topicName := "streaming-pipeline"
	bootstrapServers := "127.0.0.1"
	groupId := "sample_group"
	autoOffsetReset := "latest"
	timeout := "10s"
	batchSize := 2

	// Cassandra connection settings
	cluster := gocql.NewCluster("127.0.0.1")
	cluster.Port = 9042
	cluster.Keyspace = "streaming_pipeline"
	cluster.Consistency = gocql.Quorum
	cluster.ConnectTimeout = time.Second * 10
	cluster.Timeout = time.Second * 10
	tableName := "pipeline_data"
	rate := 5
	keyspace := "streaming_pipeline"

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

	fmt.Println("Initializing Datastore Repository Layer... ")
	session, err := cluster.CreateSession()
	if err != nil {
		fmt.Println("Error occurred while creating cassandra session as: ", err)
		return
	}
	respositoryLayer, err := repository.NewCassandraRepo(repository.CassandraParams{
		Session:   session,
		TableName: tableName,
		Keyspace:  keyspace,
		Rl:        ratelimit.New(rate),
	})
	if err != nil {
		fmt.Println("Error occurred while initializing Repository Layer as: ", err)
		return
	}

	fmt.Println("Initializing Usecase Layer... ")
	usecaseLayer, err := usecase.NewPipelineUsecase(usecase.Param{
		Repository: respositoryLayer,
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
