package delivery

import (
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/tokopedia/TKPModellingTechWorkshop/usecase"
)

type KafkaDeliveryParams struct {
	Consumer  *kafka.Consumer
	Usecase   usecase.Usecase
	Timeout   string
	BatchSize int
}

type kafkaDelivery struct {
	consumer  *kafka.Consumer
	usecase   usecase.Usecase
	timeout   time.Duration
	batchSize int
}

func NewKafkaDeliveryLayer(params KafkaDeliveryParams) (Delivery, error) {
	isValidParams := validateKafkaDeliveryParams(params)
	if !isValidParams {
		err := fmt.Errorf("invalid params are provided")
		return nil, err
	}

	timeoutValue, err := time.ParseDuration(params.Timeout)
	if err != nil {
		err = fmt.Errorf("failed to parse query timeout from string: %+v", err)
		return nil, err
	}

	kafkaDeliveryLayer := &kafkaDelivery{
		consumer:  params.Consumer,
		usecase:   params.Usecase,
		timeout:   timeoutValue,
		batchSize: params.BatchSize,
	}

	return kafkaDeliveryLayer, nil
}

func validateKafkaDeliveryParams(params KafkaDeliveryParams) bool {
	if params.Consumer == nil || params.Usecase == nil{
		return false
	}

	return true
}

func (k *kafkaDelivery) ConsumeEvents(topicName string) {
	defer k.consumer.Close()

	events := make([]interface{}, 0, k.batchSize)
	for {
		msg, err := k.consumer.ReadMessage(k.timeout)
		if err != nil {
			// The client will automatically try to recover from all errors.
			fmt.Printf("Got error while consuming events with error: %v (%v)\n", err, msg)
		} else {
			events = append(events, msg.Value)

			if len(events) >= k.batchSize {
				fmt.Printf("Consumed %+v events to form 1 batch \n", len(events))
				fmt.Println("Triggering usecase to store the events")
				k.usecase.ProcessData(events)
				events = make([]interface{}, 0, k.batchSize)
			}
		}
	}
}
