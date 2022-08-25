package delivery

type Delivery interface {
	ConsumeEvents(topicName string)
}
