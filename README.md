# TKPModellingTechWorkshop

Delivery Layer:
type Delivery interface {
	ConsumeEvents(topicName string)
}

Usecase Layer:
type Usecase interface {
	ProcessData(events []interface{})
}

Repository Layer:
type Repository interface {
	Store(data []map[string]interface{})
}

sampleEventFromKafka.json
{
    "pid": 123,
    "recommended_pids": [456,789]
}
