# TKPModellingTechWorkshop
This service is for Real-Time Streaming Pipeline Engine for Storing Hundreds Million of Data
It follows Clean Architecture.
It has below layers:

Delivery Layer: It deals with consuming events from Kafka <br>
Schema: 
```
type Delivery interface {
	ConsumeEvents(topicName string)
}
```
<br>

Usecase Layer: It deals with processing the Kafka Events and invoking Datastore Repository <br>
Schema: 
```
type Usecase interface {
	ProcessData(events []interface{})
}
```
<br>

Repository Layer: It deals with storing the processed data to Cassandra <br>

<br>

Schema: 
```
type Repository interface {
	Store(data []map[string]interface{}) (err error)
}
```


Sample Events from Kafka would be of below format <br>
```
{
    "pid": 123,
    "recommended_pids": [456,789]
}
```
