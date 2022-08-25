package usecase

type Usecase interface {
	ProcessData(events []interface{})
}