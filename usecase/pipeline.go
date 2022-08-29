package usecase

import (
	"fmt"

	"github.com/pranjalmohansaxena/TKPModellingTechWorkshop/model"
	"github.com/pranjalmohansaxena/TKPModellingTechWorkshop/repository"
)

type pipelineUsecase struct {
	repository repository.Repository
}

type Param struct {
	Repository repository.Repository
}

func NewPipelineUsecase(param Param) (Usecase, error) {
	if param.Repository == nil {
		err := fmt.Errorf("invalid params are provided")
		return nil, err
	}

	pipelineUsecase := &pipelineUsecase{
		repository: param.Repository,
	}

	return pipelineUsecase, nil
}

func (p pipelineUsecase) ProcessData(events []interface{}) {
	//Print length of events received

	// Make data model slice of events length as capacity

	// Loop through input events, unmarshal Json to looping model and append to model slice

	// Call function processDataToMap(), to process data to map

	// Call Repository Store method with input map data

}

func (p pipelineUsecase) processDataToMap(modelInfo []model.DataModel) []map[string]interface{} {
	data := []map[string]interface{}{}

	// Loop through model slice and add data to []map[string]interface{}

	return data
}
