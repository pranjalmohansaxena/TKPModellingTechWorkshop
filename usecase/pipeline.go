package usecase

import (
	"encoding/json"
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
	fmt.Println("Received events count: ", len(events))

	modelInfo := make([]model.DataModel, 0, len(events))

	for _, event := range events {
		var dataModel model.DataModel

		err := json.Unmarshal(event.([]byte), &dataModel)

		if err != nil {
			fmt.Println("Error while unmarshalling.")
			continue
		}

		modelInfo = append(modelInfo, dataModel)
	}

	// Processing the data to the map
	data := p.processDataToMap(modelInfo)

	fmt.Println("Triggering the repository to store the data")
	p.repository.Store(data)

}

func (p pipelineUsecase) processDataToMap(modelInfo []model.DataModel) []map[string]interface{} {
	data := []map[string]interface{}{}

	for _, model := range modelInfo {
		record := map[string]interface{}{}
		record["pid"] = model.Pid
		record["recommendedPids"] = model.RecommendedPids
		data = append(data, record)
	}

	return data
}
