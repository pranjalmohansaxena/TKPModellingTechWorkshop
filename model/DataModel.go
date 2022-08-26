package model

type DataModel struct {
	Pid             int64   `json:"pid"`
	RecommendedPids []int64 `json:"recommended_pids"`
}
