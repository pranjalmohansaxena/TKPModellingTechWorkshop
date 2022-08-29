package repository

type Repository interface {
	Store(data []map[string]interface{}) (err error)
}
