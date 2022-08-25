package repository

type Repository interface {
	Store(data []interface{})
}
