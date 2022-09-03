package repository

import (
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/gocql/gocql"
	"go.uber.org/ratelimit"
)

type (
	cassandraRepo struct {
		session   *gocql.Session
		tableName string
		keyspace  string
		rl        ratelimit.Limiter
	}

	CassandraParams struct {
		Session   *gocql.Session
		TableName string
		Keyspace  string
		Rl        ratelimit.Limiter
	}
)

func NewCassandraRepo(param CassandraParams) (Repository, error) {
	err := validateAndProcessCassandraParams(param)

	if err != nil {
		return nil, err
	}

	return &cassandraRepo{
		session:   param.Session,
		rl:        param.Rl,
		tableName: param.TableName,
		keyspace:  param.Keyspace,
	}, nil

}

// Store() performs Insert operation to Cassandra for chunk of records
// repesentated by "data" which is a slice of map[string]interface{}
// map[string]interface{} denotes a particular Data Row in the below format:
// 1. Key: ColumnField
// 2. Value: ColumnValue
func (c *cassandraRepo) Store(data []map[string]interface{}) (err error) {
	var wg sync.WaitGroup

	// Loop over data, create column and value variables
	for _, dataRow := range data {
		var onlyColumn []string
		var args []interface{}
		var bindValues []string

		c.rl.Take()
		//dataFields := make(map[string]interface{})
		for key, value := range dataRow {
			onlyColumn = append(onlyColumn, key)
			args = append(args, value)
			bindValues = append(bindValues, "?")
		}

		wg.Add(1)
		go func() {
			// Insert data into database
			defer wg.Done()
			queryStmt := "INSERT INTO " + c.keyspace + "." + c.tableName + " (" + strings.Join(onlyColumn, ",") + ") VALUES (" + strings.Join(bindValues, ",") + ");"
			query := c.session.Query(queryStmt, args...)
			err := query.Exec()
			if err != nil {
				fmt.Println("Error inserting data: ", err)
			}
		}()
	}
	wg.Wait()
	return err

	// Call rate limit function

	// Check clustering key, append shard key (partition key)

	// Create query statement

	// Exec query statement

	return err
}

// Validating the Cassandra Parameters
func validateAndProcessCassandraParams(params CassandraParams) (err error) {

	var InvalidCassandraRL = errors.New("invalid_rate_limiter_for_cassandra")
	var InvalidTableName = errors.New("invalid_table_name")

	if params.Rl == nil {
		return InvalidCassandraRL
	}

	if params.TableName == "" {
		return InvalidTableName
	}

	return nil

}
