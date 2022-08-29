package repository

import (
	"fmt"
	"strings"
	"sync"

	"go.uber.org/ratelimit"
)

type (
	cassandraRepo struct {
		shardDivisor int
		tableName    string
		clusterKey   string
		rl           ratelimit.Limiter
	}

	CassandraParams struct {
		ShardDivisor int
		TableName    string
		ClusterKey   string
		Rl           ratelimit.Limiter
	}
)

func NewCassandraRepo(param CassandraParams) (DatastoreRepository, error) {
	timeout, timeToLive, err := validateAndProcessCassandraParams(param)

	if err != nil {
		return nil, err
	}

	return &cassandraRepo{
		rl:           param.Rl,
		shardDivisor: param.ShardDivisor,
		tableName:    param.TableName,
		clusterKey:   param.ClusterKey,
	}, nil

}

// Store() performs Insert operation to Cassandra for chunk of records
// repesentated by "data" which is a slice of map[string]interface{}
// map[string]interface{} denotes a particular Data Row in the below format:
// 1. Key: ColumnField
// 2. Value: ColumnValue
func (c *cassandraRepo) Store(data []map[string]interface{}) (err error) {

	var wg sync.WaitGroup
	for _, dataRow := range data {
		var columnName []string
		var onlyColumn []string
		var args []interface{}
		var qm []string
		c.rl.Take()
		tableName := c.tableName
		dataFields := make(map[string]interface{})
		for key, value := range dataRow {
			dataFields[key] = value
			concat := key + " = ?"
			columnName = append(columnName, concat)
			onlyColumn = append(onlyColumn, key)
			args = append(args, value)
			qm = append(qm, "?")
		}
		if c.shardDivisor > 1 {
			clusterID, ok := dataFields[c.clusterKey].(int64)
			if !ok {
				fmt.Println("no cluster ID")
				continue
			}

			shardID := int(clusterID) / c.shardDivisor
			dataFields["shard_id"] = shardID
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			queryStmt := `INSERT INTO ` + c.keyspace + `.` + tableName + ` (` + strings.Join(onlyColumn, ",") + `)` + ` VALUES (` + strings.Join(qm, ",") + `)` + `;`
			query := c.session.Query(queryStmt, args...)
			err = query.Exec()
			if err != nil {
				return
			}
		}()
	}
	wg.Wait()
	return err
}

// Validating the Cassandra Parameters
func validateAndProcessCassandraParams(params CassandraParams) (err error) {

	var InvalidCassandraRL = errors.New("invalid_rate_limiter_for_cassandra")
	var InvalidClusterKey = errors.New("invalid_cluster key")
	var InvalidTableName = errors.New("invalid_table_name")

	if params.Rl == nil {
		return InvalidCassandraRL
	}

	if params.ShardDivisor > 1 && params.ClusterKey == "" {
		return InvalidClusterKey
	}

	if params.TableName == "" {
		return InvalidTableName
	}

	return nil

}
