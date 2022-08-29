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

// Store() performs Insert operation to Cassandra for chunk of records
// repesentated by "data" which is a slice of map[string]interface{}
// map[string]interface{} denotes a particular Data Row in the below format:
// 1. Key: ColumnField
// 2. Value: ColumnValue
func (c *cassandraRepo) Store(data []map[string]interface{}) (err error) {

	var columnName []string
	var onlyColumn []string
	var args []interface{}
	var qm []string
	var wg sync.WaitGroup
	for _, dataRow := range data {
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
