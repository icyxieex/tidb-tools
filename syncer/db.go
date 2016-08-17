// Copyright 2016 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
)

const invalidIdx = -1

func columnValue(value interface{}) string {
	var data string
	switch v := value.(type) {
	case nil:
		data = "null"
	case bool:
		if v {
			data = "1"
		} else {
			data = "0"
		}
	case int:
		data = strconv.FormatInt(int64(v), 10)
	case int8:
		data = strconv.FormatInt(int64(v), 10)
	case int16:
		data = strconv.FormatInt(int64(v), 10)
	case int32:
		data = strconv.FormatInt(int64(v), 10)
	case int64:
		data = strconv.FormatInt(int64(v), 10)
	case uint8:
		data = strconv.FormatUint(uint64(v), 10)
	case uint16:
		data = strconv.FormatUint(uint64(v), 10)
	case uint32:
		data = strconv.FormatUint(uint64(v), 10)
	case uint64:
		data = strconv.FormatUint(uint64(v), 10)
	case float32:
		data = strconv.FormatFloat(float64(v), 'f', -1, 32)
	case float64:
		data = strconv.FormatFloat(float64(v), 'f', -1, 64)
	case string:
		data = v
	case []byte:
		data = string(v)
	default:
		data = fmt.Sprintf("%v", v)
	}

	return data
}

func getTableColumns(db *sql.DB, schema string, table string) ([]string, error) {
	if schema == "" || table == "" {
		return nil, errors.New("schema/table is empty")
	}

	querySQL := fmt.Sprintf("show columns from %s.%s", schema, table)
	rows, err := db.Query(querySQL)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()

	rowColumns, err := rows.Columns()
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Show an example.
	/*
	   mysql> show columns from test.t;
	   +-------+---------+------+-----+---------+-------+
	   | Field | Type    | Null | Key | Default | Extra |
	   +-------+---------+------+-----+---------+-------+
	   | a     | int(11) | NO   | PRI | NULL    |       |
	   | b     | int(11) | NO   | PRI | NULL    |       |
	   | c     | int(11) | YES  | MUL | NULL    |       |
	   | d     | int(11) | YES  |     | NULL    |       |
	   +-------+---------+------+-----+---------+-------+
	*/

	var columns []string
	for rows.Next() {
		datas := make([]sql.RawBytes, len(rowColumns))
		values := make([]interface{}, len(rowColumns))

		for i := range values {
			values[i] = &datas[i]
		}

		err = rows.Scan(values...)
		if err != nil {
			return nil, errors.Trace(err)
		}

		field := string(datas[0])
		columns = append(columns, field)
	}

	return columns, nil
}

func getTableIndexColumns(db *sql.DB, schema string, table string) ([]string, error) {
	if schema == "" || table == "" {
		return nil, errors.New("schema/table is empty")
	}

	querySQL := fmt.Sprintf("show index from %s.%s", schema, table)
	rows, err := db.Query(querySQL)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()

	rowColumns, err := rows.Columns()
	if err != nil {
		return nil, errors.Trace(err)
	}

	// Show an example.
	/*
		mysql> show index from test.t;
		+-------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+
		| Table | Non_unique | Key_name | Seq_in_index | Column_name | Collation | Cardinality | Sub_part | Packed | Null | Index_type | Comment | Index_comment |
		+-------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+
		| t     |          0 | PRIMARY  |            1 | a           | A         |           0 |     NULL | NULL   |      | BTREE      |         |               |
		| t     |          0 | PRIMARY  |            2 | b           | A         |           0 |     NULL | NULL   |      | BTREE      |         |               |
		| t     |          0 | ucd      |            1 | c           | A         |           0 |     NULL | NULL   | YES  | BTREE      |         |               |
		| t     |          0 | ucd      |            2 | d           | A         |           0 |     NULL | NULL   | YES  | BTREE      |         |               |
		+-------+------------+----------+--------------+-------------+-----------+-------------+----------+--------+------+------------+---------+---------------+
	*/
	var keyName string
	var columns []string
	for rows.Next() {
		datas := make([]sql.RawBytes, len(rowColumns))
		values := make([]interface{}, len(rowColumns))

		for i := range values {
			values[i] = &datas[i]
		}

		err = rows.Scan(values...)
		if err != nil {
			return nil, errors.Trace(err)
		}

		nonUnique := string(datas[1])
		if nonUnique == "0" {
			if keyName == "" {
				keyName = string(datas[2])
			} else {
				if keyName != string(datas[2]) {
					break
				}
			}

			columns = append(columns, string(datas[4]))
		}
	}

	return columns, nil
}

func genInsertSQLs(schema string, table string, datas [][]interface{}, columns []string) ([]string, error) {
	sqls := make([]string, 0, len(datas))
	columnList := strings.Join(columns, ",")
	for _, data := range datas {
		if len(data) != len(columns) {
			return nil, errors.Errorf("invalid columns and datas - %d, %d", len(datas), len(columns))
		}

		values := make([]string, 0, len(data))
		for _, value := range data {
			values = append(values, columnValue(value))
		}

		valueList := strings.Join(values, ",")
		sql := fmt.Sprintf("replace into %s.%s (%s) values (%s);", schema, table, columnList, valueList)
		sqls = append(sqls, sql)
	}

	return sqls, nil
}

func findColumn(columns []string, indexColumn string) int {
	for i, column := range columns {
		if column == indexColumn {
			return i
		}
	}

	return invalidIdx
}

func findColumns(columns []string, indexColumns []string) []int {
	idxes := make([]int, 0, len(indexColumns))

	for _, column := range indexColumns {
		idx := findColumn(columns, column)
		if idx != invalidIdx {
			idxes = append(idxes, idx)
		}
	}

	return idxes
}

func getColumnsAndDatas(columns []string, indexColumns []string, data []interface{}) ([]string, []interface{}) {
	if len(indexColumns) == 0 {
		return columns, data
	}

	cols := make([]string, 0, len(columns))
	values := make([]interface{}, 0, len(data))
	idxes := findColumns(columns, indexColumns)

	for i := range idxes {
		cols = append(cols, columns[i])
		values = append(values, data[i])
	}

	return cols, values
}

func genWhere(columns []string, data []interface{}) string {
	var kvs []byte
	for i := range columns {
		kvSplit := "="
		if data[i] == nil {
			kvSplit = "is"
		}

		if i == len(columns)-1 {
			kvs = append(kvs, []byte(fmt.Sprintf("%s %s %s", columns[i], kvSplit, columnValue(data[i])))...)
		} else {
			kvs = append(kvs, []byte(fmt.Sprintf("%s %s %s and ", columns[i], kvSplit, columnValue(data[i])))...)
		}
	}

	return string(kvs)
}

func genKVs(columns []string, data []interface{}) string {
	var kvs []byte
	for i := range columns {
		if i == len(columns)-1 {
			kvs = append(kvs, []byte(fmt.Sprintf("%s = %s", columns[i], columnValue(data[i])))...)
		} else {
			kvs = append(kvs, []byte(fmt.Sprintf("%s = %s, ", columns[i], columnValue(data[i])))...)
		}
	}

	return string(kvs)
}

func genUpdateSQLs(schema string, table string, datas [][]interface{}, columns []string, indexColumns []string) ([]string, error) {
	sqls := make([]string, 0, len(datas)/2)
	for i := 0; i < len(datas); i += 2 {
		oldData := datas[i]
		newData := datas[i+1]
		if len(oldData) != len(newData) {
			return nil, errors.Errorf("invalid update datas - %d, %d", len(oldData), len(newData))
		}

		oldValues := make([]interface{}, 0, len(oldData))
		newValues := make([]interface{}, 0, len(newData))
		updateColumns := make([]string, 0, len(indexColumns))

		for j := range oldData {
			if reflect.DeepEqual(oldData[j], newData[j]) {
				continue
			}

			updateColumns = append(updateColumns, columns[j])
			oldValues = append(oldValues, oldData[j])
			newValues = append(newValues, newData[j])
		}

		kvs := genKVs(updateColumns, newValues)

		whereColumns, whereValues := updateColumns, oldValues
		if len(indexColumns) > 0 {
			whereColumns, whereValues = getColumnsAndDatas(columns, indexColumns, oldData)
		}

		where := genWhere(whereColumns, whereValues)
		sql := fmt.Sprintf("update %s.%s set %s where %s limit 1;", schema, table, kvs, where)
		sqls = append(sqls, sql)
	}

	return sqls, nil
}

func genDeleteSQLs(schema string, table string, datas [][]interface{}, columns []string, indexColumns []string) ([]string, error) {
	sqls := make([]string, 0, len(datas))
	for _, data := range datas {
		if len(data) != len(columns) {
			return nil, errors.Errorf("invalid columns and datas - %d, %d", len(datas), len(columns))
		}

		values := make([]interface{}, 0, len(data))
		for _, value := range data {
			values = append(values, columnValue(value))
		}

		whereColumns, whereValues := columns, data
		if len(indexColumns) > 0 {
			whereColumns, whereValues = getColumnsAndDatas(columns, indexColumns, data)
		}

		where := genWhere(whereColumns, whereValues)
		sql := fmt.Sprintf("delete from %s.%s where %s limit 1;", schema, table, where)
		sqls = append(sqls, sql)
	}

	return sqls, nil
}

func createDB(cfg DBConfig) (*sql.DB, error) {
	dbDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8", cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Name)
	db, err := sql.Open("mysql", dbDSN)
	if err != nil {
		return nil, errors.Trace(err)
	}

	return db, nil
}

func closeDB(db *sql.DB) error {
	if db == nil {
		return nil
	}

	return errors.Trace(db.Close())
}
