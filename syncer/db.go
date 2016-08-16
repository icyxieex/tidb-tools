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

var tableColumnsSQL = "select column_name from information_schema.columns where table_schema = ? and table_name = ?"

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
	case int64:
		data = strconv.FormatInt(int64(v), 10)
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
	if table == "" {
		return nil, errors.New("table name is empty")
	}

	var columns []string
	rows, err := db.Query(tableColumnsSQL, schema, table)
	if err != nil {
		return nil, errors.Trace(err)
	}
	defer rows.Close()

	for rows.Next() {
		var field string
		err = rows.Scan(
			&field,
		)

		if err != nil {
			return nil, errors.Trace(err)
		}

		columns = append(columns, field)
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
		sql := fmt.Sprintf("insert into %s.%s (%s) values (%s);", schema, table, columnList, valueList)
		sqls = append(sqls, sql)
	}

	return sqls, nil
}

func genWhere(columns []string, data []string) string {
	var kvs []byte
	for i := range columns {
		kvSplit := "="
		if data[i] == "null" {
			kvSplit = "is"
		}

		if i == len(columns)-1 {
			kvs = append(kvs, []byte(fmt.Sprintf("%s %s %s", columns[i], kvSplit, data[i]))...)
		} else {
			kvs = append(kvs, []byte(fmt.Sprintf("%s %s %s and ", columns[i], kvSplit, data[i]))...)
		}
	}

	return string(kvs)
}

func genKVs(columns []string, data []string) string {
	var kvs []byte
	for i := range columns {
		if i == len(columns)-1 {
			kvs = append(kvs, []byte(fmt.Sprintf("%s = %s", columns[i], data[i]))...)
		} else {
			kvs = append(kvs, []byte(fmt.Sprintf("%s = %s, ", columns[i], data[i]))...)
		}
	}

	return string(kvs)
}

func genUpdateSQLs(schema string, table string, datas [][]interface{}, columns []string) ([]string, error) {
	sqls := make([]string, 0, len(datas)/2)
	for i := 0; i < len(datas); i += 2 {
		oldData := datas[i]
		newData := datas[i+1]
		if len(oldData) != len(newData) {
			return nil, errors.Errorf("invalid update datas - %d, %d", len(oldData), len(newData))
		}

		oldValues := make([]string, 0, len(oldData))
		newValues := make([]string, 0, len(newData))
		updateColumns := make([]string, 0, len(columns))

		for j := range oldData {
			if reflect.DeepEqual(oldData[j], newData[j]) {
				continue
			}

			updateColumns = append(updateColumns, columns[j])
			oldValues = append(oldValues, columnValue(oldData[j]))
			newValues = append(newValues, columnValue(newData[j]))
			kvs := genKVs(updateColumns, newValues)
			where := genWhere(updateColumns, oldValues)
			sql := fmt.Sprintf("update %s.%s set %s where %s limit 1;", schema, table, kvs, where)
			sqls = append(sqls, sql)
		}
	}

	return sqls, nil
}

func genDeleteSQLs(schema string, table string, datas [][]interface{}, columns []string) ([]string, error) {
	sqls := make([]string, 0, len(datas))
	for _, data := range datas {
		if len(data) != len(columns) {
			return nil, errors.Errorf("invalid columns and datas - %d, %d", len(datas), len(columns))
		}

		values := make([]string, 0, len(data))
		for _, value := range data {
			values = append(values, columnValue(value))
		}

		where := genWhere(columns, values)
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
	return errors.Trace(db.Close())
}
