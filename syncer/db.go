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
	"github.com/pingcap/tidb/ast"
	"github.com/pingcap/tidb/parser"
)

type opType byte

const (
	unknown opType = iota
	insert
	update
	del
	ddl
)

type job struct {
	tp   opType
	sqls []string
	done chan struct{}
}

type column struct {
	idx      int
	name     string
	unsigned bool
}

type table struct {
	schema string
	name   string

	columns      []*column
	indexColumns []*column
}

func columnValue(value interface{}, unsigned bool) string {
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
		if unsigned {
			data = strconv.FormatUint(uint64(uint(v)), 10)
		} else {
			data = strconv.FormatInt(int64(v), 10)
		}
	case int8:
		if unsigned {
			data = strconv.FormatUint(uint64(uint8(v)), 10)
		} else {
			data = strconv.FormatInt(int64(v), 10)
		}
	case int16:
		if unsigned {
			data = strconv.FormatUint(uint64(uint16(v)), 10)
		} else {
			data = strconv.FormatInt(int64(v), 10)
		}
	case int32:
		if unsigned {
			data = strconv.FormatUint(uint64(uint32(v)), 10)
		} else {
			data = strconv.FormatInt(int64(v), 10)
		}
	case int64:
		if unsigned {
			data = strconv.FormatUint(uint64(v), 10)
		} else {
			data = strconv.FormatInt(int64(v), 10)
		}
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
		data = fmt.Sprintf("'%s'", v)
	case []byte:
		data = fmt.Sprintf("'%s'", v)
	default:
		data = fmt.Sprintf("%v", v)
	}

	return data
}

func findColumn(columns []*column, indexColumn string) *column {
	for _, column := range columns {
		if column.name == indexColumn {
			return column
		}
	}

	return nil
}

func findColumns(columns []*column, indexColumns []string) []*column {
	result := make([]*column, 0, len(indexColumns))

	for _, name := range indexColumns {
		column := findColumn(columns, name)
		if column != nil {
			result = append(result, column)
		}
	}

	return result
}

func getTable(db *sql.DB, schema string, name string) (*table, error) {
	table := &table{}
	table.schema = schema
	table.name = name

	err := getTableColumns(db, table)
	if err != nil {
		return nil, errors.Trace(err)
	}

	err = getTableIndex(db, table)
	if err != nil {
		return nil, errors.Trace(err)
	}

	if len(table.columns) == 0 {
		return nil, errors.Errorf("invalid table %s.%s", schema, name)
	}

	return table, nil
}

func getTableColumns(db *sql.DB, table *table) error {
	if table.schema == "" || table.name == "" {
		return errors.New("schema/table is empty")
	}
	if db == nil {
		return errors.New("invalid db conn")
	}

	querySQL := fmt.Sprintf("show columns from %s.%s", table.schema, table.name)
	rows, err := db.Query(querySQL)
	if err != nil {
		return errors.Trace(err)
	}
	defer rows.Close()

	rowColumns, err := rows.Columns()
	if err != nil {
		return errors.Trace(err)
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

	idx := 0
	for rows.Next() {
		datas := make([]sql.RawBytes, len(rowColumns))
		values := make([]interface{}, len(rowColumns))

		for i := range values {
			values[i] = &datas[i]
		}

		err = rows.Scan(values...)
		if err != nil {
			return errors.Trace(err)
		}

		column := &column{}
		column.idx = idx
		column.name = string(datas[0])

		// Check whether column has unsigned flag.
		if strings.Contains(strings.ToLower(string(datas[1])), "unsigned") {
			column.unsigned = true
		}

		table.columns = append(table.columns, column)
		idx++
	}

	if rows.Err() != nil {
		return errors.Trace(rows.Err())
	}

	return nil
}

func getTableIndex(db *sql.DB, table *table) error {
	if table.schema == "" || table.name == "" {
		return errors.New("schema/table is empty")
	}
	if db == nil {
		return errors.New("invalid db conn")
	}

	querySQL := fmt.Sprintf("show index from %s.%s", table.schema, table.name)
	rows, err := db.Query(querySQL)
	if err != nil {
		return errors.Trace(err)
	}
	defer rows.Close()

	rowColumns, err := rows.Columns()
	if err != nil {
		return errors.Trace(err)
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
			return errors.Trace(err)
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

	if rows.Err() != nil {
		return errors.Trace(rows.Err())
	}

	table.indexColumns = findColumns(table.columns, columns)
	return nil
}

func genColumnList(columns []*column) string {
	var columnList []byte
	for i, column := range columns {
		columnList = append(columnList, []byte(column.name)...)

		if i != len(columns)-1 {
			columnList = append(columnList, ',')
		}
	}

	return string(columnList)
}

func genInsertSQLs(schema string, table string, datas [][]interface{}, columns []*column) ([]string, error) {
	sqls := make([]string, 0, len(datas))
	columnList := genColumnList(columns)
	for _, data := range datas {
		if len(data) != len(columns) {
			return nil, errors.Errorf("invalid columns and datas - %d, %d", len(datas), len(columns))
		}

		values := make([]string, 0, len(data))
		for i, value := range data {
			values = append(values, columnValue(value, columns[i].unsigned))
		}

		valueList := strings.Join(values, ",")
		sql := fmt.Sprintf("replace into %s.%s (%s) values (%s);", schema, table, columnList, valueList)
		sqls = append(sqls, sql)
	}

	return sqls, nil
}

func getColumnDatas(columns []*column, indexColumns []*column, data []interface{}) ([]*column, []interface{}) {
	cols := make([]*column, 0, len(columns))
	values := make([]interface{}, 0, len(columns))
	for _, column := range indexColumns {
		cols = append(cols, column)
		values = append(values, data[column.idx])
	}

	return cols, values
}

func genWhere(columns []*column, data []interface{}) string {
	var kvs []byte
	for i := range columns {
		kvSplit := "="
		if data[i] == nil {
			kvSplit = "is"
		}

		if i == len(columns)-1 {
			kvs = append(kvs, []byte(fmt.Sprintf("%s %s %s", columns[i].name, kvSplit, columnValue(data[i], columns[i].unsigned)))...)
		} else {
			kvs = append(kvs, []byte(fmt.Sprintf("%s %s %s and ", columns[i].name, kvSplit, columnValue(data[i], columns[i].unsigned)))...)
		}
	}

	return string(kvs)
}

func genKVs(columns []*column, data []interface{}) string {
	var kvs []byte
	for i := range columns {
		if i == len(columns)-1 {
			kvs = append(kvs, []byte(fmt.Sprintf("%s = %s", columns[i].name, columnValue(data[i], columns[i].unsigned)))...)
		} else {
			kvs = append(kvs, []byte(fmt.Sprintf("%s = %s, ", columns[i].name, columnValue(data[i], columns[i].unsigned)))...)
		}
	}

	return string(kvs)
}

func genUpdateSQLs(schema string, table string, datas [][]interface{}, columns []*column, indexColumns []*column) ([]string, error) {
	sqls := make([]string, 0, len(datas)/2)
	for i := 0; i < len(datas); i += 2 {
		oldData := datas[i]
		newData := datas[i+1]
		if len(oldData) != len(newData) {
			return nil, errors.Errorf("invalid update datas - %d, %d", len(oldData), len(newData))
		}

		oldValues := make([]interface{}, 0, len(oldData))
		newValues := make([]interface{}, 0, len(newData))
		updateColumns := make([]*column, 0, len(indexColumns))

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
			whereColumns, whereValues = getColumnDatas(columns, indexColumns, oldData)
		}

		where := genWhere(whereColumns, whereValues)
		sql := fmt.Sprintf("update %s.%s set %s where %s limit 1;", schema, table, kvs, where)
		sqls = append(sqls, sql)
	}

	return sqls, nil
}

func genDeleteSQLs(schema string, table string, datas [][]interface{}, columns []*column, indexColumns []*column) ([]string, error) {
	sqls := make([]string, 0, len(datas))
	for _, data := range datas {
		if len(data) != len(columns) {
			return nil, errors.Errorf("invalid columns and datas - %d, %d", len(datas), len(columns))
		}

		values := make([]interface{}, 0, len(data))
		for i, value := range data {
			values = append(values, columnValue(value, columns[i].unsigned))
		}

		whereColumns, whereValues := columns, data
		if len(indexColumns) > 0 {
			whereColumns, whereValues = getColumnDatas(columns, indexColumns, data)
		}

		where := genWhere(whereColumns, whereValues)
		sql := fmt.Sprintf("delete from %s.%s where %s limit 1;", schema, table, where)
		sqls = append(sqls, sql)
	}

	return sqls, nil
}

func isDDLSQL(sql string) (bool, error) {
	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	if err != nil {
		return false, errors.Trace(err)
	}

	_, isDDL := stmt.(ast.DDLNode)
	return isDDL, nil
}

func genDDLSQL(sql string, schema string) (string, error) {
	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	if err != nil {
		return "", errors.Trace(err)
	}

	_, isCreateDatabase := stmt.(*ast.CreateDatabaseStmt)
	if isCreateDatabase {
		return fmt.Sprintf("%s;", sql), nil
	}

	return fmt.Sprintf("use %s; %s;", schema, sql), nil
}

func createDB(cfg DBConfig) (*sql.DB, error) {
	dbDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/?charset=utf8", cfg.User, cfg.Password, cfg.Host, cfg.Port)
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
