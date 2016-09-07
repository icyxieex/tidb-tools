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
	"hash/crc32"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/ast"
	tddl "github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/infoschema"
	"github.com/pingcap/tidb/parser"
	"github.com/pingcap/tidb/terror"
	gmysql "github.com/siddontang/go-mysql/mysql"
)

type opType byte

const (
	insert = iota + 1
	update
	del
	ddl
)

type job struct {
	tp    opType
	sql   string
	args  []interface{}
	key   string
	retry bool
	pos   gmysql.Position
}

func newJob(tp opType, sql string, args []interface{}, key string, retry bool, pos gmysql.Position) *job {
	return &job{tp: tp, sql: sql, args: args, key: key, retry: retry, pos: pos}
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

func castUnsigned(data interface{}, unsigned bool) interface{} {
	if !unsigned {
		return data
	}

	switch v := data.(type) {
	case int:
		return uint(v)
	case int8:
		return uint8(v)
	case int16:
		return uint16(v)
	case int32:
		return uint32(v)
	case int64:
		return strconv.FormatUint(uint64(v), 10)
	}

	return data
}

func columnValue(value interface{}, unsigned bool) string {
	castValue := castUnsigned(value, unsigned)

	var data string
	switch v := castValue.(type) {
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

func genHashKey(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

func genKeyList(columns []*column, datas []interface{}) string {
	values := make([]string, 0, len(datas))
	for i, data := range datas {
		values = append(values, columnValue(data, columns[i].unsigned))
	}

	return strings.Join(values, ",")
}

func genColumnPlaceholders(length int) string {
	values := make([]string, length, length)
	for i := 0; i < length; i++ {
		values[i] = "?"
	}
	return strings.Join(values, ",")
}

func genInsertSQLs(schema string, table string, datas [][]interface{}, columns []*column, indexColumns []*column) ([]string, []string, [][]interface{}, error) {
	sqls := make([]string, 0, len(datas))
	keys := make([]string, 0, len(datas))
	values := make([][]interface{}, 0, len(datas))
	columnList := genColumnList(columns)
	columnPlaceholders := genColumnPlaceholders(len(columns))
	for _, data := range datas {
		if len(data) != len(columns) {
			return nil, nil, nil, errors.Errorf("invalid columns and data - %d, %d", len(data), len(columns))
		}

		value := make([]interface{}, 0, len(data))
		for i := range data {
			value = append(value, castUnsigned(data[i], columns[i].unsigned))
		}

		sql := fmt.Sprintf("replace into %s.%s (%s) values (%s);", schema, table, columnList, columnPlaceholders)
		sqls = append(sqls, sql)
		values = append(values, value)

		keyColumns, keyValues := getColumnDatas(columns, indexColumns, value)
		keys = append(keys, genKeyList(keyColumns, keyValues))
	}

	return sqls, keys, values, nil
}

func getColumnDatas(columns []*column, indexColumns []*column, data []interface{}) ([]*column, []interface{}) {
	cols := make([]*column, 0, len(columns))
	values := make([]interface{}, 0, len(columns))
	for _, column := range indexColumns {
		cols = append(cols, column)
		values = append(values, castUnsigned(data[column.idx], column.unsigned))
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
			kvs = append(kvs, []byte(fmt.Sprintf("%s %s ?", columns[i].name, kvSplit))...)
		} else {
			kvs = append(kvs, []byte(fmt.Sprintf("%s %s ? and ", columns[i].name, kvSplit))...)
		}
	}

	return string(kvs)
}

func genKVs(columns []*column) string {
	var kvs []byte
	for i := range columns {
		if i == len(columns)-1 {
			kvs = append(kvs, []byte(fmt.Sprintf("%s = ?", columns[i].name))...)
		} else {
			kvs = append(kvs, []byte(fmt.Sprintf("%s = ?, ", columns[i].name))...)
		}
	}

	return string(kvs)
}

func genUpdateSQLs(schema string, table string, datas [][]interface{}, columns []*column, indexColumns []*column) ([]string, []string, [][]interface{}, error) {
	sqls := make([]string, 0, len(datas)/2)
	keys := make([]string, 0, len(datas)/2)
	values := make([][]interface{}, 0, len(datas)/2)
	for i := 0; i < len(datas); i += 2 {
		oldData := datas[i]
		newData := datas[i+1]
		if len(oldData) != len(newData) {
			return nil, nil, nil, errors.Errorf("invalid update data - %d, %d", len(oldData), len(newData))
		}

		if len(oldData) != len(columns) {
			return nil, nil, nil, errors.Errorf("invalid columns and data - %d, %d", len(oldData), len(columns))
		}

		oldValues := make([]interface{}, 0, len(oldData))
		newValues := make([]interface{}, 0, len(newData))
		updateColumns := make([]*column, 0, len(indexColumns))

		for j := range oldData {
			if reflect.DeepEqual(oldData[j], newData[j]) {
				continue
			}

			updateColumns = append(updateColumns, columns[j])
			oldValues = append(oldValues, castUnsigned(oldData[j], columns[j].unsigned))
			newValues = append(newValues, castUnsigned(newData[j], columns[j].unsigned))
		}

		value := make([]interface{}, 0, len(oldData))
		kvs := genKVs(updateColumns)
		value = append(value, newValues...)

		whereColumns, whereValues := updateColumns, oldValues
		if len(indexColumns) > 0 {
			whereColumns, whereValues = getColumnDatas(columns, indexColumns, oldData)
		}

		where := genWhere(whereColumns, whereValues)
		value = append(value, whereValues...)

		sql := fmt.Sprintf("update %s.%s set %s where %s limit 1;", schema, table, kvs, where)
		sqls = append(sqls, sql)
		values = append(values, value)

		keys = append(keys, genKeyList(whereColumns, whereValues))
	}

	return sqls, keys, values, nil
}

func genDeleteSQLs(schema string, table string, datas [][]interface{}, columns []*column, indexColumns []*column) ([]string, []string, [][]interface{}, error) {
	sqls := make([]string, 0, len(datas))
	keys := make([]string, 0, len(datas))
	values := make([][]interface{}, 0, len(datas))
	for _, data := range datas {
		if len(data) != len(columns) {
			return nil, nil, nil, errors.Errorf("invalid columns and data - %d, %d", len(data), len(columns))
		}

		value := make([]interface{}, 0, len(data))
		for i := range data {
			value = append(value, castUnsigned(data[i], columns[i].unsigned))
		}

		whereColumns, whereValues := columns, value
		if len(indexColumns) > 0 {
			whereColumns, whereValues = getColumnDatas(columns, indexColumns, value)
		}

		where := genWhere(whereColumns, whereValues)
		values = append(values, whereValues)

		sql := fmt.Sprintf("delete from %s.%s where %s limit 1;", schema, table, where)
		sqls = append(sqls, sql)
		keys = append(keys, genKeyList(whereColumns, whereValues))
	}

	return sqls, keys, values, nil
}

func ignoreDDLError(err error) bool {
	if err == nil {
		return true
	}

	mysqlErr, ok := errors.Cause(err).(*mysql.MySQLError)
	if !ok {
		return false
	}

	errCode := terror.ErrCode(mysqlErr.Number)
	switch errCode {
	case infoschema.ErrDatabaseExists.Code(), infoschema.ErrDatabaseNotExists.Code(), infoschema.ErrDatabaseDropExists.Code(),
		infoschema.ErrTableExists.Code(), infoschema.ErrTableNotExists.Code(), infoschema.ErrTableDropExists.Code(),
		infoschema.ErrColumnExists.Code(), infoschema.ErrColumnNotExists.Code(),
		infoschema.ErrIndexExists.Code(), tddl.ErrCantDropFieldOrKey.Code():
		return true
	default:
		return false
	}
}

func isDDLSQL(sql string) (bool, error) {
	stmt, err := parser.New().ParseOneStmt(sql, "", "")
	if err != nil {
		return false, errors.Errorf("[sql]%s[error]%v", sql, err)
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

func querySQL(db *sql.DB, query string) (*sql.Rows, error) {
	var (
		err  error
		rows *sql.Rows
	)

	for i := 0; i < maxRetryCount; i++ {
		if i > 0 {
			log.Warnf("query sql retry %d - %s", i, query)
			time.Sleep(retryTimeout)
		}

		log.Debugf("[query][sql]%s", query)

		rows, err = db.Query(query)
		if err != nil {
			log.Warnf("[query][sql]%s[error]%v", query, err)
			continue
		}

		return rows, nil
	}

	if err != nil {
		log.Errorf("query sql[%s] failed %v", query, errors.ErrorStack(err))
		return nil, errors.Trace(err)
	}

	return nil, errors.Errorf("query sql[%s] failed", query)
}

func executeSQL(db *sql.DB, sqls []string, args [][]interface{}, retry bool) error {
	if len(sqls) == 0 {
		return nil
	}

	var (
		err error
		txn *sql.Tx
	)

	retryCount := 1
	if retry {
		retryCount = maxRetryCount
	}

LOOP:
	for i := 0; i < retryCount; i++ {
		if i > 0 {
			log.Warnf("exec sql retry %d - %v - %v", i, sqls, args)
			time.Sleep(retryTimeout)
		}

		txn, err = db.Begin()
		if err != nil {
			log.Errorf("exec sqls[%v] begin failed %v", sqls, errors.ErrorStack(err))
			continue
		}

		for i := range sqls {
			log.Debugf("[exec][sql]%s[args]%v", sqls[i], args[i])

			_, err = txn.Exec(sqls[i], args[i]...)
			if err != nil {
				log.Warnf("[exec][sql]%s[args]%v[error]%v", sqls[i], args[i], err)
				rerr := txn.Rollback()
				if rerr != nil {
					log.Errorf("[exec][sql]%s[args]%v[error]%v", sqls[i], args[i], rerr)
				}
				continue LOOP
			}
		}

		err = txn.Commit()
		if err != nil {
			log.Errorf("exec sqls[%v] commit failed %v", sqls, errors.ErrorStack(err))
			continue
		}

		return nil
	}

	if err != nil {
		log.Errorf("exec sqls[%v] failed %v", sqls, errors.ErrorStack(err))
		return errors.Trace(err)
	}

	return errors.Errorf("exec sqls[%v] failed", sqls)
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

func createDBs(cfg DBConfig, count int) ([]*sql.DB, error) {
	dbs := make([]*sql.DB, 0, count)
	for i := 0; i < count; i++ {
		db, err := createDB(cfg)
		if err != nil {
			return nil, errors.Trace(err)
		}

		dbs = append(dbs, db)
	}

	return dbs, nil
}

func closeDBs(dbs ...*sql.DB) {
	for _, db := range dbs {
		err := closeDB(db)
		if err != nil {
			log.Errorf("close db failed - %v", err)
		}
	}
}
