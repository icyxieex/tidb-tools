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
	"math"
	"strconv"
	"sync/atomic"

	_ "github.com/go-sql-driver/mysql"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb/mysql"
)

type datum struct {
	intValue   int64
	uintValue  uint64
	floatValue float64
}

// const (
// 	TypeDecimal byte = iota
// 	TypeTiny
// 	TypeShort
// 	TypeLong
// 	TypeFloat
// 	TypeDouble
// 	TypeNull
// 	TypeTimestamp
// 	TypeLonglong
// 	TypeInt24
// 	TypeDate
// 	TypeDuration /* Original name was TypeTime, renamed to Duration to resolve the conflict with Go type Time.*/
// 	TypeDatetime
// 	TypeYear
// 	TypeNewDate
// 	TypeVarchar
// 	TypeBit
// )

// TypeNewDecimal byte = iota + 0xf6
// 	TypeEnum
// 	TypeSet
// 	TypeTinyBlob
// 	TypeMediumBlob
// 	TypeLongBlob
// 	TypeBlob
// 	TypeVarString
// 	TypeString
// 	TypeGeometry

func genRowData(table *table) (string, error) {
	var values []byte
	for _, column := range table.columns {
		data, err := genColumnData(table, column)
		if err != nil {
			return "", errors.Trace(err)
		}
		values = append(values, []byte(data)...)
		values = append(values, ',')
	}

	values = values[:len(values)-1]
	sql := fmt.Sprintf("insert into %s (%s) values (%s);", table.name, table.columnList, string(values))
	return sql, nil
}

func genColumnData(table *table, column *column) (string, error) {
	tp := column.tp
	_, isUnique := table.uniqIndices[column.name]
	_, isUnsigned := table.unsignedCols[column.name]

	switch tp.Tp {
	case mysql.TypeTiny:
		var data int64
		if isUnique {
			data = atomic.AddInt64(&column.data.intValue, 1)
		} else {
			if isUnsigned {
				data = randInt64(0, math.MaxUint8)
			} else {
				data = randInt64(math.MinInt8, math.MaxInt8)
			}
		}
		return strconv.FormatInt(data, 10), nil
	case mysql.TypeShort:
		var data int64
		if isUnique {
			data = atomic.AddInt64(&column.data.intValue, 1)
		} else {
			if isUnsigned {
				data = randInt64(0, math.MaxUint16)
			} else {
				data = randInt64(math.MinInt16, math.MaxInt16)
			}
		}
		return strconv.FormatInt(data, 10), nil
	case mysql.TypeLong:
		var data int64
		if isUnique {
			data = atomic.AddInt64(&column.data.intValue, 1)
		} else {
			if isUnsigned {
				data = randInt64(0, math.MaxUint32)
			} else {
				data = randInt64(math.MinInt32, math.MaxInt32)
			}
		}
		return strconv.FormatInt(data, 10), nil
	case mysql.TypeLonglong:
		var data int64
		if isUnique {
			data = atomic.AddInt64(&column.data.intValue, 1)
		} else {
			if isUnsigned {
				data = randInt64(0, math.MaxInt64)
			} else {
				data = randInt64(math.MinInt64, math.MaxInt64)
			}
		}
		return strconv.FormatInt(data, 10), nil
	case mysql.TypeVarchar:
		data := []byte{'\''}
		n := randInt(1, tp.Flen)
		if isUnique {
			data = append(data, []byte(fmt.Sprintf("%s", atomic.AddInt64(&column.data.intValue, 1)))...)
		} else {
			data = append(data, []byte(randString(n))...)
		}

		data = append(data, '\'')
		return string(data), nil
	default:
		return "", errors.Errorf("unsupported column type - %v", column)
	}
}

func createDB(cfg DBConfig) (*sql.DB, error) {
	dbDSN := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?charset=utf8", cfg.User, cfg.Password, cfg.Host, cfg.Port, cfg.Name)
	db, err := sql.Open("mysql", dbDSN)
	if err != nil {
		return nil, errors.Trace(err)
	}

	db.SetMaxIdleConns(cfg.MaxIdleConns)
	db.SetMaxOpenConns(cfg.MaxOpenConns)

	return db, nil
}

func closeDB(db *sql.DB) error {
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

func closeDBs(dbs []*sql.DB) {
	for _, db := range dbs {
		err := closeDB(db)
		if err != nil {
			log.Errorf("close db failed - %v", err)
		}
	}
}
