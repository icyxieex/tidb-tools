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
	"sync"

	"github.com/juju/errors"
	"github.com/siddontang/go-mysql/client"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go/sync2"
)

// Syncer can sync your MySQL data into another MySQL database.
type Syncer struct {
	cfg *Config

	syncer *replication.BinlogSyncer

	connLock sync.Mutex
	conn     *client.Conn

	pos mysql.Position

	wg sync.WaitGroup

	tableLock sync.Mutex
	// tables    map[string]*schema.Table

	fromDB *sql.DB
	toDB   *sql.DB

	quit   chan struct{}
	closed sync2.AtomicBool
}

// NewSyncer creates a new Syncer.
func NewSyncer(cfg *Config) *Syncer {
	syncer := new(Syncer)
	syncer.cfg = cfg
	syncer.closed.Set(false)
	syncer.quit = make(chan struct{})

	// syncer.tables = make(map[string]*schema.Table)

	syncer.pos = mysql.Position{cfg.File, uint32(cfg.Pos)}

	return syncer
}

// StartSync starts sync.
func (s *Syncer) StartSync() error {
	s.syncer = replication.NewBinlogSyncer(uint32(s.cfg.ServerID), "mysql")
	err := s.syncer.RegisterSlave(s.cfg.From.Host, uint16(s.cfg.From.Port), s.cfg.From.User, s.cfg.From.Password)
	if err != nil {
		return errors.Errorf("Register slave error: %v", err)
	}

	s.fromDB, err = createDB(s.cfg.From)
	if err != nil {
		return errors.Errorf("Start sync error: %v", err)
	}

	// s.toDB, err = createDB(s.cfg.To)
	// if err != nil {
	// 	return errors.Errorf("Start sync error: %v", errors.ErrorStack(err))
	// }

	streamer, err := s.syncer.StartSync(s.pos)
	if err != nil {
		return errors.Errorf("Start sync error: %v", err)
	}

	for {
		e, err := streamer.GetEvent()
		if err != nil {
			return errors.Errorf("Get event error: %v", err)
		}

		switch ev := e.Event.(type) {
		case *replication.RowsEvent:
			schema := string(ev.Table.Schema)
			table := string(ev.Table.Table)

			columns, err := getTableColumns(s.fromDB, schema, table)
			if err != nil {
				return errors.Errorf("get table columns failed: %v", err)
			}

			indexColumns, err := getTableIndexColumns(s.fromDB, schema, table)
			if err != nil {
				return errors.Errorf("get table index columns failed: %v", err)
			}

			switch e.Header.EventType {
			case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
				sqls, err := genInsertSQLs(schema, table, ev.Rows, columns)
				if err != nil {
					return errors.Errorf("gen insert sqls failed: %v", err)
				}

				for i, sql := range sqls {
					fmt.Printf("[insert]%d - %s\n", i, sql)
				}
			case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
				sqls, err := genUpdateSQLs(schema, table, ev.Rows, columns, indexColumns)
				if err != nil {
					return errors.Errorf("gen update sqls failed: %v", err)
				}

				for i, sql := range sqls {
					fmt.Printf("[update]%d - %s\n", i, sql)
				}
			case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
				sqls, err := genDeleteSQLs(schema, table, ev.Rows, columns, indexColumns)
				if err != nil {
					return errors.Errorf("gen delete sqls failed: %v", err)
				}

				for i, sql := range sqls {
					fmt.Printf("[delete]%d - %s\n", i, sql)
				}
			}
		}
	}
}
