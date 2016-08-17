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
	"sync"
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go/sync2"
)

var (
	maxRetryCount = 3

	retryTimeout = time.Second
	eventTimeout = 3 * time.Second
)

// Syncer can sync your MySQL data into another MySQL database.
type Syncer struct {
	m sync.Mutex

	cfg *Config

	syncer *replication.BinlogSyncer

	pos mysql.Position

	wg sync.WaitGroup

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
	syncer.pos = mysql.Position{cfg.File, uint32(cfg.Pos)}
	return syncer
}

// StartSync starts syncer.
func (s *Syncer) Start() error {
	s.wg.Add(1)
	err := s.run()
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (s *Syncer) run() error {
	defer s.wg.Done()

	s.syncer = replication.NewBinlogSyncer(uint32(s.cfg.ServerID), "mysql")
	err := s.syncer.RegisterSlave(s.cfg.From.Host, uint16(s.cfg.From.Port), s.cfg.From.User, s.cfg.From.Password)
	if err != nil {
		return errors.Errorf("Register slave error: %v", err)
	}

	s.fromDB, err = createDB(s.cfg.From)
	if err != nil {
		return errors.Errorf("Start sync error: %v", err)
	}

	s.toDB, err = createDB(s.cfg.To)
	if err != nil {
		return errors.Errorf("Start sync error: %v", errors.ErrorStack(err))
	}

	streamer, err := s.syncer.StartSync(s.pos)
	if err != nil {
		return errors.Errorf("Start sync error: %v", err)
	}

	for {
		e, err := streamer.GetEventTimeout(eventTimeout)
		if err != nil && !mysql.ErrorEqual(err, replication.ErrGetEventTimeout) {
			return errors.Trace(err)
		}

		select {
		case _, ok := <-s.quit:
			if !ok {
				log.Info("quit channel has been closed")
				return nil
			}
		default:
		}

		if mysql.ErrorEqual(err, replication.ErrGetEventTimeout) {
			continue
		}

		switch ev := e.Event.(type) {
		case *replication.RowsEvent:
			schema := string(ev.Table.Schema)
			table := string(ev.Table.Table)

			columns, err := getTableColumns(s.toDB, schema, table)
			if err != nil {
				return errors.Errorf("get table columns failed: %v", err)
			}

			indexColumns, err := getTableIndexColumns(s.toDB, schema, table)
			if err != nil {
				return errors.Errorf("get table index columns failed: %v", err)
			}

			switch e.Header.EventType {
			case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
				sqls, err := genInsertSQLs(schema, table, ev.Rows, columns)
				if err != nil {
					return errors.Errorf("gen insert sqls failed: %v", err)
				}

				for _, sql := range sqls {
					log.Debug(sql)
					err = s.executeSQL(sql)
					if err != nil {
						return errors.Trace(err)
					}
				}
			case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
				sqls, err := genUpdateSQLs(schema, table, ev.Rows, columns, indexColumns)
				if err != nil {
					return errors.Errorf("gen update sqls failed: %v", err)
				}

				for _, sql := range sqls {
					log.Debug(sql)
					err = s.executeSQL(sql)
					if err != nil {
						return errors.Trace(err)
					}
				}
			case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
				sqls, err := genDeleteSQLs(schema, table, ev.Rows, columns, indexColumns)
				if err != nil {
					return errors.Errorf("gen delete sqls failed: %v", err)
				}

				for _, sql := range sqls {
					log.Debug(sql)
					err = s.executeSQL(sql)
					if err != nil {
						return errors.Trace(err)
					}
				}
			}
		case *replication.QueryEvent:
			sql := string(ev.Query)
			ok, err := isDDLSQL(sql)
			if err != nil {
				return errors.Errorf("parse query event failed: %v", err)
			}
			if ok {
				sql, err = genDDLSQL(sql, string(ev.Schema))
				if err != nil {
					return errors.Trace(err)
				}

				log.Debug(sql)

				err = s.executeSQL(sql)
				if err != nil {
					return errors.Trace(err)
				}
			}
		}
	}
}

func (s *Syncer) executeSQL(sql string) error {
	var err error
	for i := 0; i < maxRetryCount; i++ {
		if s.toDB == nil {
			time.Sleep(retryTimeout)

			s.toDB, err = createDB(s.cfg.To)
			if err != nil {
				return errors.Trace(err)
			}
		}

		_, err = s.toDB.Exec(sql)
		if err != nil {
			s.toDB = nil
			continue
		}

		return nil
	}

	if err != nil {
		log.Errorf("execute sql[%s] failed %v", sql, errors.ErrorStack(err))
		return errors.Trace(err)
	}

	return nil
}

func (s *Syncer) isClosed() bool {
	return s.closed.Get()
}

// Close closes syncer.
func (s *Syncer) Close() {
	s.m.Lock()
	defer s.m.Unlock()

	if s.isClosed() {
		return
	}

	close(s.quit)

	s.wg.Wait()

	err := closeDB(s.fromDB)
	if err != nil {
		log.Error(err)
	}

	err = closeDB(s.toDB)
	if err != nil {
		log.Error(err)
	}

	if s.syncer != nil {
		s.syncer.Close()
		s.syncer = nil
	}

	s.closed.Set(true)
}
