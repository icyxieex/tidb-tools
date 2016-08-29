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
	"time"

	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go-mysql/replication"
	"github.com/siddontang/go/sync2"
)

var (
	maxRetryCount = 2

	retryTimeout = time.Second
	eventTimeout = 3 * time.Second
	statusTime   = 30 * time.Second
)

// Syncer can sync your MySQL data into another MySQL database.
type Syncer struct {
	sync.Mutex

	cfg *Config

	meta Meta

	syncer *replication.BinlogSyncer

	wg    sync.WaitGroup
	jobWg sync.WaitGroup

	tables map[string]*table

	fromDB *sql.DB
	toDBs  []*sql.DB

	quit chan struct{}
	done chan struct{}
	jobs []chan *job

	closed sync2.AtomicBool

	start    time.Time
	lastTime time.Time

	ddlCount    sync2.AtomicInt64
	insertCount sync2.AtomicInt64
	updateCount sync2.AtomicInt64
	deleteCount sync2.AtomicInt64
	lastCount   sync2.AtomicInt64
	count       sync2.AtomicInt64
}

// NewSyncer creates a new Syncer.
func NewSyncer(cfg *Config) *Syncer {
	syncer := new(Syncer)
	syncer.cfg = cfg
	syncer.meta = NewLocalMeta(cfg.Meta)
	syncer.closed.Set(false)
	syncer.lastCount.Set(0)
	syncer.count.Set(0)
	syncer.insertCount.Set(0)
	syncer.updateCount.Set(0)
	syncer.deleteCount.Set(0)
	syncer.quit = make(chan struct{})
	syncer.done = make(chan struct{})
	syncer.jobs = newJobChans(cfg.WorkerCount)
	syncer.tables = make(map[string]*table)
	return syncer
}

func newJobChans(count int) []chan *job {
	jobs := make([]chan *job, 0, count)
	for i := 0; i < count; i++ {
		jobs = append(jobs, make(chan *job, 1000))
	}

	return jobs
}

func closeJobChans(jobs []chan *job) {
	for _, ch := range jobs {
		close(ch)
	}
}

// Start starts syncer.
func (s *Syncer) Start() error {
	err := s.meta.Load()
	if err != nil {
		return errors.Trace(err)
	}

	s.wg.Add(1)

	err = s.run()
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}

func (s *Syncer) checkBinlogFormat() error {
	rows, err := s.fromDB.Query(`SHOW GLOBAL VARIABLES LIKE "binlog_format";`)
	if err != nil {
		return errors.Trace(err)
	}
	defer rows.Close()

	// Show an example.
	/*
		mysql> SHOW GLOBAL VARIABLES LIKE "binlog_format";
		+---------------+-------+
		| Variable_name | Value |
		+---------------+-------+
		| binlog_format | ROW   |
		+---------------+-------+
	*/
	for rows.Next() {
		var (
			variable string
			value    string
		)

		err = rows.Scan(&variable, &value)
		if err != nil {
			return errors.Trace(err)
		}
	}

	if rows.Err() != nil {
		return errors.Trace(rows.Err())
	}

	return nil
}

func (s *Syncer) getTable(schema string, table string) (*table, error) {
	key := fmt.Sprintf("%s.%s", schema, table)

	value, ok := s.tables[key]
	if ok {
		return value, nil
	}

	db := s.toDBs[len(s.toDBs)-1]
	t, err := getTable(db, schema, table)
	if err != nil {
		return nil, errors.Trace(err)
	}

	s.tables[key] = t
	return t, nil
}

func (s *Syncer) addCount(tp opType) {
	switch tp {
	case insert:
		s.insertCount.Add(1)
	case update:
		s.updateCount.Add(1)
	case del:
		s.deleteCount.Add(1)
	case ddl:
		s.ddlCount.Add(1)
	}

	s.count.Add(1)
}

func (s *Syncer) checkWait(job *job) bool {
	if job.tp == ddl {
		return true
	}

	if s.meta.Check() {
		return true
	}

	return false
}

func (s *Syncer) addJob(job *job) error {
	s.jobWg.Add(1)

	idx := int(genHashKey(job.key)) % s.cfg.WorkerCount
	s.jobs[idx] <- job

	wait := s.checkWait(job)
	if wait {
		s.jobWg.Wait()

		err := s.meta.Save(job.pos)
		if err != nil {
			return errors.Trace(err)
		}
	}

	return nil
}

func (s *Syncer) sync(db *sql.DB, jobChan chan *job) {
	defer s.wg.Done()

	for job := range jobChan {
		err := s.executeSQL(db, job.sql)
		if err != nil {
			log.Fatalf(errors.ErrorStack(err))
		}

		s.addCount(job.tp)
		s.jobWg.Done()
	}
}

func (s *Syncer) run() error {
	defer s.wg.Done()

	s.syncer = replication.NewBinlogSyncer(uint32(s.cfg.ServerID), "mysql")
	err := s.syncer.RegisterSlave(s.cfg.From.Host, uint16(s.cfg.From.Port), s.cfg.From.User, s.cfg.From.Password)
	if err != nil {
		return errors.Trace(err)
	}

	s.fromDB, err = createDB(s.cfg.From)
	if err != nil {
		return errors.Trace(err)
	}

	s.toDBs, err = createDBs(s.cfg.To, s.cfg.WorkerCount+1)
	if err != nil {
		return errors.Trace(err)
	}

	err = s.checkBinlogFormat()
	if err != nil {
		return errors.Trace(err)
	}

	streamer, err := s.syncer.StartSync(s.meta.Pos())
	if err != nil {
		return errors.Trace(err)
	}

	s.start = time.Now()
	s.lastTime = s.start

	s.wg.Add(s.cfg.WorkerCount)
	for i := 0; i < s.cfg.WorkerCount; i++ {
		go s.sync(s.toDBs[i], s.jobs[i])
	}

	s.wg.Add(1)
	go s.printStatus()

	pos := s.meta.Pos()

	for {
		e, err := streamer.GetEventTimeout(eventTimeout)
		if err != nil && !mysql.ErrorEqual(err, replication.ErrGetEventTimeout) {
			return errors.Trace(err)
		}

		select {
		case <-s.quit:
			log.Infof("ready to quit! [%v]", pos)
			return nil
		default:
		}

		if mysql.ErrorEqual(err, replication.ErrGetEventTimeout) {
			continue
		}

		pos.Pos = e.Header.LogPos

		switch ev := e.Event.(type) {
		case *replication.RotateEvent:
			pos.Name = string(ev.NextLogName)
			pos.Pos = uint32(ev.Position)

			err = s.meta.Save(pos)
			if err != nil {
				return errors.Trace(err)
			}

			log.Infof("rotate binlog to %v", pos)
		case *replication.RowsEvent:
			table := &table{}
			table, err = s.getTable(string(ev.Table.Schema), string(ev.Table.Table))
			if err != nil {
				return errors.Trace(err)
			}

			var (
				sqls []string
				keys []string
			)
			switch e.Header.EventType {
			case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
				sqls, keys, err = genInsertSQLs(table.schema, table.name, ev.Rows, table.columns, table.indexColumns)
				if err != nil {
					return errors.Errorf("gen insert sqls failed: %v", err)
				}

				for i := range sqls {
					job := &job{tp: insert, sql: sqls[i], key: keys[i], pos: pos}
					err = s.addJob(job)
					if err != nil {
						return errors.Trace(err)
					}
				}
			case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
				sqls, keys, err = genUpdateSQLs(table.schema, table.name, ev.Rows, table.columns, table.indexColumns)
				if err != nil {
					return errors.Errorf("gen update sqls failed: %v", err)
				}

				for i := range sqls {
					job := &job{tp: insert, sql: sqls[i], key: keys[i], pos: pos}
					err = s.addJob(job)
					if err != nil {
						return errors.Trace(err)
					}
				}
			case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
				sqls, keys, err = genDeleteSQLs(table.schema, table.name, ev.Rows, table.columns, table.indexColumns)
				if err != nil {
					return errors.Errorf("gen delete sqls failed: %v", err)
				}

				for i := range sqls {
					job := &job{tp: insert, sql: sqls[i], key: keys[i], pos: pos}
					err = s.addJob(job)
					if err != nil {
						return errors.Trace(err)
					}
				}
			}
		case *replication.QueryEvent:
			ok := false
			sql := string(ev.Query)
			ok, err = isDDLSQL(sql)
			if err != nil {
				return errors.Errorf("parse query event failed: %v", err)
			}
			if ok {
				sql, err = genDDLSQL(sql, string(ev.Schema))
				if err != nil {
					return errors.Trace(err)
				}

				job := &job{tp: ddl, sql: sql, pos: pos}
				err = s.addJob(job)
				if err != nil {
					return errors.Trace(err)
				}
			}
		}
	}
}

func (s *Syncer) printStatus() {
	defer s.wg.Done()

	timer := time.NewTicker(statusTime)
	defer timer.Stop()

	for {
		select {
		case <-s.quit:
			return
		case <-timer.C:
			now := time.Now()
			seconds := now.Unix() - s.lastTime.Unix()
			totalSeconds := now.Unix() - s.start.Unix()
			last := s.lastCount.Get()
			total := s.count.Get()

			tps, totalTps := int64(0), int64(0)
			if seconds > 0 {
				tps = (total - last) / seconds
				totalTps = total / totalSeconds
			}

			log.Infof("[syncer]total events = %d , insert = %d, update = %d, delete = %d, total tps = %d, recent tps = %d, %s.",
				total, s.insertCount.Get(), s.updateCount.Get(), s.deleteCount.Get(), totalTps, tps, s.meta)

			s.lastCount.Set(total)
			s.lastTime = time.Now()
		}
	}
}

func (s *Syncer) executeSQL(db *sql.DB, sqls ...string) error {
	if len(sqls) == 0 {
		return nil
	}

	var (
		err error
		txn *sql.Tx
	)

LOOP:
	for i := 0; i < maxRetryCount; i++ {
		if db == nil {
			log.Debugf("execute sql retry %d - %v", i, sqls)
			time.Sleep(retryTimeout)

			db, err = createDB(s.cfg.To)
			if err != nil {
				return errors.Trace(err)
			}
		}

		txn, err = db.Begin()
		if err != nil {
			db = nil
			continue
		}

		for _, sql := range sqls {
			log.Debug(sql)

			_, err = db.Exec(sql)
			if err != nil {
				db = nil
				continue LOOP
			}
		}

		err = txn.Commit()
		if err != nil {
			db = nil
			continue
		}

		return nil
	}

	if err != nil {
		log.Errorf("execute sqls[%v] failed %v", sqls, errors.ErrorStack(err))
		return errors.Trace(err)
	}

	return nil
}

func (s *Syncer) isClosed() bool {
	return s.closed.Get()
}

// Close closes syncer.
func (s *Syncer) Close() {
	s.Lock()
	defer s.Unlock()

	if s.isClosed() {
		return
	}

	close(s.quit)

	closeJobChans(s.jobs)

	s.wg.Wait()

	closeDBs(s.fromDB)
	closeDBs(s.toDBs...)

	if s.syncer != nil {
		s.syncer.Close()
		s.syncer = nil
	}

	s.closed.Set(true)
}
