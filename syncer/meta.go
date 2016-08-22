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
	"bytes"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/BurntSushi/toml"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/siddontang/go-mysql/mysql"
	"github.com/siddontang/go/ioutil2"
)

var (
	maxSaveTime = time.Second
)

// Meta is the binlog meta information from sync source.
// When syncer restarts, we should reload meta info to guarantee continuous transmission.
type Meta interface {
	// Load loads meta information.
	Load() error

	// Save saves meta information.
	Save(pos mysql.Position, force bool) error

	// Pos gets position information.
	Pos() mysql.Position
}

// LocalMeta is local meta struct.
type LocalMeta struct {
	sync.RWMutex

	name     string
	saveTime time.Time

	BinLogName string `toml:"binlog-name" json:"binlog-name"`
	BinLogPos  uint32 `toml:"binlog-pos" json:"binlog-pos"`
}

// NewLocalMeta creates a new LocalMeta.
func NewLocalMeta(name string) *LocalMeta {
	return &LocalMeta{name: name, BinLogPos: 4}
}

func (lm *LocalMeta) Load() error {
	file, err := os.Open(lm.name)
	if err != nil && !os.IsNotExist(errors.Cause(err)) {
		return errors.Trace(err)
	}
	if os.IsNotExist(errors.Cause(err)) {
		return nil
	}
	defer file.Close()

	_, err = toml.DecodeReader(file, lm)
	return errors.Trace(err)
}

func (lm *LocalMeta) Save(pos mysql.Position, force bool) error {
	lm.Lock()
	defer lm.Unlock()

	now := time.Now()
	if !force && now.Sub(lm.saveTime) < maxSaveTime {
		return nil
	}

	lm.BinLogName = pos.Name
	lm.BinLogPos = pos.Pos

	var buf bytes.Buffer
	e := toml.NewEncoder(&buf)
	err := e.Encode(lm)
	if err != nil {
		log.Errorf("syncer save meta info to file %s err %v", lm.name, errors.ErrorStack(err))
		return errors.Trace(err)
	}

	err = ioutil2.WriteFileAtomic(lm.name, buf.Bytes(), 0644)
	if err != nil {
		log.Errorf("syncer save meta info to file %s err %v", lm.name, errors.ErrorStack(err))
		return errors.Trace(err)
	}

	lm.saveTime = now
	return nil
}

func (lm *LocalMeta) Pos() mysql.Position {
	lm.RLock()
	defer lm.RUnlock()

	return mysql.Position{Name: lm.BinLogName, Pos: lm.BinLogPos}
}

func (lm *LocalMeta) String() string {
	pos := lm.Pos()
	return fmt.Sprintf("binlog name = %s, binlog pos = %d", pos.Name, pos.Pos)
}
