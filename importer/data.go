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
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

type datum struct {
	sync.Mutex

	intValue  int64
	timeValue time.Time
}

func newDatum() *datum {
	return &datum{intValue: -1}
}

func (d *datum) uniqInt64() int64 {
	data := atomic.AddInt64(&d.intValue, 1)
	return data
}

func (d *datum) uniqFloat64() float64 {
	data := atomic.AddInt64(&d.intValue, 1)
	return float64(data)
}

func (d *datum) uniqString(n int) string {
	data := atomic.AddInt64(&d.intValue, 1)

	var value []byte
	for ; ; n-- {
		if n == 0 {
			break
		}

		idx := data % int64(len(alphabet))
		data = data / int64(len(alphabet))

		value = append(value, alphabet[idx])

		if data == 0 {
			break
		}
	}

	for i, j := 0, len(value)-1; i < j; i, j = i+1, j-1 {
		value[i], value[j] = value[j], value[i]
	}

	return string(value)
}

func (d *datum) uniqTime() string {
	d.Lock()
	defer d.Unlock()

	if d.timeValue.IsZero() {
		d.timeValue = time.Now()
	} else {
		d.timeValue = d.timeValue.Add(time.Second)
	}

	return fmt.Sprintf("%02d:%02d:%02d", d.timeValue.Hour(), d.timeValue.Minute(), d.timeValue.Second())
}

func (d *datum) uniqDate() string {
	d.Lock()
	defer d.Unlock()

	if d.timeValue.IsZero() {
		d.timeValue = time.Now()
	} else {
		d.timeValue = d.timeValue.AddDate(0, 0, 1)
	}

	return fmt.Sprintf("%04d-%02d-%02d", d.timeValue.Year(), d.timeValue.Month(), d.timeValue.Day())
}

func (d *datum) uniqTimestamp() string {
	d.Lock()
	defer d.Unlock()

	if d.timeValue.IsZero() {
		d.timeValue = time.Now()
	} else {
		d.timeValue = d.timeValue.Add(time.Second)
	}

	return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d",
		d.timeValue.Year(), d.timeValue.Month(), d.timeValue.Day(),
		d.timeValue.Hour(), d.timeValue.Minute(), d.timeValue.Second())
}

func (d *datum) uniqYear() string {
	d.Lock()
	defer d.Unlock()

	if d.timeValue.IsZero() {
		d.timeValue = time.Now()
	} else {
		d.timeValue = d.timeValue.AddDate(1, 0, 0)
	}

	return fmt.Sprintf("%04d", d.timeValue.Year())
}
