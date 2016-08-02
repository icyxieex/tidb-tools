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
	"math/rand"
	"strconv"
	"time"
)

const (
	alpha       = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	format_time = "2006-01-02 15:04:05"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func randInt() int {
	return rand.Int()
}

func randIntn(n int) int {
	return rand.Intn(n)
}

func randFloat() float64 {
	return rand.Float64()
}

func randString(n int) string {
	var bytes = make([]byte, n)
	for i, _ := range bytes {
		bytes[i] = alpha[randIntn(len(alpha))]
	}
	return string(bytes)
}

// 0 -> min
// 1 -> max
// randNum(1,10) -> [1,10)
// randNum(-1) -> random
// randNum() -> random
func randNum(args ...int) int {
	if len(args) > 1 {
		return args[0] + randIntn(args[1]-args[0])
	} else if len(args) == 1 {
		return randIntn(args[0])
	} else {
		return randInt()
	}
}

// 0 -> min
// 1 -> max
// 2 -> prec
// randFloat64(1,10) -> [1.0,10.0]
// randFloat64(1,10,3) -> [1.000,10.000]
// randFloat64(-1) -> random
// randFloat64() -> random
func randFloat64(args ...int) float64 {
	value := float64(randNum(args...))

	if len(args) > 2 {
		fvalue := strconv.FormatFloat(value, 'f', args[2], 64)
		value, _ = strconv.ParseFloat(fvalue, 64)
	}

	return value
}

// true/false
func randBool() bool {
	value := randIntn(2)
	return value == 1
}

func randDate() string {
	year := time.Now().Year()
	month := randNum(1, 13)
	day := randNum(1, 30)
	date := fmt.Sprintf("%04d-%02d-%02d", year, month, day)
	return date
}

func randDuration(n time.Duration) time.Duration {
	duration := randIntn(int(n))
	return time.Duration(duration)
}

func randTimestamp() string {
	now := time.Now()
	year := now.Year() - randIntn(3)
	month := randNum(1, 13)
	day := randNum(1, 30)
	hour := randNum(0, 25)
	min := randNum(0, 61)
	sec := randNum(0, 61)

	randTime := time.Date(year, time.Month(month), day, hour, min, sec, 0, time.UTC)
	return randTime.Format(format_time)
}
