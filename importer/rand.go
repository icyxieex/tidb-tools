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
	alphabet = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
)

func init() {
	rand.Seed(time.Now().UnixNano())
}

func randInt(min int, max int) int {
	return min + rand.Intn(max-min+1)
}

func randInt64(min int64, max int64) int64 {
	return min + rand.Int63n(max-min+1)
}

func randFloat64(min int64, max int64, prec int) float64 {
	value := float64(randInt64(min, max))
	fvalue := strconv.FormatFloat(value, 'f', prec, 64)
	value, _ = strconv.ParseFloat(fvalue, 64)
	return value
}

func randBool() bool {
	value := randInt(0, 1)
	return value == 1
}

func randString(n int) string {
	bytes := make([]byte, n)
	for i, _ := range bytes {
		bytes[i] = alphabet[randInt(0, len(alphabet)-1)]
	}
	return string(bytes)
}

func randDate() string {
	year := time.Now().Year()
	month := randInt(1, 12)
	day := randInt(1, 28)
	return fmt.Sprintf("%04d-%02d-%02d", year, month, day)
}

func randDuration(n time.Duration) time.Duration {
	duration := randInt(0, int(n))
	return time.Duration(duration)
}

func randTime() string {
	hour := randInt(0, 23)
	min := randInt(0, 59)
	sec := randInt(0, 59)
	return fmt.Sprintf("%02d:%02d:%02d", hour, min, sec)
}

func randTimestamp() string {
	now := time.Now()
	year := now.Year() - randInt(0, 3)
	month := randInt(1, 12)
	day := randInt(1, 28)
	hour := randInt(0, 23)
	min := randInt(0, 59)
	sec := randInt(0, 59)
	return fmt.Sprintf("%04d-%02d-%02d %02d:%02d:%02d", year, month, day, hour, min, sec)
}

func randYear() string {
	return fmt.Sprintf("%04d", time.Now().Year()-randInt(0, 3))
}
