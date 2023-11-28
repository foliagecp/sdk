// Copyright 2023 NJWS Inc.

// Foliage primary statefun system package.
// Provides shared system functions for statefun packages
package system

import (
	"crypto/md5"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"strconv"
	"time"

	lg "github.com/foliagecp/sdk/statefun/logger"
)

func CreateDimSizeChannel[T interface{}](maxBufferElements int, onBufferOverflow func()) (in chan T, out chan T) {
	in = make(chan T)
	out = make(chan T)
	notifier := make(chan bool)

	var buffer []T

	puller := func(notifier chan bool) {
		defer close(notifier) // notifier channel is being closed
		for {
			val, ok := <-in
			if !ok { // in channel is closed
				return
			}
			buffer = append(buffer, val)
			if len(buffer) > maxBufferElements {
				if onBufferOverflow != nil {
					onBufferOverflow()
				}
			}

			select {
			case notifier <- true:
			default:
				continue
			}

			/*lg.Logf("%d, %d\n", len(notifier), cap(notifier))
			if len(notifier) < cap(notifier) { // If room is available in the notifier channel
				notifier <- true
			}*/
		}
	}
	pusher := func(notifier chan bool) {
		defer close(out) // out channel is being closed
		for {
			if len(buffer) == 0 {
				_, ok := <-notifier
				if !ok { // notifier channel is closed
					return
				}
			}
			out <- buffer[0]
			if len(buffer) == 1 {
				buffer = nil
			} else {
				buffer = buffer[1:]
			}
		}
	}
	go puller(notifier)
	go pusher(notifier)

	return
}

func MsgOnErrorReturn(retVars ...interface{}) {
	le := lg.GetCustomLogEntry(runtime.Caller(1))
	for _, retVar := range retVars {
		if err, ok := retVar.(error); ok {
			lg.LogfEntry(lg.ErrorLevel, le, "%s\n", err)
		}
	}
}

func GetEnvMustProceed[T interface{}](key string, defaultVal T) T {
	v, _ := GetEnv(key, defaultVal)
	return v
}

func GetEnv[T interface{}](key string, defaultVal T) (value T, err error) {
	value = defaultVal
	err = nil

	if strValue, exists := os.LookupEnv(key); exists {
		switch interface{}(defaultVal).(type) {
		case string:
			value = interface{}(strValue).(T)
		case uint:
			v, e := strconv.ParseUint(strValue, 10, 0)
			value = interface{}(uint(v)).(T)
			err = e
		case uint8:
			v, e := strconv.ParseUint(strValue, 10, 8)
			value = interface{}(uint8(v)).(T)
			err = e
		case uint16:
			v, e := strconv.ParseUint(strValue, 10, 16)
			value = interface{}(uint16(v)).(T)
			err = e
		case uint32:
			v, e := strconv.ParseUint(strValue, 10, 32)
			value = interface{}(uint32(v)).(T)
			err = e
		case uint64:
			v, e := strconv.ParseUint(strValue, 10, 0)
			value = interface{}(v).(T)
			err = e
		case int:
			v, e := strconv.ParseInt(strValue, 10, 0)
			value = interface{}(int(v)).(T)
			err = e
		case int8:
			v, e := strconv.ParseInt(strValue, 10, 8)
			value = interface{}(int8(v)).(T)
			err = e
		case int16:
			v, e := strconv.ParseInt(strValue, 10, 16)
			value = interface{}(int16(v)).(T)
			err = e
		case int32:
			v, e := strconv.ParseInt(strValue, 10, 32)
			value = interface{}(int32(v)).(T)
			err = e
		case int64:
			v, e := strconv.ParseInt(strValue, 10, 64)
			value = interface{}(v).(T)
			err = e
		case bool:
			v, e := strconv.ParseBool(strValue)
			value = interface{}(v).(T)
			err = e
		case float32:
			v, e := strconv.ParseFloat(strValue, 32)
			value = interface{}(float32(v)).(T)
			err = e
		case float64:
			v, e := strconv.ParseFloat(strValue, 64)
			value = interface{}(v).(T)
			err = e
		}
	}
	return
}

func Str2Int(s string) int64 {
	value, err := strconv.ParseInt(s, 10, 64)
	if err == nil {
		return value
	}
	return 0
}

func MergeMaps[T interface{}](m1 map[string]T, m2 map[string]T) map[string]T {
	merged := make(map[string]T)
	for k, v := range m1 {
		merged[k] = v
	}
	for key, value := range m2 {
		merged[key] = value
	}
	return merged
}

func Int64ToBytes(v int64) []byte {
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, uint64(v))
	return b
}

func BytesToInt64(v []byte) int64 {
	if len(v) < 8 {
		return 0
	}
	return int64(binary.LittleEndian.Uint64(v))
}

func GetCurrentTimeNs() int64 {
	return time.Now().UnixNano()
}

func GetUniqueStrID() string {
	baseStr := fmt.Sprintf("%d-%f", GetCurrentTimeNs(), rand.Float64())
	data := []byte(baseStr)
	hash := md5.Sum(data)
	id := hex.EncodeToString(hash[:])
	return id
}

func GetHashStr(str string) string {
	data := []byte(str)
	hash := md5.Sum(data)
	id := hex.EncodeToString(hash[:])
	return id
}
