// Foliage primary statefun system package.
// Provides shared system functions for statefun packages
package system

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/foliagecp/easyjson"
	lg "github.com/foliagecp/sdk/statefun/logger"
)

var (
	GlobalPrometrics *Prometrics
)

// refMutex wraps a sync.Mutex together with a reference counter.
type refMutex struct {
	mu   sync.Mutex
	refs int32
}

// KeyMutex provides per-key mutexes with automatic cleanup
// once no goroutine holds or is waiting on a given key.
type KeyMutex struct {
	m sync.Map // map[key]interface{} => *refMutex
}

// NewKeyMutex constructs a new KeyMutex.
func NewKeyMutex() *KeyMutex {
	return &KeyMutex{}
}

// Lock acquires the mutex for the specified key.
// It increments the reference count before locking to
// prevent removal of the mutex while it’s in use.
func (k *KeyMutex) Lock(key interface{}) {
	// 1. Atomically load or create the refMutex for this key.
	actual, _ := k.m.LoadOrStore(key, &refMutex{})

	// 2. Obtain ref mutex
	rm := actual.(*refMutex)

	// 3. Bump reference count to signal active usage.
	atomic.AddInt32(&rm.refs, 1)

	// 4. Lock the underlying mutex.
	rm.mu.Lock()

	// 5. Put again if someone managed to delete it while we were making step 2
	k.m.Store(key, rm)
}

// Unlock releases the mutex for the specified key.
// It decrements the reference count and, if it drops to zero,
// deletes the refMutex entry to free resources.
func (k *KeyMutex) Unlock(key interface{}) {
	// Retrieve the stored refMutex.
	v, ok := k.m.Load(key)
	if !ok {
		panic("KeyMutex: unlock of unlocked key")
	}
	rm := v.(*refMutex)

	// Release the underlying mutex.
	rm.mu.Unlock()

	// Decrement reference count and remove from map when unused.
	if atomic.AddInt32(&rm.refs, -1) == 0 {
		k.m.Delete(key)
	}
}

func SortJSONs(jsonArray []*easyjson.JSON, fields []string) []*easyjson.JSON {
	sorted := make([]*easyjson.JSON, len(jsonArray))
	copy(sorted, jsonArray)

	sort.Slice(sorted, func(i, j int) bool {
		for _, field := range fields {
			// Split the field into field name and sorting direction
			parts := strings.Split(field, ":")
			fieldName := parts[0]
			direction := "asc" // Default direction is ascending
			if len(parts) > 1 {
				direction = strings.ToLower(parts[1])
			}

			// Get field values for comparison
			valI := sorted[i].GetByPath(fieldName)
			valJ := sorted[j].GetByPath(fieldName)

			// Determine the types of the values
			valIType := 0
			if valI.IsNumeric() {
				valIType = 1
			}
			if valI.IsString() {
				valIType = 2
			}
			if valI.IsBool() {
				valIType = 3
			}

			valJType := 0
			if valJ.IsNumeric() {
				valJType = 1
			}
			if valJ.IsString() {
				valJType = 2
			}
			if valJ.IsBool() {
				valJType = 3
			}

			// Treat missing values as smaller
			if valIType == 0 || valJType == 0 {
				if valIType != 0 {
					return direction == "asc"
				}
				if valJType != 0 {
					return direction == "dsc"
				}
				// If both are missing, move to the next field
				continue
			}

			// Compare based on type
			var less, equal bool
			switch valIType {
			case 1: // Numeric comparison
				if valJType == 1 {
					vi := valI.AsNumericDefault(0)
					vj := valJ.AsNumericDefault(0)
					if vi != vj {
						less = vi < vj
					} else {
						equal = true
					}
				}
			case 2: // String comparison
				if valJType == 2 {
					vi := valI.AsStringDefault("")
					vj := valJ.AsStringDefault("")
					if vi != vj {
						less = vi < vj
					} else {
						equal = true
					}
				}
			case 3: // Boolean comparison
				if valJType == 3 {
					vi := valI.AsBoolDefault(false)
					vj := valJ.AsBoolDefault(false)
					if vi != vj {
						less = !vi && vj
					} else {
						equal = true
					}
				}
			}

			// If not equal, respect sorting direction
			if !equal {
				if less {
					return direction == "asc"
				}
				return direction == "dsc"
			}
			// If equal, move to the next field
		}
		// If all fields are equal, maintain original order
		return false
	})

	return sorted
}

func SortUUIDs(uuids []string, ascending bool) []string {
	sorted := make([]string, len(uuids))
	copy(sorted, uuids)
	if ascending {
		sort.Strings(sorted)
	} else {
		sort.Sort(sort.Reverse(sort.StringSlice(sorted)))
	}
	return sorted
}

func StrToBase64(xmlText string) string {
	xmlBytes := []byte(xmlText)
	base64Encoded := base64.StdEncoding.EncodeToString(xmlBytes)
	return base64Encoded
}

func Base64ToStr(base64Encoded string) string {
	xmlBytes, err := base64.StdEncoding.DecodeString(base64Encoded)
	if err != nil {
		return ""
	}
	return string(xmlBytes)
}

func CreateDimSizeChannel[T interface{}](maxBufferElements int, onBufferOverflow func()) (in chan T, out chan T) {
	in = make(chan T)
	out = make(chan T)
	notifier := make(chan struct{})
	var mutex sync.Mutex

	var buffer []T

	puller := func() {
		GlobalPrometrics.GetRoutinesCounter().Started("CreateDimSizeChannel-puller")
		defer GlobalPrometrics.GetRoutinesCounter().Stopped("CreateDimSizeChannel-puller")
		defer close(notifier) // notifier channel is being closed
		for {
			val, ok := <-in
			if !ok { // in channel is closed
				return
			}
			mutex.Lock()
			buffer = append(buffer, val)
			if len(buffer) > maxBufferElements {
				if onBufferOverflow != nil {
					go onBufferOverflow() // Call user's function in a separate routines
				}
			}
			mutex.Unlock()

			select {
			case notifier <- struct{}{}:
			default:
				continue
			}
		}
	}
	pusher := func() {
		GlobalPrometrics.GetRoutinesCounter().Started("CreateDimSizeChannel-pusher")
		defer GlobalPrometrics.GetRoutinesCounter().Stopped("CreateDimSizeChannel-pusher")
		defer close(out) // out channel is being closed
		for {
			mutex.Lock()
			if len(buffer) == 0 {
				mutex.Unlock()
				_, ok := <-notifier
				if !ok { // notifier channel is closed
					return
				}
			} else {
				v := buffer[0]
				if len(buffer) == 1 {
					buffer = nil
				} else {
					buffer = buffer[1:]
				}
				mutex.Unlock()
				out <- v
			}
		}
	}
	go puller()
	go pusher()

	return
}

func MsgOnErrorReturn(retVars ...interface{}) {
	le := lg.GetLogger()
	for _, retVar := range retVars {
		if err, ok := retVar.(error); ok {
			le.Error(context.TODO(), fmt.Sprintf("%s\n", err))
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

func IntToStr(i int64) string {
	return strconv.FormatInt(i, 10)
}

func Str2Int(s string) int64 {
	value, err := strconv.ParseInt(s, 10, 64)
	if err == nil {
		return value
	}
	return 0
}

func Str2Bool(boolStr string) bool {
	s := strings.ToLower(boolStr)
	return s == "true" || s == "1"
}

func MapsUnion[T interface{}](m1 map[string]T, m2 map[string]T) map[string]T {
	merged := make(map[string]T)
	for k1, v1 := range m1 {
		merged[k1] = v1
	}
	for k2, v2 := range m2 {
		merged[k2] = v2
	}
	return merged
}

func MapsIntersection[T interface{}](m1 map[string]T, m2 map[string]T, valuesFromMap1 bool) map[string]T {
	intersection := make(map[string]T)
	for k, v1 := range m1 {
		if v2, ok := m2[k]; ok {
			if valuesFromMap1 {
				intersection[k] = v1
			} else {
				intersection[k] = v2
			}

		}
	}
	return intersection
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

func Float64ToBytes(f float64) []byte {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, f)
	if err != nil {
		return []byte{}
	}
	return buf.Bytes()
}

func BytesToFloat64(b []byte) float64 {
	buf := bytes.NewReader(b)
	var f float64
	err := binary.Read(buf, binary.LittleEndian, &f)
	if err != nil {
		return 0.0
	}
	return f
}

func StringToFloat(s string) float64 {
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0
	}
	return f
}

func BoolToBytes(b bool) []byte {
	if b {
		return []byte{1}
	}
	return []byte{0}
}

func BytesToBool(data []byte) bool {
	if len(data) == 0 {
		return false
	}
	return data[0] != 0
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
