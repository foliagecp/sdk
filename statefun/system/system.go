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

type FinalFunc func()

type FinalFunctions struct {
	finalFuncs []FinalFunc
}

func NewFinalFunctions() *FinalFunctions {
	return &FinalFunctions{
		finalFuncs: []FinalFunc{},
	}
}

func (ff *FinalFunctions) Add(f FinalFunc) {
	ff.finalFuncs = append(ff.finalFuncs, f)
}

func (ff *FinalFunctions) Exec() {
	for _, f := range ff.finalFuncs {
		f()
	}
}

// refMutex wraps a sync.RWMutex with a reference counter and a FIFO entry gate.
//
// gate makes the per-key lock FAIR. Go's sync.RWMutex alone is writer-preferring:
// a goroutine blocked on Lock excludes new RLock callers, so an unbroken stream of
// writers starves readers indefinitely (the export / front-end "can't read while
// everyone keeps deleting" convoy). The gate fixes this: every acquirer — reader
// or writer — must take the single gate token before touching mu, and holds it
// only until mu is acquired. Go hands a channel's token to waiters in FIFO order,
// so a reader that arrives mid-write blocks later writers behind it at the gate
// and gets mu as soon as the current holder releases. The gate is dropped the
// instant mu is held, so it never serializes the critical sections themselves —
// concurrent readers still run in parallel; only the brief entry is ordered.
type refMutex struct {
	gate chan struct{} // FIFO entry token; cap 1, holds one token while free
	mu   sync.RWMutex
	refs int32
}

// gateChanPool recycles the per-key FIFO gate channels, so cycling a key's lock
// entry (created/deleted whenever its ref-count passes through 0) does not allocate
// a fresh channel each time. A channel is only ever returned to the pool at
// ref-count 0, where the gate provably holds its single token, so a pooled channel
// is always in the correct "free" state for the next key to reuse as-is.
var gateChanPool = sync.Pool{
	New: func() interface{} {
		ch := make(chan struct{}, 1)
		ch <- struct{}{} // a fresh gate starts free
		return ch
	},
}

// Contended-acquire retry backoff for the timeout paths. While holding the FIFO
// gate, a waiter polls TryLock/TryRLock; it starts retrying quickly and backs off
// up to the cap, so a lock that frees soon is reacquired in tens of microseconds
// instead of waiting a fixed 5ms tick — which matters because the gate serializes
// waiters, so a coarse tick would stack across them.
const (
	gateRetryMin = 50 * time.Microsecond
	gateRetryMax = 5 * time.Millisecond
)

// keyMutexShardCount is how many independent shards the per-key lock table is
// split into. Lock/unlock of keys in different shards never contend on the same
// map mutex, so high-concurrency graph writes (each locking several keys) no
// longer serialize on a single global mutex. Power of two => the shard index is
// a cheap mask. Cost is keyMutexShardCount tiny empty maps.
const keyMutexShardCount = 64

type keyMutexShard struct {
	m  map[interface{}]*refMutex // map[key] => *refMutex
	mx sync.Mutex                // protects this shard's map
}

// KeyMutex provides per-key read-write mutexes with automatic cleanup.
// Multiple readers can access a key concurrently if no writer holds the lock.
// The key table is sharded so operations on different keys do not serialize on
// one global map mutex.
type KeyMutex struct {
	shards []keyMutexShard
}

// NewKeyMutex constructs a new KeyMutex.
func NewKeyMutex() *KeyMutex {
	k := &KeyMutex{shards: make([]keyMutexShard, keyMutexShardCount)}
	for i := range k.shards {
		k.shards[i].m = make(map[interface{}]*refMutex)
	}
	return k
}

// shardFor returns the shard owning key (FNV-1a over string keys; non-string
// keys — none in practice — land deterministically in shard 0, still correct).
func (k *KeyMutex) shardFor(key interface{}) *keyMutexShard {
	var h uint32 = 2166136261 // FNV-1a offset basis
	if s, ok := key.(string); ok {
		for i := 0; i < len(s); i++ {
			h ^= uint32(s[i])
			h *= 16777619
		}
	}
	return &k.shards[h&(keyMutexShardCount-1)]
}

// acquire finds-or-creates the refMutex for key in its shard and bumps refs.
func (k *KeyMutex) acquire(key interface{}) *refMutex {
	sh := k.shardFor(key)
	sh.mx.Lock()
	rm, ok := sh.m[key]
	if !ok {
		rm = &refMutex{gate: gateChanPool.Get().(chan struct{})} // pooled, already free
		sh.m[key] = rm
	}
	atomic.AddInt32(&rm.refs, 1)
	sh.mx.Unlock()
	return rm
}

// release decrements refs for key and deletes the entry when it reaches zero.
func (k *KeyMutex) release(key interface{}, rm *refMutex) {
	sh := k.shardFor(key)
	sh.mx.Lock()
	if atomic.AddInt32(&rm.refs, -1) == 0 {
		delete(sh.m, key)
		gateChanPool.Put(rm.gate) // recycle the gate; at refs==0 it holds its token
	}
	sh.mx.Unlock()
}

// Lock acquires exclusive (write) lock for the specified key.
func (k *KeyMutex) Lock(key interface{}) {
	rm := k.acquire(key)
	<-rm.gate    // pass the FIFO gate
	rm.mu.Lock() // take the write lock while holding the gate
	rm.gate <- struct{}{}
}

// LockTimeout acquires an exclusive (write) lock for the key, giving up after
// `timeout` if it cannot be obtained. Returns true if acquired (caller must
// Unlock), false on timeout (caller must NOT Unlock). It bounds how long a
// caller blocks on a contended key, so a stuck lock holder cannot make
// waiters hang indefinitely.
func (k *KeyMutex) LockTimeout(key interface{}, timeout time.Duration) bool {
	rm := k.acquire(key)

	// timer is allocated lazily — only when we actually have to block. The
	// uncontended fast path (gate free + lock free) pays nothing for it, matching
	// the original's cheap fast path.
	var timer *time.Timer

	// Take the FIFO gate. Try non-blocking first: a buffered channel holds a token
	// only when no receiver is queued, so this fast take can never jump ahead of
	// waiters. If the gate is busy, block for it in FIFO order, bounded by timeout.
	select {
	case <-rm.gate:
	default:
		timer = time.NewTimer(timeout)
		defer timer.Stop()
		select {
		case <-rm.gate:
		case <-timer.C:
			k.release(key, rm)
			return false
		}
	}

	// Holding the gate, take the write lock. Fast path is immediate; otherwise poll
	// until the holder releases or we time out, with later acquirers parked behind
	// us at the gate (starvation-free). One shared deadline spans both phases.
	if rm.mu.TryLock() {
		rm.gate <- struct{}{}
		return true
	}
	if timer == nil {
		timer = time.NewTimer(timeout)
		defer timer.Stop()
	}
	wait := gateRetryMin
	backoff := time.NewTimer(wait)
	defer backoff.Stop()
	for {
		select {
		case <-timer.C:
			rm.gate <- struct{}{} // give the gate back; we never took mu
			k.release(key, rm)
			return false
		case <-backoff.C:
			if rm.mu.TryLock() {
				rm.gate <- struct{}{}
				return true
			}
			if wait < gateRetryMax {
				wait *= 2
			}
			backoff.Reset(wait)
		}
	}
}

// RLockTimeout is the shared-lock counterpart of LockTimeout. Same lazy-timer fast
// path; the FIFO gate is what stops a continuous write stream from starving this
// reader (the front-end / export convoy this fixes).
func (k *KeyMutex) RLockTimeout(key interface{}, timeout time.Duration) bool {
	rm := k.acquire(key)

	var timer *time.Timer

	select {
	case <-rm.gate:
	default:
		timer = time.NewTimer(timeout)
		defer timer.Stop()
		select {
		case <-rm.gate:
		case <-timer.C:
			k.release(key, rm)
			return false
		}
	}

	if rm.mu.TryRLock() {
		rm.gate <- struct{}{}
		return true
	}
	if timer == nil {
		timer = time.NewTimer(timeout)
		defer timer.Stop()
	}
	wait := gateRetryMin
	backoff := time.NewTimer(wait)
	defer backoff.Stop()
	for {
		select {
		case <-timer.C:
			rm.gate <- struct{}{} // give the gate back; we never took mu
			k.release(key, rm)
			return false
		case <-backoff.C:
			if rm.mu.TryRLock() {
				rm.gate <- struct{}{}
				return true
			}
			if wait < gateRetryMax {
				wait *= 2
			}
			backoff.Reset(wait)
		}
	}
}

// Unlock releases an exclusive (write) lock for the specified key.
func (k *KeyMutex) Unlock(key interface{}) {
	sh := k.shardFor(key)
	sh.mx.Lock()
	rm, ok := sh.m[key]
	if !ok {
		sh.mx.Unlock()
		panic("KeyMutex: unlock of unlocked key")
	}
	rm.mu.Unlock()
	if atomic.AddInt32(&rm.refs, -1) == 0 {
		delete(sh.m, key)
		gateChanPool.Put(rm.gate) // recycle the gate; at refs==0 it holds its token
	}
	sh.mx.Unlock()
}

// RLock acquires a shared (read) lock for the specified key.
func (k *KeyMutex) RLock(key interface{}) {
	rm := k.acquire(key)
	<-rm.gate
	rm.mu.RLock()
	rm.gate <- struct{}{}
}

// RUnlock releases a shared (read) lock for the specified key.
func (k *KeyMutex) RUnlock(key interface{}) {
	sh := k.shardFor(key)
	sh.mx.Lock()
	rm, ok := sh.m[key]
	if !ok {
		sh.mx.Unlock()
		panic("KeyMutex: runlock of unlocked key")
	}
	rm.mu.RUnlock()
	if atomic.AddInt32(&rm.refs, -1) == 0 {
		delete(sh.m, key)
		gateChanPool.Put(rm.gate) // recycle the gate; at refs==0 it holds its token
	}
	sh.mx.Unlock()
}

func UniqueStrings(input []string) []string {
	seen := make(map[string]struct{})
	var result []string
	for _, val := range input {
		if _, exists := seen[val]; !exists {
			seen[val] = struct{}{}
			result = append(result, val)
		}
	}
	return result
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
