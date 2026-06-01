package cache

// Benchmarks that de-risk the adaptive-container + struct-shrink change BEFORE
// touching the production StoreValue. They isolate the two concurrency-design
// risks identified in the analysis:
//
//   Risk 1 (CRUD write): a HOT high-fanout node (the "objects" enumeration node
//   that gets one __object child per object created) is written concurrently by
//   many rebuild workers. The 8-shard ShardedMap stripes those inserts across 8
//   locks; if the adaptive container collapsed such a node to a single lock,
//   writes would serialize. Bench: concurrent insert into ONE container,
//   8-shard vs single-lock. This quantifies how much striping the hot node
//   needs — i.e. why the adaptive container MUST upgrade back to sharded at high
//   fanout.
//
//   Risk 2 (search read): JPGQL traversal calls Range/GetKeysByPattern per hop.
//   ShardedMap.Range allocates a temp slice PER shard (8 allocs even for a
//   1-child node — 88% of containers). Bench: Range over low-fanout nodes,
//   8-shard vs lean slice. This quantifies the allocation/GC win for search.
//
//   Bonus (read latency): LoadChild lookup on a low-fanout node (<=4 children,
//   98.6% of containers). Bench: hash lookup (ShardedMap.Get) vs linear scan of
//   a small slice. Confirms the lean path is not slower for the common case.
//
// Run:
//   go test ./statefun/cache/ -run '^$' -bench 'Container' -benchmem -cpu 1,8

import (
	"strconv"
	"sync"
	"testing"

	"github.com/foliagecp/sdk/statefun/system"
)

var benchSink interface{}

// singleLockMap models an adaptive container that did NOT upgrade to sharded at
// high fanout: one mutex guards the whole child map.
type singleLockMap struct {
	mu sync.Mutex
	m  map[string]interface{}
}

func (s *singleLockMap) loadOrStore(k string, v interface{}) (interface{}, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.m == nil {
		s.m = make(map[string]interface{})
	}
	if got, ok := s.m[k]; ok {
		return got, true
	}
	s.m[k] = v
	return v, false
}

// ---------------------------------------------------------------------------
// Risk 1 — concurrent inserts into ONE hot node.
// Each goroutine inserts a disjoint key range so all inserts are NEW children
// hitting the SAME container (the contention point). Compare with -cpu 1,8.
// ---------------------------------------------------------------------------

func BenchmarkContainerConcurrentInsert_Sharded8(b *testing.B) {
	sm := system.SharedMapMustNewHashed(8)
	dummy := struct{}{}
	var gid int64
	var gmu sync.Mutex
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		gmu.Lock()
		gid++
		base := gid * 1_000_000_000
		gmu.Unlock()
		i := int64(0)
		for pb.Next() {
			sm.LoadOrStore(strconv.FormatInt(base+i, 10), dummy)
			i++
		}
	})
}

func BenchmarkContainerConcurrentInsert_SingleLock(b *testing.B) {
	slm := &singleLockMap{}
	dummy := struct{}{}
	var gid int64
	var gmu sync.Mutex
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		gmu.Lock()
		gid++
		base := gid * 1_000_000_000
		gmu.Unlock()
		i := int64(0)
		for pb.Next() {
			slm.loadOrStore(strconv.FormatInt(base+i, 10), dummy)
			i++
		}
	})
}

// ---------------------------------------------------------------------------
// Risk 2 — Range allocations on low-fanout nodes (88% are single-child).
// ---------------------------------------------------------------------------

func benchRangeSharded(b *testing.B, nChildren int) {
	sm := system.SharedMapMustNewHashed(8)
	for i := 0; i < nChildren; i++ {
		sm.LoadOrStore("child"+strconv.Itoa(i), struct{}{})
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cnt := 0
		sm.Range(func(k string, v interface{}) bool { cnt++; return true })
		benchSink = cnt
	}
}

func benchRangeLeanSlice(b *testing.B, nChildren int) {
	keys := make([]string, nChildren)
	vals := make([]interface{}, nChildren)
	for i := 0; i < nChildren; i++ {
		keys[i] = "child" + strconv.Itoa(i)
		vals[i] = struct{}{}
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cnt := 0
		for j := range keys {
			_ = keys[j]
			_ = vals[j]
			cnt++
		}
		benchSink = cnt
	}
}

func BenchmarkContainerRange1_Sharded8(b *testing.B)  { benchRangeSharded(b, 1) }
func BenchmarkContainerRange1_LeanSlice(b *testing.B) { benchRangeLeanSlice(b, 1) }
func BenchmarkContainerRange4_Sharded8(b *testing.B)  { benchRangeSharded(b, 4) }
func BenchmarkContainerRange4_LeanSlice(b *testing.B) { benchRangeLeanSlice(b, 4) }

// ---------------------------------------------------------------------------
// Bonus — read lookup on a low-fanout node: hash (ShardedMap.Get) vs linear
// scan of a small slice (the lean path for <=4 children).
// ---------------------------------------------------------------------------

func benchGetSharded(b *testing.B, nChildren int) {
	sm := system.SharedMapMustNewHashed(8)
	for i := 0; i < nChildren; i++ {
		sm.LoadOrStore("child"+strconv.Itoa(i), struct{}{})
	}
	target := "child" + strconv.Itoa(nChildren-1) // worst case: last
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		v, _ := sm.Get(target)
		benchSink = v
	}
}

func benchGetLeanSlice(b *testing.B, nChildren int) {
	keys := make([]string, nChildren)
	vals := make([]interface{}, nChildren)
	for i := 0; i < nChildren; i++ {
		keys[i] = "child" + strconv.Itoa(i)
		vals[i] = struct{}{}
	}
	target := "child" + strconv.Itoa(nChildren-1) // worst case: last
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var found interface{}
		for j := range keys {
			if keys[j] == target {
				found = vals[j]
				break
			}
		}
		benchSink = found
	}
}

func BenchmarkContainerGet1_Sharded8(b *testing.B)  { benchGetSharded(b, 1) }
func BenchmarkContainerGet1_LeanSlice(b *testing.B) { benchGetLeanSlice(b, 1) }
func BenchmarkContainerGet4_Sharded8(b *testing.B)  { benchGetSharded(b, 4) }
func BenchmarkContainerGet4_LeanSlice(b *testing.B) { benchGetLeanSlice(b, 4) }
