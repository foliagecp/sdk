// Package db — regression tests for singleflight-deduplicated Read methods.
//
// The Read wrappers in cmdb.go / graph.go run through a shared
// *singleflight.Group: concurrent identical reads collapse into a
// single underlying NATS request, and waiters receive a Clone of the
// shared result so they can mutate it independently. The contract
// pinned here:
//
//   1. N concurrent identical Read calls trigger exactly one SDK
//      request; (N-1) callers are dedupe waiters.
//   2. Different request "shapes" (e.g. ObjectRead vs ObjectReadV2,
//      VertexRead with/without details) DO NOT collapse — they target
//      different server endpoints / response formats.
//   3. Different ids on the same Read DO NOT collapse.
//   4. Sequential calls (in-flight completes before the next starts)
//      DO trigger separate requests — singleflight only deduplicates
//      in-flight callers.
//   5. Waiters receive isolated JSON values: one waiter mutating the
//      returned data cannot be observed by another.
//   6. Errors (including ErrNotFound for IDLE) propagate to every
//      waiter on the in-flight call.
//   7. When the readFlight pointer is nil (client constructed without
//      the New... factory) the wrappers fall back to direct calls —
//      no deduplication but no panic either.
package db

import (
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/foliagecp/easyjson"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfp "github.com/foliagecp/sdk/statefun/plugins"
)

// blockingRequest produces a mock SFRequestFunc whose returned closure
// blocks until `release` is closed. The atomic counter tracks how many
// times the function was actually invoked (so the test can assert N
// goroutines yielded only 1 invocation).
func blockingRequest(t *testing.T, release <-chan struct{}, calls *atomic.Int64, status sfMediators.OpStatus, details string, data *easyjson.JSON) sfp.SFRequestFunc {
	t.Helper()
	return func(_ sfp.RequestProvider, _, _ string, _, _ *easyjson.JSON, _ ...time.Duration) (*easyjson.JSON, error) {
		calls.Add(1)
		<-release
		reply := easyjson.NewJSONObjectWithKeyValue("status", easyjson.NewJSON(sfMediators.OpStatusNames[status]))
		if details != "" {
			reply.SetByPath("details", easyjson.NewJSON(details))
		}
		if data != nil {
			reply.SetByPath("data", *data)
		}
		return &reply, nil
	}
}

// Test_Singleflight_CMDB_ObjectRead_DedupeConcurrent: N goroutines all
// call ObjectRead("same") simultaneously; exactly one SDK request must
// be observed, and all N must see the same data.
func Test_Singleflight_CMDB_ObjectRead_DedupeConcurrent(t *testing.T) {
	const N = 32
	release := make(chan struct{})
	var calls atomic.Int64
	data := easyjson.NewJSONObjectWithKeyValue("body", easyjson.NewJSONObjectWithKeyValue("hello", easyjson.NewJSON("world")))

	cmdb, err := NewCMDBSyncClientFromRequestFunction(blockingRequest(t, release, &calls, sfMediators.SYNC_OP_STATUS_OK, "", &data))
	if err != nil {
		t.Fatalf("client init: %v", err)
	}

	type out struct {
		j   easyjson.JSON
		err error
	}
	results := make(chan out, N)
	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func() {
			defer wg.Done()
			j, e := cmdb.ObjectRead("same-id")
			results <- out{j, e}
		}()
	}
	// Give the goroutines time to all enter Do and attach to the same
	// flight. 50ms is comfortably above scheduler latency on any
	// realistic CI host.
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()
	close(results)

	if got := calls.Load(); got != 1 {
		t.Fatalf("expected exactly 1 underlying request for %d concurrent ObjectRead calls, got %d", N, got)
	}

	count := 0
	for r := range results {
		count++
		if r.err != nil {
			t.Errorf("waiter got unexpected err: %v", r.err)
			continue
		}
		if r.j.GetByPath("body.hello").AsStringDefault("") != "world" {
			t.Errorf("waiter got wrong data: %s", r.j.ToString())
		}
	}
	if count != N {
		t.Fatalf("expected %d waiter results, got %d", N, count)
	}
}

// Test_Singleflight_CMDB_DifferentShapes_NotDeduped: ObjectRead and
// ObjectReadV2 hit the same id but expect different response shapes —
// they must NOT collapse into one underlying call.
func Test_Singleflight_CMDB_DifferentShapes_NotDeduped(t *testing.T) {
	release := make(chan struct{})
	var calls atomic.Int64
	data := easyjson.NewJSONObjectWithKeyValue("ok", easyjson.NewJSON(true))
	cmdb, err := NewCMDBSyncClientFromRequestFunction(blockingRequest(t, release, &calls, sfMediators.SYNC_OP_STATUS_OK, "", &data))
	if err != nil {
		t.Fatalf("client init: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _, _ = cmdb.ObjectRead("same-id") }()
	go func() { defer wg.Done(); _, _ = cmdb.ObjectReadV2("same-id") }()
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()

	if got := calls.Load(); got != 2 {
		t.Fatalf("ObjectRead and ObjectReadV2 must NOT dedupe, expected 2 underlying calls, got %d", got)
	}
}

// Test_Singleflight_CMDB_DifferentIds_NotDeduped: same Read method
// different ids must produce separate underlying calls.
func Test_Singleflight_CMDB_DifferentIds_NotDeduped(t *testing.T) {
	release := make(chan struct{})
	var calls atomic.Int64
	data := easyjson.NewJSONObjectWithKeyValue("ok", easyjson.NewJSON(true))
	cmdb, err := NewCMDBSyncClientFromRequestFunction(blockingRequest(t, release, &calls, sfMediators.SYNC_OP_STATUS_OK, "", &data))
	if err != nil {
		t.Fatalf("client init: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _, _ = cmdb.ObjectRead("id-A") }()
	go func() { defer wg.Done(); _, _ = cmdb.ObjectRead("id-B") }()
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()

	if got := calls.Load(); got != 2 {
		t.Fatalf("different ids must NOT dedupe, expected 2 underlying calls, got %d", got)
	}
}

// Test_Singleflight_CMDB_Sequential_NotDeduped: in-flight only — once
// the first call returns, the next should not see a cached entry.
func Test_Singleflight_CMDB_Sequential_NotDeduped(t *testing.T) {
	var calls atomic.Int64
	data := easyjson.NewJSONObjectWithKeyValue("ok", easyjson.NewJSON(true))
	// non-blocking — each call completes immediately
	req := func(_ sfp.RequestProvider, _, _ string, _, _ *easyjson.JSON, _ ...time.Duration) (*easyjson.JSON, error) {
		calls.Add(1)
		reply := easyjson.NewJSONObjectWithKeyValue("status", easyjson.NewJSON(sfMediators.OpStatusNames[sfMediators.SYNC_OP_STATUS_OK]))
		reply.SetByPath("data", data)
		return &reply, nil
	}
	cmdb, err := NewCMDBSyncClientFromRequestFunction(req)
	if err != nil {
		t.Fatalf("client init: %v", err)
	}

	_, _ = cmdb.ObjectRead("seq")
	_, _ = cmdb.ObjectRead("seq")
	_, _ = cmdb.ObjectRead("seq")

	if got := calls.Load(); got != 3 {
		t.Fatalf("sequential calls must NOT dedupe, expected 3 underlying calls, got %d", got)
	}
}

// Test_Singleflight_CMDB_ResultIsolation: waiters receive Clone'd JSON
// so one mutating their result cannot be observed by another. This is
// the critical safety property that prevents one caller from
// corrupting another's view.
func Test_Singleflight_CMDB_ResultIsolation(t *testing.T) {
	release := make(chan struct{})
	var calls atomic.Int64
	data := easyjson.NewJSONObjectWithKeyValue("nested", easyjson.NewJSONObjectWithKeyValue("k", easyjson.NewJSON("v")))
	cmdb, err := NewCMDBSyncClientFromRequestFunction(blockingRequest(t, release, &calls, sfMediators.SYNC_OP_STATUS_OK, "", &data))
	if err != nil {
		t.Fatalf("client init: %v", err)
	}

	var wg sync.WaitGroup
	results := make([]easyjson.JSON, 2)
	for i := range results {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			j, _ := cmdb.ObjectRead("iso")
			results[i] = j
		}()
	}
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()

	// Mutate the first waiter's result; the second waiter's result
	// must NOT observe the change.
	results[0].SetByPath("nested.k", easyjson.NewJSON("hijacked"))
	if results[1].GetByPath("nested.k").AsStringDefault("") != "v" {
		t.Fatalf("waiters share JSON state — mutation by one observed by another. Got: %s", results[1].ToString())
	}
}

// Test_Singleflight_CMDB_ErrorPropagation: an IDLE reply must produce
// ErrNotFound for every waiter on the in-flight call.
func Test_Singleflight_CMDB_ErrorPropagation(t *testing.T) {
	const N = 8
	release := make(chan struct{})
	var calls atomic.Int64
	cmdb, err := NewCMDBSyncClientFromRequestFunction(blockingRequest(t, release, &calls, sfMediators.SYNC_OP_STATUS_IDLE, "object does not exist", nil))
	if err != nil {
		t.Fatalf("client init: %v", err)
	}

	errs := make(chan error, N)
	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func() {
			defer wg.Done()
			_, e := cmdb.ObjectRead("missing")
			errs <- e
		}()
	}
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()
	close(errs)

	if got := calls.Load(); got != 1 {
		t.Fatalf("error case must still dedupe; expected 1 call, got %d", got)
	}

	count := 0
	for e := range errs {
		count++
		if !errors.Is(e, ErrNotFound) {
			t.Errorf("waiter got %v, want errors.Is(err, ErrNotFound) to hold", e)
		}
		if !strings.Contains(e.Error(), "does not exist") {
			t.Errorf("waiter lost details substring: %q", e.Error())
		}
	}
	if count != N {
		t.Fatalf("expected %d error results, got %d", N, count)
	}
}

// Test_Singleflight_NilGroup_FallsBackToDirect: a client struct
// constructed without the constructor has readFlight == nil; the
// wrappers must still work, just without deduplication.
func Test_Singleflight_NilGroup_FallsBackToDirect(t *testing.T) {
	var calls atomic.Int64
	data := easyjson.NewJSONObjectWithKeyValue("ok", easyjson.NewJSON(true))
	req := func(_ sfp.RequestProvider, _, _ string, _, _ *easyjson.JSON, _ ...time.Duration) (*easyjson.JSON, error) {
		calls.Add(1)
		reply := easyjson.NewJSONObjectWithKeyValue("status", easyjson.NewJSON(sfMediators.OpStatusNames[sfMediators.SYNC_OP_STATUS_OK]))
		reply.SetByPath("data", data)
		return &reply, nil
	}
	// Construct directly — readFlight stays nil.
	cmdb := CMDBSyncClient{request: req}

	const N = 3
	for i := 0; i < N; i++ {
		j, err := cmdb.ObjectRead("x")
		if err != nil {
			t.Fatalf("ObjectRead with nil readFlight: %v", err)
		}
		if !j.GetByPath("ok").AsBoolDefault(false) {
			t.Fatalf("ObjectRead with nil readFlight returned wrong data: %s", j.ToString())
		}
	}
	// No deduplication — N calls → N underlying requests.
	if got := calls.Load(); got != N {
		t.Fatalf("nil readFlight must not dedupe; expected %d calls, got %d", N, got)
	}
}

// Test_Singleflight_Graph_VertexRead_DedupeConcurrent mirrors the CMDB
// test for the Graph client surface to ensure both clients dedupe.
func Test_Singleflight_Graph_VertexRead_DedupeConcurrent(t *testing.T) {
	const N = 16
	release := make(chan struct{})
	var calls atomic.Int64
	data := easyjson.NewJSONObjectWithKeyValue("body", easyjson.NewJSON("v"))
	gc, err := NewGraphSyncClientFromRequestFunction(blockingRequest(t, release, &calls, sfMediators.SYNC_OP_STATUS_OK, "", &data))
	if err != nil {
		t.Fatalf("client init: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(N)
	for i := 0; i < N; i++ {
		go func() { defer wg.Done(); _, _ = gc.VertexRead("v-1") }()
	}
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()

	if got := calls.Load(); got != 1 {
		t.Fatalf("expected 1 underlying VertexRead, got %d", got)
	}
}

// Test_Singleflight_Graph_VertexRead_DetailsDistinguish: details=true
// and details=false must be different singleflight keys, since they
// produce different response shapes (with/without links).
func Test_Singleflight_Graph_VertexRead_DetailsDistinguish(t *testing.T) {
	release := make(chan struct{})
	var calls atomic.Int64
	data := easyjson.NewJSONObjectWithKeyValue("body", easyjson.NewJSON("v"))
	gc, err := NewGraphSyncClientFromRequestFunction(blockingRequest(t, release, &calls, sfMediators.SYNC_OP_STATUS_OK, "", &data))
	if err != nil {
		t.Fatalf("client init: %v", err)
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _, _ = gc.VertexRead("v-1") }()              // no details
	go func() { defer wg.Done(); _, _ = gc.VertexRead("v-1", true) }()        // details=true
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()

	if got := calls.Load(); got != 2 {
		t.Fatalf("VertexRead with different details flags must NOT dedupe, got %d calls", got)
	}
}
