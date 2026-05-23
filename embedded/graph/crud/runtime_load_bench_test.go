package crud

// End-to-end load benchmark for profiling the real runtime hot path.
//
// Unlike the isolated ser/de micro-benchmarks in statefun/, this drives a
// FULL runtime over an in-process NATS+JetStream server: every iteration is
// a real request/reply round-trip through buildNatsData → NATS → handler →
// cache write → WAL publish → reply. Capture a CPU profile to see which
// stage actually dominates under load (ser/de vs cache vs mutex vs NATS):
//
//   go test ./embedded/graph/crud/ -run '^$' -bench BenchmarkRuntimeVertexUpdate \
//     -benchmem -benchtime=10s -cpuprofile=/tmp/crud_cpu.out
//   go tool pprof -top -nodecount=40 /tmp/crud_cpu.out

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/statefun"
	"github.com/foliagecp/sdk/statefun/cache"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/nats-io/nats-server/v2/server"
	natsservertest "github.com/nats-io/nats-server/v2/test"
)

func runBenchServer(b *testing.B) *server.Server {
	b.Helper()
	opts := natsservertest.DefaultTestOptions
	opts.JetStream = true
	opts.Port = -1
	opts.StoreDir = b.TempDir()
	return natsservertest.RunServer(&opts)
}

// startBenchRuntime spins up a runtime with all CRUD functions registered,
// waits for the built-in CMDB schema to settle, and returns it.
func startBenchRuntime(b *testing.B) (*statefun.Runtime, context.CancelFunc) {
	b.Helper()

	srv := runBenchServer(b)
	b.Cleanup(srv.Shutdown)

	cfg := statefun.NewRuntimeConfigSimple(srv.ClientURL(), "bench_crud")
	runtime, err := statefun.NewRuntime(*cfg)
	if err != nil {
		b.Fatalf("NewRuntime: %v", err)
	}
	RegisterAllFunctionTypes(runtime)

	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = runtime.Start(ctx, cache.NewCacheConfig("bench_cache")) }()

	// Wait for the built-in types/objects vertices to appear (schema ready).
	// Cache() is nil until Start() initialises it, so guard for that first.
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if c := runtime.Domain.Cache(); c != nil {
			id := runtime.Domain.CreateObjectIDWithThisDomain(BUILT_IN_TYPES, true)
			if _, err := c.GetValueJSON(id); err == nil {
				return runtime, cancel
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	cancel()
	b.Fatal("runtime did not become ready within 20s")
	return nil, nil
}

// BenchmarkRuntimeVertexUpdate measures the most common production op: a
// repeated vertex update (upsert with a changing body so it is a real
// write, not a no-op short-circuit). Exercises the full NATS round-trip,
// cache read+write, op_stack and WAL publish path.
func BenchmarkRuntimeVertexUpdate(b *testing.B) {
	runtime, cancel := startBenchRuntime(b)
	defer cancel()

	// Seed the vertex once.
	create := easyjson.NewJSONObject()
	create.SetByPath("body.n", easyjson.NewJSON(0))
	if _, err := runtime.Request(sfPlugins.AutoRequestSelect,
		"functions.graph.api.vertex.create",
		runtime.Domain.CreateObjectIDWithThisDomain("benchv", true),
		&create, nil); err != nil {
		b.Fatalf("seed vertex.create: %v", err)
	}

	vid := runtime.Domain.CreateObjectIDWithThisDomain("benchv", true)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := easyjson.NewJSONObject()
		p.SetByPath("body.n", easyjson.NewJSON(i+1)) // changing value → real write
		if _, err := runtime.Request(sfPlugins.AutoRequestSelect,
			"functions.graph.api.vertex.update", vid, &p, nil); err != nil {
			b.Fatalf("vertex.update: %v", err)
		}
	}
}

// BenchmarkRuntimeVertexUpdateParallel saturates the runtime with many
// concurrent updates so the system becomes CPU-bound rather than
// latency-bound. Each goroutine owns its own vertex (upsert=true) to avoid
// per-key lock serialization dominating the measurement. Profile THIS to
// see where CPU actually goes under load.
//
//   go test ./embedded/graph/crud/ -run '^$' -bench BenchmarkRuntimeVertexUpdateParallel \
//     -benchmem -benchtime=8s -cpuprofile=/tmp/crud_par_cpu.out -o /tmp/crud_bench.test
//   go tool pprof -top -nodecount=40 /tmp/crud_bench.test /tmp/crud_par_cpu.out
func BenchmarkRuntimeVertexUpdateParallel(b *testing.B) {
	runtime, cancel := startBenchRuntime(b)
	defer cancel()

	var gid int64

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		myID := atomic.AddInt64(&gid, 1)
		vid := runtime.Domain.CreateObjectIDWithThisDomain(fmt.Sprintf("benchpar%d", myID), true)
		n := 0
		for pb.Next() {
			n++
			p := easyjson.NewJSONObject()
			p.SetByPath("upsert", easyjson.NewJSON(true)) // create on first touch, update after
			p.SetByPath("body.n", easyjson.NewJSON(n))
			if _, err := runtime.Request(sfPlugins.AutoRequestSelect,
				"functions.graph.api.vertex.update", vid, &p, nil); err != nil {
				b.Fatalf("vertex.update: %v", err)
			}
		}
	})
}

// BenchmarkRuntimeVertexUpdateNoop measures the no-op upsert path (identical
// body) — the idempotent-state-push case PR de2793f optimized. Useful to
// confirm the short-circuit really avoids the write/WAL cost end-to-end.
func BenchmarkRuntimeVertexUpdateNoop(b *testing.B) {
	runtime, cancel := startBenchRuntime(b)
	defer cancel()

	create := easyjson.NewJSONObject()
	create.SetByPath("body.cpu", easyjson.NewJSON(16))
	create.SetByPath("body.role", easyjson.NewJSON("worker"))
	if _, err := runtime.Request(sfPlugins.AutoRequestSelect,
		"functions.graph.api.vertex.create",
		runtime.Domain.CreateObjectIDWithThisDomain("benchnoop", true),
		&create, nil); err != nil {
		b.Fatalf("seed vertex.create: %v", err)
	}

	vid := runtime.Domain.CreateObjectIDWithThisDomain("benchnoop", true)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p := easyjson.NewJSONObject()
		p.SetByPath("body.cpu", easyjson.NewJSON(16))
		p.SetByPath("body.role", easyjson.NewJSON("worker"))
		if _, err := runtime.Request(sfPlugins.AutoRequestSelect,
			"functions.graph.api.vertex.update", vid, &p, nil); err != nil {
			b.Fatalf("vertex.update noop: %v", err)
		}
	}
}
