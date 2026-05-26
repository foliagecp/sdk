package crud_test

// Bug hunt: WAL/snapshot durability across instances.
//
// A second runtime joining the same NATS + domain must reconstruct the graph
// from KV at startup. In the cache-as-source-of-truth model there is no
// ongoing kv.Watch echo into the cache after initial load, so the only thing
// rtB sees during its load is what rtA has already pushed durably to KV.
// Therefore the test must explicitly drain the WAL→committer→KV chain on
// rtA (via Domain.WaitForKVCaughtUp) BEFORE starting rtB — otherwise rtA's
// writes can still be in flight and rtB's initial load will miss them.
//
// Two real runtimes in one process; the deadlock fix (HA transitions) is what
// lets them coexist.

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/clients/go/db"
	"github.com/foliagecp/sdk/embedded/graph/crud"
	"github.com/foliagecp/sdk/embedded/graph/jpgql"
	"github.com/foliagecp/sdk/statefun"
	"github.com/foliagecp/sdk/statefun/cache"
	"github.com/foliagecp/sdk/statefun/system"
	natsservertest "github.com/nats-io/nats-server/v2/test"
	"github.com/stretchr/testify/require"
)

func waitUntil(d time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return cond()
}

func Test_Hunt_WAL_SecondRuntimeSyncsState(t *testing.T) {
	if system.GlobalPrometrics == nil {
		system.GlobalPrometrics = system.NewPrometrics("", "")
	}
	opts := natsservertest.DefaultTestOptions
	opts.JetStream = true
	opts.Port = -1
	// DefaultTestOptions leaves StoreDir empty, which makes nats-server fall
	// back to the FIXED shared path $TMPDIR/jetstream (see jetstream.go:214).
	// That dir persists across `go test` runs, so leftover streams/KV/RAFT
	// state from a prior run poisons this one — surfacing as
	// "vertex WalType already exists" plus a cascade of consumer/timeout
	// errors. Give every run its own auto-cleaned store dir, mirroring the
	// harness fix in statefun/test/env.go.
	opts.StoreDir = t.TempDir()
	srv := natsservertest.RunServer(&opts)
	defer srv.Shutdown()
	url := srv.ClientURL()

	mkRT := func(withCRUD bool) (*statefun.Runtime, context.CancelFunc) {
		cfg := statefun.NewRuntimeConfigSimple(url, "waldom").SetKVMutexLifeTimeSec(4)
		rt, err := statefun.NewRuntime(*cfg)
		require.NoError(t, err)
		if withCRUD {
			crud.RegisterAllFunctionTypes(rt)
			jpgql.RegisterAllFunctionTypes(rt)
		}
		ctx, cancel := context.WithCancel(context.Background())
		go func() { _ = rt.Start(ctx, cache.NewCacheConfig("waldom_cache")) }()
		return rt, cancel
	}

	// First runtime: becomes active and writes data.
	rtA, cancelA := mkRT(true)
	defer cancelA()
	require.Truef(t, waitUntil(30*time.Second, rtA.IsActiveInstance), "first runtime must become active")
	// IsActiveInstance can flip true before Start finishes wiring the request
	// path (isReady is set late), so a write fired in that gap fails with
	// "runtime has not started yet". Gate on readiness, not just active role.
	require.Truef(t, waitUntil(30*time.Second, rtA.IsReady), "first runtime must become ready to serve requests")

	dbcA, err := db.NewDBSyncClientFromRequestFunction(rtA.Request)
	require.NoError(t, err)
	require.Truef(t, waitUntil(15*time.Second, func() bool {
		if rtA.Domain == nil || rtA.Domain.Cache() == nil {
			return false
		}
		_, e := rtA.Domain.Cache().GetValueJSON(rtA.Domain.CreateObjectIDWithThisDomain(crud.BUILT_IN_OBJECTS, true))
		return e == nil
	}), "built-ins must be ready on the first runtime")

	require.NoError(t, dbcA.CMDB.TypeCreate("WalType"))
	require.NoError(t, dbcA.CMDB.ObjectUpdate("wal-1", easyjson.NewJSONObjectWithKeyValue("v", easyjson.NewJSON(42)), false, "WalType"))

	// Drain rtA's WAL→committer→KV chain before rtB joins. Without this,
	// rtA's writes may still be buffered (in pendingTxs or in the WAL stream
	// not yet applied by the committer) when rtB's initial load samples KV,
	// and rtB's cache would miss them — there is no continuous kv.Watch in
	// the cache-as-source-of-truth model to fill in afterwards.
	require.NoError(t, rtA.Domain.WaitForKVCaughtUp(context.Background(), 30*time.Second),
		"rtA writes must reach KV before rtB joins")

	// Second runtime joins the same NATS/KV/domain and must sync the object.
	rtB, cancelB := mkRT(false)
	defer cancelB()
	// Wait until rtB is fully started before touching its cache: Start builds the
	// cache store in a goroutine, so reading Domain.Cache() mid-construction
	// races with it (Store.rootValue). IsReady is set after construction (it is
	// reached by passive instances too — only subscriptions are active-only).
	require.Truef(t, waitUntil(30*time.Second, rtB.IsReady), "second runtime must become ready")

	synced := waitUntil(30*time.Second, func() bool {
		if rtB.Domain == nil || rtB.Domain.Cache() == nil {
			return false
		}
		v, e := rtB.Domain.Cache().GetValueJSON(rtB.Domain.CreateObjectIDWithThisDomain("wal-1", true))
		return e == nil && strings.Contains(v.ToString(), "42")
	})
	require.Truef(t, synced, "second runtime must sync wal-1 (value 42) from the shared KV/WAL")
}
