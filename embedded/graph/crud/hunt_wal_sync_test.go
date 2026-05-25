package crud_test

// Bug hunt: WAL/snapshot durability across instances.
//
// A second runtime joining the same NATS + domain must reconstruct the graph
// written by the first (startup snapshot of existing KV + ongoing WAL/KV watch).
// This is the cross-instance replication that backs HA passives and restarts.
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

	// Second runtime joins the same NATS/KV/domain and must sync the object.
	rtB, cancelB := mkRT(false)
	defer cancelB()

	synced := waitUntil(30*time.Second, func() bool {
		if rtB.Domain == nil || rtB.Domain.Cache() == nil {
			return false
		}
		v, e := rtB.Domain.Cache().GetValueJSON(rtB.Domain.CreateObjectIDWithThisDomain("wal-1", true))
		return e == nil && strings.Contains(v.ToString(), "42")
	})
	require.Truef(t, synced, "second runtime must sync wal-1 (value 42) from the shared KV/WAL")
}
