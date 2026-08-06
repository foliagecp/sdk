package statefun

// Tests for the salted-context index that lets vertex deletion drop every
// `<typename>.<id>===<hash>` context sibling as point deletes instead of the
// previous scan of the whole `<typename>.*` context level per vertex.delete.
//
// Covered here:
//   - restore-on-first-use: contexts persisted by a "previous process" (keys
//     planted in the cache behind the index's back) are found by the one-time
//     level scan;
//   - the live path: contexts written through setContext are dropped via the
//     index, other ids' contexts stay untouched, and an id's index entry is
//     rebuilt correctly after a drop;
//   - pruning: setContext(nil) and the expired-context gc sweep both remove
//     the salt from the index so it cannot accumulate dead entries.

import (
	"sync"
	"testing"
	"time"

	"github.com/foliagecp/easyjson"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/stretchr/testify/require"
)

func ctxBody(tag string) *easyjson.JSON {
	b := easyjson.NewJSONObject()
	b.SetByPath("tag", easyjson.NewJSON(tag))
	return &b
}

func saltsInIndex(ft *FunctionType, origId string) []string {
	var salts []string
	if set, ok := ft.saltedCtxIndex.Load(origId); ok {
		set.(*sync.Map).Range(func(k, _ any) bool {
			salts = append(salts, k.(string))
			return true
		})
	}
	return salts
}

func Test_SaltedCtxIndex_DropDeletesExactAndSaltedVariants(t *testing.T) {
	noop := func(_ sfPlugins.StatefunExecutor, _ *sfPlugins.StatefunContextProcessor) {}
	var ft *FunctionType
	rt, srv := startWorkerPoolTestRuntime(t, func(rt *Runtime) {
		ft = NewFunctionType(rt, "test.saltidx", noop, *NewFunctionTypeConfig())
	})
	defer srv.Shutdown()
	cs := rt.Domain.Cache()

	// Contexts persisted by a previous process: KV survives restarts, the
	// in-memory index starts empty — plant keys behind the index's back.
	cs.SetValueJSON("test.saltidx.obj1===aaaa0001", ctxBody("legacy-1"), true, -1)
	cs.SetValueJSON("test.saltidx.obj1===aaaa0002", ctxBody("legacy-2"), true, -1)
	cs.SetValueJSON("test.saltidx.obj2===bbbb0001", ctxBody("other-object"), true, -1)

	// Live contexts written through the normal path.
	ft.setContext("test.saltidx.obj1", ctxBody("plain"))
	ft.setContext("test.saltidx.obj1===cccc0003", ctxBody("fresh"))

	ft.dropContextsForID("obj1", -1)

	require.False(t, cs.ExistsJson("test.saltidx.obj1"), "exact context must die")
	require.False(t, cs.ExistsJson("test.saltidx.obj1===aaaa0001"), "legacy salted context must die (restore scan)")
	require.False(t, cs.ExistsJson("test.saltidx.obj1===aaaa0002"), "legacy salted context must die (restore scan)")
	require.False(t, cs.ExistsJson("test.saltidx.obj1===cccc0003"), "live salted context must die (index)")
	require.True(t, cs.ExistsJson("test.saltidx.obj2===bbbb0001"), "another id's context must stay")

	// The restore scan ran once; obj2 is now served purely by the index.
	ft.dropContextsForID("obj2", -1)
	require.False(t, cs.ExistsJson("test.saltidx.obj2===bbbb0001"))

	// After a drop the id's index entry is rebuilt from scratch by new writes.
	ft.setContext("test.saltidx.obj1===dddd0004", ctxBody("reborn"))
	ft.dropContextsForID("obj1", -1)
	require.False(t, cs.ExistsJson("test.saltidx.obj1===dddd0004"))
}

func Test_SaltedCtxIndex_PrunedByNilWriteAndGCSweep(t *testing.T) {
	noop := func(_ sfPlugins.StatefunExecutor, _ *sfPlugins.StatefunContextProcessor) {}
	var ft *FunctionType
	rt, srv := startWorkerPoolTestRuntime(t, func(rt *Runtime) {
		ft = NewFunctionType(rt, "test.saltidx2", noop, *NewFunctionTypeConfig())
	})
	defer srv.Shutdown()
	cs := rt.Domain.Cache()

	// A salted write lands in the index; deleting the context via
	// setContext(nil) prunes it again.
	ft.setContext("test.saltidx2.obj===s0000001", ctxBody("transient"))
	require.Equal(t, []string{"obj===s0000001"}, saltsInIndex(ft, "obj"))
	ft.setContext("test.saltidx2.obj===s0000001", nil)
	require.Empty(t, saltsInIndex(ft, "obj"), "nil write must prune the salt from the index")
	require.False(t, cs.ExistsJson("test.saltidx2.obj===s0000001"))

	// An expired salted context swept by gc is pruned from the index too.
	expired := ctxBody("expiring")
	expired.SetByPath(contextExpirationKey, easyjson.NewJSON(time.Now().Add(-time.Second).UnixNano()))
	ft.setContext("test.saltidx2.obj===s0000002", expired)
	require.Equal(t, []string{"obj===s0000002"}, saltsInIndex(ft, "obj"))
	ft.gc(60_000)
	require.False(t, cs.ExistsJson("test.saltidx2.obj===s0000002"), "expired salted context must be swept")
	require.Empty(t, saltsInIndex(ft, "obj"), "gc sweep must prune the salt from the index")
}
