package cache

// Write/delete invariants that must hold in ANY representation of the cache.
//
// The cache has two working representations (CACHE_MODE=tree, and records by
// default), and the graph must behave identically in both: a divergence here
// means production on one mode loses what the other keeps.
//
// Three things are checked:
//   - the last-writer-wins guard is symmetrical: a stale delete does not erase
//     a newer write, a stale write does not resurrect a deleted key;
//   - rewriting a vertex body does not disturb its edges;
//   - deleting one key does not disturb its neighbours.
//
// All of it is the root-cause area for half-written edges: an operation writes
// a PAIR of keys (the owner's out-side and the mirror in-key on the target)
// separately, and losing either half leaves an object unreadable as an object
// — exactly what happened in production.

import (
	"fmt"
	"sync"
	"testing"

	"github.com/foliagecp/easyjson"
	"github.com/stretchr/testify/require"
)

// forEachCacheMode runs the body in both cache representations.
func forEachCacheMode(t *testing.T, body func(t *testing.T, cs *Store)) {
	t.Helper()
	for _, mode := range []string{"tree", "records"} {
		mode := mode
		t.Run(mode, func(t *testing.T) {
			restore := SetCacheModeForTest(mode)
			defer restore()
			body(t, NewStoreForTest("inv_"+mode))
		})
	}
}

func Test_CacheInvariant_StaleDeleteMustNotEraseNewerWrite(t *testing.T) {
	forEachCacheMode(t, func(t *testing.T, cs *Store) {
		require.True(t, cs.SetValue("dom/v.out.to.l1", []byte("__object.dom/x"), false, 2000))

		// A delete OLDER than the write: a late tail of the key's previous life.
		cs.DeleteValue("dom/v.out.to.l1", false, 1000)

		require.Truef(t, cs.Exists("dom/v.out.to.l1"),
			"a stale delete (t=1000) erased a newer write (t=2000)")
	})
}

func Test_CacheInvariant_StaleWriteMustNotResurrectDeletedKey(t *testing.T) {
	forEachCacheMode(t, func(t *testing.T, cs *Store) {
		require.True(t, cs.SetValue("dom/v.out.to.l2", []byte("__object.dom/x"), false, 2000))
		cs.DeleteValue("dom/v.out.to.l2", false, 3000)
		require.False(t, cs.Exists("dom/v.out.to.l2"))

		require.Falsef(t, cs.SetValue("dom/v.out.to.l2", []byte("__object.dom/x"), false, 2500),
			"a stale write (t=2500) reported success on a key deleted later (t=3000)")
		require.Falsef(t, cs.Exists("dom/v.out.to.l2"),
			"a deleted key was resurrected by a stale write")
	})
}

func Test_CacheInvariant_DeleteAtSameOrNewerTimeApplies(t *testing.T) {
	forEachCacheMode(t, func(t *testing.T, cs *Store) {
		require.True(t, cs.SetValue("dom/v.out.to.same", []byte("__object.dom/x"), false, 3000))
		cs.DeleteValue("dom/v.out.to.same", false, 3000)
		require.Falsef(t, cs.Exists("dom/v.out.to.same"), "a delete at the same op_time must apply")

		require.True(t, cs.SetValue("dom/v.out.to.newer", []byte("__object.dom/x"), false, 3000))
		cs.DeleteValue("dom/v.out.to.newer", false, 4000)
		require.Falsef(t, cs.Exists("dom/v.out.to.newer"), "a delete newer than the write must apply")
	})
}

// Rewriting a vertex body is the most frequent operation in production (the
// model builder updates objects continuously). It must not touch the vertex's
// edges.
func Test_CacheInvariant_VertexBodyRewriteKeepsItsEdges(t *testing.T) {
	forEachCacheMode(t, func(t *testing.T, cs *Store) {
		body := easyjson.NewJSONObjectWithKeyValue("cpu", easyjson.NewJSON(8))
		require.True(t, cs.SetValueJSON("dom/v", &body, false, 1000))
		require.True(t, cs.SetValue("dom/v.out.to.type", []byte("__type.dom/t"), false, 1000))
		require.True(t, cs.SetValue("dom/v.in.dom/objects.v", []byte("__object"), false, 1000))
		require.True(t, cs.SetValue("dom/v.in.dom/t.v", []byte("__object"), false, 1000))

		newBody := easyjson.NewJSONObjectWithKeyValue("cpu", easyjson.NewJSON(16))
		require.True(t, cs.SetValueJSON("dom/v", &newBody, false, 2000))

		require.Truef(t, cs.Exists("dom/v.out.to.type"), "the body rewrite lost an out-edge")
		require.Truef(t, cs.Exists("dom/v.in.dom/objects.v"), "the body rewrite lost the in-key from objects")
		require.Truef(t, cs.Exists("dom/v.in.dom/t.v"), "the body rewrite lost the in-key from the type")
		require.Lenf(t, cs.GetKeysByPattern("dom/v.in.>"), 2, "in-key enumeration after a body rewrite")
	})
}

// Deleting one in-key must not disturb the vertex's other edges.
func Test_CacheInvariant_DeletingOneEdgeKeepsSiblings(t *testing.T) {
	forEachCacheMode(t, func(t *testing.T, cs *Store) {
		require.True(t, cs.SetValue("dom/v.in.dom/objects.v", []byte("__object"), false, 1000))
		require.True(t, cs.SetValue("dom/v.in.dom/t.v", []byte("__object"), false, 1000))
		require.True(t, cs.SetValue("dom/v.out.to.type", []byte("__type.dom/t"), false, 1000))

		cs.DeleteValue("dom/v.in.dom/objects.v", false, 2000)

		require.False(t, cs.Exists("dom/v.in.dom/objects.v"))
		require.Truef(t, cs.Exists("dom/v.in.dom/t.v"), "deleting one in-key took its neighbour with it")
		require.Truef(t, cs.Exists("dom/v.out.to.type"), "deleting an in-key took an out-edge with it")
		require.Lenf(t, cs.GetKeysByPattern("dom/v.in.>"), 1, "in-key enumeration after a single deletion")
	})
}

// Concurrent load of the same shape as production: the vertex body is rewritten
// continuously while its edges appear and disappear alongside. No live edge may
// vanish, and no deleted one may come back.
func Test_CacheInvariant_ConcurrentBodyRewriteAndEdges(t *testing.T) {
	forEachCacheMode(t, func(t *testing.T, cs *Store) {
		const rounds = 300
		var wg sync.WaitGroup

		// Permanent edges that must survive the whole load.
		stable := []string{
			"dom/v.in.dom/objects.v",
			"dom/v.in.dom/t.v",
			"dom/v.out.to.type",
		}
		for i, k := range stable {
			require.True(t, cs.SetValue(k, []byte("__object"), false, int64(1000+i)))
		}

		wg.Add(3)
		// 1. body rewrites
		go func() {
			defer wg.Done()
			for i := 0; i < rounds; i++ {
				b := easyjson.NewJSONObjectWithKeyValue("n", easyjson.NewJSON(i))
				cs.SetValueJSON("dom/v", &b, false, int64(2000+i))
			}
		}()
		// 2. transient edges coming and going
		go func() {
			defer wg.Done()
			for i := 0; i < rounds; i++ {
				k := fmt.Sprintf("dom/v.in.dom/tmp%d.v", i%7)
				cs.SetValue(k, []byte("__object"), false, int64(2000+i))
				cs.DeleteValue(k, false, int64(2000+i+1))
			}
		}()
		// 3. reads and enumeration running alongside the writes
		go func() {
			defer wg.Done()
			for i := 0; i < rounds; i++ {
				cs.GetKeysByPattern("dom/v.in.>")
				cs.Exists("dom/v.in.dom/objects.v")
			}
		}()
		wg.Wait()

		for _, k := range stable {
			require.Truef(t, cs.Exists(k), "permanent edge %q vanished under concurrent load", k)
		}
		keys := cs.GetKeysByPattern("dom/v.in.>")
		require.Containsf(t, keys, "dom/v.in.dom/objects.v",
			"in-key enumeration lost the edge from objects: %v", keys)
	})
}
