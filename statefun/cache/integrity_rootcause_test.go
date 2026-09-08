package cache

// Two paths on which half an edge could vanish without CRUD noticing:
// rehydrate from KV and the maintenance sweep.
//
// A graph edge is two keys (the owner's out-side, the mirror in-key on the
// target) written separately. Neither path may leave one half without the
// other: production showed exactly that state — a live objects.out.to with the
// in-key gone — while KV was intact, so the divergence arose in the operating
// representation.
//
// Everything is checked in both cache representations: a divergence between
// them means production on one mode loses what the other keeps.

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// edgeKeys returns the four keys CRUD writes for one edge: three on the owner,
// one on the target.
func edgeKeys(from, to, name, linkType string) (outTo, ltype, outBody, in string) {
	return from + ".out.to." + name,
		from + ".ltype." + linkType + "." + to,
		from + ".out.body." + name,
		to + ".in." + from + "." + name
}

func requireEdgeWhole(t *testing.T, cs *Store, from, to, name, linkType, ctx string) {
	t.Helper()
	outTo, ltype, _, in := edgeKeys(from, to, name, linkType)
	require.Truef(t, cs.Exists(outTo), "%s: the out half is gone [%s]", ctx, outTo)
	require.Truef(t, cs.Exists(ltype), "%s: the ltype entry is gone [%s]", ctx, ltype)
	require.Truef(t, cs.Exists(in), "%s: the in half is gone [%s]", ctx, in)
}

func requireEdgeGone(t *testing.T, cs *Store, from, to, name, linkType, ctx string) {
	t.Helper()
	outTo, ltype, _, in := edgeKeys(from, to, name, linkType)
	require.Falsef(t, cs.Exists(outTo), "%s: the out half survived [%s]", ctx, outTo)
	require.Falsef(t, cs.Exists(ltype), "%s: the ltype entry survived [%s]", ctx, ltype)
	require.Falsef(t, cs.Exists(in), "%s: the in half survived [%s]", ctx, in)
}

// writeEdge writes an edge the way CRUD does: three keys on the owner and the
// mirror key on the target, all under one opTime.
func writeEdge(cs *Store, from, to, name, linkType string, opTime int64, toKV bool) {
	outTo, ltype, outBody, in := edgeKeys(from, to, name, linkType)
	cs.SetValue(outTo, []byte(linkType+"."+to), toKV, opTime)
	cs.SetValue(ltype, []byte(name), toKV, opTime)
	cs.SetValue(outBody, []byte(`{}`), toKV, opTime)
	cs.SetValue(in, []byte(linkType), toKV, opTime)
}

func deleteEdge(cs *Store, from, to, name, linkType string, opTime int64, toKV bool) {
	outTo, ltype, outBody, in := edgeKeys(from, to, name, linkType)
	for _, k := range []string{outTo, ltype, outBody, in} {
		cs.DeleteValue(k, toKV, opTime)
	}
}

// --- rehydrate from KV ------------------------------------------------------

// Rehydrate is the only path by which the operating representation re-reads KV
// in production (a passive→active promotion). It must return the graph exactly
// as KV holds it: a half lost in memory has to come back.
func Test_RootCause_RehydrateRestoresLostHalfEdge(t *testing.T) {
	for _, mode := range []string{"tree", "records"} {
		mode := mode
		t.Run(mode, func(t *testing.T) {
			restore := SetCacheModeForTest(mode)
			defer restore()

			js, kvs := newKVForTest(t, "rehy_"+mode)
			fillKV(t, kvs, map[string][]byte{
				"dom/objects":                        []byte(`{}`),
				"dom/obj":                            []byte(`{"a":1}`),
				"dom/objects.out.to.obj":             []byte("__object.dom/obj"),
				"dom/objects.ltype.__object.dom/obj": []byte("obj"),
				"dom/objects.out.body.obj":           []byte(`{}`),
				"dom/obj.in.dom/objects.obj":         []byte("__object"),
			})

			cs, cancel := loadStore(t, js, kvs, "rehy_"+mode)
			defer cancel()
			requireEdgeWhole(t, cs, "dom/objects", "dom/obj", "obj", "__object", "after loading")

			// Exactly the production defect: the in half disappears from MEMORY
			// only. The deletion time is deliberately newer than the write —
			// otherwise the LWW guard refuses it and we would be testing the
			// guard rather than rehydrate.
			cs.DeleteValue("dom/obj.in.dom/objects.obj", false, int64(1)<<62)
			require.False(t, cs.Exists("dom/obj.in.dom/objects.obj"))

			require.NoError(t, cs.RehydrateFromKV(context.Background()))
			requireEdgeWhole(t, cs, "dom/objects", "dom/obj", "obj", "__object",
				"rehydrate must restore the half that KV still holds")
		})
	}
}

// The other side: rehydrate must not resurrect what was deleted properly, or
// every promotion returns deleted objects to the model — and half-edges with
// them.
func Test_RootCause_RehydrateDoesNotResurrectDeleted(t *testing.T) {
	for _, mode := range []string{"tree", "records"} {
		mode := mode
		t.Run(mode, func(t *testing.T) {
			restore := SetCacheModeForTest(mode)
			defer restore()

			js, kvs := newKVForTest(t, "resur_"+mode)
			fillKV(t, kvs, map[string][]byte{"dom/objects": []byte(`{}`), "dom/obj": []byte(`{}`)})

			cs, cancel := loadStore(t, js, kvs, "resur_"+mode)
			defer cancel()

			writeEdge(cs, "dom/objects", "dom/obj", "obj", "__object", 1000, true)
			requireEdgeWhole(t, cs, "dom/objects", "dom/obj", "obj", "__object", "after creation")
			deleteEdge(cs, "dom/objects", "dom/obj", "obj", "__object", 2000, true)
			requireEdgeGone(t, cs, "dom/objects", "dom/obj", "obj", "__object", "after deletion")

			waitKVDrained(t, cs)
			require.NoError(t, cs.RehydrateFromKV(context.Background()))
			requireEdgeGone(t, cs, "dom/objects", "dom/obj", "obj", "__object",
				"rehydrate resurrected an edge that was deleted properly")
		})
	}
}

// --- maintenance sweep ------------------------------------------------------

// The sweep walks the whole tree about once a second, collapsing tombstones. It
// may not take live keys with it — neither half of an edge, nor its neighbours.
func Test_RootCause_MaintenanceKeepsLiveEdges(t *testing.T) {
	for _, mode := range []string{"tree", "records"} {
		mode := mode
		t.Run(mode, func(t *testing.T) {
			restore := SetCacheModeForTest(mode)
			defer restore()
			cs := NewStoreForTest("sweep_" + mode)

			const n = 50
			for i := 0; i < n; i++ {
				name := fmt.Sprintf("l%03d", i)
				to := fmt.Sprintf("dom/t-%03d", i)
				cs.SetValue(to, []byte(`{}`), false, int64(100+i))
				writeEdge(cs, "dom/objects", to, name, "__object", int64(100+i), false)
			}
			// Half of them are deleted properly, so the sweep has work to do.
			for i := 0; i < n; i += 2 {
				deleteEdge(cs, "dom/objects", fmt.Sprintf("dom/t-%03d", i), fmt.Sprintf("l%03d", i), "__object", int64(1000+i), false)
			}
			for pass := 0; pass < 3; pass++ {
				cs.RunMaintenanceForTest()
			}
			for i := 1; i < n; i += 2 {
				requireEdgeWhole(t, cs, "dom/objects", fmt.Sprintf("dom/t-%03d", i), fmt.Sprintf("l%03d", i), "__object",
					fmt.Sprintf("the sweep took live edge %d", i))
			}
			for i := 0; i < n; i += 2 {
				requireEdgeGone(t, cs, "dom/objects", fmt.Sprintf("dom/t-%03d", i), fmt.Sprintf("l%03d", i), "__object",
					fmt.Sprintf("the sweep left half of deleted edge %d", i))
			}
		})
	}
}

// waitKVDrained waits for the lazy writer to hand everything to KV, so that a
// rehydrate reads the state that was actually written out.
func waitKVDrained(t *testing.T, cs *Store) {
	t.Helper()
	require.Eventuallyf(t, func() bool { return !cs.HasPendingWrites() },
		30*time.Second, 20*time.Millisecond, "the WAL did not drain into KV")
}

// --- writing into what the maintenance pass is taking away --------------------

// The tree half. A key whose path does not exist yet is written in two steps:
// the path is walked (creating the empty nodes along it) and then the value is
// put at the end of it. A sweep in between finds those nodes carrying nothing
// and unlinks them — and the value then lands in a subtree nothing reaches.
//
// This is the shape of the production damage: the vertex kept its out-side,
// where the path `objects.out.to` was long established, and lost the in-key,
// whose path `<id>.in.<objects>` was being created for the first time.
func Test_RootCause_SweepUnlinkingAPathUnderAWriteLosesNothing(t *testing.T) {
	restore := SetCacheModeForTest("tree")
	defer restore()
	cs := NewStoreForTest("detach_tree")

	const key = "dom/v.in.dom/objects.v"
	require.True(t, cs.SetValue("dom/v", []byte(`{}`), false, 100))

	var once sync.Once
	afterTreePathWalkForTest = func(k string) {
		if k != key {
			return
		}
		// Exactly the window: the path is walked, the value is not written
		// yet, and the sweep collapses the empty chain it just made.
		once.Do(func() { cs.RunMaintenanceForTest() })
	}
	defer func() { afterTreePathWalkForTest = nil }()

	require.True(t, cs.SetValue(key, []byte("__object"), false, 200))
	require.Truef(t, cs.Exists(key), "the write landed in a subtree the sweep had unlinked")
	require.Containsf(t, cs.GetKeysByPattern("dom/v.in.>"), key,
		"the key is not in the enumeration the graph reads in-links through")
}
