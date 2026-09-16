package cache

// The delete markers a KV bucket keeps, and the purger that takes them back.
//
// A marker is removed by its own stream sequence, so the purge is safe from
// the side of a pipeline that has moved on: whatever was written to the key
// after the marker stays. The load meets the markers nobody removed and hands
// them to the purger, in either representation of the cache. And a purger
// that cannot reach the broker gives up after a few tries, says so, and does
// not wedge.

import (
	"context"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/foliagecp/easyjson"
	customNatsKv "github.com/foliagecp/sdk/embedded/nats/kv"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
)

// deleteMarkers lists the keys under prefix whose last message is a delete or
// purge marker — what a load without IgnoreDeletes would meet.
func deleteMarkers(t *testing.T, kvs nats.KeyValue, prefix string) []string {
	t.Helper()
	w, err := kvs.Watch(prefix + ".>")
	require.NoError(t, err)
	defer func() { _ = w.Stop() }()
	var markers []string
	for entry := range w.Updates() {
		if entry == nil {
			break
		}
		if op := entry.Operation(); op == nats.KeyValueDelete || op == nats.KeyValuePurge {
			markers = append(markers, entry.Key())
		}
	}
	sort.Strings(markers)
	return markers
}

// streamMsgs is how many messages the bucket's stream holds.
func streamMsgs(t *testing.T, js nats.JetStreamContext, kvs nats.KeyValue) uint64 {
	t.Helper()
	info, err := js.StreamInfo(customNatsKv.KVStreamName(kvs))
	require.NoError(t, err)
	return info.State.Msgs
}

// lastOf is what the bucket's stream holds last for key: "absent" when nothing,
// "marker" when a delete marker, otherwise the value. kv.Get cannot tell the
// first two apart — it reports a deleted key as not found — so the stream is
// asked directly.
func lastOf(t *testing.T, js nats.JetStreamContext, kvs nats.KeyValue, key string) string {
	t.Helper()
	m, err := js.GetLastMsg(customNatsKv.KVStreamName(kvs), customNatsKv.KVStoredSubject(kvs, key))
	if err != nil {
		require.ErrorIs(t, err, nats.ErrMsgNotFound)
		return "absent"
	}
	if op := m.Header.Get("KV-Operation"); op == "DEL" || op == "PURGE" {
		return "marker"
	}
	return string(m.Data)
}

// markerSeq deletes key and returns the stream sequence of the marker it left.
func markerSeq(t *testing.T, js nats.JetStreamContext, kvs nats.KeyValue, key string) uint64 {
	t.Helper()
	f, err := customNatsKv.KVDeleteAsync(js, kvs, key)
	require.NoError(t, err)
	select {
	case pa := <-f.Ok():
		return pa.Sequence
	case err := <-f.Err():
		t.Fatalf("delete of %s was not acked: %v", key, err)
	case <-time.After(10 * time.Second):
		t.Fatalf("delete of %s was not acked in time", key)
	}
	return 0
}

func waitPurged(t *testing.T, cs *Store, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if cs.KVTombstonesPendingForTest() == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("the purger still has %d marker(s) pending after %s", cs.KVTombstonesPendingForTest(), within)
}

// Test_PurgeTombstone_RemovesTheMarkerAndNothingAfter — the purge is bounded
// by the marker's sequence: the marker goes, a later write of the same key
// stays, and a later marker of the same key stays until its own purge.
func Test_PurgeTombstone_RemovesTheMarkerAndNothingAfter(t *testing.T) {
	js, kvs := newKVForTest(t, "purge_bound")
	const key = KVStorePrefix + ".dom/x"

	// Deleted and left deleted: the marker is the key's only message, and the
	// purge takes it — the key is then not deleted but absent.
	_, err := kvs.Put(key, []byte("v1"))
	require.NoError(t, err)
	seq := markerSeq(t, js, kvs, key)
	require.Equal(t, "marker", lastOf(t, js, kvs, key), "sanity: a KV delete leaves a marker")
	before := streamMsgs(t, js, kvs)
	require.NoError(t, customNatsKv.KVPurgeTombstone(js, kvs, key, seq))
	require.Equal(t, "absent", lastOf(t, js, kvs, key), "the marker must be gone, not just superseded")
	require.Equal(t, before-1, streamMsgs(t, js, kvs), "exactly the marker left the stream")
	require.Empty(t, deleteMarkers(t, kvs, KVStorePrefix))

	// Deleted and written again before the purge ran: the new value must
	// survive a purge issued for the old marker.
	_, err = kvs.Put(key, []byte("v2"))
	require.NoError(t, err)
	seq = markerSeq(t, js, kvs, key)
	_, err = kvs.Put(key, []byte("v3"))
	require.NoError(t, err)
	require.NoError(t, customNatsKv.KVPurgeTombstone(js, kvs, key, seq))
	require.Equal(t, "v3", lastOf(t, js, kvs, key), "the value written after the marker must survive its purge")
	entry, err := kvs.Get(key)
	require.NoError(t, err)
	require.Equal(t, "v3", string(entry.Value()))

	// Deleted, written, deleted again: the purge for the first marker leaves
	// the second one — it is the second purge's job.
	seq1 := markerSeq(t, js, kvs, key)
	_, err = kvs.Put(key, []byte("v4"))
	require.NoError(t, err)
	seq2 := markerSeq(t, js, kvs, key)
	require.Greater(t, seq2, seq1)
	require.NoError(t, customNatsKv.KVPurgeTombstone(js, kvs, key, seq1))
	require.Equal(t, "marker", lastOf(t, js, kvs, key), "the newer marker must still be there")
	require.NoError(t, customNatsKv.KVPurgeTombstone(js, kvs, key, seq2))
	require.Equal(t, "absent", lastOf(t, js, kvs, key))
}

// Test_Load_SweepsLeftoverMarkers — markers nobody removed are met by the
// load, skipped as data, and removed; what was not deleted loads as before.
// In both representations.
func Test_Load_SweepsLeftoverMarkers(t *testing.T) {
	for _, mode := range []string{"tree", "records"} {
		t.Run(mode, func(t *testing.T) {
			restore := SetCacheModeForTest(mode)
			defer restore()

			js, kvs := newKVForTest(t, "leftovers_"+mode)
			entries, keys := kvGraph(60, 120)
			fillKV(t, kvs, entries)

			// Every third key deleted straight in KV, the way an older version
			// left them: a marker each, nothing to remove them.
			var deleted, kept []string
			for i, k := range keys {
				if i%3 == 0 {
					deleted = append(deleted, k)
					markerSeq(t, js, kvs, KVStorePrefix+"."+k)
				} else {
					kept = append(kept, k)
				}
			}
			require.Len(t, deleteMarkers(t, kvs, KVStorePrefix), len(deleted), "sanity: the markers are there before the load")
			live := streamMsgs(t, js, kvs) - uint64(len(deleted))

			cs, cancel := loadStore(t, js, kvs, "leftovers_"+mode)
			defer cancel()
			waitPurged(t, cs, 15*time.Second)

			require.Empty(t, deleteMarkers(t, kvs, KVStorePrefix), "the load must have handed every marker to the purger")
			require.Equal(t, live, streamMsgs(t, js, kvs), "only the markers may have left the stream")
			for _, k := range deleted {
				require.Falsef(t, cs.Exists(k), "a deleted key %s must not have loaded", k)
			}
			for _, k := range kept {
				require.Truef(t, cs.Exists(k), "a kept key %s must have loaded", k)
				// A body is stored parsed and comes back re-serialized, so it
				// is compared as JSON; anything else byte for byte.
				v, _ := cs.GetValue(k)
				if want, ok := easyjson.JSONFromBytes(entries[k]); ok && want.IsObject() {
					got, ok := easyjson.JSONFromBytes(v)
					require.Truef(t, ok && want.Equals(got), "value of %s: want %s, got %s", k, entries[k], v)
				} else if len(entries[k]) > 0 {
					require.Equalf(t, entries[k], v, "value of %s", k)
				}
			}
		})
	}
}

// Test_Purger_GivesUpOnABrokerItCannotReach — a purge that keeps failing is
// retried a bounded number of times and then dropped, counted, and the purger
// is idle again; nothing hangs and nothing panics.
func Test_Purger_GivesUpOnABrokerItCannotReach(t *testing.T) {
	_, kvs := newKVForTest(t, "unreachable")
	// A JetStream context on a connection that is already closed: every
	// request fails at once.
	nc, err := nats.Connect(lastTestNatsURL)
	require.NoError(t, err)
	deadJS, err := nc.JetStream()
	require.NoError(t, err)
	nc.Close()

	p := newTombstonePurger(deadJS, kvs, "unreachable")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.run(ctx)

	p.note(KVStorePrefix+".dom/gone", 7)
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) && p.pending() > 0 {
		time.Sleep(50 * time.Millisecond)
	}
	require.Equal(t, 0, p.pending(), "the purger must give the marker up, not keep it forever")
	require.EqualValues(t, 1, p.dropped.Load(), "and count it as dropped")
	require.EqualValues(t, 0, p.purged.Load())
}

// Test_Purger_RemovesWhatItIsTold — the purger removes every marker it is
// handed, in batches, with the bucket's live keys untouched.
func Test_Purger_RemovesWhatItIsTold(t *testing.T) {
	js, kvs := newKVForTest(t, "told")
	const n = 300
	seqs := map[string]uint64{}
	for i := 0; i < n; i++ {
		k := fmt.Sprintf("%s.dom/t-%03d", KVStorePrefix, i)
		_, err := kvs.Put(k, []byte("v"))
		require.NoError(t, err)
		if i%2 == 0 {
			seqs[k] = markerSeq(t, js, kvs, k)
		}
	}
	require.Len(t, deleteMarkers(t, kvs, KVStorePrefix), n/2)

	p := newTombstonePurger(js, kvs, "told")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go p.run(ctx)
	for k, s := range seqs {
		p.note(k, s)
	}
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) && p.pending() > 0 {
		time.Sleep(20 * time.Millisecond)
	}
	require.Equal(t, 0, p.pending())
	require.EqualValues(t, n/2, p.purged.Load())
	require.Empty(t, deleteMarkers(t, kvs, KVStorePrefix))
	require.EqualValues(t, n/2, streamMsgs(t, js, kvs), "the live keys are all still there")
}
