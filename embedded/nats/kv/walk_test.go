package kv

// KVWalkLatest: what it visits and how it ends.
//
// It visits the latest message of every key, values and delete markers alike,
// and returns when the bucket has been read: at once for an empty bucket, on
// the last message otherwise, and also when messages it was going to deliver
// are removed from the stream while it reads. A read that neither advances nor
// can be shown complete is reported as stalled instead of waiting forever.

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	natsservertest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
)

func walkTestBucket(t *testing.T, bucket string) (*server.Server, nats.JetStreamContext, nats.KeyValue) {
	t.Helper()
	opts := natsservertest.DefaultTestOptions
	opts.JetStream = true
	opts.Port = -1
	opts.StoreDir = t.TempDir()
	srv := natsservertest.RunServer(&opts)
	t.Cleanup(srv.Shutdown)
	nc, err := nats.Connect(srv.ClientURL())
	require.NoError(t, err)
	t.Cleanup(nc.Close)
	js, err := nc.JetStream(nats.MaxWait(time.Second))
	require.NoError(t, err)
	kvs, err := CreateKeyValue(nc, js, &nats.KeyValueConfig{Bucket: bucket})
	require.NoError(t, err)
	return srv, js, kvs
}

type walked struct {
	value    string
	revision uint64
	deleted  bool
}

func walkAll(t *testing.T, js nats.JetStreamContext, kvs nats.KeyValue, keys string) map[string]walked {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	got := map[string]walked{}
	require.NoError(t, KVWalkLatest(ctx, js, kvs, keys, func(key string, value []byte, revision uint64, deleted bool) {
		got[key] = walked{string(value), revision, deleted}
	}))
	return got
}

func Test_KVWalkLatest_VisitsTheLatestOfEveryKey(t *testing.T) {
	_, js, kvs := walkTestBucket(t, "walk_latest")
	for _, kv := range [][2]string{{"store.a", "1"}, {"store.b", "1"}, {"store.c", "1"}, {"other.x", "1"}, {"store.a", "2"}, {"store.d", ""}} {
		_, err := kvs.Put(kv[0], []byte(kv[1]))
		require.NoError(t, err)
	}
	require.NoError(t, kvs.Delete("store.b"))

	got := walkAll(t, js, kvs, "store.>")
	require.Len(t, got, 4, "one visit per key under the pattern, and none outside it")
	require.Equal(t, "2", got["store.a"].value, "the latest value, not the first")
	require.False(t, got["store.a"].deleted)
	require.True(t, got["store.b"].deleted, "a delete marker is visited and told apart")
	require.Equal(t, "1", got["store.c"].value)
	require.False(t, got["store.d"].deleted, "an empty value is a value")

	entry, err := kvs.Get("store.a")
	require.NoError(t, err)
	require.Equal(t, entry.Revision(), got["store.a"].revision, "the revision is the message's stream sequence")
}

func Test_KVWalkLatest_EmptyBucketReturnsAtOnce(t *testing.T) {
	_, js, kvs := walkTestBucket(t, "walk_empty")
	defer func(d time.Duration) { walkQuietInterval = d }(walkQuietInterval)
	walkQuietInterval = time.Minute // returning by waiting out the quiet interval would fail the test

	started := time.Now()
	require.Empty(t, walkAll(t, js, kvs, "store.>"))
	require.Less(t, time.Since(started), 10*time.Second)
}

// Markers at the end of the stream are removed by their sequence once the walk
// has started and before it reaches them: the walk still ends, having visited
// every key that is left.
func Test_KVWalkLatest_EndsWhenUndeliveredMessagesAreRemoved(t *testing.T) {
	_, js, kvs := walkTestBucket(t, "walk_removed")
	const live, dead = 3000, 300
	for i := 0; i < live+dead; i++ {
		_, err := js.PublishAsync(KVStoredSubject(kvs, fmt.Sprintf("store.k%05d", i)), []byte("v"))
		require.NoError(t, err)
	}
	var markers []nats.PubAckFuture
	for i := live; i < live+dead; i++ {
		f, err := KVDeleteAsync(js, kvs, fmt.Sprintf("store.k%05d", i))
		require.NoError(t, err)
		markers = append(markers, f)
	}
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(30 * time.Second):
		t.Fatal("the bucket was not filled in time")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	visited, removed := 0, 0
	err := KVWalkLatest(ctx, js, kvs, "store.>", func(_ string, _ []byte, _ uint64, deleted bool) {
		if visited++; visited == 1 {
			for _, f := range markers {
				pa := <-f.Ok()
				if js.DeleteMsg(KVStreamName(kvs), pa.Sequence) == nil {
					removed++
				}
			}
		}
	})
	require.NoError(t, err, "the walk must end although %d of the messages it was to deliver were removed", removed)
	require.Positive(t, removed, "sanity: markers were removed during the walk")
	require.GreaterOrEqual(t, visited, live, "every live key is visited")
	require.LessOrEqual(t, visited, live+dead)
	t.Logf("visited %d of %d, removed %d markers during the walk", visited, live+dead, removed)
}

func Test_KVWalkLatest_ReportsAStalledRead(t *testing.T) {
	srv, js, kvs := walkTestBucket(t, "walk_stalled")
	for i := 0; i < 2000; i++ {
		_, err := js.PublishAsync(KVStoredSubject(kvs, fmt.Sprintf("store.k%05d", i)), []byte("v"))
		require.NoError(t, err)
	}
	<-js.PublishAsyncComplete()
	defer func(q, s time.Duration) { walkQuietInterval, walkStallTimeout = q, s }(walkQuietInterval, walkStallTimeout)
	walkQuietInterval, walkStallTimeout = 50*time.Millisecond, time.Second

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	visited := 0
	err := KVWalkLatest(ctx, js, kvs, "store.>", func(string, []byte, uint64, bool) {
		if visited++; visited == 1 {
			srv.Shutdown() // the rest never arrives and nobody can confirm the end
		}
	})
	require.ErrorIs(t, err, ErrWalkStalled)
	require.Less(t, visited, 2000)
}

func Test_KVWalkLatest_StopsWithItsContext(t *testing.T) {
	_, js, kvs := walkTestBucket(t, "walk_cancelled")
	for i := 0; i < 2000; i++ {
		_, err := js.PublishAsync(KVStoredSubject(kvs, fmt.Sprintf("store.k%05d", i)), []byte("v"))
		require.NoError(t, err)
	}
	<-js.PublishAsyncComplete()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err := KVWalkLatest(ctx, js, kvs, "store.>", func(string, []byte, uint64, bool) { cancel() })
	require.ErrorIs(t, err, context.Canceled)
}
