package statefun_test

// A delete that travels the whole way — cache, WAL, committer, bucket — leaves
// no marker behind in the bucket's stream: the committer reports where the
// marker landed and the purger removes it by that sequence. Deleted keys are
// absent from the stream afterwards, not merely marked; a key deleted and
// written again keeps the new value; and nothing that was not deleted moves.
// The bucket is read from the outside, the way backup tooling reads it.

import (
	"context"
	"fmt"
	"testing"
	"time"

	customNatsKv "github.com/foliagecp/sdk/embedded/nats/kv"
	"github.com/foliagecp/sdk/statefun/system"
	"github.com/foliagecp/sdk/statefun/test"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/suite"
)

type KVTombstonesTestSuite struct {
	test.StatefunTestSuite
}

func TestKVTombstonesTestSuite(t *testing.T) {
	suite.Run(t, new(KVTombstonesTestSuite))
}

// bucketFromOutside opens the domain's cache bucket on a connection of its own.
func (s *KVTombstonesTestSuite) bucketFromOutside() (nats.JetStreamContext, nats.KeyValue) {
	s.T().Helper()
	nc, err := nats.Connect(s.NatsURL())
	s.Require().NoError(err)
	s.T().Cleanup(nc.Close)
	js, err := nc.JetStream()
	s.Require().NoError(err)
	bucket := fmt.Sprintf("%s_test_cache_cache_bucket", s.Runtime().Domain.Name())
	kv, err := js.KeyValue(bucket)
	s.Require().NoErrorf(err, "the runtime's cache bucket %q is not where Domain.start puts it", bucket)
	return js, kv
}

// markersUnder lists the store keys whose last message in the bucket is a
// delete or purge marker.
func (s *KVTombstonesTestSuite) markersUnder(kv nats.KeyValue, prefix string) []string {
	s.T().Helper()
	w, err := kv.Watch(prefix + ">")
	s.Require().NoError(err)
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
	return markers
}

// lastOf is what the stream holds last for a store key: "absent", "marker"
// or the value.
func (s *KVTombstonesTestSuite) lastOf(js nats.JetStreamContext, kv nats.KeyValue, storeKey string) string {
	s.T().Helper()
	m, err := js.GetLastMsg(customNatsKv.KVStreamName(kv), customNatsKv.KVStoredSubject(kv, storeKey))
	if err != nil {
		s.Require().ErrorIs(err, nats.ErrMsgNotFound)
		return "absent"
	}
	if op := m.Header.Get("KV-Operation"); op == "DEL" || op == "PURGE" {
		return "marker"
	}
	return string(m.Data)
}

func (s *KVTombstonesTestSuite) Test_DeletesLeaveNoMarkerInTheBucket() {
	s.Require().NoError(s.StartRuntime())
	js, kv := s.bucketFromOutside()
	cs := s.Runtime().Domain.Cache()
	prefix := cs.GetStorePrefix() + "."
	dom := s.Runtime().Domain.Name()

	const n = 200
	keys := make([]string, 0, n)
	inOne := func(f func(opTime int64)) {
		opTime := system.GetCurrentTimeNs()
		cs.MarkOperationActive(opTime)
		f(opTime)
		cs.MarkOperationDone(opTime)
	}
	inOne(func(opTime int64) {
		for i := 0; i < n; i++ {
			key := fmt.Sprintf("%s/tomb-k%04d", dom, i)
			s.Require().True(cs.SetValue(key, []byte(fmt.Sprintf("v%04d", i)), true, opTime))
			keys = append(keys, key)
		}
	})
	// Half of them deleted, in a transaction of their own.
	inOne(func(opTime int64) {
		for i := 0; i < n; i += 2 {
			s.Require().True(cs.DeleteValue(keys[i], true, opTime), "delete of %s was refused", keys[i])
		}
	})
	// One of the deleted ones written again right after — the purge of its
	// marker must not take the new value with it.
	const reborn = 0
	inOne(func(opTime int64) {
		s.Require().True(cs.SetValue(keys[reborn], []byte("again"), true, opTime))
	})

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	s.Require().NoError(s.Runtime().Domain.WaitForKVCaughtUp(ctx, 60*time.Second))

	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) && cs.KVTombstonesPendingForTest() > 0 {
		time.Sleep(20 * time.Millisecond)
	}
	s.Require().Zero(cs.KVTombstonesPendingForTest(), "the purger did not remove every marker the committer reported")

	s.Empty(s.markersUnder(kv, prefix), "the bucket still holds delete markers")
	for i, key := range keys {
		switch {
		case i == reborn:
			s.Equalf("again", s.lastOf(js, kv, prefix+key), "%s was deleted and written again: the new value must be what the bucket holds", key)
		case i%2 == 0:
			s.Equalf("absent", s.lastOf(js, kv, prefix+key), "%s was deleted: the bucket must hold nothing for it, not a marker", key)
		default:
			s.Equalf(fmt.Sprintf("v%04d", i), s.lastOf(js, kv, prefix+key), "%s was not deleted and must be untouched", key)
		}
	}
}
