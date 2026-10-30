package crud_test

// What a runtime acknowledged and put into the WAL must be what the next
// runtime's cache holds, however the previous one died.
//
// A transaction leaves the cache for the WAL stream and is applied to KV by
// the committer later. A runtime killed in between leaves transactions in the
// stream that KV has not seen yet. The next start loads the cache from KV and
// only then starts the committer, which applies those transactions to KV —
// after the cache was built, and the cache never reads KV again. The writes
// are in KV and missing from the graph: deleted objects are back, written ones
// are not there, and KV and the cache disagree until something overwrites the
// keys.

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/statefun"
	"github.com/foliagecp/sdk/statefun/cache"
	"github.com/foliagecp/sdk/statefun/system"
	"github.com/nats-io/nats.go"
)

func (s *TrashCanTestSuite) Test_Restart_PendingWALTransactionsReachTheCache() {
	s.boot()
	s.NoError(s.cmdb.TypeCreate("rw_t"))
	s.NoError(s.cmdb.ObjectCreate("rw_gone", "rw_t", easyjson.NewJSONObjectWithKeyValue("v", easyjson.NewJSON(1))))
	goneID := s.SetThisDomainPreffix("rw_gone")
	newID := s.SetThisDomainPreffix("rw_new")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	s.Require().NoError(s.Runtime().Domain.WaitForKVCaughtUp(ctx, 60*time.Second))

	// The runtime dies.
	s.Runtime().Shutdown(true)
	time.Sleep(2 * time.Second) // its connection closes on the way out of Start

	nc, err := nats.Connect(s.NatsURL())
	s.Require().NoError(err)
	defer nc.Close()
	js, err := nc.JetStream()
	s.Require().NoError(err)

	bucket := ""
	for name := range js.KeyValueStoreNames() {
		if strings.Contains(name, "test_cache") {
			bucket = name
		}
	}
	s.Require().NotEmpty(bucket)
	kv, err := js.KeyValue(bucket)
	s.Require().NoError(err)

	// The store form of a key, as the WAL carries it: whatever the bucket
	// holds the body under.
	storeKeyOf := func(id string) string { return "store." + id }
	_, err = kv.Get(storeKeyOf(goneID))
	s.Require().NoError(err, "the body is in KV under %s", storeKeyOf(goneID))

	// What it had published to the WAL and the committer had not applied yet:
	// one body deleted, one written.
	newBody := []byte(`{"v":2}`)
	ops, err := json.Marshal([]cache.WALOp{
		{OpType: string(cache.OpTypeDelete), Key: storeKeyOf(goneID)},
		{OpType: string(cache.OpTypePUT), Key: storeKeyOf(newID), Value: newBody},
	})
	s.Require().NoError(err)
	msg := nats.NewMsg(statefun.WALCommitsSubject[:len(statefun.WALCommitsSubject)-1] + bucket)
	msg.Header.Set("tx_id", strconv.FormatInt(system.GetCurrentTimeNs(), 10))
	msg.Header.Set("ops_count", "2")
	msg.Data = ops
	_, err = js.PublishMsg(msg)
	s.Require().NoError(err)
	time.Sleep(time.Second)
	info, err := js.ConsumerInfo(statefun.WALCommitsStreamName, statefun.CommitterDurableName+"-"+bucket)
	s.Require().NoError(err)
	s.Require().Equal(uint64(1), info.NumPending+uint64(info.NumAckPending),
		"the transaction must still be waiting for a committer when the next runtime starts")

	// The next runtime on the same broker.
	cfg := statefun.NewRuntimeConfigSimple(s.NatsURL(), "test_app").
		SetActivePassiveMode(false).
		SetNatsAPITimeoutSec(30)
	rt2, err := statefun.NewRuntime(*cfg)
	s.Require().NoError(err)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = rt2.Start(context.Background(), cache.NewCacheConfig("test_cache"))
	}()
	defer func() {
		rt2.Shutdown(true)
		select {
		case <-done:
		case <-time.After(30 * time.Second):
		}
	}()
	s.Require().Eventually(rt2.IsReady, 30*time.Second, 10*time.Millisecond)

	_, goneErr := kv.Get(storeKeyOf(goneID))
	newEntry, newErr := kv.Get(storeKeyOf(newID))
	goneInKV := goneErr == nil
	newInKV := newErr == nil && string(newEntry.Value()) == string(newBody)
	c := rt2.Domain.Cache()
	goneInCache, newInCache := c.ExistsJson(goneID), c.ExistsJson(newID)
	s.T().Logf("after restart: deleted body in KV=%v in cache=%v; written body in KV=%v in cache=%v",
		goneInKV, goneInCache, newInKV, newInCache)

	s.False(goneInKV, "the committer applies the pending delete to KV")
	s.True(newInKV, "the committer applies the pending write to KV")
	s.False(goneInCache, "the cache holds a body the WAL had deleted before the restart")
	s.True(newInCache, "the cache misses a body the WAL had written before the restart")
}
