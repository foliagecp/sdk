package crud_test

// A WAL transaction the committer cannot apply must not be applied by half,
// and must not be dropped while the runtime reports KV as caught up.
//
// applyTransactionOps publishes a transaction's ops one by one and returns at
// the first failure; the WAL message is Nak'ed and redelivered. What was
// published before the failure stays in KV. The consumer delivers a message at
// most commitMaxDeliver (5) times; after that the transaction is never applied
// again, nothing waits for it, and WaitForKVCaughtUp reports success.
//
// In production the failure is "nats: stalled with too many outstanding async
// published messages": the runtime's JetStream context allows 256 pending
// async publishes (runtime.go), the committer pipelines up to 1024 per chunk
// (wal.go), so a transaction of a few hundred ops stalls whenever the broker
// acks slower than the stall wait. Here the failure is forced by limiting the
// message size of the bucket's stream, which fails the same publish the same
// way, deterministically.

import (
	"context"
	"strings"
	"time"

	"github.com/foliagecp/sdk/statefun"
	"github.com/foliagecp/sdk/statefun/system"
	"github.com/nats-io/nats.go"
)

func (s *TrashCanTestSuite) Test_WALApply_FailingTransactionIsNotHalfAppliedOrDropped() {
	s.boot()
	dm := s.Runtime().Domain
	c := dm.Cache()

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
	consumer := statefun.CommitterDurableName + "-" + bucket

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	s.Require().NoError(dm.WaitForKVCaughtUp(ctx, 60*time.Second))

	// The broker starts refusing large publishes.
	info, err := js.StreamInfo("KV_" + bucket)
	s.Require().NoError(err)
	cfg := info.Config
	original := cfg.MaxMsgSize
	cfg.MaxMsgSize = 256
	_, err = js.UpdateStream(&cfg)
	s.Require().NoError(err)
	restored := false
	restore := func() {
		if restored {
			return
		}
		restored = true
		cfg.MaxMsgSize = original
		_, err := js.UpdateStream(&cfg)
		s.Require().NoError(err)
	}
	defer restore()

	// One transaction: a small key the broker accepts, then a large one it
	// refuses.
	small, large := s.SetThisDomainPreffix("wa-small"), s.SetThisDomainPreffix("wa-large")
	opTime := system.GetCurrentTimeNs()
	s.Require().True(c.SetValue(small, []byte("small"), true, opTime))
	s.Require().True(c.SetValue(large, []byte(strings.Repeat("x", 1024)), true, opTime))

	// Every delivery fails; a Nak'ed one comes back at once, so the consumer
	// is through its deliveries well within this.
	time.Sleep(5 * time.Second)
	ci, err := js.ConsumerInfo(statefun.WALCommitsStreamName, consumer)
	s.Require().NoError(err)
	si, _ := js.StreamInfo(statefun.WALCommitsStreamName)

	// The broker is healthy again.
	restore()
	time.Sleep(7 * time.Second) // longer than the committer's AckWait: a redelivery would have come

	_, smallErr := kv.Get("store." + small)
	_, largeErr := kv.Get("store." + large)
	caughtUp := dm.WaitForKVCaughtUp(ctx, 5*time.Second)
	s.T().Logf("consumer delivered=%d ackfloor=%d pending=%d ackPending=%d redelivered=%d; WAL stream holds %d msgs; small in KV=%v large in KV=%v; WaitForKVCaughtUp=%v",
		ci.Delivered.Stream, ci.AckFloor.Stream, ci.NumPending, ci.NumAckPending, ci.NumRedelivered, si.State.Msgs,
		smallErr == nil, largeErr == nil, caughtUp)

	s.Equal(smallErr == nil, largeErr == nil, "the transaction is applied to KV by half")
	s.NoError(largeErr, "the transaction was dropped: a write the cache applied never reached KV")
	if largeErr != nil {
		s.Error(caughtUp, "WaitForKVCaughtUp reports KV caught up while a transaction was never applied")
	}
}
