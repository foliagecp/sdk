package statefun_test

// WaitForKVCaughtUp is the durability barrier of the whole system: when it
// returns, everything written through the cache is supposed to be in KV.
// Backup tooling drains on it, a runtime handing its bucket to another one
// drains on it, and anything that rebuilds the cache from KV — a restore, a
// restart, an HA promotion — trusts it before it throws the in-memory graph
// away.
//
// The barrier watches two things: the cache has nothing left to publish to the
// WAL, and the committer has no WAL transaction left to apply. The second half
// is the delicate one. The committer is a push consumer, and the KV writes a
// transaction carries happen AFTER it has been handed the message and BEFORE
// it acknowledges it — so an in-flight transaction is no longer counted among
// the undelivered ones. A barrier that only counts undelivered messages
// returns while the last transaction is still being written, and whoever reads
// KV next is short exactly that transaction: the tail of the graph, silently,
// with no error anywhere.
//
// One transaction per round is the shape that makes this visible — the same
// shape a real write has, where one operation's keys travel together.

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/foliagecp/sdk/statefun/system"
	"github.com/foliagecp/sdk/statefun/test"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/suite"
)

type WALKVBarrierTestSuite struct {
	test.StatefunTestSuite
}

func TestWALKVBarrierTestSuite(t *testing.T) {
	suite.Run(t, new(WALKVBarrierTestSuite))
}

// kvBucket opens the bucket the domain keeps the graph in, from the outside —
// the same way the backup tooling reads it.
func (s *WALKVBarrierTestSuite) kvBucket() nats.KeyValue {
	s.T().Helper()
	nc, err := nats.Connect(s.NatsURL())
	s.Require().NoError(err)
	s.T().Cleanup(nc.Close)

	js, err := nc.JetStream()
	s.Require().NoError(err)

	// Domain.start names it "<domain>_<cache id>_cache_bucket"; the harness
	// calls its cache "test_cache".
	bucket := fmt.Sprintf("%s_test_cache_cache_bucket", s.Runtime().Domain.Name())
	kv, err := js.KeyValue(bucket)
	s.Require().NoErrorf(err, "the runtime's cache bucket %q is not where Domain.start puts it", bucket)
	return kv
}

func (s *WALKVBarrierTestSuite) Test_BarrierHoldsUntilTheLastTransactionIsInKV() {
	s.Require().NoError(s.StartRuntime())

	kv := s.kvBucket()
	cs := s.Runtime().Domain.Cache()
	prefix := cs.GetStorePrefix() + "."

	// Enough keys that the transaction takes real time to apply — the ops go
	// to KV in chunks of 1024, each chunk waited on.
	const (
		rounds   = 3
		perRound = 2000
	)

	for r := 0; r < rounds; r++ {
		// One operation, one write time, one WAL transaction — and the mark
		// every write path holds while it fills it, so the publisher does not
		// take the transaction half-written.
		opTime := system.GetCurrentTimeNs()
		cs.MarkOperationActive(opTime)
		keys := make([]string, 0, perRound)
		for i := 0; i < perRound; i++ {
			key := fmt.Sprintf("%s/wal-barrier-r%d.k%05d", s.Runtime().Domain.Name(), r, i)
			// updateInKV: this is a write that must travel to KV, which is
			// the whole point of the barrier.
			s.Require().Truef(cs.SetValue(key, []byte(fmt.Sprintf("v%05d", i)), true, opTime),
				"round %d: the cache refused key %s", r, key)
			keys = append(keys, key)
		}
		cs.MarkOperationDone(opTime)

		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		err := s.Runtime().Domain.WaitForKVCaughtUp(ctx, 60*time.Second)
		cancel()
		s.Require().NoErrorf(err, "round %d: the barrier never reported KV caught up", r)

		// The newest key first: transactions are applied one at a time, so the
		// one the committer is still holding is the newest, and this is where
		// the tail goes missing.
		newest := keys[len(keys)-1]
		_, gerr := kv.Get(prefix + newest)
		s.Require().NoErrorf(gerr,
			"round %d: the barrier reported KV caught up while the newest key written (%s) was not in KV",
			r, newest)

		present := make(map[string]struct{}, perRound)
		all, err := kv.Keys()
		s.Require().NoErrorf(err, "round %d: cannot list the bucket", r)
		for _, k := range all {
			present[k] = struct{}{}
		}
		missing := make([]string, 0)
		for _, k := range keys {
			if _, ok := present[prefix+k]; !ok {
				missing = append(missing, k)
			}
		}
		s.Require().Emptyf(missing,
			"round %d: the barrier reported KV caught up with %d of %d keys of the last transaction still missing from KV (first missing: %s)",
			r, len(missing), perRound, firstOr(missing, "-"))
	}
}

func firstOr(s []string, fallback string) string {
	if len(s) == 0 {
		return fallback
	}
	return s[0]
}
