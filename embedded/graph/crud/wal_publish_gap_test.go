package crud_test

// What the cache writes must reach KV, however the write lands relative to
// the WAL publish: a restart rebuilds the cache from KV alone.
//
// The lazy writer takes a pending transaction's ops (Collect), publishes them
// and only then forgets the transaction (pendingTxs.Delete). An op appended to
// that same transaction in between is in neither: not in the published
// message, and gone with the entry. The cache keeps the write, KV never sees
// it, and the next load from KV disagrees with what the graph answered.
//
// Ops share a transaction when they share an op_time. One operation writes
// all its keys under one op_time and holds its activeOps mark while it does,
// so the writer waits for it — but the mark only says "do not start", it
// cannot recall a publish already under way. Any write that arrives with that
// op_time after the writer has looked (a sub-request still running after its
// parent gave up waiting, a second operation stamped with the same time)
// falls into the gap. The publish is a broker round trip, so under load the
// gap is as wide as the broker is slow.

import (
	"strings"
	"sync"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/statefun"
	"github.com/foliagecp/sdk/statefun/cache"
	"github.com/foliagecp/sdk/statefun/system"
)

// gatedWAL holds the publish of the first transaction with a key containing
// trip until
// release is closed — a broker that answers slowly.
type gatedWAL struct {
	inner    *statefun.Domain
	trip     string
	once     sync.Once
	entered  chan struct{}
	release  chan struct{}
	opsTaken []cache.WALOp
}

func (g *gatedWAL) PublishTransaction(txID string, ops []cache.WALOp) error {
	for _, op := range ops {
		if strings.Contains(op.Key, g.trip) {
			hold := false
			g.once.Do(func() { hold = true })
			if hold {
				g.opsTaken = append([]cache.WALOp(nil), ops...)
				close(g.entered)
				<-g.release
			}
			break
		}
	}
	return g.inner.PublishTransaction(txID, ops)
}

func (g *gatedWAL) GenerateTransactionID() string { return g.inner.GenerateTransactionID() }

// walGapWrite writes the first key under opTime, and the second one under the
// same opTime while the first one's publish is on the wire. It returns
// whether each key is there after the cache is reloaded from KV.
type walGapMode int

const (
	gapSameTimeUnmarked walGapMode = iota // a straggler of the operation that has returned
	gapSameTimeMarked                     // another operation stamped with the same time
	gapOtherTime                          // control: a different op_time is a different transaction
	gapSameTimeAfter                      // control: the same op_time once the publish is over
)

func (s *TrashCanTestSuite) walGapWrite(prefix string, mode walGapMode) (firstBack, secondBack bool) {
	s.boot()
	dm := s.Runtime().Domain
	c := dm.Cache()
	first, second := s.SetThisDomainPreffix(prefix+"-first"), s.SetThisDomainPreffix(prefix+"-second")

	gate := &gatedWAL{inner: dm, entered: make(chan struct{}), release: make(chan struct{})}
	gate.trip = prefix + "-first" // the WAL carries the store form of the key
	c.SetTransactionGenerator(gate)
	s.T().Cleanup(func() { c.SetTransactionGenerator(dm) })

	opTime := system.GetCurrentTimeNs()
	body := easyjson.NewJSONObjectWithKeyValue("v", easyjson.NewJSON(1))
	s.Require().True(c.SetValueJSON(first, &body, true, opTime))

	select {
	case <-gate.entered:
	case <-time.After(10 * time.Second):
		s.T().Fatalf("the publish carrying %s never came", first)
	}

	switch mode {
	case gapSameTimeUnmarked:
		s.Require().True(c.SetValueJSON(second, &body, true, opTime))
		close(gate.release)
	case gapSameTimeMarked:
		c.MarkOperationActive(opTime)
		s.Require().True(c.SetValueJSON(second, &body, true, opTime))
		c.MarkOperationDone(opTime)
		close(gate.release)
	case gapOtherTime:
		s.Require().True(c.SetValueJSON(second, &body, true, opTime+1))
		close(gate.release)
	case gapSameTimeAfter:
		close(gate.release)
		s.Require().Eventually(func() bool { return !c.HasPendingWrites() }, 10*time.Second, time.Millisecond)
		s.Require().True(c.SetValueJSON(second, &body, true, opTime))
	}

	s.True(c.ExistsJson(first) && c.ExistsJson(second), "the cache holds both writes")
	s.reloadFromKV()
	firstBack, secondBack = c.ExistsJson(first), c.ExistsJson(second)
	carried := false
	for _, op := range gate.opsTaken {
		carried = carried || strings.Contains(op.Key, prefix+"-second")
	}
	s.T().Logf("mode=%d: the held publish carried the second key: %v; after reload from KV: first=%v second=%v",
		mode, carried, firstBack, secondBack)
	return firstBack, secondBack
}

// A straggler with no mark of its own — a sub-request still writing after the
// operation that stamped its op_time has returned.
func (s *TrashCanTestSuite) Test_WALGap_UnmarkedWriteDuringPublishReachesKV() {
	first, second := s.walGapWrite("wg1", gapSameTimeUnmarked)
	s.True(first)
	s.True(second, "a write the cache applied never reached KV")
}

// A second operation with the same op_time that does hold its mark: the mark
// comes after the writer has looked, and does not help.
func (s *TrashCanTestSuite) Test_WALGap_MarkedWriteDuringPublishReachesKV() {
	first, second := s.walGapWrite("wg2", gapSameTimeMarked)
	s.True(first)
	s.True(second, "a write the cache applied never reached KV")
}

// Controls: the same writes outside the window reach KV.
func (s *TrashCanTestSuite) Test_WALGap_ControlOtherOpTimeDuringPublish() {
	first, second := s.walGapWrite("wg3", gapOtherTime)
	s.True(first)
	s.True(second)
}

func (s *TrashCanTestSuite) Test_WALGap_ControlSameOpTimeAfterPublish() {
	first, second := s.walGapWrite("wg4", gapSameTimeAfter)
	s.True(first)
	s.True(second)
}
