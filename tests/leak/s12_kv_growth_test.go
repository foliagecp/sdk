//go:build leak

package leak

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/suite"
)

// S12 — NATS-side storage under fresh-id churn. Deleting a cache key appends
// a KV DEL marker that the broker keeps for as long as the key stays deleted
// (MaxMsgsPerSubject=1 keeps the last message per subject — the marker — and
// the KV stream has no size/age limits by default). The committer reports
// every marker it leaves and the purger removes it by its sequence, so churn
// must NOT grow the KV stream: the scenario measures the stream per cycle for
// the run report and then asserts that, once the purger has caught up, the
// stream holds no delete markers at all.

type S12Suite struct{ leakSuite }

func TestS12KVGrowthReport(t *testing.T) { suite.Run(t, new(S12Suite)) }

// streamStats sums msgs/bytes over all JetStream streams whose name contains
// substr.
func (s *S12Suite) streamStats(substr string) (msgs, size float64) {
	js, err := s.Runtime().GetNatsConnection().JetStream()
	if err != nil {
		return 0, 0
	}
	for name := range js.StreamNames() {
		if !strings.Contains(name, substr) {
			continue
		}
		if info, err := js.StreamInfo(name); err == nil {
			msgs += float64(info.State.Msgs)
			size += float64(info.State.Bytes)
		}
	}
	return msgs, size
}

func (s *S12Suite) Test_KVStreamGrowth() {
	s.bootCRUD()
	k := scaled(50)

	cycle := func(c int) error {
		for i := 0; i < k; i++ {
			id := fmt.Sprintf("s12v-%d-%d", c, i)
			if err := s.dbc.Graph.VertexCreate(id, leakBody(80)); err != nil {
				return err
			}
		}
		for i := 0; i < k; i++ {
			if err := s.dbc.Graph.VertexDelete(fmt.Sprintf("s12v-%d-%d", c, i)); err != nil {
				return err
			}
		}
		return nil
	}

	collect := func(smp *Sample) {
		s.collectCore(smp)
		kvMsgs, kvBytes := s.streamStats("cache_bucket")
		smp.Custom["kv_stream_msgs"] = kvMsgs
		smp.Custom["kv_stream_bytes"] = kvBytes
		// All JetStream streams together (WAL, trace, KV, ...): the broker-
		// side total the deployment actually pays for.
		allMsgs, allBytes := s.streamStats("")
		smp.Custom["js_total_msgs"] = allMsgs
		smp.Custom["js_total_bytes"] = allBytes
	}
	rep := s.newRunner("s12_kv_growth", cycle, collect).Run(s.T())
	rep.AssertClean(s.T())
	s.assertCoreStable(rep)
	rep.ReportMetric(s.T(), "kv_stream_msgs")
	rep.ReportMetric(s.T(), "kv_stream_bytes")
	rep.ReportMetric(s.T(), "js_total_msgs")
	rep.ReportMetric(s.T(), "js_total_bytes")

	// Every marker the churn left is reported to the purger by the committer
	// as it lands; once the purger has caught up, the bucket must hold no
	// delete markers — the churned keys are absent, not marked.
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) && s.cacheStore().KVTombstonesPendingForTest() > 0 {
		time.Sleep(50 * time.Millisecond)
	}
	pending := s.cacheStore().KVTombstonesPendingForTest()
	markers := s.deleteMarkers()
	msgs, _ := s.streamStats("cache_bucket")
	if pending == 0 && markers == 0 {
		emitCheck("s12_kv_growth", "kv_delete_markers_purged", "PASS",
			"markers=0", "stream_msgs="+f1(msgs))
	} else {
		emitCheck("s12_kv_growth", "kv_delete_markers_purged", "FAIL",
			"markers="+f1(float64(markers)), "pending="+f1(float64(pending)), "stream_msgs="+f1(msgs))
		s.T().Errorf("the churn left %d delete marker(s) in the KV stream (%d still pending in the purger)", markers, pending)
	}
}

// deleteMarkers counts the keys of the cache bucket whose last message is a
// delete or purge marker.
func (s *S12Suite) deleteMarkers() int {
	js, err := s.Runtime().GetNatsConnection().JetStream()
	s.Require().NoError(err)
	// Domain.start names the bucket "<domain>_<cache id>_cache_bucket"; the
	// harness calls its cache "test_cache".
	kv, err := js.KeyValue(fmt.Sprintf("%s_test_cache_cache_bucket", s.Runtime().Domain.Name()))
	s.Require().NoError(err)
	w, err := kv.Watch(s.cacheStore().GetStorePrefix() + ".>")
	s.Require().NoError(err)
	defer func() { _ = w.Stop() }()
	n := 0
	for entry := range w.Updates() {
		if entry == nil {
			break
		}
		if op := entry.Operation(); op == nats.KeyValueDelete || op == nats.KeyValuePurge {
			n++
		}
	}
	return n
}
