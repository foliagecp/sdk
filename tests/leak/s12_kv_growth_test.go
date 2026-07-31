//go:build leak

package leak

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/suite"
)

// S12 — NATS-side storage growth REPORT (no assertion). Deleting a cache key
// appends a KV DEL tombstone that is retained forever (MaxMsgsPerSubject=1
// keeps the last message per subject — the tombstone — and the KV stream has
// no size/age limits by default). Fresh-id churn therefore grows the KV
// stream monotonically even though the SDK heap and cache tree stay flat.
// This scenario measures that by-design growth per cycle so the run report
// states it explicitly; the SDK-side invariants are still asserted.

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
}
