package cache

import (
	"math"
	"sync/atomic"
	"testing"
	"time"

	"github.com/foliagecp/sdk/statefun/system"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func gaugeValue(t *testing.T, families []*dto.MetricFamily, name, idLabel string) (float64, string) {
	t.Helper()
	for _, mf := range families {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			for _, l := range m.GetLabel() {
				if l.GetName() == "id" && l.GetValue() == idLabel {
					return m.GetGauge().GetValue(), mf.GetHelp()
				}
			}
		}
		t.Fatalf("metric %q has no id=%q series", name, idLabel)
	}
	t.Fatalf("metric %q was never registered", name)
	return 0, ""
}

func newMetricsTestStore(id string) *Store {
	if system.GlobalPrometrics == nil {
		// Empty addr: collectors only, no HTTP listener.
		system.GlobalPrometrics = system.NewPrometrics("", "")
	}
	return &Store{cacheConfig: NewCacheConfig(id)}
}

func Test_PublishWALGauges_ExportsTheBarrierState(t *testing.T) {
	const id = "walmetrics_populated"
	cs := newMetricsTestStore(id)

	now := system.GetCurrentTimeNs()
	oldestPending := now - int64(7*time.Second)
	oldestActive := now - int64(3*time.Second)

	cs.pendingTxs.Store(oldestPending, &pendingTx{})
	cs.pendingTxs.Store(now-int64(time.Second), &pendingTx{})

	// Two operations share one opTime; the gauge must sum refcounts.
	shared := new(atomic.Int32)
	shared.Add(2)
	cs.activeOps.Store(oldestActive, shared)
	single := new(atomic.Int32)
	single.Add(1)
	cs.activeOps.Store(now-int64(time.Second), single)

	atomic.StoreInt64(&cs.totalWALPublishes, 41)
	atomic.StoreInt64(&cs.totalWALPublishErrors, 2)

	cs.publishWALGauges(now)

	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	for _, name := range []string{
		"cache_pending_transactions",
		"cache_active_operations",
		"cache_oldest_pending_age_seconds",
		"cache_oldest_active_operation_age_seconds",
		"cache_wal_publish_total",
		"cache_wal_publish_errors_total",
	} {
		_, help := gaugeValue(t, families, name, id)
		require.NotEmptyf(t, help, "metric %q must carry a help string naming its unit", name)
	}

	pending, _ := gaugeValue(t, families, "cache_pending_transactions", id)
	require.Equal(t, float64(2), pending)

	operations, _ := gaugeValue(t, families, "cache_active_operations", id)
	require.Equal(t, float64(3), operations)

	pendingAge, _ := gaugeValue(t, families, "cache_oldest_pending_age_seconds", id)
	require.InDelta(t, 7.0, pendingAge, 0.5)

	activeAge, _ := gaugeValue(t, families, "cache_oldest_active_operation_age_seconds", id)
	require.InDelta(t, 3.0, activeAge, 0.5)

	publishes, _ := gaugeValue(t, families, "cache_wal_publish_total", id)
	require.Equal(t, float64(41), publishes)

	errs, _ := gaugeValue(t, families, "cache_wal_publish_errors_total", id)
	require.Equal(t, float64(2), errs)
}

func Test_PublishWALGauges_EmptyBacklogHasZeroAge(t *testing.T) {
	const id = "walmetrics_empty"
	cs := newMetricsTestStore(id)

	cs.publishWALGauges(system.GetCurrentTimeNs())

	families, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)

	pendingAge, _ := gaugeValue(t, families, "cache_oldest_pending_age_seconds", id)
	require.Equal(t, float64(0), pendingAge)

	activeAge, _ := gaugeValue(t, families, "cache_oldest_active_operation_age_seconds", id)
	require.Equal(t, float64(0), activeAge)

	pending, _ := gaugeValue(t, families, "cache_pending_transactions", id)
	require.Equal(t, float64(0), pending)
}

func Test_AgeNsSince_ClampsToZero(t *testing.T) {
	now := int64(1_000_000_000)

	require.Equal(t, int64(0), ageNsSince(now, 0), "an absent timestamp has no age")
	require.Equal(t, int64(0), ageNsSince(now, -5), "a negative timestamp has no age")
	require.Equal(t, int64(0), ageNsSince(now, now+math.MaxInt32),
		"a caller-supplied op_time may sit in the future; a negative age would render as a nonsense gauge")
	require.Equal(t, int64(400), ageNsSince(now, now-400))
}
