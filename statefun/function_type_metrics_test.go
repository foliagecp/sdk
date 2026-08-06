package statefun

// Tests for the cached per-function-type prometheus series: the hot paths
// (token acquire/release, execution time, delivery histogram, id-channels
// gauge) must publish into exactly the same registered time series as the
// old per-call Ensure*VecSimple resolution — without taking the global
// Prometrics.metricsMutex on every message.

import (
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/foliagecp/sdk/statefun/system"
)

func Test_FTMetrics_CachedSeriesPublishRealValues(t *testing.T) {
	if system.GlobalPrometrics == nil {
		system.GlobalPrometrics = system.NewPrometrics("", "")
	}

	ft := &FunctionType{
		name:   "test.metrics.cached",
		tokens: *system.NewTokenBucket(4),
	}

	// The cache must build once and then be stable.
	m1 := ft.getMetrics()
	require.NotNil(t, m1)
	require.Same(t, m1, ft.getMetrics(), "metrics must be resolved once and cached")

	// id-channels gauge reflects the incremental counter.
	ft.activeIDChannels.Add(3)
	ft.prometricsMeasureIdChannels()
	gv, err := system.GlobalPrometrics.EnsureGaugeVecSimple("ft_active_id_channels", "", []string{"typename"})
	require.NoError(t, err)
	require.Equal(t, 3.0, testutil.ToFloat64(gv.With(prometheus.Labels{"typename": ft.name})))

	// tokens gauge reflects the bucket load through acquire/release.
	require.True(t, ft.TokenTryAcquire())
	tv, err := system.GlobalPrometrics.EnsureGaugeVecSimple("ft_tokens_percentage", "", []string{"typename"})
	require.NoError(t, err)
	require.Equal(t, 25.0, testutil.ToFloat64(tv.With(prometheus.Labels{"typename": ft.name})), "1 of 4 tokens taken")
	ft.TokenRelease()
	require.Equal(t, 0.0, testutil.ToFloat64(tv.With(prometheus.Labels{"typename": ft.name})))

	// The delivery histogram has a pre-resolved observer for every delivery
	// type and observing must not panic.
	for _, dt := range []MeasureMsgDeliverType{NatsPub, NatsPubRedelivery, NatsReq, GolangReq} {
		require.Contains(t, m1.msgDeliver, dt)
		ft.prometricsMeasureMsgDeliver(dt)
	}

	// exec-time gauge series exists and is writable through the cache.
	m1.execTime.Set(42)
	ev, err := system.GlobalPrometrics.EnsureGaugeVecSimple("ft_execution_time", "", []string{"typename"})
	require.NoError(t, err)
	require.Equal(t, 42.0, testutil.ToFloat64(ev.With(prometheus.Labels{"typename": ft.name})))
}
