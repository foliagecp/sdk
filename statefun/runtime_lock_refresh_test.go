package statefun

// The runtime lock decides who is active, and losing it drops every responder
// on this instance. Two properties keep that decision honest.
//
// It must SURVIVE a slow KV. The refresh is two JetStream calls whose timeout
// is sized for data under saturation — seconds. One slow call used to consume
// the whole tolerance window, and the instance stepped down while it still held
// the lock. Attempts are short now and repeated until the next tick is due.
//
// It must step down BEFORE the lock becomes stealable. KeyMutexLock lets
// another instance seize a lock once lockTime+TTL < now; tolerating right up to
// TTL meant the decision was taken exactly when someone else could already be
// active. Three quarters of the lifetime leaves the last quarter as margin.

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// The window a runtime tolerates without a confirmed refresh must end before
// another instance may lawfully take the lock.
func Test_LockToleranceEndsBeforeTheLockIsStealable(t *testing.T) {
	for _, ttlSec := range []int{4, 10, 30, 60} {
		ttl := time.Duration(ttlSec) * time.Second
		tolerance := ttl * 3 / 4
		tick := ttl / 4

		require.Lessf(t, tolerance, ttl,
			"TTL=%s: an instance must be passive before the lock can be seized", ttl)
		require.GreaterOrEqualf(t, ttl-tolerance, tick,
			"TTL=%s: the margin must leave room for at least one more tick", ttl)
		require.GreaterOrEqualf(t, int(tolerance/tick), 3,
			"TTL=%s: the tolerance window must span several ticks, or one slow attempt still decides", ttl)
	}
}

// A refresh attempt is bounded, and the bound is short relative to the tick —
// otherwise "retry within the tick" cannot happen at all.
func Test_LockRefreshAttemptTimeoutLeavesRoomForRetries(t *testing.T) {
	for _, ttlSec := range []int{4, 10, 30, 60} {
		tick := time.Duration(ttlSec) * time.Second / 4
		attempt := lockRefreshAttemptTimeoutFor(tick)

		require.Greaterf(t, attempt, time.Duration(0), "TTL=%ds: an attempt must be bounded, not unbounded", ttlSec)
		require.LessOrEqualf(t, attempt*2, tick,
			"TTL=%ds: one attempt (%s) must leave room for another inside a tick (%s)", ttlSec, attempt, tick)
	}
}
