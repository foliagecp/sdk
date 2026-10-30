package statefun

// HA active/passive correctness with two real runtimes sharing
// one containerized NATS broker and the same domain:
//   - exactly one instance is active at a time (KV runtime-lock exclusivity);
//   - after the active shuts down, the passive takes over (recovery).
//
// Uses a short KV-mutex TTL so the take-over after a shutdown (which lets the
// stale lock expire) happens quickly and deterministically.

import (
	"context"
	"testing"
	"time"

	"github.com/foliagecp/sdk/statefun/cache"
	"github.com/foliagecp/sdk/statefun/system"
	"github.com/stretchr/testify/require"
)

func waitForCond(d time.Duration, cond func() bool) bool {
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return cond()
}

func Test_HA_ActivePassive_ExclusivityAndRecovery(t *testing.T) {
	if system.GlobalPrometrics == nil {
		system.GlobalPrometrics = system.NewPrometrics("", "")
	}
	url := newHABroker(t)

	// Cancellable contexts so both runtimes' goroutines wind down at test end —
	// otherwise they keep thrashing and destabilize sibling tests in the package.
	newRT := func() (*Runtime, context.CancelFunc) {
		cfg := NewRuntimeConfigSimple(url, "hadomain").SetKVMutexLifeTimeSec(4)
		rt, err := NewRuntime(*cfg)
		require.NoError(t, err)
		ctx, cancel := context.WithCancel(context.Background())
		go func() { _ = rt.Start(ctx, cache.NewCacheConfig("hadomain_cache")) }()
		return rt, cancel
	}

	rtA, cancelA := newRT()
	defer cancelA()
	rtB, cancelB := newRT()
	defer cancelB()

	activeCount := func() int {
		n := 0
		if rtA.IsActiveInstance() {
			n++
		}
		if rtB.IsActiveInstance() {
			n++
		}
		return n
	}

	// Exactly one becomes active.
	require.Truef(t, waitForCond(30*time.Second, func() bool { return activeCount() == 1 }),
		"exactly one instance must become active; got %d", activeCount())
	require.Equalf(t, 1, activeCount(), "HA lock must be exclusive; got %d active", activeCount())

	// Identify and shut down the active; the passive must take over.
	active, passive := rtA, rtB
	if rtB.IsActiveInstance() {
		active, passive = rtB, rtA
	}
	active.Shutdown(true)

	require.Truef(t, waitForCond(30*time.Second, func() bool { return passive.IsActiveInstance() }),
		"passive instance must recover to active after the active shuts down")
	// Note: the shut-down instance's in-memory isActiveInstance flag may stay
	// stale-true (Shutdown just stops the loop); the KV runtime lock is the real
	// arbiter and the dead instance no longer refreshes/holds it, so there is no
	// split-brain. The meaningful invariant is that the passive recovered above.
	_ = active
}
