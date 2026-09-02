package cache

// The store must not hang when it cannot load itself.
//
// A failed initial load used to leave the constructor's init channel open, so
// NewCacheStore blocked forever: the process stayed alive, served nothing, and
// no orchestrator restarted it because nothing had exited. The load now retries
// within a budget and then reports the failure to the caller.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func Test_LoadWithRetries_SucceedsOnALaterAttempt(t *testing.T) {
	attempts := 0
	err := loadWithRetries(context.Background(), 30*time.Second, func() error {
		attempts++
		if attempts < 3 {
			return errors.New("kv not ready")
		}
		return nil
	})
	require.NoError(t, err)
	require.Equal(t, 3, attempts, "it must keep trying until the load goes through")
}

func Test_LoadWithRetries_GivesUpWithinTheBudget(t *testing.T) {
	boom := errors.New("kv unavailable")
	attempts := 0
	started := time.Now()

	err := loadWithRetries(context.Background(), 1500*time.Millisecond, func() error {
		attempts++
		return boom
	})

	require.Error(t, err)
	require.ErrorIs(t, err, boom, "the reason must survive: the caller reports why, not just that it timed out")
	require.GreaterOrEqual(t, attempts, 2, "the budget must cover more than a single attempt")
	require.Less(t, time.Since(started), 10*time.Second, "and the wait must stay inside the budget")
}

func Test_LoadWithRetries_AbortsWhenTheContextIsDone(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	started := time.Now()
	err := loadWithRetries(ctx, time.Hour, func() error { return errors.New("kv unavailable") })

	require.Error(t, err)
	require.Less(t, time.Since(started), 5*time.Second,
		"a cancelled context ends the wait at once — the budget is not a sentence")
}
