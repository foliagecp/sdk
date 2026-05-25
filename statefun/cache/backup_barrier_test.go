package cache

// Bug hunt: backup write-barrier semantics.
//
// While a backup barrier is active, writes whose opTime is NEWER than the
// barrier timestamp must be blocked (so the backup captures a consistent point),
// while writes at/before the barrier are allowed; clearing the barrier resumes
// all writes. Exercised on the in-memory barrier state (no KV round-trip:
// updateBackupBarrier stamps lastChecked, so getBackupBarrierState does not
// refresh from KV within the check interval).

import (
	"testing"

	"github.com/foliagecp/sdk/statefun/system"
	"github.com/stretchr/testify/require"
)

func Test_BackupBarrier_BlocksNewerWritesUntilCleared(t *testing.T) {
	cs := &Store{}

	barrierTs := system.GetCurrentTimeNs()
	cs.updateBackupBarrier(BackupBarrierStatusLocked, barrierTs)

	require.True(t, cs.IsBackupBarrierActive(), "barrier must report active when locked")

	// A write newer than the barrier point is blocked.
	require.Errorf(t, cs.checkBackupBarrierInfoBeforeWrite(barrierTs+1_000_000),
		"a write newer than the barrier timestamp must be blocked")
	// A write at/before the barrier point is allowed.
	require.NoErrorf(t, cs.checkBackupBarrierInfoBeforeWrite(barrierTs-1_000_000),
		"a write older than the barrier timestamp must be allowed")

	// Clearing the barrier resumes all writes.
	cs.updateBackupBarrier(BackupBarrierStatusUnlocked, 0)
	require.False(t, cs.IsBackupBarrierActive(), "barrier must report inactive when unlocked")
	require.NoErrorf(t, cs.checkBackupBarrierInfoBeforeWrite(barrierTs+1_000_000),
		"writes must resume after the barrier is cleared")
}

func Test_BackupBarrier_LockingAlsoBlocks(t *testing.T) {
	cs := &Store{}
	barrierTs := system.GetCurrentTimeNs()
	// "Locking" (in-progress) must already gate newer writes, not only "Locked".
	cs.updateBackupBarrier(BackupBarrierStatusLocking, barrierTs)
	require.True(t, cs.IsBackupBarrierActive())
	require.Error(t, cs.checkBackupBarrierInfoBeforeWrite(barrierTs+1_000_000))
}
