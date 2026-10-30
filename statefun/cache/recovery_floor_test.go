package cache

import (
	"context"
	"testing"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/statefun/system"
	"github.com/stretchr/testify/require"
)

func Test_RecoveryFloor_RejectsOldOperationsIncludingMissingKeys(t *testing.T) {
	for _, mode := range []string{"tree", "records", "zstd", "zstd-dict"} {
		t.Run(mode, func(t *testing.T) {
			restore := SetCacheModeForTest(mode)
			defer restore()
			js, kv := newKVForTest(t, "floor_"+mode)
			_, err := kv.Put(KVStorePrefix+".dom/live", []byte(`{"value":1}`))
			require.NoError(t, err)
			before := system.GetCurrentTimeNs()
			cs, cancel := loadStore(t, js, kv, "floor_"+mode)
			defer cancel()
			body := easyjson.NewJSONObjectWithKeyValue("value", easyjson.NewJSON(2))
			require.False(t, cs.SetValueJSON("dom/deleted", &body, true, before))
			require.False(t, cs.SetValue("dom/missing.out.to.edge", []byte("relation.dom/live"), true, before))
			require.False(t, cs.SetValueIfDoesNotExist("dom/missing-lock", []byte("old"), true, before))
			require.False(t, cs.DeleteValue("dom/live", true, before))
			require.True(t, cs.ExistsJson("dom/live"))
			require.False(t, cs.Exists("dom/deleted"))
			require.False(t, cs.Exists("dom/missing.out.to.edge"))
			require.False(t, cs.HasPendingWrites(), "rejected operations must not enter WAL")
			now := system.GetCurrentTimeNs()
			require.True(t, cs.SetValueJSON("dom/new", &body, false, now))
			require.True(t, cs.SetValueIfDoesNotExist("dom/new-lock", []byte("new"), false, now))
			require.True(t, cs.DeleteValue("dom/live", false, now))
		})
	}
}

func Test_RecoveryFloor_AdvancesAfterEachCompletedReload(t *testing.T) {
	js, kv := newKVForTest(t, "floor_reload")
	cs, cancel := loadStore(t, js, kv, "floor_reload")
	defer cancel()
	oldGeneration := system.GetCurrentTimeNs()
	require.True(t, cs.SetValue("dom/old", []byte("before"), false, oldGeneration))
	require.NoError(t, cs.RehydrateFromKV(context.Background()))
	require.False(t, cs.SetValue("dom/old", []byte("late"), false, oldGeneration))
	require.False(t, cs.Exists("dom/old"))
	require.True(t, cs.SetValue("dom/old", []byte("new"), false, system.GetCurrentTimeNs()))
}

func Test_ConditionalCreate_RejectsOlderWriteOverDeletion(t *testing.T) {
	forEachCacheMode(t, func(t *testing.T, cs *Store) {
		key := "dom/deleted"
		require.True(t, cs.SetValue(key, []byte("first"), false, 100))
		require.True(t, cs.DeleteValue(key, false, 300))
		require.False(t, cs.SetValueIfDoesNotExist(key, []byte("stale"), false, 200))
		require.False(t, cs.Exists(key))
		require.True(t, cs.SetValueIfDoesNotExist(key, []byte("new"), false, 400))
	})
}

func Test_RecoveryFloor_TrustedSnapshotSurvivesClockRollback(t *testing.T) {
	for _, mode := range []string{"tree", "records", "zstd", "zstd-dict"} {
		t.Run(mode, func(t *testing.T) {
			restore := SetCacheModeForTest(mode)
			defer restore()
			js, kv := newKVForTest(t, "clock_"+mode)
			_, err := kv.Put(KVStorePrefix+".dom/live", []byte(`{"value":"keep"}`))
			require.NoError(t, err)
			cs, cancel := loadStore(t, js, kv, "clock_"+mode)
			defer cancel()
			// Model a previous recovery boundary ahead of the current clock.
			boundary := system.GetCurrentTimeNs() + int64(time.Hour)
			cs.recoveryFloor.Store(boundary)
			require.NoError(t, cs.RehydrateFromKV(context.Background()))
			body, err := cs.GetValueJSON("dom/live")
			require.NoError(t, err, "the recovered snapshot must not silently lose its values")
			require.Equal(t, "keep", body.GetByPath("value").AsStringDefault(""))
			require.False(t, cs.AcceptsOperationTime(system.GetCurrentTimeNs()), "reloading must not weaken the old-operation fence")
		})
	}
}
