package cache

import (
	"errors"
	"fmt"
	"github.com/foliagecp/easyjson"
	customNatsKv "github.com/foliagecp/sdk/embedded/nats/kv"
	"github.com/foliagecp/sdk/statefun/system"
	"github.com/nats-io/nats.go"
	"sync/atomic"
	"time"
)

const (
	BackupBarrierLockKey = "__backup_lock_"

	BackupBarrierStatusUnlocked = 0
	BackupBarrierStatusLocking  = 1
	BackupBarrierStatusLocked   = 2

	BackupBarrierCheckInterval = 5 * time.Second
)

func (cs *Store) getBackupBarrierInfo() (*easyjson.JSON, error) {
	entry, err := customNatsKv.KVGet(cs.js, cs.kv, BackupBarrierLockKey)
	if err != nil {
		if errors.Is(err, nats.ErrKeyNotFound) {
			barrier := easyjson.NewJSONObject()
			barrier.SetByPath("status", easyjson.NewJSON(BackupBarrierStatusUnlocked))
			return barrier.GetPtr(), nil
		}
		return nil, err
	}

	if barrier, ok := easyjson.JSONFromBytes(entry.Value()); ok {
		return &barrier, nil
	}

	return nil, fmt.Errorf("failed to parse backup barrier JSON")
}

func (cs *Store) checkBackupBarrierInfoBeforeWrite(opTime int64) error {
	barrierTime, status := cs.getBackupBarrierState()
	if status == BackupBarrierStatusLocked || status == BackupBarrierStatusLocking {
		if opTime > barrierTime {
			return fmt.Errorf("operation blocked by barrier: %d > %d", opTime, barrierTime)
		}
	}

	return nil
}

func (cs *Store) getBackupBarrierState() (timestamp int64, status int32) {
	if cs.shouldRefreshBackupBarrier() {
		cs.refreshBackupBarrierFromKV()
	}

	return atomic.LoadInt64(&cs.backupBarrierTimestamp), atomic.LoadInt32(&cs.backupBarrierStatus)
}

func (cs *Store) shouldRefreshBackupBarrier() bool {
	currentTime := system.GetCurrentTimeNs()
	lastChecked := atomic.LoadInt64(&cs.backupBarrierLastChecked)

	return (currentTime - lastChecked) > BackupBarrierCheckInterval.Nanoseconds()
}

func (cs *Store) refreshBackupBarrierFromKV() {
	barrier, err := cs.getBackupBarrierInfo()
	if err != nil {
		return
	}

	barrierTs := int64(barrier.GetByPath("barrier_timestamp").AsNumericDefault(0))
	status := int32(barrier.GetByPath("status").AsNumericDefault(BackupBarrierStatusUnlocked))

	cs.updateBackupBarrier(status, barrierTs)
}

func (cs *Store) clearBackupBarrier() error {
	barrier := easyjson.NewJSONObject()
	barrier.SetByPath("status", easyjson.NewJSON(BackupBarrierStatusUnlocked))
	barrier.SetByPath("barrier_timestamp", easyjson.NewJSON(0))

	system.MsgOnErrorReturn(customNatsKv.KVPut(cs.js, cs.kv, BackupBarrierLockKey, barrier.ToBytes()))

	cs.updateBackupBarrier(BackupBarrierStatusUnlocked, 0)

	return nil
}

func (cs *Store) updateBackupBarrier(status int32, timestamp int64) {
	atomic.StoreInt64(&cs.backupBarrierTimestamp, timestamp)
	atomic.StoreInt32(&cs.backupBarrierStatus, status)
	atomic.StoreInt64(&cs.backupBarrierLastChecked, system.GetCurrentTimeNs())
}

func (cs *Store) markCacheReadyForBackup() {
	backupBarrierTimestamp := atomic.LoadInt64(&cs.backupBarrierTimestamp)
	cs.updateBackupBarrier(BackupBarrierStatusLocked, backupBarrierTimestamp)

	barrier := easyjson.NewJSONObject()
	barrier.SetByPath("status", easyjson.NewJSON(BackupBarrierStatusLocked))
	barrier.SetByPath("barrier_timestamp", easyjson.NewJSON(cs.backupBarrierTimestamp))
	system.MsgOnErrorReturn(customNatsKv.KVPut(cs.js, cs.kv, BackupBarrierLockKey, barrier.ToBytes()))
}

func (cs *Store) updateBackupBarrierWithTimestamp(timestamp int64) error {
	barrier := easyjson.NewJSONObject()
	barrier.SetByPath("status", easyjson.NewJSON(BackupBarrierStatusLocking))
	barrier.SetByPath("barrier_timestamp", easyjson.NewJSON(timestamp))
	barrier.SetByPath("set_by", easyjson.NewJSON("kvLazyWriter"))

	_, err := customNatsKv.KVPut(cs.js, cs.kv, BackupBarrierLockKey, barrier.ToBytes())
	if err != nil {
		return err
	}

	cs.updateBackupBarrier(BackupBarrierStatusLocking, timestamp)

	return nil
}
