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
	BackupLockKey = "__backup_lock_"

	BarrierStatusUnlocked = 0
	BarrierStatusLocked   = 1

	BarrierCheckInterval = 5 * time.Second
)

func (cs *Store) getBackupBarrierInfo() (*easyjson.JSON, error) {
	entry, err := customNatsKv.KVGet(cs.js, cs.kv, BackupLockKey)
	if err != nil {
		if errors.Is(err, nats.ErrKeyNotFound) {
			barrier := easyjson.NewJSONObject()
			barrier.SetByPath("status", easyjson.NewJSON(BarrierStatusUnlocked))
			return barrier.GetPtr(), nil
		}
		return nil, err
	}

	if barrier, ok := easyjson.JSONFromBytes(entry.Value()); ok {
		return &barrier, nil
	}

	return nil, fmt.Errorf("failed to parse backup barrier JSON")
}

func (cs *Store) checkBarrierInfoBeforeWrite(opTime int64) error {
	if cs.shouldRefreshBarrier() {
		cs.refreshBarrierFromKV()
	}

	barrierTime, status := cs.getBarrierState()
	if status == BarrierStatusLocked {
		if opTime > barrierTime {
			return fmt.Errorf("operation blocked by barrier: %d > %d", opTime, barrierTime)
		}
	}

	return nil
}

func (cs *Store) getBarrierState() (timestamp int64, status int32) {
	return atomic.LoadInt64(&cs.barrierTimestamp), atomic.LoadInt32(&cs.barrierStatus)
}

func (cs *Store) shouldRefreshBarrier() bool {
	currentTime := system.GetCurrentTimeNs()
	lastChecked := atomic.LoadInt64(&cs.barrierLastChecked)

	return (currentTime - lastChecked) > BarrierCheckInterval.Nanoseconds()
}

func (cs *Store) refreshBarrierFromKV() {
	barrier, err := cs.getBackupBarrierInfo()
	if err != nil {
		return
	}

	barrierTs := int64(barrier.GetByPath("barrier_timestamp").AsNumericDefault(0))
	status := int32(barrier.GetByPath("status").AsNumericDefault(BarrierStatusUnlocked))

	atomic.StoreInt64(&cs.barrierTimestamp, barrierTs)
	atomic.StoreInt32(&cs.barrierStatus, status)
	atomic.StoreInt64(&cs.barrierLastChecked, system.GetCurrentTimeNs())
}
