package statefun

import (
	"fmt"
	"sync"
	"time"

	"github.com/foliagecp/sdk/statefun/logger"
	"github.com/foliagecp/sdk/statefun/system"
	"github.com/prometheus/client_golang/prometheus"
)

type SFWorkerPoolConfig struct {
	MinWorkers   int
	MaxWorkers   int
	IdleTimeout  time.Duration
	TaskQueueLen int
}

type WPLoadType int

const (
	WPLoadDefault WPLoadType = iota
	WPLoadVeryLight
	WPLoadLight
	WPLoadNormal
	WPLoadHigh
	WPLoadVeryHigh
)

func NewSFWorkerPoolConfig(loadType WPLoadType) (config SFWorkerPoolConfig) {
	switch loadType {
	case WPLoadVeryLight:
		config = SFWorkerPoolConfig{
			MinWorkers:   0,
			MaxWorkers:   5,
			IdleTimeout:  5000 * time.Millisecond,
			TaskQueueLen: 5,
		}
	case WPLoadLight:
		config = SFWorkerPoolConfig{
			MinWorkers:   2,
			MaxWorkers:   25,
			IdleTimeout:  5000 * time.Millisecond,
			TaskQueueLen: 25,
		}
	case WPLoadHigh:
		config = SFWorkerPoolConfig{
			MinWorkers:   50,
			MaxWorkers:   500,
			IdleTimeout:  5000 * time.Millisecond,
			TaskQueueLen: 500,
		}
	case WPLoadVeryHigh:
		config = SFWorkerPoolConfig{
			MinWorkers:   250,
			MaxWorkers:   2500,
			IdleTimeout:  5000 * time.Millisecond,
			TaskQueueLen: 2500,
		}
	case WPLoadNormal:
		config = SFWorkerPoolConfig{
			MinWorkers:   10,
			MaxWorkers:   100,
			IdleTimeout:  5000 * time.Millisecond,
			TaskQueueLen: 100,
		}
	default:
		config = SFWorkerPoolConfig{
			MinWorkers:   system.GetEnvMustProceed[int]("DEFAULT_FT_WP_WORKERS_MIN", 10),
			MaxWorkers:   system.GetEnvMustProceed[int]("DEFAULT_FT_WP_WORKERS_MAX", 100),
			IdleTimeout:  time.Duration(system.GetEnvMustProceed[int]("DEFAULT_FT_WP_WORKERS_IDLE_TIMEOUT_MS", 5000)) * time.Millisecond,
			TaskQueueLen: system.GetEnvMustProceed[int]("DEFAULT_FT_WP_TASK_QUEUE_LEN", 100),
		}
	}
	return
}

type SFWorkerMessage struct {
	ID   string
	Data FunctionTypeMsg
}

type SFWorkerTask struct {
	Msg SFWorkerMessage
}

// SFWorkerPool - controls the statefun pool
type SFWorkerPool struct {
	ft *FunctionType

	taskQueue   chan SFWorkerTask
	minWorkers  int
	maxWorkers  int
	idleTimeout time.Duration

	mu          sync.Mutex
	workers     int
	idleWorkers int

	// pendingIds carries IDs whose per-id channel has at least one buffered
	// message waiting for the manager. Producers (FunctionType.handleMsgForID)
	// push the ID after enqueueing into the per-id channel; the manager pops
	// from pendingIds and drains exactly ONE message from that id's channel.
	// This replaces the previous design where the manager iterated the entire
	// idHandlersChannel sync.Map every 10 ms looking for the busiest id —
	// which became O(N_unique_ids) per tick and showed up at ~24% of CPU in
	// profiles once the seed grew past a few thousand objects.
	//
	// Capacity = taskQueue capacity: enough headroom that bursts of "new
	// message arrived" notifications do not block the producer. Duplicate
	// pushes (same id pushed twice while the first is unconsumed) are
	// harmless — the manager pops twice, the second drainOne finds an empty
	// channel and returns.
	pendingIds chan string

	stopCh  chan struct{}
	stopped bool

	prometricsUpdatedTime time.Time

	wg sync.WaitGroup
}

func NewSFWorkerPool(ft *FunctionType, conf SFWorkerPoolConfig) *SFWorkerPool {
	wp := &SFWorkerPool{
		ft:          ft,
		taskQueue:   make(chan SFWorkerTask, conf.TaskQueueLen),
		minWorkers:  conf.MinWorkers,
		maxWorkers:  conf.MaxWorkers,
		idleTimeout: conf.IdleTimeout,
		pendingIds:  make(chan string, conf.TaskQueueLen),
		stopCh:      make(chan struct{}),
	}
	go wp.manager()
	return wp
}

// IsStopped reports whether Stop() has been called. A stopped pool no longer
// accepts tasks and must be replaced (see FunctionType.ensureWorkerPool)
// before the function type can process messages again.
func (wp *SFWorkerPool) IsStopped() bool {
	wp.mu.Lock()
	defer wp.mu.Unlock()
	return wp.stopped
}

func (wp *SFWorkerPool) manager() {
	submit := func(task SFWorkerTask) error {
		wp.mu.Lock()
		if wp.stopped {
			wp.mu.Unlock()
			return fmt.Errorf("worker pool is alredy stopped")
		}

		hasIdle := wp.idleWorkers > 0
		canGrow := wp.workers < wp.maxWorkers
		if !hasIdle && canGrow {
			wp.workers++
			wp.wg.Add(1)
			wp.mu.Unlock()
			logger.Logln(logger.DebugLevel, "WP %s GROW: %d", wp.ft.name, wp.workers)
			go wp.worker()
		} else {
			wp.mu.Unlock()
		}

		select {
		case wp.taskQueue <- task:
			return nil
		case <-wp.stopCh:
			return fmt.Errorf("worker pool is going to stop")
		}
	}

	// drainOne consumes exactly one message from the named id's channel and
	// submits it as a task. A "stale" pendingIds entry (channel already
	// drained by a concurrent worker, or id GC'd) simply finds nothing and
	// returns — harmless. We do NOT loop here over the same id: per-id FIFO
	// is preserved by workerTaskExecutor's idKeyMutex, and processing one
	// id-msg per pop keeps the manager interleaving fairly across ids.
	drainOne := func(id string) {
		chRaw, ok := wp.ft.idHandlersChannel.Load(id)
		if !ok {
			return
		}
		ch := chRaw.(chan FunctionTypeMsg)
		select {
		case msg := <-ch:
			task := SFWorkerTask{
				Msg: SFWorkerMessage{ID: id, Data: msg},
			}
			system.MsgOnErrorReturn(submit(task))
			// Self-renotify if more messages remain in this id's channel:
			// covers the case where a producer's NotifyId was dropped because
			// pendingIds was momentarily full.
			if len(ch) > 0 {
				select {
				case wp.pendingIds <- id:
				default:
				}
			}
		default:
			// already drained by another path; nothing to do
		}
	}

	// 1 Hz fallback: emit metrics AND scan for any id-channels left with
	// buffered messages whose NotifyId was dropped (both producer-side and
	// drain-side). Range is O(N_unique_ids) but at 1 Hz instead of 100 Hz —
	// total CPU cost negligible compared to the old 10 ms ticker that did the
	// same scan and dominated profiles.
	fallbackTicker := time.NewTicker(1 * time.Second)
	defer fallbackTicker.Stop()
	for {
		select {
		case id := <-wp.pendingIds:
			drainOne(id)
		case <-fallbackTicker.C:
			wp.prometricsMeasures()
			wp.ft.idHandlersChannel.Range(func(key, value any) bool {
				if ch, ok := value.(chan FunctionTypeMsg); ok && len(ch) > 0 {
					select {
					case wp.pendingIds <- key.(string):
					default:
					}
				}
				return true
			})
		case <-wp.stopCh:
			return
		}
	}
}

func (wp *SFWorkerPool) prometricsMeasures() {
	if gaugeVec, err := system.GlobalPrometrics.EnsureGaugeVecSimple("ft_worker_pool_task_queue_load_percentage", "", []string{"typename"}); err == nil {
		gaugeVec.With(prometheus.Labels{"typename": wp.ft.name}).Set(wp.GetWorkerPoolLoadPercentage())
	}
	loadedWorkersPercent, idleWorkersPercent := wp.GetWorkerPercentage()
	if gaugeVec, err := system.GlobalPrometrics.EnsureGaugeVecSimple("ft_worker_pool_loaded_workers_percentage", "", []string{"typename"}); err == nil {
		gaugeVec.With(prometheus.Labels{"typename": wp.ft.name}).Set(loadedWorkersPercent)
	}
	if gaugeVec, err := system.GlobalPrometrics.EnsureGaugeVecSimple("ft_worker_pool_idle_workers_percentage", "", []string{"typename"}); err == nil {
		gaugeVec.With(prometheus.Labels{"typename": wp.ft.name}).Set(idleWorkersPercent)
	}
}

// NotifyId tells the manager that the named id's channel has a fresh message.
// Non-blocking: if pendingIds is full (extreme producer pressure), the
// notification is dropped and the next NotifyId for any id will give the
// manager an opportunity to wake up; the message itself stays in the per-id
// channel and is recoverable via any subsequent notify or via the manager's
// next periodic tick.
func (wp *SFWorkerPool) NotifyId(id string) {
	select {
	case wp.pendingIds <- id:
	default:
	}
}

func (wp *SFWorkerPool) worker() {
	defer func() {
		wp.mu.Lock()
		wp.workers--
		wp.wg.Done()
		wp.mu.Unlock()
		logger.Logln(logger.DebugLevel, "WP %s SHRINK: %d", wp.ft.name, wp.workers)
	}()

	timer := time.NewTimer(wp.idleTimeout)
	defer timer.Stop()

	working := true
	for working {
		wp.mu.Lock()
		wp.idleWorkers++
		wp.mu.Unlock()

		select {
		case task := <-wp.taskQueue:
			wp.mu.Lock()
			wp.idleWorkers--
			wp.mu.Unlock()

			{
				ft := wp.ft
				id := task.Msg.ID

				ft.workerTaskExecutor(id, task.Msg.Data)
			}

			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(wp.idleTimeout)

			wp.ft.TokenRelease()
			// No Notify here: producers push to pendingIds directly, and
			// drainOne self-renotifies if the id's channel still has buffered
			// messages after consuming one. The metricsTicker fallback at 1Hz
			// guarantees recovery even if both producer- and drain-side
			// pendingIds pushes are dropped under extreme overflow.

		case <-timer.C:
			wp.mu.Lock()
			wp.idleWorkers--
			if wp.workers > wp.minWorkers {
				working = false
			} else {
				timer.Reset(wp.idleTimeout)
			}
			wp.mu.Unlock()
		case <-wp.stopCh:
			wp.mu.Lock()
			wp.idleWorkers--
			wp.mu.Unlock()
			working = false
		}
	}
}

// Stop tears the pool down WITHOUT blocking the caller. It marks the pool
// stopped (no new tasks; idle workers and the manager exit at once), then
// observes the in-flight drain in the background. This is critical for the
// lifecycle ticker / becomePassive: a slow or wedged handler must never hold
// up the transition or, worse, recovery (the 116 incident, where an unbounded
// wg.Wait() inside becomePassive killed the ticker forever).
//
// In-flight handlers are NOT interrupted — they run to completion naturally
// (Go cannot kill a goroutine). A passive→active transition meanwhile spins
// up a fresh pool (FunctionType.ensureWorkerPool), so the draining old pool
// never delays serving; a stuck handler is simply abandoned.
func (wp *SFWorkerPool) Stop() {
	wp.mu.Lock()
	if wp.stopped {
		wp.mu.Unlock()
		return
	}
	wp.stopped = true
	close(wp.stopCh)
	wp.mu.Unlock()

	go wp.drain()
}

// drain observes — off the critical path — whether in-flight handlers finish
// within the configured window, logging if any are still running afterwards.
// It never blocks the caller and never kills a worker; abandoned handlers
// finish on their own.
func (wp *SFWorkerPool) drain() {
	done := make(chan struct{})
	go func() {
		wp.wg.Wait()
		close(done)
	}()

	timeout := time.Duration(wp.ft.runtime.config.functionPoolDrainTimeoutSec) * time.Second
	select {
	case <-done:
		// All in-flight handlers finished naturally.
	case <-time.After(timeout):
		wp.mu.Lock()
		stuck := wp.workers
		wp.mu.Unlock()
		logger.Logf(logger.WarnLevel,
			"worker pool %s: %d handler(s) still running after %s drain window; abandoning them",
			wp.ft.name, stuck, timeout)
	}
}

// DropPendingTasks removes tasks that have not started yet
// Running tasks are not interrupted
func (wp *SFWorkerPool) DropPendingTasks() (dropped int) {
	// Drop tasks already moved into worker queue
	drainingSharedQueue := true
	for drainingSharedQueue {
		select {
		case task := <-wp.taskQueue:
			dropped++
			wp.ft.TokenRelease()
			if task.Msg.Data.AckCallback != nil {
				// do not try to redeliver
				task.Msg.Data.AckCallback(true)
			}
		default:
			drainingSharedQueue = false
		}
	}

	// Drop tasks still waiting in per-id channels
	wp.ft.idHandlersChannel.Range(func(_, value any) bool {
		ch := value.(chan FunctionTypeMsg)
		for {
			select {
			case msg := <-ch:
				dropped++
				wp.ft.TokenRelease()
				if msg.AckCallback != nil {
					// do not try to redeliver
					msg.AckCallback(true)
				}
			default:
				return true
			}
		}
	})

	return dropped
}

func (wp *SFWorkerPool) GetWorkerPoolLoadPercentage() float64 {
	return 100.0 * float64(len(wp.taskQueue)) / float64(cap(wp.taskQueue))
}

func (wp *SFWorkerPool) GetWorkerPercentage() (loadedWorkers float64, idleWorkers float64) {
	wp.mu.Lock()
	defer wp.mu.Unlock()

	loadedWorkers = 100.0 * float64(wp.workers) / float64(wp.maxWorkers)
	idleWorkers = 100.0 * float64(wp.idleWorkers) / float64(wp.maxWorkers)

	return
}

func (wp *SFWorkerPool) GetActiveWorkersCount() int {
	wp.mu.Lock()
	defer wp.mu.Unlock()
	return wp.workers - wp.idleWorkers
}
