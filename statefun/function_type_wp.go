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

	notifyCh chan struct{}
	stopCh   chan struct{}
	stopped  bool

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
		notifyCh:    make(chan struct{}, 1),
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
	drainFunctionTypeIDChannels := func() {
		for {
			var maxLen int
			var selectedChan chan FunctionTypeMsg
			var selectedId string

			wp.ft.idHandlersChannel.Range(func(key, value any) bool {
				id := key.(string)
				ch := value.(chan FunctionTypeMsg)
				if l := len(ch); l > maxLen {
					maxLen = l
					selectedChan = ch
					selectedId = id
				}
				return true
			})

			if maxLen == 0 || selectedChan == nil {
				return
			}
			if len(wp.taskQueue) >= cap(wp.taskQueue) {
				return
			}

			msg := <-selectedChan
			task := SFWorkerTask{
				Msg: SFWorkerMessage{
					ID:   selectedId,
					Data: msg,
				},
			}

			system.MsgOnErrorReturn(submit(task))
		}
	}

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if time.Since(wp.prometricsUpdatedTime) > 1*time.Second {
			wp.prometricsUpdatedTime = time.Now()
			wp.prometricsMeasures()
		}
		select {
		case <-wp.notifyCh:
			drainFunctionTypeIDChannels()
		case <-ticker.C:
			drainFunctionTypeIDChannels()
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

func (wp *SFWorkerPool) Notify() {
	select {
	case wp.notifyCh <- struct{}{}:
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
			wp.Notify()

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
