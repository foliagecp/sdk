package statefun

import (
	"fmt"
	"sync"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/statefun/logger"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
)

type SFWorkerPoolConfig struct {
	minWorkers   int
	maxWorkers   int
	idleTimeout  time.Duration
	taskQueueLen int
}

func NewSFWorkerPoolConfigFromEnvOrDefault() SFWorkerPoolConfig {
	return SFWorkerPoolConfig{
		minWorkers:   system.GetEnvMustProceed[int]("WP_WORKERS_MIN", 20),
		maxWorkers:   system.GetEnvMustProceed[int]("WP_WORKERS_MAX", 10000),
		idleTimeout:  time.Duration(system.GetEnvMustProceed[int]("WP_WORKERS_IDLE_TIMEOUT_MS", 5000)) * time.Millisecond,
		taskQueueLen: system.GetEnvMustProceed[int]("WP_TASK_QUEUE_LEN", 10000),
	}
}

type SFWorkerMessage struct {
	ID   string
	Data FunctionTypeMsg
}

type SFWorkerTask struct {
	Ft  *FunctionType
	Msg SFWorkerMessage
}

// SFWorkerPool - controls the statefun pool
type SFWorkerPool struct {
	taskQueue   chan SFWorkerTask
	minWorkers  int
	maxWorkers  int
	idleTimeout time.Duration

	mu          sync.Mutex
	workers     int
	idleWorkers int

	stopCh  chan struct{}
	stopped bool

	wg sync.WaitGroup
}

func NewSFWorkerPool(conf SFWorkerPoolConfig) *SFWorkerPool {
	return &SFWorkerPool{
		taskQueue:   make(chan SFWorkerTask, conf.taskQueueLen),
		minWorkers:  conf.minWorkers,
		maxWorkers:  conf.maxWorkers,
		idleTimeout: conf.idleTimeout,
		stopCh:      make(chan struct{}),
	}
}

func (wp *SFWorkerPool) Submit(task SFWorkerTask) error {
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
		logger.Logln(logger.DebugLevel, ">>>>>>>>>>>>>> + WP GROW: %d", wp.workers)
		go wp.worker()
	} else {
		wp.mu.Unlock()
	}

	select {
	case wp.taskQueue <- task:
		return nil
	case <-wp.stopCh:
		return fmt.Errorf("worker pool is going to stop")
	default:
		return fmt.Errorf("worker pool is full")
	}
}

func (wp *SFWorkerPool) worker() {
	defer func() {
		wp.mu.Lock()
		wp.workers--
		wp.wg.Add(-1)
		wp.mu.Unlock()
		logger.Logln(logger.DebugLevel, ">>>>>>>>>>>>>> - WP SHRINK: %d", wp.workers)
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
				ft := task.Ft
				id := task.Msg.ID

				ft.idKeyMutex.Lock(id)

				var typenameIDContextProcessor *sfPlugins.StatefunContextProcessor

				if v, ok := ft.contextProcessors.Load(id); ok {
					typenameIDContextProcessor = v.(*sfPlugins.StatefunContextProcessor)
				} else {
					v := sfPlugins.StatefunContextProcessor{
						GetFunctionContext:        func() *easyjson.JSON { return ft.getContext(ft.name + "." + id) },
						SetFunctionContext:        func(context *easyjson.JSON) { ft.setContext(ft.name+"."+id, context) },
						SetContextExpirationAfter: func(after time.Duration) { ft.setContextExpirationAfter(ft.name+"."+id, after) },
						GetObjectContext:          func() *easyjson.JSON { return ft.getContext(id) },
						SetObjectContext:          func(context *easyjson.JSON) { ft.setContext(id, context) },
						Domain:                    ft.runtime.Domain,
						Self:                      sfPlugins.StatefunAddress{Typename: ft.name, ID: id},
						Signal: func(signalProvider sfPlugins.SignalProvider, targetTypename string, targetID string, j *easyjson.JSON, o *easyjson.JSON) error {
							return ft.runtime.signal(signalProvider, ft.name, id, targetTypename, targetID, j, o)
						},
						Request: func(requestProvider sfPlugins.RequestProvider, targetTypename string, targetID string, j *easyjson.JSON, o *easyjson.JSON, timeout ...time.Duration) (*easyjson.JSON, error) {
							return ft.runtime.request(requestProvider, ft.name, id, targetTypename, targetID, j, o)
						},
						Egress: func(egressProvider sfPlugins.EgressProvider, j *easyjson.JSON, customId ...string) error {
							egressId := id
							if len(customId) > 0 {
								egressId = customId[0]
							}
							return ft.runtime.egress(egressProvider, ft.name, egressId, j)
						},
						// To be assigned later:
						// Call: ...
						// Payload: ...
						// Options: ... // Otions from initial typename declaration will be merged and overwritten by the incoming one in message
						// Caller: ...
					}
					ft.contextProcessors.Store(id, &v)
					typenameIDContextProcessor = &v
				}

				task.Ft.handleMsgForID(id, task.Msg.Data, typenameIDContextProcessor)
				ft.idKeyMutex.Unlock(id)
			}

			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(wp.idleTimeout)

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

func (wp *SFWorkerPool) Stop() {
	wp.mu.Lock()
	if wp.stopped {
		wp.mu.Unlock()
		return
	}
	wp.stopped = true
	close(wp.stopCh)
	wp.mu.Unlock()

	wp.wg.Wait()
}
