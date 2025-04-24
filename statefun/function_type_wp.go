package statefun

import (
	"fmt"
	"sync"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/statefun/logger"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
)

type SFWorkerPoolConfig struct {
	MinWorkers   int
	MaxWorkers   int
	IdleTimeout  time.Duration
	TaskQueueLen int
}

type WPLoadType int

const (
	WPLoadVeryLight WPLoadType = iota
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
		fallthrough
	default:
		config = SFWorkerPoolConfig{
			MinWorkers:   10,
			MaxWorkers:   100,
			IdleTimeout:  5000 * time.Millisecond,
			TaskQueueLen: 100,
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
			logger.Logln(logger.DebugLevel, ">>>>>>>>>>>>>> + WP %s GROW: %d", wp.ft.name, wp.workers)
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

			msg := <-selectedChan
			task := SFWorkerTask{
				Msg: SFWorkerMessage{
					ID:   selectedId,
					Data: msg,
				},
			}

			submit(task)
		}
	}

	for {
		select {
		case <-wp.notifyCh:
			drainFunctionTypeIDChannels()
		case <-wp.stopCh:
			return
		}
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
		wp.wg.Add(-1)
		wp.mu.Unlock()
		logger.Logln(logger.DebugLevel, ">>>>>>>>>>>>>> - WP %s SHRINK: %d", wp.ft.name, wp.workers)
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

				ft.handleMsgForID(id, task.Msg.Data, typenameIDContextProcessor)
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
