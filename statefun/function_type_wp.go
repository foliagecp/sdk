package statefun

import (
	"fmt"
	"sync"
	"time"

	"github.com/foliagecp/easyjson"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
)

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
	maxWorkers  int
	idleTimeout time.Duration

	mu          sync.Mutex
	workers     int
	idleWorkers int

	stopCh  chan struct{}
	stopped bool

	wg sync.WaitGroup
}

func NewSFWorkerPool(maxWorkers int, idleTimeout time.Duration, taskQueueLen int) *SFWorkerPool {
	return &SFWorkerPool{
		taskQueue:   make(chan SFWorkerTask, taskQueueLen),
		maxWorkers:  maxWorkers,
		idleTimeout: idleTimeout,
		stopCh:      make(chan struct{}),
	}
}

func (wp *SFWorkerPool) Submit(task SFWorkerTask) {
	wp.mu.Lock()
	if wp.stopped {
		wp.mu.Unlock()
		return
	}

	hasIdle := wp.idleWorkers > 0
	canGrow := wp.workers < wp.maxWorkers
	if !hasIdle && canGrow {
		wp.workers++
		wp.wg.Add(1)
		wp.mu.Unlock()
		fmt.Println(">>>>>>>>>>>>>> WP GROW:", wp.workers)
		go wp.worker()
	} else {
		wp.mu.Unlock()
	}

	select {
	case wp.taskQueue <- task:
	case <-wp.stopCh:
	}
}

func (wp *SFWorkerPool) worker() {
	defer func() {
		wp.mu.Lock()
		wp.workers--
		wp.mu.Unlock()
	}()

	timer := time.NewTimer(wp.idleTimeout)
	defer timer.Stop()

	for {
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

				ft.idKeyMutex.Lock(id) // Will be unlocked after function execution

				var typenameIDContextProcessor *sfPlugins.StatefunContextProcessor
				if v, ok := ft.contextProcessors[id]; ok {
					typenameIDContextProcessor = v
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
					ft.contextProcessors[id] = &v
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
			wp.mu.Unlock()
			fmt.Println(">>>>>>>>>>>>>> ---- WP SHRINK:", wp.workers)
			return
		case <-wp.stopCh:
			return
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
