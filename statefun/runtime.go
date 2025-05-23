package statefun

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	lg "github.com/foliagecp/sdk/statefun/logger"
	"github.com/foliagecp/sdk/statefun/system"
	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/foliagecp/sdk/statefun/cache"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
)

type OnAfterStartFunction func(ctx context.Context, runtime *Runtime) error

type onAfterStartFunctionWithMode struct {
	f     OnAfterStartFunction
	async bool
}

// Runtime represents the runtime environment for stateful functions.
type Runtime struct {
	config RuntimeConfig
	nc     *nats.Conn
	js     nats.JetStreamContext
	Domain *Domain

	registeredFunctionTypes       map[string]*FunctionType
	onAfterStartFunctionsWithMode []onAfterStartFunctionWithMode

	gt0  int64 // Global time 0 - time of the very first message receiving by any function type
	glce int64 // Global last call ended - time of last call of last function handling id of any function type
	gc   int64 // Global counter - max total id handlers for all function types

	shutdown chan struct{}
	wg       sync.WaitGroup
}

// NewRuntime initializes a new Runtime instance with the given configuration.
func NewRuntime(config RuntimeConfig) (*Runtime, error) {
	r := &Runtime{
		config:                  config,
		registeredFunctionTypes: make(map[string]*FunctionType),
		shutdown:                make(chan struct{}),
	}

	var err error
	r.nc, err = nats.Connect(config.natsURL)
	if err != nil {
		return nil, err
	}

	r.js, err = r.nc.JetStream(nats.PublishAsyncMaxPending(256))
	if err != nil {
		return nil, err
	}

	r.Domain, err = NewDomain(r.nc, r.js, config.desiredHUBDomainName, config.natsReplicasCount)
	if err != nil {
		return nil, err
	}
	r.config.desiredHUBDomainName = r.Domain.hubDomainName

	return r, nil
}

// RegisterOnAfterStartFunction registers a function to be called after the runtime starts.
// The function can be set to run asynchronously.
func (r *Runtime) RegisterOnAfterStartFunction(f OnAfterStartFunction, async bool) {
	if f != nil {
		r.onAfterStartFunctionsWithMode = append(r.onAfterStartFunctionsWithMode, onAfterStartFunctionWithMode{f, async})
	}
}

// Start initializes streams and starts function subscriptions.
// It also handles graceful shutdown via context.Context.
func (r *Runtime) Start(ctx context.Context, cacheConfig *cache.Config) error {
	logger := lg.NewLogger(lg.Options{ReportCaller: true, Level: lg.InfoLevel})

	// Create streams if they do not exist.
	if err := r.createStreams(ctx); err != nil {
		return err
	}

	// Start the domain.
	if err := r.Domain.start(cacheConfig, r.config.handlesDomainRouters); err != nil {
		return err
	}

	if r.config.activePassiveMode {
		revID, err := KeyMutexLock(ctx, r, system.GetHashStr(RuntimeName), true)
		if err != nil {
			if errors.Is(err, ErrMutexLocked) {
				lg.Logf(lg.WarnLevel, "Cant lock. Another runtime is already active")
				r.config.isActiveInstance = false
			} else {
				return err
			}
		} else {
			r.config.activeRevID = revID
			defer KeyMutexUnlock(ctx, r, system.GetHashStr(RuntimeName), revID)
		}
	} else {
		r.config.isActiveInstance = true
	}

	// Handle single-instance functions.
	singleInstanceFunctionRevisions := make(map[string]uint64)
	if err := r.handleSingleInstanceFunctions(ctx, singleInstanceFunctionRevisions); err != nil {
		return err
	}

	// Start function subscriptions.
	if r.config.isActiveInstance {
		if err := r.startFunctionSubscriptions(ctx, singleInstanceFunctionRevisions); err != nil {
			return err
		}
	}

	// Run after-start functions.
	r.runAfterStartFunctions(ctx)

	// Start garbage collector.
	r.wg.Add(1)
	go r.runGarbageCollector(ctx)

	// Wait for shutdown signal.
	<-r.shutdown

	// Perform cleanup.
	logger.Infof(context.TODO(), "Shutting down runtime...")
	r.wg.Wait()
	return nil
}

// Shutdown gracefully stops the runtime.
func (r *Runtime) Shutdown() {
	close(r.shutdown)
}

// createStreams ensures that the necessary NATS streams exist.
func (r *Runtime) createStreams(ctx context.Context) error {
	logger := lg.NewLogger(lg.Options{ReportCaller: true, Level: lg.InfoLevel})
	var existingStreams []string

	streamInfoCh := r.js.StreamsInfo(nats.Context(ctx))
	for info := range streamInfoCh {
		existingStreams = append(existingStreams, info.Config.Name)
	}

	for _, ft := range r.registeredFunctionTypes {
		if ft.config.IsSignalProviderAllowed(sfPlugins.JetstreamGlobalSignal) {
			if !contains(existingStreams, ft.getStreamName()) {
				_, err := r.js.AddStream(&nats.StreamConfig{
					Name:      ft.getStreamName(),
					Subjects:  []string{ft.subject},
					Retention: nats.InterestPolicy,
					Replicas:  r.Domain.natsJsReplicasCount,
				})
				if err != nil {
					logger.Errorf(context.TODO(), "Failed to add stream: %v", err)
					return err
				}
			}
		}
	}
	return nil
}

// handleSingleInstanceFunctions manages single-instance function locks.
func (r *Runtime) handleSingleInstanceFunctions(ctx context.Context, revisions map[string]uint64) error {
	for ftName, ft := range r.registeredFunctionTypes {
		if !ft.config.multipleInstancesAllowed {
			revID, err := KeyMutexLock(ctx, r, system.GetHashStr(ftName), true)
			if err != nil {
				if errors.Is(err, ErrMutexLocked) {
					lg.Logf(lg.WarnLevel, "Function type %s is already running elsewhere; skipping", ft.name)
					revisions[ftName] = 0 // 0 means that the function is already running elsewhere
					continue
				}
				return err
			}
			revisions[ftName] = revID
		}
	}

	// Start lock updater for single-instance functions.
	if len(revisions) > 0 {
		r.wg.Add(1)
		go r.singleInstanceFunctionLocksUpdater(ctx, revisions)
	}

	return nil
}

// startFunctionSubscriptions starts the function subscriptions based on the configuration.
func (r *Runtime) startFunctionSubscriptions(ctx context.Context, revisions map[string]uint64) error {
	for _, ft := range r.registeredFunctionTypes {
		revision, exist := revisions[ft.name]
		if !exist {
			lg.Logf(lg.WarnLevel, "Function type %s is not registered; skipping", ft.name)
			continue
		}
		if !ft.config.multipleInstancesAllowed && revision == 0 {
			lg.Logf(lg.WarnLevel, "Function type %s is already running; skipping", ft.name)
			continue
		}

		if ft.config.IsSignalProviderAllowed(sfPlugins.JetstreamGlobalSignal) {
			if err := AddSignalSourceJetstreamQueuePushConsumer(ft); err != nil {
				return err
			}
		}
		if ft.config.IsRequestProviderAllowed(sfPlugins.NatsCoreGlobalRequest) {
			if err := AddRequestSourceNatsCore(ft); err != nil {
				return err
			}
		}
	}
	return nil
}

// runAfterStartFunctions executes the registered OnAfterStart functions.
func (r *Runtime) runAfterStartFunctions(ctx context.Context) {
	for _, fnWithMode := range r.onAfterStartFunctionsWithMode {
		if fnWithMode.async {
			r.wg.Add(1)
			go func(f OnAfterStartFunction) {
				defer r.wg.Done()
				system.GlobalPrometrics.GetRoutinesCounter().Started("runtime_onAfterStart")
				defer system.GlobalPrometrics.GetRoutinesCounter().Stopped("runtime_onAfterStart")
				if err := f(ctx, r); err != nil {
					lg.Logf(lg.ErrorLevel, "OnAfterStartFunction error: %v", err)
				}
			}(fnWithMode.f)
		} else {
			if err := fnWithMode.f(ctx, r); err != nil {
				lg.Logf(lg.ErrorLevel, "OnAfterStartFunction error: %v", err)
			}
		}
	}
}

// runGarbageCollector periodically cleans up expired function instances.
func (r *Runtime) runGarbageCollector(ctx context.Context) {
	defer r.wg.Done()
	ticker := time.NewTicker(time.Duration(r.config.gcIntervalSec) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-r.shutdown:
			return
		case <-ticker.C:
			r.collectGarbage()
		}
	}
}

// collectGarbage performs the garbage collection.
func (r *Runtime) collectGarbage() {
	var totalGarbageCollected int
	var totalHandlersRunning int

	measureName := "statefun_instances"
	gaugeVec, err := system.GlobalPrometrics.EnsureGaugeVecSimple(measureName, "Stateful function instances", []string{"typename"})
	if err != nil {
		lg.Logf(lg.ErrorLevel, "Error ensuring GaugeVec: %v", err)
	}

	for _, ft := range r.registeredFunctionTypes {
		collected, running := ft.gc(r.config.functionTypeIDLifetimeMs)
		totalGarbageCollected += collected
		totalHandlersRunning += running

		if gaugeVec != nil {
			gaugeVec.With(prometheus.Labels{"typename": ft.name}).Set(float64(running))
		}
	}

	if totalGarbageCollected > 0 && totalHandlersRunning == 0 {
		r.reportPerformanceMetrics()
	}
}

// reportPerformanceMetrics logs performance metrics when all handlers are idle.
func (r *Runtime) reportPerformanceMetrics() {
	glce := atomic.LoadInt64(&r.glce)
	gt0 := atomic.LoadInt64(&r.gt0)
	gc := atomic.LoadInt64(&r.gc)

	dt := glce - gt0

	if gc > 0 && dt > 0 {
		lg.Logf(lg.TraceLevel, "%d runs, total time (ns/ms): %d/%d, function dt (ns/ms): %d/%d -> %dHz",
			gc, dt, dt/1e6, dt/gc, (dt/gc)/1e6, (gc*1e9)/dt)
		atomic.StoreInt64(&r.gc, 0)
	}
}

// singleInstanceFunctionLocksUpdater periodically updates locks for single-instance functions.
func (r *Runtime) singleInstanceFunctionLocksUpdater(ctx context.Context, revisions map[string]uint64) {
	defer r.wg.Done()
	ticker := time.NewTicker(time.Duration(r.config.kvMutexLifeTimeSec) / 2 * time.Second)
	defer ticker.Stop()

	//release all functions
	releaseAllLocks := func(ctx context.Context, runtime *Runtime, revisions map[string]uint64) {
		for ftName, revID := range revisions {
			KeyMutexUnlock(ctx, runtime, system.GetHashStr(ftName), revID)
		}
	}
	defer releaseAllLocks(ctx, r, revisions)

	for {
		select {
		case <-ctx.Done():
			return
		case <-r.shutdown:
			return
		case <-ticker.C:
			if r.config.activePassiveMode {
				if r.config.isActiveInstance {
					newRevID, err := KeyMutexLockUpdate(ctx, r, system.GetHashStr(RuntimeName), r.config.activeRevID)
					if err != nil {
						lg.Logf(lg.ErrorLevel, "KeyMutexLockUpdate failed for %s: %v", RuntimeName, err)
					} else {
						r.config.activeRevID = newRevID
					}
				} else {
					newRevID, err := KeyMutexLock(ctx, r, system.GetHashStr(RuntimeName), true)
					if err != nil {
						if errors.Is(err, ErrMutexLocked) {
							lg.Logf(lg.WarnLevel, "Cant lock. Another runtime is already active")
							continue
						} else {
							lg.Logf(lg.ErrorLevel, "KeyMutexLock failed for %s: %v", RuntimeName, err)
							return
						}
					} else {
						r.config.isActiveInstance = true
						r.config.activeRevID = newRevID
					}
				}
			}

			subscribeRequired := false //if true, need to subscribe on all functions
			for ftName, revID := range revisions {
				if revID != 0 {
					newRevID, err := KeyMutexLockUpdate(ctx, r, system.GetHashStr(ftName), revID)
					if err != nil {
						lg.Logf(lg.ErrorLevel, "KeyMutexLockUpdate failed for %s: %v", ftName, err)
					} else {
						revisions[ftName] = newRevID
					}
				} else {
					newRevID, err := KeyMutexLock(ctx, r, system.GetHashStr(ftName), true)
					if err != nil {
						lg.Logf(lg.TraceLevel, "KeyMutexLock failed for %s: %v", ftName, err) //try to take the lock
					} else {
						subscribeRequired = true
						revisions[ftName] = newRevID
						lg.Logf(lg.DebugLevel, "KeyMutexLock succeeded for %s", ftName)
					}
				}
			}

			if subscribeRequired {
				if err := r.startFunctionSubscriptions(ctx, revisions); err != nil {
					lg.Logf(lg.ErrorLevel, "function subscriptions failed: %v", err)
				}
			}
		}
	}
}

// contains checks if a slice contains a particular string.
func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}
