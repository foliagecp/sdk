package statefun

import (
	"context"
	"crypto/tls"
	"errors"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/foliagecp/sdk/statefun/cache"
	lg "github.com/foliagecp/sdk/statefun/logger"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"

	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
)

type ShutdownPhase = uint32

const (
	ShutdownPhaseNone ShutdownPhase = iota
	ShutdownPhaseOne
	ShutdownPhaseTwo
	ShutdownPhaseThree
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
	canRegisterNewFunctionType    bool
	onAfterStartFunctionsWithMode []onAfterStartFunctionWithMode

	gt0  int64 // Global time 0 - time of the very first message receiving by any function type
	glce int64 // Global last call ended - time of last call of last function handling id of any function type
	gc   int64 // Global counter - max total id handlers for all function types

	isReady                      bool
	shutdown                     chan struct{}
	gs                           *GracefullyShutdown
	functionsStopCh              chan struct{}
	wg                           sync.WaitGroup
	afterStartFunctionsWaitGroup sync.WaitGroup
	afterStartRunning            atomic.Bool
	activeInstanceMu             sync.RWMutex
}

type GracefullyShutdown struct {
	Phase atomic.Uint32

	cancelPhaseOne   context.CancelFunc
	cancelPhaseTwo   context.CancelFunc
	cancelPhaseThree context.CancelFunc

	CtxPhaseOne   context.Context
	CtxPhaseTwo   context.Context
	CtxPhaseThree context.Context
}

// NewRuntime initializes a new Runtime instance with the given configuration.
func NewRuntime(config RuntimeConfig) (*Runtime, error) {
	r := &Runtime{
		config:                     config,
		registeredFunctionTypes:    make(map[string]*FunctionType),
		canRegisterNewFunctionType: true,
		isReady:                    false,
		shutdown:                   make(chan struct{}),
		functionsStopCh:            make(chan struct{}),
	}

	natsOpts := nats.GetDefaultOptions()
	natsOpts.Url = r.config.natsURL
	natsOpts.MaxReconnect = -1 // -1 - infinity attempts
	natsOpts.ReconnectedCB = func(nc *nats.Conn) {
		lg.GetLogger().Warnf(context.TODO(), "NATS reconnected %d times", nc.Statistics.Reconnects)
	}

	if r.config.enableTLS {
		natsOpts.Secure = true
		natsOpts.TLSConfig = &tls.Config{InsecureSkipVerify: true} // for self-assigned certificates
	}

	var err error
	maxAttempts := system.GetEnvMustProceed("RETRIES_NATS_CONNECT", 10)
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		r.nc, err = natsOpts.Connect()
		if err != nil && attempt < maxAttempts {
			lg.GetLogger().Errorf(context.TODO(), "Can't connect to NATS at attempt %d/%d: %v", attempt, maxAttempts, err)
			// Linear backoff: increase delay by 1 second on each attempt
			time.Sleep(time.Duration(1+attempt) * time.Second)
		} else {
			break
		}
	}
	if err != nil {
		return nil, err
	}

	r.js, err = r.nc.JetStream(nats.PublishAsyncMaxPending(256))
	if err != nil {
		return nil, err
	}

	ftStreamConfig := streamConfig{
		replicasCount: config.natsReplicasCount,
		maxMsgs:       config.ftStreamMaxMsgs,
		maxBytes:      config.ftStreamMaxBytes,
		maxAge:        config.ftStreamMaxAge,
	}
	sysStreamConfig := streamConfig{
		replicasCount: config.natsReplicasCount,
		maxMsgs:       config.sysStreamMaxMsgs,
		maxBytes:      config.sysStreamMaxBytes,
		maxAge:        config.sysStreamMaxAge,
	}
	kvStreamConfig := streamConfig{
		replicasCount: config.natsReplicasCount,
		maxMsgs:       config.kvStreamMaxMsgs,
		maxBytes:      config.kvStreamMaxBytes,
		maxAge:        config.kvStreamMaxAge,
	}
	traceStreamConfig := streamConfig{
		replicasCount: config.natsReplicasCount,
		maxMsgs:       config.traceStreamMaxMsgs,
		maxBytes:      config.traceStreamMaxBytes,
		maxAge:        config.traceStreamMaxAge,
	}

	r.Domain, err = NewDomain(r.nc, r.js, config.desiredHUBDomainName, ftStreamConfig, sysStreamConfig, kvStreamConfig, traceStreamConfig)
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
	logger := lg.GetLogger()
	r.gs = NewGracefullyShutdown(context.Background())

	gracefulShutdownFunc := func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
		<-sig
		if !r.config.isActiveInstance {
			logger.Debugf(ctx, "Runtime is not active. Shutting down immediately")
			r.gs.cancelAllContexts()
			r.Shutdown()
			return
		}
		startShutdown := time.Now()
		r.gs.setPhase(ShutdownPhaseOne)
		logger.Debugf(ctx, "Received shutdown signal, shutting down gracefully...")
		logger.Debugf(ctx, "Shutdown phase 1")
		r.gs.cancelPhaseOne()
		timeout := time.NewTimer(10 * time.Second)
		defer timeout.Stop()

		done := make(chan struct{}, 1)
		go func() {
			r.afterStartFunctionsWaitGroup.Wait()
			done <- struct{}{}
		}()

		select {
		case <-timeout.C:
			logger.Debugf(ctx, "AfterStart functions timed out")
		case <-done:
			logger.Debugf(ctx, "AfterStart functions completed")
		}

		r.drainSignalSubscriptions()

		r.gs.setPhase(ShutdownPhaseTwo)

		logger.Debugf(ctx, "Shutdown phase 2")

		<-r.functionsStopCh

		r.gs.cancelPhaseTwo()

		logger.Debugf(ctx, "Shutdown phase 3")

		<-r.Domain.cache.Synced

		if r.config.isActiveInstance {
			logger.Debugf(ctx, "Shutdown - waiting for transaction committer")
			<-r.Domain.shutdown
		}
		r.Shutdown()
		r.gs.cancelPhaseThree()
		logger.Debugf(ctx, "Shutdown took %v s", time.Since(startShutdown))
	}

	go gracefulShutdownFunc()

	// Disable registering new functions after the runtime has started.
	r.canRegisterNewFunctionType = false

	if intervalMins := system.GetEnvMustProceed("HEAP_WATCHER_INTERVAL_MINS", 0); intervalMins > 0 {
		go system.StartHeapWatcher(float32(intervalMins))
	}

	// Create streams if they do not exist.
	if err := r.createStreams(ctx); err != nil {
		return err
	}

	// Start the domain.
	if err := r.Domain.start(r.gs.CtxPhaseTwo, cacheConfig, r.config.handlesDomainRouters); err != nil {
		return err
	}

	if r.config.activePassiveMode {
		revID, err := KeyMutexLock(ctx, r, system.GetHashStr(RuntimeName), true)
		if err != nil {
			if errors.Is(err, ErrMutexLocked) {
				logger.Debugf(ctx, "Cant lock. Another runtime is already active")
				r.config.isActiveInstance = false
			} else {
				return err
			}
		} else {
			r.config.activeRevID = revID
			defer func() {
				system.MsgOnErrorReturn(KeyMutexUnlock(ctx, r, system.GetHashStr(RuntimeName), r.config.activeRevID))
			}()
		}
	} else {
		r.config.isActiveInstance = true
	}

	// Handle single-instance functions.
	singleInstanceFunctionRevisions := make(map[string]uint64)
	if err := r.handleSingleInstanceFunctions(r.gs.CtxPhaseThree, singleInstanceFunctionRevisions); err != nil {
		return err
	}

	// Start function subscriptions.
	if r.config.isActiveInstance {
		if err := r.startFunctionSubscriptions(ctx, singleInstanceFunctionRevisions); err != nil {
			return err
		}
	}

	// Start garbage collector.
	r.wg.Add(1)
	go r.runGarbageCollector(r.gs.CtxPhaseThree)

	// Set Runtime ready
	r.isReady = true

	// Wait for shutdown signal.
	<-r.shutdown

	// Perform cleanup.
	logger.Info(ctx, "Shutting down...")

	// Wait for last goroutines
	waitCh := make(chan struct{})
	go func() {
		r.wg.Wait()
		close(waitCh)
	}()
	timeoutCh := time.After(5 * time.Second)
	select {
	case <-waitCh:
	case <-timeoutCh:
		logger.Info(ctx, "Timed out waiting WG for runtime to finish")
	}
	return nil
}

func (r *Runtime) drainSignalSubscriptions() {
	var wg sync.WaitGroup
	for ftName, ft := range r.registeredFunctionTypes {
		wg.Add(1)
		go func(name string, ft *FunctionType) {
			defer wg.Done()
			ft.stopSignalSubscription()
		}(ftName, ft)
	}
	wg.Wait()
}

func (r *Runtime) stopRequestSubscriptions() {
	for {
		allFunctionsReadyForShutdown := true
		for _, ft := range r.registeredFunctionTypes {
			if (int(ft.lastMsgTimeNs.Load()) + r.config.functionTypeIDLifetimeMs*1000) > int(system.GetCurrentTimeNs()) {
				allFunctionsReadyForShutdown = false
				continue
			}
		}
		if allFunctionsReadyForShutdown {
			break
		}
		time.Sleep(1 * time.Second)
	}

	for _, ft := range r.registeredFunctionTypes {
		ft.stopRequestSubscription()
	}
}

func NewGracefullyShutdown(rootCtx context.Context) *GracefullyShutdown {
	gs := &GracefullyShutdown{}

	gs.setPhase(ShutdownPhaseNone)
	gs.CtxPhaseOne, gs.cancelPhaseOne = context.WithCancel(rootCtx)
	gs.CtxPhaseTwo, gs.cancelPhaseTwo = context.WithCancel(rootCtx)
	gs.CtxPhaseThree, gs.cancelPhaseThree = context.WithCancel(rootCtx)

	return gs
}

func (gs *GracefullyShutdown) setPhase(phase ShutdownPhase) {
	gs.Phase.Store(phase)
}

func (gs *GracefullyShutdown) phase() ShutdownPhase {
	return gs.Phase.Load()
}

// cancelAllContexts force stops passive runtime
func (gs *GracefullyShutdown) cancelAllContexts() {
	gs.cancelPhaseOne()
	gs.cancelPhaseTwo()
	gs.cancelPhaseThree()
}

// Shutdown stops the runtime.
func (r *Runtime) Shutdown() {
	close(r.shutdown)
}

// GetNatsConnection returns nats connection from runtime
func (r *Runtime) GetNatsConnection() *nats.Conn {
	return r.nc
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
					Replicas:  r.Domain.ftSC.replicasCount,
					MaxMsgs:   r.Domain.ftSC.maxMsgs,
					MaxBytes:  r.Domain.ftSC.maxBytes,
					MaxAge:    r.Domain.ftSC.maxAge,
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
		if !ft.config.multipleInstancesAllowed {
			revision, exist := revisions[ft.name]
			if !exist {
				lg.Logf(lg.WarnLevel, "Function type %s is not registered; skipping", ft.name)
				continue
			}
			if revision == 0 {
				lg.Logf(lg.WarnLevel, "Function type %s is already running; skipping", ft.name)
				continue
			}
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

func (r *Runtime) stopFunctionSubscriptions(ctx context.Context) {
	for _, ft := range r.registeredFunctionTypes {
		ft.stopSignalSubscription()
		ft.stopRequestSubscription()
	}
}

// runAfterStartFunctions executes the registered OnAfterStart functions.
func (r *Runtime) runAfterStartFunctions(ctx context.Context) {
	for _, fnWithMode := range r.onAfterStartFunctionsWithMode {
		if fnWithMode.async {
			r.afterStartFunctionsWaitGroup.Add(1)
			go func(f OnAfterStartFunction) {
				defer r.afterStartFunctionsWaitGroup.Done()
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

	isShutdown := r.gs.phase() == ShutdownPhaseTwo
	functionsReadyForStop := isShutdown

	for _, ft := range r.registeredFunctionTypes {
		collected, running := ft.gc(r.config.functionTypeIDLifetimeMs)
		totalGarbageCollected += collected
		totalHandlersRunning += running

		if gaugeVec != nil {
			gaugeVec.With(prometheus.Labels{"typename": ft.name}).Set(float64(running))
		}

		if isShutdown &&
			(running > 0 ||
				ft.sfWorkerPool.GetActiveWorkersCount() > 0 ||
				len(ft.sfWorkerPool.taskQueue) > 0 ||
				(int64(ft.lastMsgTimeNs.Load())+int64(r.config.functionTypeIDLifetimeMs*1000*1000) > system.GetCurrentTimeNs())) {
			lg.GetLogger().Debugf(context.TODO(), "for function %s is running=%d, is active=%d", ft.name, running, ft.sfWorkerPool.GetActiveWorkersCount())
			functionsReadyForStop = false
			ft.lastMsgTimeNs.Store(uint64(system.GetCurrentTimeNs()))
		}
	}

	if functionsReadyForStop {
		lg.GetLogger().Infof(context.TODO(), "all functions are ready for stop")
		for _, ft := range r.registeredFunctionTypes {
			ft.stopRequestSubscription()
		}
		r.gs.setPhase(ShutdownPhaseThree)
		r.functionsStopCh <- struct{}{}
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
		if runtime.config.isActiveInstance {
			for ftName, revID := range revisions {
				system.MsgOnErrorReturn(KeyMutexUnlock(ctx, runtime, system.GetHashStr(ftName), revID))
			}
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
			subscribeRequired := false //if true, need to subscribe on all functions

			if r.config.activePassiveMode {
				if r.config.isActiveInstance {
					newRevID, err := KeyMutexLockUpdate(ctx, r, system.GetHashStr(RuntimeName), r.config.activeRevID)
					if err != nil {
						lg.Logf(lg.WarnLevel, "Active instance lost lock, becoming passive")
						r.activeInstanceMu.Lock()
						r.config.isActiveInstance = false
						r.config.activeRevID = 0
						r.activeInstanceMu.Unlock()
						r.stopFunctionSubscriptions(ctx)
						if r.afterStartRunning.Load() {
							r.gs.cancelPhaseOne()
							r.afterStartRunning.Store(false)
						}
						continue
					} else {
						r.config.activeRevID = newRevID
					}
				} else {
					newRevID, err := KeyMutexLock(ctx, r, system.GetHashStr(RuntimeName), true)
					if err == nil {
						lg.Logf(lg.DebugLevel, "Passive instance becoming active")
						r.activeInstanceMu.Lock()
						r.config.isActiveInstance = true
						r.config.activeRevID = newRevID
						r.activeInstanceMu.Unlock()

						if err := r.Domain.checkKvConsistency(ctx); err != nil {
							lg.Logf(lg.ErrorLevel, "Failed to become active: %v", err)
							system.MsgOnErrorReturn(KeyMutexUnlock(ctx, r, system.GetHashStr(RuntimeName), newRevID))

							r.activeInstanceMu.Lock()
							r.config.isActiveInstance = false
							r.config.activeRevID = 0
							r.activeInstanceMu.Unlock()
						} else {
							r.gs.CtxPhaseOne, r.gs.cancelPhaseOne = context.WithCancel(context.Background())
							subscribeRequired = true
						}
					} else if !errors.Is(err, ErrMutexLocked) {
						lg.Logf(lg.ErrorLevel, "KeyMutexLock failed for %s: %v", RuntimeName, err)
						continue
					}
				}
			} else {
				r.config.isActiveInstance = true
			}

			tryLock := func(ftName string) {
				newRevID, err := KeyMutexLock(ctx, r, system.GetHashStr(ftName), true)
				if err == nil {
					subscribeRequired = true
					revisions[ftName] = newRevID
					lg.Logf(lg.TraceLevel, "KeyMutexLock succeeded for %s", ftName)
				}
			}

			if r.config.isActiveInstance {
				if r.config.isActiveInstance {
					if r.afterStartRunning.CompareAndSwap(false, true) {
						r.gs.CtxPhaseOne, r.gs.cancelPhaseOne = context.WithCancel(context.Background())
						lg.GetLogger().Debugf(ctx, "runtime is active, run afterStartFunctions")
						r.runAfterStartFunctions(r.gs.CtxPhaseOne)
					}
				}
				for ftName, revID := range revisions {
					if revID == 0 {
						tryLock(ftName)
						continue
					}
					newRevID, err := KeyMutexLockUpdate(ctx, r, system.GetHashStr(ftName), revID)
					if err == nil {
						revisions[ftName] = newRevID
						continue
					}
					if strings.Contains(err.Error(), "already unlocked") {
						revisions[ftName] = 0
						tryLock(ftName)
						continue
					}
					lg.Logf(lg.ErrorLevel, "KeyMutexLockUpdate failed for %s: %v", ftName, err)
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
