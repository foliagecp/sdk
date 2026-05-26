package statefun

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
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
	ftMu                          sync.RWMutex // protects registeredFunctionTypes
	canRegisterNewFunctionType    bool
	onAfterStartFunctionsWithMode []onAfterStartFunctionWithMode

	gt0  int64 // Global time 0 - time of the very first message receiving by any function type
	glce int64 // Global last call ended - time of last call of last function handling id of any function type
	gc   int64 // Global counter - max total id handlers for all function types

	isReady                      atomic.Bool
	shutdown                     chan struct{}
	gs                           *GracefulShutdown
	functionsStopCh              chan struct{}
	wg                           sync.WaitGroup
	afterStartFunctionsWaitGroup sync.WaitGroup
	afterStartRunning            atomic.Bool
	activeInstanceMu             sync.RWMutex

	// cacheDirty drives the "rehydrate cache from KV before going active"
	// decision. true means the cache snapshot may be out of date with KV.
	//
	// Initial value: true. NewCacheStore did an initial load, but between
	// that load and our first lock acquisition any other active instance
	// is free to keep writing to KV — there is no continuous kv.Watch in
	// the cache-as-source-of-truth model, so we cannot prove our snapshot
	// is current. Better to pay the cost of one reload than to serve stale
	// reads. becomePassive() also sets it to true; it is cleared only by a
	// successful RehydrateFromKV during a promotion.
	cacheDirty atomic.Bool
}

type GracefulShutdown struct {
	phase atomic.Uint32
	mu    sync.RWMutex

	// phaseTransitionMu serializes the transition from ShutdownPhaseNone to any
	// shutdown phase with long-running state-change operations (e.g. dynamic
	// function type registration). Hold RLock while performing work that must
	// not start once shutdown has begun; Lock is taken before flipping the
	// phase so that the shutdown waits for all in-flight operations to finish.
	phaseTransitionMu sync.RWMutex

	cancelPhaseOne   context.CancelFunc
	cancelPhaseTwo   context.CancelFunc
	cancelPhaseThree context.CancelFunc

	ctxPhaseOne   context.Context
	ctxPhaseTwo   context.Context
	ctxPhaseThree context.Context
}

// NewRuntime initializes a new Runtime instance with the given configuration.
func NewRuntime(config RuntimeConfig) (*Runtime, error) {
	r := &Runtime{
		config:                     config,
		registeredFunctionTypes:    make(map[string]*FunctionType),
		canRegisterNewFunctionType: true,
		shutdown:                   make(chan struct{}),
		functionsStopCh:            make(chan struct{}),
	}
	// Default cacheDirty=true. Between NewCacheStore's initial load and our
	// first lock acquisition any other active instance may have written to
	// KV without our cache seeing it (the cache-as-source-of-truth model
	// has no continuous kv.Watch). The first passive→active promotion will
	// pay a RehydrateFromKV to guarantee a fresh snapshot before serving.
	r.cacheDirty.Store(true)

	natsOpts := nats.GetDefaultOptions()
	natsOpts.Servers = strings.Split(r.config.natsURL, ",")
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

	r.Domain.exportEnabled = config.exportEnabled
	r.Domain.exportSC = streamConfig{
		replicasCount: config.natsReplicasCount,
		maxMsgs:       config.exportStreamMaxMsgs,
		maxBytes:      config.exportStreamMaxBytes,
		maxAge:        config.exportStreamMaxAge,
	}

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
	r.gs = NewGracefulShutdown(context.Background())

	gracefulShutdownFunc := func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
		<-sig
		if !r.IsActiveInstance() {
			logger.Debugf(ctx, "Runtime is not active. Shutting down immediately")
			r.gs.beginShutdownPhaseOne()
			r.gs.cancelAllContexts()
			r.Shutdown()
			return
		}
		startShutdown := time.Now()
		r.gs.beginShutdownPhaseOne()
		logger.Debugf(ctx, "Received shutdown signal, shutting down gracefully...")
		logger.Debugf(ctx, "Shutdown currentPhase 1")
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

		logger.Debugf(ctx, "Shutdown currentPhase 2")

		<-r.functionsStopCh

		r.gs.cancelPhaseTwo()

		logger.Debugf(ctx, "Shutdown currentPhase 3")

		<-r.Domain.cache.Synced

		if r.IsActiveInstance() {
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
	if err := r.Domain.start(r.gs.ctxPhaseTwo, cacheConfig, r.config.handlesDomainRouters); err != nil {
		return err
	}

	if r.config.activePassiveMode {
		revID, err := KeyMutexLock(ctx, r, system.GetHashStr(RuntimeName), true)
		if err != nil {
			if errors.Is(err, ErrMutexLocked) {
				logger.Debugf(ctx, "Cant lock. Another runtime is already active")
				r.setActiveInstance(false)
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
		r.setActiveInstance(true)
	}

	// if active - can publish to WAL, passive - can not
	r.Domain.Cache().SetWALWriteEnabled(r.IsActiveInstance())

	// Handle single-instance functions.
	singleInstanceFunctionRevisions := make(map[string]uint64)
	if err := r.handleSingleInstanceFunctions(r.gs.ctxPhaseThree, singleInstanceFunctionRevisions); err != nil {
		return err
	}

	// Start function subscriptions.
	if r.IsActiveInstance() {
		if err := r.startFunctionSubscriptions(ctx, singleInstanceFunctionRevisions); err != nil {
			return err
		}
	}

	// Start garbage collector.
	r.wg.Add(1)
	go r.runGarbageCollector(r.gs.ctxPhaseThree)

	// Set Runtime ready
	r.isReady.Store(true)

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
	r.ftMu.RLock()
	for ftName, ft := range r.registeredFunctionTypes {
		wg.Add(1)
		go func(name string, ft *FunctionType) {
			defer wg.Done()
			ft.stopSignalSubscription()
		}(ftName, ft)
	}
	r.ftMu.RUnlock()
	wg.Wait()
}

func NewGracefulShutdown(rootCtx context.Context) *GracefulShutdown {
	gs := &GracefulShutdown{}

	gs.setPhase(ShutdownPhaseNone)
	gs.mu.Lock()
	gs.ctxPhaseOne, gs.cancelPhaseOne = context.WithCancel(rootCtx)
	gs.ctxPhaseTwo, gs.cancelPhaseTwo = context.WithCancel(rootCtx)
	gs.ctxPhaseThree, gs.cancelPhaseThree = context.WithCancel(rootCtx)
	gs.mu.Unlock()

	return gs
}

func (gs *GracefulShutdown) setPhase(phase ShutdownPhase) {
	gs.phase.Store(phase)
}

// beginShutdownPhaseOne atomically transitions from ShutdownPhaseNone to
// ShutdownPhaseOne. It takes phaseTransitionMu in write mode, which blocks
// until all in-flight state-change operations (holding RLock) complete, then
// flips the phase. Once this returns, no new state-change operation can start.
func (gs *GracefulShutdown) beginShutdownPhaseOne() {
	gs.phaseTransitionMu.Lock()
	defer gs.phaseTransitionMu.Unlock()
	gs.setPhase(ShutdownPhaseOne)
}

func (gs *GracefulShutdown) currentPhase() ShutdownPhase {
	return gs.phase.Load()
}

func (gs *GracefulShutdown) phaseOneCtx() context.Context {
	gs.mu.RLock()
	defer gs.mu.RUnlock()
	return gs.ctxPhaseOne
}

func (gs *GracefulShutdown) resetPhaseOneCtx() {
	gs.mu.Lock()
	defer gs.mu.Unlock()
	gs.ctxPhaseOne, gs.cancelPhaseOne = context.WithCancel(context.Background())
}

// cancelAllContexts force stops passive runtime
func (gs *GracefulShutdown) cancelAllContexts() {
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

// jsStartupRetryTimeout bounds how long startup JetStream operations (stream /
// consumer / subscription creation) keep retrying a transient cluster-
// availability error before giving up. Tunable via JS_STARTUP_RETRY_TIMEOUT_SEC.
var jsStartupRetryTimeout = time.Duration(system.GetEnvMustProceed[int]("JS_STARTUP_RETRY_TIMEOUT_SEC", 60)) * time.Second

// isTransientJSError reports whether a JetStream error is a transient cluster-
// availability condition — no leader/quorum yet, an election in progress — that
// clears on its own, as opposed to a real config/logic error which must surface
// immediately. Safe to retry at any time.
func isTransientJSError(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "temporarily unavailable") ||
		strings.Contains(s, "no responders")
}

// isStartupTransientJSError additionally treats conditions that occur while a
// clustered JetStream is still forming / electing — request timeouts, missing
// leader, no placement peers yet — as transient. These are only safe to retry
// during the bounded STARTUP window: in steady state a deadline/placement error
// is a real problem, but during formation it just means "not ready yet".
func isStartupTransientJSError(err error) bool {
	if isTransientJSError(err) {
		return true
	}
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "deadline exceeded") ||
		strings.Contains(s, "no suitable peers") ||
		strings.Contains(s, "no leader") ||
		strings.Contains(s, "leadership")
}

// retryJS runs op, retrying with backoff while it returns an error the transient
// predicate accepts, up to jsStartupRetryTimeout. It keeps a freshly started
// runtime from aborting when a (clustered) JetStream is briefly unavailable
// during an election or initial formation; permanent errors return at once.
func retryJS(ctx context.Context, what string, transient func(error) bool, op func() error) error {
	deadline := time.Now().Add(jsStartupRetryTimeout)
	for attempt := 1; ; attempt++ {
		err := op()
		if err == nil || !transient(err) {
			return err
		}
		if time.Now().After(deadline) {
			lg.Logf(lg.ErrorLevel, "%s: JetStream still failing after %s of retries: %s", what, jsStartupRetryTimeout, err)
			return err
		}
		lg.Logf(lg.WarnLevel, "%s: transient JetStream error (attempt %d), retrying: %s", what, attempt, err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(2 * time.Second):
		}
	}
}

// retryStartupJS retries on the broad startup-transient set (use only on the
// runtime startup path: stream / consumer / KV-bucket / subscription creation).
func retryStartupJS(ctx context.Context, what string, op func() error) error {
	return retryJS(ctx, what, isStartupTransientJSError, op)
}

// createStreams ensures that the necessary NATS streams exist.
func (r *Runtime) createStreams(ctx context.Context) error {
	logger := lg.NewLogger(lg.Options{ReportCaller: true, Level: lg.InfoLevel})
	var existingStreams []string

	streamInfoCh := r.js.StreamsInfo(nats.Context(ctx))
	for info := range streamInfoCh {
		existingStreams = append(existingStreams, info.Config.Name)
	}

	r.ftMu.RLock()
	defer r.ftMu.RUnlock()
	for _, ft := range r.registeredFunctionTypes {
		if ft.config.IsSignalProviderAllowed(sfPlugins.JetstreamGlobalSignal) {
			if !contains(existingStreams, ft.getStreamName()) {
				streamName := ft.getStreamName()
				err := retryStartupJS(ctx, "createStreams "+streamName, func() error {
					_, e := r.js.AddStream(&nats.StreamConfig{
						Name:      streamName,
						Subjects:  []string{ft.subject},
						Retention: nats.InterestPolicy,
						Replicas:  r.Domain.ftSC.replicasCount,
						MaxMsgs:   r.Domain.ftSC.maxMsgs,
						MaxBytes:  r.Domain.ftSC.maxBytes,
						MaxAge:    r.Domain.ftSC.maxAge,
					})
					return e
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
	// Snapshot under RLock to avoid holding it across KeyMutexLock (which does NATS I/O)
	r.ftMu.RLock()
	ftSnapshot := make([]*FunctionType, 0, len(r.registeredFunctionTypes))
	for _, ft := range r.registeredFunctionTypes {
		ftSnapshot = append(ftSnapshot, ft)
	}
	r.ftMu.RUnlock()

	for _, ft := range ftSnapshot {
		if !ft.config.multipleInstancesAllowed {
			revID, err := KeyMutexLock(ctx, r, system.GetHashStr(ft.name), true)
			if err != nil {
				if errors.Is(err, ErrMutexLocked) {
					lg.Logf(lg.WarnLevel, "Function type %s is already running elsewhere; skipping", ft.name)
					revisions[ft.name] = 0 // 0 means that the function is already running elsewhere
					continue
				}
				return err
			}
			revisions[ft.name] = revID
		}
	}

	// Always start runtime lifecycle updater
	r.wg.Add(1)
	go r.runtimeLifecycleUpdater(ctx, revisions)

	return nil
}

// startFunctionSubscriptions starts the function subscriptions based on the configuration.
func (r *Runtime) startFunctionSubscriptions(ctx context.Context, revisions map[string]uint64) error {
	// Snapshot under RLock to avoid holding it during NATS subscription I/O
	r.ftMu.RLock()
	ftSnapshot := make([]*FunctionType, 0, len(r.registeredFunctionTypes))
	for _, ft := range r.registeredFunctionTypes {
		ftSnapshot = append(ftSnapshot, ft)
	}
	r.ftMu.RUnlock()

	for _, ft := range ftSnapshot {
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

		if err := retryStartupJS(ctx, "startSubscriptions "+ft.name, ft.startSubscriptions); err != nil {
			return err
		}
	}
	return nil
}

func (r *Runtime) stopFunctionSubscriptions(ctx context.Context) {
	r.ftMu.RLock()
	defer r.ftMu.RUnlock()
	for _, ft := range r.registeredFunctionTypes {
		ft.stopSignalSubscription()
		ft.stopRequestSubscription()
	}
}

func (r *Runtime) dropAllFunctionPendingTasks() {
	totalDropped := 0
	r.ftMu.RLock()
	for _, ft := range r.registeredFunctionTypes {
		wp := ft.sfWorkerPool.Load()
		if wp == nil {
			continue
		}
		dropped := wp.DropPendingTasks()
		totalDropped += dropped
		if dropped > 0 {
			lg.Logf(lg.DebugLevel, "Dropped %d pending tasks for function %s on passive transition", dropped, ft.name)
		}
	}
	r.ftMu.RUnlock()
	if totalDropped > 0 {
		lg.Logf(lg.DebugLevel, "Dropped %d pending tasks in total on passive transition", totalDropped)
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

	isShutdown := r.gs.currentPhase() == ShutdownPhaseTwo
	functionsReadyForStop := isShutdown

	r.ftMu.RLock()
	for _, ft := range r.registeredFunctionTypes {
		collected, running := ft.gc(r.config.functionTypeIDLifetimeMs)
		totalGarbageCollected += collected
		totalHandlersRunning += running

		if gaugeVec != nil {
			gaugeVec.With(prometheus.Labels{"typename": ft.name}).Set(float64(running))
		}

		wp := ft.sfWorkerPool.Load()
		activeWorkers := 0
		taskQueueLen := 0
		if wp != nil {
			activeWorkers = wp.GetActiveWorkersCount()
			taskQueueLen = len(wp.taskQueue)
		}
		if isShutdown &&
			(running > 0 ||
				activeWorkers > 0 ||
				taskQueueLen > 0 ||
				(int64(ft.lastMsgTimeNs.Load())+int64(r.config.functionTypeIDLifetimeMs*1000*1000) > system.GetCurrentTimeNs())) {
			lg.GetLogger().Debugf(context.TODO(), "for function %s is running=%d, is active=%d", ft.name, running, activeWorkers)
			functionsReadyForStop = false
			ft.lastMsgTimeNs.Store(uint64(system.GetCurrentTimeNs()))
		}
	}
	r.ftMu.RUnlock()

	if functionsReadyForStop {
		lg.GetLogger().Infof(context.TODO(), "all functions are ready for stop")
		r.ftMu.RLock()
		for _, ft := range r.registeredFunctionTypes {
			ft.stopRequestSubscription()
		}
		r.ftMu.RUnlock()
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

// runtimeLifecycleUpdater periodically updates runtime/function locks and lifecycle hooks
func (r *Runtime) runtimeLifecycleUpdater(ctx context.Context, revisions map[string]uint64) {
	defer r.wg.Done()
	ticker := time.NewTicker(time.Duration(r.config.kvMutexLifeTimeSec) / 2 * time.Second)
	defer ticker.Stop()

	//release all functions
	releaseAllLocks := func(ctx context.Context, runtime *Runtime, revisions map[string]uint64) {
		if runtime.IsActiveInstance() {
			for ftName, revID := range revisions {
				system.MsgOnErrorReturn(KeyMutexUnlock(ctx, runtime, system.GetHashStr(ftName), revID))
			}
		}
	}
	defer releaseAllLocks(ctx, r, revisions)

	// Channel for async KV consistency check result (non-nil = check in progress)
	var kvConsistencyCheck chan error

	// Soft-TTL tolerance for the runtime lock. lastRuntimeLockRefresh is the last
	// moment we KNOW we held it (initial acquisition or a successful refresh).
	// A transient KV outage — e.g. the KV bucket stream briefly has no RAFT
	// leader while a cluster node rejoins — makes KeyMutexLockUpdate fail even
	// though the lock is still ours in KV. Demoting on that first failure needlessly
	// drops every responder. We keep the lock across such failures until the
	// soft-TTL elapses; only then may another instance legitimately seize it
	// (see KeyMutexLock: a lock is stealable once lockTime+kvMutexLifeTimeSec<now),
	// so that is exactly when we MUST step down to avoid split-brain.
	lastRuntimeLockRefresh := time.Now()
	lockSoftTTL := time.Duration(r.config.kvMutexLifeTimeSec) * time.Second

	becomePassive := func(cause string) {
		lg.Logf(lg.WarnLevel, "%s, becoming passive", cause)
		r.activeInstanceMu.Lock()
		r.config.isActiveInstance = false // lock already held; setActiveInstance would re-lock the same mutex (reentrant deadlock)
		revToRelease := r.config.activeRevID
		r.config.activeRevID = 0
		r.activeInstanceMu.Unlock()
		// Release our own runtime lock so a fresh acquisition (recovery, or
		// another instance) does not have to wait out the full soft-TTL on a
		// stale-but-unrefreshed lock. Best-effort: if the lock was already
		// lost/expired this is a no-op.
		if revToRelease != 0 {
			system.MsgOnErrorReturn(KeyMutexUnlock(ctx, r, system.GetHashStr(RuntimeName), revToRelease))
		}
		r.Domain.Cache().SetWALWriteEnabled(false)
		r.stopFunctionSubscriptions(ctx)
		r.dropAllFunctionPendingTasks()
		if r.afterStartRunning.Load() {
			r.gs.cancelPhaseOne()
		}
		kvConsistencyCheck = nil
		// While we sit passive, another instance can become active and mutate
		// KV without our cache learning of it (the cache-as-source-of-truth
		// model has no continuous kv.Watch). Mark the cache dirty so the next
		// promotion does a full RehydrateFromKV before serving requests.
		r.cacheDirty.Store(true)
	}

	for {
		select {
		case <-ctx.Done():
			return
		case <-r.shutdown:
			return
		case <-ticker.C:
			subscribeRequired := false

			if r.config.activePassiveMode {
				// Refresh runtime lock if held (active or activating)
				if r.config.activeRevID != 0 {
					newRevID, err := KeyMutexLockUpdate(ctx, r, system.GetHashStr(RuntimeName), r.config.activeRevID)
					if err != nil {
						// Three classes of error, ranked safety-critical first:
						//
						//   1) ErrMutexRevisionViolated — the lock entry in KV
						//      now carries someone else's revision id, i.e.
						//      another instance legitimately seized the lock
						//      while we couldn't refresh. We MUST demote
						//      immediately or we get split-brain (both A and
						//      B convinced they are active). NOT tolerated by
						//      the soft-TTL window — strict safety beats
						//      availability here.
						//
						//   2) "already unlocked" — explicit release path. Also
						//      a genuine loss → demote.
						//
						//   3) Anything else — transient KV reachability error.
						//      The lock is still ours in KV. Keep it through
						//      the soft-TTL window since our last successful
						//      refresh; demote only when that budget is
						//      exhausted (at which point another instance
						//      could lawfully seize the lock, so we MUST be
						//      passive by then).
						genuineLoss := errors.Is(err, ErrMutexRevisionViolated) ||
							strings.Contains(err.Error(), "already unlocked")
						if genuineLoss || time.Since(lastRuntimeLockRefresh) >= lockSoftTTL {
							becomePassive("Lost runtime lock")
							continue
						}
						lg.Logf(lg.WarnLevel, "runtime lock refresh failed transiently (%v); keeping lock (%.1fs/%ds within soft-TTL)",
							err, time.Since(lastRuntimeLockRefresh).Seconds(), r.config.kvMutexLifeTimeSec)
					} else {
						r.config.activeRevID = newRevID
						lastRuntimeLockRefresh = time.Now()
					}
				}

				if r.IsActiveInstance() {
					// Already active — nothing to do
				} else if kvConsistencyCheck != nil {
					// Activating: consistency check in progress, lock is being refreshed above
					select {
					case err := <-kvConsistencyCheck:
						kvConsistencyCheck = nil
						if err != nil {
							lg.Logf(lg.ErrorLevel, "KV consistency check failed: %v", err)
							system.MsgOnErrorReturn(KeyMutexUnlock(ctx, r, system.GetHashStr(RuntimeName), r.config.activeRevID))
							r.activeInstanceMu.Lock()
							r.config.isActiveInstance = false // lock already held; setActiveInstance would re-lock the same mutex (reentrant deadlock)
							r.config.activeRevID = 0
							r.activeInstanceMu.Unlock()
						} else {
							lg.Logf(lg.DebugLevel, "KV consistent, fully active now")
							r.activeInstanceMu.Lock()
							r.config.isActiveInstance = true // lock already held; setActiveInstance would re-lock the same mutex (reentrant deadlock)
							r.activeInstanceMu.Unlock()
							r.Domain.Cache().SetWALWriteEnabled(true)
							r.gs.resetPhaseOneCtx()
							r.afterStartRunning.Store(false)
							subscribeRequired = true
						}
					default:
						// Still checking — lock was refreshed above, just wait
					}
				} else if r.config.activeRevID == 0 {
					// Passive: try to acquire lock
					newRevID, err := KeyMutexLock(ctx, r, system.GetHashStr(RuntimeName), true)
					if err == nil {
						lg.Logf(lg.DebugLevel, "Passive instance acquired lock, checking KV consistency")
						r.config.activeRevID = newRevID
						lastRuntimeLockRefresh = time.Now()
						ch := make(chan error, 1)
						kvConsistencyCheck = ch
						go func() {
							// 1) Wait for the committer to drain pending WAL
							//    transactions into KV — otherwise our cache
							//    rehydrate below would read a KV that's
							//    missing transactions still buffered in the
							//    wal_commits stream.
							if err := r.Domain.checkKvConsistency(ctx); err != nil {
								ch <- err
								return
							}
							// 2) Rehydrate the cache from KV if it may be
							//    stale. A passive instance's cache is frozen
							//    at the moment its initial load completed; any
							//    other active that ran during our passive
							//    spell wrote to KV without our cache knowing.
							//    cacheDirty starts false (NewCacheStore did
							//    the initial load) and is set true on every
							//    becomePassive, so the very first promotion
							//    after startup correctly skips this reload.
							if r.cacheDirty.Load() {
								if err := r.Domain.Cache().RehydrateFromKV(ctx); err != nil {
									ch <- fmt.Errorf("cache rehydrate failed: %w", err)
									return
								}
								r.cacheDirty.Store(false)
							}
							ch <- nil
						}()
					} else if !errors.Is(err, ErrMutexLocked) {
						lg.Logf(lg.ErrorLevel, "KeyMutexLock failed for %s: %v", RuntimeName, err)
					}
				}
			} else {
				r.setActiveInstance(true)
			}

			tryLock := func(ftName string) {
				newRevID, err := KeyMutexLock(ctx, r, system.GetHashStr(ftName), true)
				if err == nil {
					subscribeRequired = true
					revisions[ftName] = newRevID
					lg.Logf(lg.TraceLevel, "KeyMutexLock succeeded for %s", ftName)
				}
			}

			if r.IsActiveInstance() {
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
					// Function-level lock: both "already unlocked" and
					// ErrMutexRevisionViolated mean we no longer own this
					// per-function token. Drop our stale revision so the
					// next tryLock starts fresh.
					if errors.Is(err, ErrMutexRevisionViolated) ||
						strings.Contains(err.Error(), "already unlocked") {
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

			// Run afterStart functions only once the runtime is ready to serve:
			// they typically issue requests (e.g. cmdbSchemaPrepare creates the
			// built-in types/objects roots via runtime.Request), and Request
			// rejects calls before isReady. On fast (single-node) startup isReady
			// is set before this tick, so it is a no-op; on slow startup (a 3-node
			// R=3 cluster takes much longer to wire up ~30 subscriptions) this
			// prevents afterStart from firing prematurely and silently dropping
			// schema initialization — which otherwise leaves every CMDB op failing
			// with "vertex hub/types does not exist". isReady.Load short-circuits
			// the CompareAndSwap so a not-yet-ready tick does not consume the
			// one-shot guard; a later tick runs it once ready.
			if r.isReady.Load() && r.afterStartRunning.CompareAndSwap(false, true) {
				lg.GetLogger().Debugf(ctx, "run afterStartFunctions")
				r.runAfterStartFunctions(r.gs.phaseOneCtx())
			}
		}
	}
}

// IsActiveInstance indicates whether this runtime instance currently owns
// the active role in HA mode
func (r *Runtime) IsActiveInstance() bool {
	r.activeInstanceMu.RLock()
	defer r.activeInstanceMu.RUnlock()
	return r.config.isActiveInstance
}

// IsReady reports whether Start has finished bringing the runtime up to the
// point where it can serve Request/Signal. It is set true late in Start (after
// streams, subscriptions and the startup snapshot), so it can lag
// IsActiveInstance — callers that need to send traffic should gate on this, not
// just on active ownership. Useful as a readiness probe (health checks, tests
// waiting for a freshly started runtime). Request/Signal already reject calls
// made before this is true.
func (r *Runtime) IsReady() bool {
	return r.isReady.Load()
}

func (r *Runtime) setActiveInstance(active bool) {
	r.activeInstanceMu.Lock()
	defer r.activeInstanceMu.Unlock()
	r.config.isActiveInstance = active
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
