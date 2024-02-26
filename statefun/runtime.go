

// Foliage primary statefun package.
// Provides all everything that is needed for Foliage stateful functions and Foliage applications
package statefun

import (
	"context"
	"slices"
	"sync/atomic"
	"time"

	lg "github.com/foliagecp/sdk/statefun/logger"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/foliagecp/sdk/statefun/cache"
	"github.com/foliagecp/sdk/statefun/system"
	"github.com/nats-io/nats.go"

	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
)

type OnAfterStartFunction func(runtime *Runtime) error

type onAfterStartFunctionWithMode struct {
	f     OnAfterStartFunction
	async bool
}

type Runtime struct {
	config RuntimeConfig
	nc     *nats.Conn
	js     nats.JetStreamContext
	Domain *Domain

	registeredFunctionTypes       map[string]*FunctionType
	onAfterStartFunctionsWithMode []onAfterStartFunctionWithMode

	gt0  int64 // Global time 0 - time of the very first message receving by any function type
	glce int64 // Global last call ended - time of last call of last function handling id of any function type
	gc   int64 // Global counter - max total id handlers for all function types
}

func NewRuntime(config RuntimeConfig) (r *Runtime, err error) {
	r = &Runtime{
		config:                  config,
		registeredFunctionTypes: make(map[string]*FunctionType),
	}

	r.nc, err = nats.Connect(config.natsURL)
	if err != nil {
		return
	}

	r.js, err = r.nc.JetStream(nats.PublishAsyncMaxPending(256))
	if err != nil {
		return
	}

	r.Domain, err = NewDomain(r.nc, r.js, config.hubDomainName)
	if err != nil {
		return
	}

	return
}

func (r *Runtime) RegisterOnAfterStartFunction(f OnAfterStartFunction, async bool) {
	if f != nil {
		r.onAfterStartFunctionsWithMode = append(r.onAfterStartFunctionsWithMode, onAfterStartFunctionWithMode{f, async})
	}
}

func (r *Runtime) Start(cacheConfig *cache.Config) (err error) {
	// Create streams if does not exist ------------------------------
	/* Each stream contains a single subject (topic).
	 * Differently named stream with overlapping subjects cannot exist!
	 */
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var existingStreams []string
	for info := range r.js.StreamsInfo(nats.Context(ctx)) {
		existingStreams = append(existingStreams, info.Config.Name)
	}
	for _, ft := range r.registeredFunctionTypes {
		if ft.config.IsSignalProviderAllowed(sfPlugins.JetstreamGlobalSignal) {
			if !slices.Contains(existingStreams, ft.getStreamName()) {
				_, err := r.js.AddStream(&nats.StreamConfig{
					Name:     ft.getStreamName(),
					Subjects: []string{ft.subject},
				})
				system.MsgOnErrorReturn(err)
			}
		}
	}
	// --------------------------------------------------------------

	if err := r.Domain.start(cacheConfig); err != nil {
		return err
	}

	// Functions running in a single instance controller --------------------------------
	singleInstanceFunctionRevisions := map[string]uint64{}
	singleInstanceFunctionLocksUpdater := func(sifr map[string]uint64) {
		system.GlobalPrometrics.GetRoutinesCounter().Started("singleInstanceFunctionLocksUpdater")
		defer system.GlobalPrometrics.GetRoutinesCounter().Stopped("singleInstanceFunctionLocksUpdater")
		if len(sifr) > 0 {
			for {
				time.Sleep(time.Duration(r.config.kvMutexLifeTimeSec) / 2 * time.Second)
				for ftName, revId := range sifr {
					newRevId, err := KeyMutexLockUpdate(r, system.GetHashStr(ftName), revId)
					if err != nil {
						lg.Logf(lg.ErrorLevel, "KeyMutexLockUpdate for single instance function type %s failed", ftName)
					} else {
						sifr[ftName] = newRevId
					}
				}
			}
		}
	}
	// ----------------------------------------------------------------------------------

	// Start function subscriptions ---------------------------------
	for ftName, ft := range r.registeredFunctionTypes {
		if !ft.config.multipleInstancesAllowed {
			revId, err := KeyMutexLock(r, system.GetHashStr(ftName), true)
			if err != nil {
				if err == mutexLockedError {
					lg.Logf(lg.WarnLevel, "Function type %s is already running somewhere and multipleInstancesAllowed==false, skipping", ft.name)
					continue
				} else {
					return err
				}
			}
			singleInstanceFunctionRevisions[ftName] = revId
		}

		if ft.config.IsSignalProviderAllowed(sfPlugins.JetstreamGlobalSignal) {
			system.MsgOnErrorReturn(AddSignalSourceJetstreamQueuePushConsumer(ft))
		}
		if ft.config.IsRequestProviderAllowed(sfPlugins.NatsCoreGlobalRequest) {
			system.MsgOnErrorReturn(AddRequestSourceNatsCore(ft))
		}
	}
	// --------------------------------------------------------------

	go singleInstanceFunctionLocksUpdater(singleInstanceFunctionRevisions)

	for _, onAfterStartFuncWithMode := range r.onAfterStartFunctionsWithMode {
		if onAfterStartFuncWithMode.async {
			go func(f OnAfterStartFunction) {
				system.GlobalPrometrics.GetRoutinesCounter().Started("runtime_onAfterStart")
				defer system.GlobalPrometrics.GetRoutinesCounter().Stopped("runtime_onAfterStart")
				system.MsgOnErrorReturn(f(r))
			}(onAfterStartFuncWithMode.f)
		} else {
			system.MsgOnErrorReturn(onAfterStartFuncWithMode.f(r))
		}
	}

	system.MsgOnErrorReturn(r.runGarbageCellector())

	return
}

func (r *Runtime) runGarbageCellector() (err error) {
	for {
		// Start function subscriptions ---------------------------------
		var totalIdsGrbageCollected int
		var totalIDHandlersRunning int

		measureName := "stetefun_instances"
		var gaugeVec *prometheus.GaugeVec
		var gaugeVecErr error
		gaugeVec, gaugeVecErr = system.GlobalPrometrics.EnsureGaugeVecSimple(measureName, "Stateful function instances", []string{"typename"})

		for _, ft := range r.registeredFunctionTypes {
			n1, n2 := ft.gc(r.config.functionTypeIDLifetimeMs)
			totalIdsGrbageCollected += n1
			totalIDHandlersRunning += n2
			if gaugeVec != nil && gaugeVecErr == nil {
				gaugeVec.With(prometheus.Labels{"typename": ft.name}).Set(float64(n2))
			}
		}

		if totalIdsGrbageCollected > 0 && totalIDHandlersRunning == 0 {
			// Result time output -----------------------------------------------------------------
			if totalIDHandlersRunning == 0 {
				glce := atomic.LoadInt64(&r.glce)
				gt0 := atomic.LoadInt64(&r.gt0)
				gc := atomic.LoadInt64(&r.gc)

				dt := glce - gt0

				if gc > 0 && dt > 0 {
					lg.Logf(lg.TraceLevel, "!!!!!!!!!!!!!!!!! %d runs, total time (ns/ms): %d/%d, function dt (ns/ms): %d/%d -> %dHz\n", gc, dt, dt/1000000, dt/gc, dt/gc/1000000, gc*1000000000/dt)
					atomic.StoreInt64(&r.gc, 0)
				}
			}
			// ------------------------------------------------------------------------------------
		}
		// --------------------------------------------------------------

		time.Sleep(time.Duration(r.config.gcIntervalSec) * time.Second)
	}
}

/*func (r *Runtime) TestKVCleanup() {
	fmt.Println("!!!!!!!!!!!!!!!!! TestKVCleanup")
	if w, err := r.kv.WatchAll(); err == nil {
		for entry := range w.Updates() {
			if entry == nil {
				break
			}
			kv.DeleteKeyValueValue(r.js, r.kv, entry.Key())
		}
	}
}*/
