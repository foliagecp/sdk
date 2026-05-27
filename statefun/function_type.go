package statefun

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/foliagecp/easyjson"

	lg "github.com/foliagecp/sdk/statefun/logger"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
)

type FunctionLogicHandler func(sfPlugins.StatefunExecutor, *sfPlugins.StatefunContextProcessor)

type FunctionType struct {
	runtime      *Runtime
	name         string
	subject      string
	config       FunctionTypeConfig
	logicHandler FunctionLogicHandler

	idHandlersChannel     sync.Map
	idKeyMutex            *system.KeyMutex
	idHandlersLastMsgTime sync.Map
	contextProcessors     sync.Map

	executor      *sfPlugins.TypenameExecutorPlugin
	resourceMutex sync.Mutex

	// sfWorkerPool is an atomic.Pointer so it can be replaced on
	// re-activation: becomePassive stops the pool (Stop marks it stopped
	// for good), and a passive→active transition needs a fresh, working
	// pool. Atomic swap keeps the hot read path (sendMsg) lock-free.
	sfWorkerPool atomic.Pointer[SFWorkerPool]
	tokens       system.TokenBucket
	//-------for graceful shutdown-------
	signalSubscription  *nats.Subscription
	requestSubscription *nats.Subscription
	shutdownCh          chan struct{}
	lastMsgTimeNs       atomic.Uint64
	//-----------------------------------
}

const (
	contextExpirationKey = "____ctx_expires_after_ms"
	sendMsgFuncErrorMsg  = "task refuse for statefun %s with id=%s: %s"
)

func NewFunctionType(runtime *Runtime, name string, logicHandler FunctionLogicHandler, config FunctionTypeConfig) *FunctionType {
	ft := &FunctionType{
		runtime:      runtime,
		name:         name,
		subject:      fmt.Sprintf(DomainIngressSubjectsTmpl, runtime.Domain.name, fmt.Sprintf("%s.%s.%s.%s", SignalPrefix, runtime.Domain.name, name, "*")),
		logicHandler: logicHandler,
		idKeyMutex:   system.NewKeyMutex(),
		config:       config,
		tokens:       *system.NewTokenBucket(config.functionWorkerPoolConfig.MaxWorkers + config.functionWorkerPoolConfig.TaskQueueLen),
		shutdownCh:   make(chan struct{}, 1),
	}
	if runtime.canRegisterNewFunctionType {
		ft.sfWorkerPool.Store(NewSFWorkerPool(ft, config.functionWorkerPoolConfig))
		runtime.ftMu.Lock()
		runtime.registeredFunctionTypes[ft.name] = ft
		runtime.ftMu.Unlock()
	} else {
		lg.Logf(lg.ErrorLevel, "Function type '%s' is not registered. Ensure that all function types are registered before starting the runtime, or use RegisterDynamicFunctionType.", ft.name)
	}
	return ft
}

// RegisterDynamicFunctionType registers a function type after the runtime has already started.
// Returns an error if:
//   - runtime has not started yet (use NewFunctionType instead),
//   - runtime is shutting down,
//   - a function type with the same name is already registered,
//   - JetStream stream / NATS subscription creation fails.
//
// On success the function type is fully operational: worker pool is running and NATS
// subscriptions (according to the config) are live.
//
// Note: in active/passive HA mode this method will still register the function type
// locally, but subscriptions will be created only when this instance is active.
// If the instance is passive, subscriptions will be set up later by the lifecycle
// updater when it transitions to active.
func RegisterDynamicFunctionType(runtime *Runtime, name string, logicHandler FunctionLogicHandler, config FunctionTypeConfig) (*FunctionType, error) {
	if !runtime.isReady.Load() {
		return nil, fmt.Errorf("runtime has not started yet; use NewFunctionType for compile-time registration")
	}

	// Hold phaseTransitionMu in read mode for the entire registration so that
	// a concurrent shutdown waits for us to finish (or we see shutdown started
	// and abort before doing any work).
	runtime.gs.phaseTransitionMu.RLock()
	defer runtime.gs.phaseTransitionMu.RUnlock()

	if runtime.gs.currentPhase() != ShutdownPhaseNone {
		return nil, fmt.Errorf("runtime is shutting down; cannot register new function type '%s'", name)
	}

	ft := &FunctionType{
		runtime:      runtime,
		name:         name,
		subject:      fmt.Sprintf(DomainIngressSubjectsTmpl, runtime.Domain.name, fmt.Sprintf("%s.%s.%s.%s", SignalPrefix, runtime.Domain.name, name, "*")),
		logicHandler: logicHandler,
		idKeyMutex:   system.NewKeyMutex(),
		config:       config,
		tokens:       *system.NewTokenBucket(config.functionWorkerPoolConfig.MaxWorkers + config.functionWorkerPoolConfig.TaskQueueLen),
		shutdownCh:   make(chan struct{}, 1),
	}
	ft.sfWorkerPool.Store(NewSFWorkerPool(ft, config.functionWorkerPoolConfig))

	// Register in map (check uniqueness atomically)
	runtime.ftMu.Lock()
	if _, exists := runtime.registeredFunctionTypes[name]; exists {
		runtime.ftMu.Unlock()
		return nil, fmt.Errorf("function type '%s' is already registered", name)
	}
	runtime.registeredFunctionTypes[name] = ft
	runtime.ftMu.Unlock()

	// Rollback helper in case any subsequent step fails
	rollback := func() {
		runtime.ftMu.Lock()
		delete(runtime.registeredFunctionTypes, name)
		runtime.ftMu.Unlock()
	}

	// Ensure JetStream stream exists for signal-enabled functions
	if ft.config.IsSignalProviderAllowed(sfPlugins.JetstreamGlobalSignal) {
		streamName := ft.getStreamName()
		streamExists := false
		for info := range runtime.js.StreamsInfo() {
			if info.Config.Name == streamName {
				streamExists = true
				break
			}
		}
		if !streamExists {
			if _, err := runtime.js.AddStream(&nats.StreamConfig{
				Name:      streamName,
				Subjects:  []string{ft.subject},
				Retention: nats.InterestPolicy,
				Replicas:  runtime.Domain.ftSC.replicasCount,
				MaxMsgs:   runtime.Domain.ftSC.maxMsgs,
				MaxBytes:  runtime.Domain.ftSC.maxBytes,
				MaxAge:    runtime.Domain.ftSC.maxAge,
			}); err != nil {
				rollback()
				return nil, fmt.Errorf("failed to create JetStream stream for function type '%s': %w", name, err)
			}
		}
	}

	// For single-instance FT: try to acquire the distributed lock.
	// If lock is already taken by another runtime — register locally but don't subscribe.
	singleInstanceLocked := true // true means "this instance owns the lock (or FT is multi-instance)"
	if !ft.config.multipleInstancesAllowed {
		_, err := KeyMutexLock(runtime.gs.ctxPhaseThree, runtime, system.GetHashStr(name), true)
		if err != nil {
			if errors.Is(err, ErrMutexLocked) {
				lg.Logf(lg.WarnLevel, "Dynamic function type '%s' is single-instance and already running on another runtime; registered locally but subscriptions not started", name)
				singleInstanceLocked = false
			} else {
				rollback()
				return nil, fmt.Errorf("failed to acquire single-instance lock for '%s': %w", name, err)
			}
		}
	}

	// Start NATS subscriptions only if this runtime is active and (for single-instance) owns the lock.
	isActive := runtime.IsActiveInstance()

	if isActive && singleInstanceLocked {
		if err := ft.startSubscriptions(); err != nil {
			rollback()
			return nil, fmt.Errorf("failed to start subscriptions for function type '%s': %w", name, err)
		}
	}

	return ft, nil
}

// --------------------------------------------------------------------------------------------------------------------

func (ft *FunctionType) SetExecutor(alias string, content string, constructor func(alias string, source string) sfPlugins.StatefunExecutor) error {
	ft.executor = sfPlugins.NewTypenameExecutor(alias, content, constructor)
	return nil
}

// startSubscriptions creates NATS signal/request subscriptions according to the FT config.
// Used both at runtime startup and for dynamically registered function types.
// ensureWorkerPool guarantees a live (non-stopped) worker pool. becomePassive
// stops the pool permanently; a passive→active transition calls this to spin
// up a fresh one so the function type can process messages again.
func (ft *FunctionType) ensureWorkerPool() {
	if wp := ft.sfWorkerPool.Load(); wp == nil || wp.IsStopped() {
		ft.sfWorkerPool.Store(NewSFWorkerPool(ft, ft.config.functionWorkerPoolConfig))
	}
}

func (ft *FunctionType) startSubscriptions() error {
	// Re-activation path: the pool may have been stopped by a previous
	// becomePassive. Make sure we have a working one before resubscribing.
	ft.ensureWorkerPool()
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
	return nil
}

func (ft *FunctionType) prometricsMeasureIdChannels() {
	activeIDChannels := 0
	ft.idHandlersChannel.Range(func(key, value any) bool {
		activeIDChannels++
		return true
	})
	if gaugeVec, err := system.GlobalPrometrics.EnsureGaugeVecSimple("ft_active_id_channels", "", []string{"typename"}); err == nil {
		gaugeVec.With(prometheus.Labels{"typename": ft.name}).Set(float64(activeIDChannels))
	}
}

type MeasureMsgDeliverType string

const (
	NatsPub           MeasureMsgDeliverType = "nats_pub"
	NatsPubRedelivery MeasureMsgDeliverType = "nats_pub_redeliver"
	NatsReq           MeasureMsgDeliverType = "nats_req"
	GolangReq         MeasureMsgDeliverType = "golang_req"
)

func (ft *FunctionType) prometricsMeasureMsgDeliver(deliveryType MeasureMsgDeliverType) {
	buckets := []float64{1.0}
	labelNames := []string{"typename", "delivery_type"}

	histogram, err := system.GlobalPrometrics.EnsureHistogramVecSimple("ft_msg_delivery", "messages receive", buckets, labelNames)
	if err != nil {
		lg.Logf(lg.ErrorLevel, "Failed to create histogram: %s", err.Error())
	}

	histogram.WithLabelValues(ft.name, string(deliveryType)).Observe(1.0)

	/*activeIDChannels := 0
	ft.idHandlersChannel.Range(func(key, value any) bool {
		activeIDChannels++
		return true
	})
	system.GlobalPrometrics.EnsureHistogramVec()
	if gaugeVec, err := system.GlobalPrometrics.EnsureGaugeVecSimple("ft_msg_delivery", "", []string{"typename", "delivery_type"}); err == nil {
		gaugeVec.With(prometheus.Labels{"typename": ft.name, "delivery_type": string(deliveryType)}).Set(float64(1.0))
	}*/
}

func (ft *FunctionType) prometricsMeasureTokensLoad() {
	if gaugeVec, err := system.GlobalPrometrics.EnsureGaugeVecSimple("ft_tokens_percentage", "", []string{"typename"}); err == nil {
		gaugeVec.With(prometheus.Labels{"typename": ft.name}).Set(ft.tokens.GetLoadPercentage())
	}
}

func (ft *FunctionType) TokenTryAcquire() bool {
	defer ft.prometricsMeasureTokensLoad()
	return ft.tokens.TryAcquire()
}

func (ft *FunctionType) TokenRelease() {
	defer ft.prometricsMeasureTokensLoad()
	ft.tokens.Release()
}

func (ft *FunctionType) TokenCapacity() int {
	return ft.tokens.Capacity
}

func (ft *FunctionType) sendMsg(originId string, msg FunctionTypeMsg) {
	id := ft.runtime.Domain.CreateObjectIDWithThisDomain(originId, false)

	// In HA mode passive runtime must not enqueue new tasks
	if ft.runtime.config.activePassiveMode && !ft.runtime.IsActiveInstance() {
		if msg.RefusalCallback != nil {
			msg.RefusalCallback(false) // try to redeliver
		}
		lg.Logf(lg.DebugLevel, sendMsgFuncErrorMsg, ft.name, id, "runtime is passive")
		return
	}

	if !ft.TokenTryAcquire() {
		msg.RefusalCallback(true) // No redelivering cause system have no more scaling resources!
		lg.Logf(lg.ErrorLevel, sendMsgFuncErrorMsg, ft.name, id, "no tokens left")
		return
	}

	ft.idKeyMutex.Lock(id)
	defer ft.idKeyMutex.Unlock(id)

	var msgChannel chan FunctionTypeMsg
	if value, ok := ft.idHandlersChannel.Load(id); ok {
		msgChannel = value.(chan FunctionTypeMsg)
	} else {
		msgChannel = make(chan FunctionTypeMsg, ft.config.idChannelSize)
		ft.idHandlersChannel.Store(id, msgChannel)
		ft.idHandlersLastMsgTime.Store(id, int64(0)) // no time yet
	}
	ft.prometricsMeasureIdChannels()

	select {
	case msgChannel <- msg:
		ft.idHandlersLastMsgTime.Store(id, time.Now().UnixNano())
		if wp := ft.sfWorkerPool.Load(); wp != nil {
			wp.NotifyId(id)
		}
	default:
		ft.TokenRelease()
		msg.RefusalCallback(false) // Can try to rediliver cause free tokens still exists, system have scaling resources
		lg.Logf(lg.WarnLevel, sendMsgFuncErrorMsg, ft.name, id, "queue for current id is full")
	}
}

func (ft *FunctionType) workerTaskExecutor(id string, msg FunctionTypeMsg) {
	id = ft.runtime.Domain.CreateObjectIDWithThisDomain(id, false)
	ft.idKeyMutex.Lock(id)
	defer func() {
		if r := recover(); r != nil {
			lg.Logf(lg.ErrorLevel, "panic in workerTaskExecutor for %s:%s: %v", ft.name, id, r)
		}
		ft.idKeyMutex.Unlock(id)
	}()

	// Refresh idHandlersLastMsgTime on every invocation, regardless of which
	// path got us here. Without this, the gc loop in (*FunctionType).gc would
	// only ever see ids that arrived via sendMsg/handleMsgForID through the
	// worker pool — calls from io.go:343 (goLangLocalRequest, an in-process
	// ctx.Request from one statefun to another in the same runtime) bypass
	// sendMsg entirely and would therefore never register their id with the
	// gc map. The contextProcessors entry they create on the next line, plus
	// its ~10 closures and captured Payload/Options/Caller/Reply state, would
	// then leak until either the same id arrived through the NATS path or
	// the runtime shut down. Under a workload with high unique-id rate (e.g.
	// CMDB CRUD with fresh vertex/link ids every iteration) this accumulates
	// hundreds of thousands of orphan processors and drives heap_objects
	// growth — diagnosed against tests/soak/leak-hunt: heap_objects climbed
	// 3 M → 11 M in 10 min with the older code, and pprof traced 90.82 % of
	// inuse_objects back to a Request-closure (this function's `Request`
	// field assigned below) retained via the contextProcessors map.
	//
	// Store is unconditional (overwrite-OK) so calls that DID come through
	// sendMsg also keep their lastMsgTime monotonically updated — sendMsg's
	// own Store happens earlier in the path and would otherwise stay stale
	// across long in-process call chains.
	ft.idHandlersLastMsgTime.Store(id, time.Now().UnixNano())

	if ft.executor != nil {
		ft.executor.AddForID(id)
	}

	var typenameIDContextProcessor *sfPlugins.StatefunContextProcessor

	if v, ok := ft.contextProcessors.Load(id); ok {
		typenameIDContextProcessor = v.(*sfPlugins.StatefunContextProcessor)
	} else {
		v := sfPlugins.StatefunContextProcessor{
			GetFunctionContext:        func() *easyjson.JSON { return ft.getContext(ft.name + "." + id) },
			SetFunctionContext:        func(context *easyjson.JSON) { ft.setContext(ft.name+"."+id, context) },
			SetContextExpirationAfter: func(after time.Duration) { ft.setContextExpirationAfter(ft.name+"."+id, after) },
			GetObjectContext:          func() *easyjson.JSON { return ft.getObjectContext(id) },
			SetObjectContext:          func(context *easyjson.JSON) { ft.setObjectContext(id, context) },
			GetObjectImplTypes:        func() (types []string, err error) { return ft.getObjectImplTypes(id) },
			Domain:                    ft.runtime.Domain,
			Self:                      sfPlugins.StatefunAddress{Typename: ft.name, ID: id},
			ObjectSignal: func(signalProvider sfPlugins.SignalProvider, query sfPlugins.LinkQuery, typename string, id string, payload *easyjson.JSON, options *easyjson.JSON) (map[string]error, error) {
				return ft.runtime.ObjectCallSignal(signalProvider, query, typename, id, payload, options)
			},
			ObjectRequest: func(requestProvider sfPlugins.RequestProvider, query sfPlugins.LinkQuery, typename string, id string, payload *easyjson.JSON, options *easyjson.JSON, timeout ...time.Duration) (map[string]*sfPlugins.ObjectRequestReply, error) {
				return ft.runtime.ObjectCallRequest(requestProvider, query, typename, id, payload, options, timeout...)
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
			// Options: ... // Options from initial typename declaration will be merged and overwritten by the incoming one in message
			// Caller: ...
		}
		ft.contextProcessors.Store(id, &v)
		typenameIDContextProcessor = &v
		// Signal and Request are assigned after initialization because they depend on the initialized context for TraceContext propagation
		typenameIDContextProcessor.Signal = func(sp sfPlugins.SignalProvider, targetTypename, targetID string, p, o *easyjson.JSON) error {
			var child *easyjson.JSON
			if cur := typenameIDContextProcessor.GetTraceContext(); cur != nil {
				if tc := TraceContextFromJSON(cur); tc != nil {
					child = NewTraceContext(tc.TraceID, tc.SpanID).ToJSON()
				}
			}
			return ft.runtime.signal(sp, ft.name, id, targetTypename, targetID, p, o, child)
		}
		typenameIDContextProcessor.Request = func(rp sfPlugins.RequestProvider, targetTypename, targetID string, p, o *easyjson.JSON, timeout ...time.Duration) (*easyjson.JSON, error) {
			var child *easyjson.JSON
			if cur := typenameIDContextProcessor.GetTraceContext(); cur != nil {
				if tc := TraceContextFromJSON(cur); tc != nil {
					child = NewTraceContext(tc.TraceID, tc.SpanID).ToJSON()
				}
			}
			return ft.runtime.request(rp, ft.name, id, targetTypename, targetID, p, o, child, timeout...)
		}
	}

	ft.handleMsgForID(id, msg, typenameIDContextProcessor)
}

func (ft *FunctionType) handleMsgForID(id string, msg FunctionTypeMsg, typenameIDContextProcessor *sfPlugins.StatefunContextProcessor) {
	// In HA mode passive runtime must not enqueue new tasks
	// request will reject by timeout
	if ft.runtime.config.activePassiveMode && !ft.runtime.IsActiveInstance() {
		if msg.AckCallback != nil {
			msg.AckCallback(true) // we dont want to redeliver this
		}
		lg.Logf(lg.DebugLevel, sendMsgFuncErrorMsg, ft.name, id, "runtime is passive")
		return
	}

	ft.lastMsgTimeNs.Store(uint64(system.GetCurrentTimeNs()))
	msgRequestCallback := msg.RequestCallback
	replyDataChannel := make(chan *easyjson.JSON, 1)
	typenameIDContextProcessor.Reply = nil
	if msgRequestCallback != nil {
		typenameIDContextProcessor.Reply = &sfPlugins.SyncReply{}

		replyDataChannel <- easyjson.NewJSONObject().GetPtr()
		cancelReplyIfExists := func() {
			select { // Remove old value if exists
			case <-replyDataChannel:
			default:
			}
		}
		typenameIDContextProcessor.Reply.CancelDefaultReply = func() {
			cancelReplyIfExists()
		}
		typenameIDContextProcessor.Reply.With = func(data *easyjson.JSON) {
			cancelReplyIfExists()
			replyDataChannel <- data // Put new value that will replace existing
		}
		typenameIDContextProcessor.Reply.OverrideRequestCallback = func() *sfPlugins.SyncReply {
			msgRequestCallback = nil

			overridenReply := &sfPlugins.SyncReply{}
			overridenReply.With = func(data *easyjson.JSON) {
				msg.RequestCallback(data)
			}
			overridenReply.CancelDefaultReply = func() {}
			overridenReply.OverrideRequestCallback = func() *sfPlugins.SyncReply { return nil }
			return overridenReply
		}
	}

	typenameIDContextProcessor.Payload = msg.Payload
	if typenameIDContextProcessor.Payload == nil {
		typenameIDContextProcessor.Payload = easyjson.NewJSONObject().GetPtr()
	}
	ft.resourceMutex.Lock()
	typenameIDContextProcessor.Options = ft.config.options.Clone().GetPtr()
	ft.resourceMutex.Unlock()
	if msg.Options != nil {
		typenameIDContextProcessor.Options.DeepMerge(*msg.Options)
	}
	// Guard against a nil Caller: a malformed message must degrade to an empty
	// caller address, not panic the worker and silently drop the task.
	if msg.Caller != nil {
		typenameIDContextProcessor.Caller = *msg.Caller
	} else {
		typenameIDContextProcessor.Caller = sfPlugins.StatefunAddress{}
	}

	typenameIDContextProcessor.SetTraceContext(msg.TraceContext)
	if msg.TraceContext != nil {
		traceCtx := TraceContextFromJSON(msg.TraceContext)
		if traceCtx != nil {
			startEvent := &TraceEvent{
				TraceID:      traceCtx.TraceID,
				SpanID:       traceCtx.SpanID,
				ParentSpanID: traceCtx.ParentSpanID,
				EventType:    "span_start",
				FuncTypename: ft.name,
				VertexID:     id,
				Timestamp:    system.GetCurrentTimeNs(),
			}
			PublishTraceEvent(ft.runtime.nc, ft.runtime.Domain.name, startEvent)
		}
	}

	typenameIDContextProcessor.ObjectMutexLock = func(objectId string, errorOnLocked bool) error {
		lockId := fmt.Sprintf("%s-lock", objectId)
		revId, err := KeyMutexLock(context.TODO(), ft.runtime, lockId, errorOnLocked)
		if err == nil {
			objCtx := ft.getContext(lockId)
			objCtx.SetByPath("__lock_rev_id", easyjson.NewJSON(revId))
			ft.setContext(lockId, objCtx)
			return nil
		}
		return err
	}
	typenameIDContextProcessor.ObjectMutexUnlock = func(objectId string) error {
		lockId := fmt.Sprintf("%s-lock", objectId)

		objCtx := ft.getContext(lockId)
		v, ok := objCtx.GetByPath("__lock_rev_id").AsNumeric()
		if !ok {
			return fmt.Errorf("object:%s was not locked", lockId)
		}
		revId := uint64(v)

		err := KeyMutexUnlock(context.TODO(), ft.runtime, lockId, revId)
		if err != nil {
			return err
		}
		ft.runtime.Domain.cache.DeleteValue(lockId, true, -1)
		return nil
	}

	start := time.Now()

	// Calling typename handler function --------------------
	if ft.executor != nil {
		ft.logicHandler(ft.executor.GetForID(id), typenameIDContextProcessor)
	} else {
		ft.logicHandler(nil, typenameIDContextProcessor)
	}
	// -------------------------------------------------------

	if gaugeVec, err := system.GlobalPrometrics.EnsureGaugeVecSimple("ft_execution_time", "", []string{"typename"}); err == nil {
		gaugeVec.With(prometheus.Labels{"typename": ft.name}).Set(float64(time.Since(start).Microseconds()))
	}

	if msg.AckCallback != nil {
		msg.AckCallback(true)
	}
	if msgRequestCallback != nil {
		var replyData *easyjson.JSON = nil
		timer := time.NewTimer(time.Duration(ft.runtime.config.requestTimeoutSec) * time.Second)
		defer timer.Stop()
		select {
		case replyData = <-replyDataChannel:
		case <-timer.C:
			replyData.SetByPath("status", easyjson.NewJSON("timeout"))
		}
		msgRequestCallback(replyData)
	}

	atomic.StoreInt64(&ft.runtime.glce, time.Now().UnixNano())
}

func (ft *FunctionType) gc(typenameIDLifetimeMs int) (garbageCollected int, handlersRunning int) {
	now := time.Now().UnixNano()

	// Deleting function contexts which are expired ---------
	for _, funcCtxKey := range ft.runtime.Domain.Cache().GetKeysByPattern(ft.name + ".>") {
		expirationTime := int64(ft.getContext(funcCtxKey).GetByPath(contextExpirationKey).AsNumericDefault(-1))
		if expirationTime > 0 {
			if expirationTime < now {
				ft.runtime.Domain.Cache().DeleteValue(funcCtxKey, true, -1)
			}
		}
	}
	// ------------------------------------------------------

	ft.idHandlersLastMsgTime.Range(func(key, value interface{}) bool {
		id := key.(string)
		lastMsgTime := value.(int64)

		if lastMsgTime+int64(typenameIDLifetimeMs)*int64(time.Millisecond) < now {
			ft.idKeyMutex.Lock(id)

			remove := true
			if chRaw, ok := ft.idHandlersChannel.Load(id); ok {
				ch := chRaw.(chan FunctionTypeMsg)
				if len(ch) > 0 {
					remove = false
				}
			}
			if remove {
				ft.idHandlersLastMsgTime.Delete(id)
				ft.idHandlersChannel.Delete(id)
				ft.contextProcessors.Delete(id)
				if ft.executor != nil {
					ft.executor.RemoveForID(id)
				}
				ft.prometricsMeasureIdChannels()
				garbageCollected++
			} else {
				handlersRunning++
			}

			ft.idKeyMutex.Unlock(id)
		} else {
			handlersRunning++
		}
		return true
	})
	if garbageCollected > 0 && handlersRunning == 0 {
		lg.Logf(lg.TraceLevel, "Garbage collected for typename %s - no id handlers left", ft.name)
	}
	return
}

func (ft *FunctionType) getContext(keyValueID string) *easyjson.JSON {
	if j, err := ft.runtime.Domain.cache.GetValueJSON(keyValueID); err == nil {
		return j
	}
	j := easyjson.NewJSONObject()
	return &j
}

func (ft *FunctionType) setContext(keyValueID string, context *easyjson.JSON) {
	if context == nil {
		ft.runtime.Domain.cache.DeleteValue(keyValueID, true, -1)
	} else {
		ft.runtime.Domain.cache.SetValueJSON(keyValueID, context, true, -1)
	}
}

func (ft *FunctionType) getObjectContext(keyValueID string) *easyjson.JSON {
	response, err := ft.runtime.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.read", keyValueID, nil, nil)
	if err == nil {
		body := response.GetByPath("data.body")
		if body.IsObject() {
			return &body
		}
	}
	j := easyjson.NewJSONObject()
	return &j
}

func (ft *FunctionType) setObjectContext(keyValueID string, context *easyjson.JSON) {
	if context == nil {
		payload := easyjson.NewJSONObject()
		payload.SetByPath("op_time", easyjson.NewJSON(system.GetCurrentTimeNs()))
		system.MsgOnErrorReturn(ft.runtime.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.delete", keyValueID, &payload, nil))
	} else {
		payload := easyjson.NewJSONObject()
		payload.SetByPath("op_time", easyjson.NewJSON(system.GetCurrentTimeNs()))
		payload.SetByPath("replace", easyjson.NewJSON(true))
		payload.SetByPath("body", *context)
		system.MsgOnErrorReturn(ft.runtime.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.update", keyValueID, &payload, nil))
	}
}

// Negative duration removes expiration
func (ft *FunctionType) setContextExpirationAfter(keyValueID string, after time.Duration) {
	if j, err := ft.runtime.Domain.cache.GetValueJSON(keyValueID); err == nil {
		if after < 0 {
			j.RemoveByPath(contextExpirationKey)
		} else {
			j.SetByPath(contextExpirationKey, easyjson.NewJSON(time.Now().Add(after).UnixNano()))
		}
		ft.runtime.Domain.cache.SetValueJSON(keyValueID, j, true, -1)
	}
}

func (ft *FunctionType) getStreamName() string {
	return fmt.Sprintf("%s_stream", system.GetHashStr(ft.subject))
}

func (ft *FunctionType) getObjectImplTypes(id string) ([]string, error) {
	objectType, err := ft.findObjectType(id)
	if err != nil {
		return nil, err
	}
	if objectType == "" {
		return nil, fmt.Errorf("object type is empty for id: %s", id)
	}

	types := []string{objectType}

	result := map[string]struct{}{}
	result[objectType] = struct{}{}

	response, err := ft.runtime.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.type.read", objectType, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to read type %s: %s", objectType, err.Error())
	}

	if response.PathExists("data.body.cache.parent_types") {
		parentTypes := response.GetByPath("data.body.cache.parent_types")
		for i := 0; i < parentTypes.ArraySize(); i++ {
			parentType := parentTypes.ArrayElement(i).AsStringDefault("")
			parentType = ft.runtime.Domain.CreateObjectIDWithHubDomain(parentType, true)
			if len(parentType) > 0 {
				result[parentType] = struct{}{}
			}
		}
	}

	for _type := range result {
		if _type != objectType {
			types = append(types, _type)
		}
	}

	return types, nil
}

func (ft *FunctionType) findObjectType(id string) (string, error) {
	response, err := ft.runtime.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.object.read", id, nil, nil)
	if err != nil {
		return "", err
	}
	return response.GetByPath("data.type").AsStringDefault(""), nil
}

// stopSignalSubscription tears the signal subscription down for a passive
// transition. Uses Unsubscribe(), not Drain(): becomePassive is an emergency
// stop, not a graceful shutdown.
//
// Why this matters — production incident 116-class:
//
// becomePassive runs synchronously on the lifecycle ticker goroutine. The
// previous Drain() + 10-second per-subscription wait turned this into a
// 20-30 second hostage situation across all ~26 function types whenever
// NATS was momentarily unreachable (Drain() blocks on round-trips that
// won't complete). During those 20-30 s the lifecycle ticker is frozen
// and CANNOT attempt to re-acquire the runtime lock, so by the time
// becomePassive returns the lock TTL has expired and recovery is already
// far behind. The soak test (tests/soak/nats-stall-recovery) reproduced
// exactly this: a 30 s NATS pause + 26 s drain pushed total stall past
// the 30 s SLO every time.
//
// Unsubscribe() returns once the local subscription is cleared, regardless
// of NATS reachability. Any in-flight signal that already crossed sendMsg
// into ft.idHandlersChannel is then dropped by dropAllFunctionPendingTasks
// in the very next line of becomePassive; anything still unacked at the
// JetStream broker is automatically redelivered to the next active
// subscriber in the queue group. Both cases are safe — what we lose by
// not waiting for in-flight callbacks is exactly nothing of correctness,
// only the symmetric "graceful" log line. Worth it: recovery now starts
// within milliseconds of the lock being lost.
func (ft *FunctionType) stopSignalSubscription() {
	if ft.signalSubscription == nil || !ft.signalSubscription.IsValid() {
		return
	}
	if err := ft.signalSubscription.Unsubscribe(); err != nil {
		lg.Logf(lg.ErrorLevel, "failed to unsubscribe signal subscription for typename %s: %s", ft.name, err.Error())
		return
	}
	lg.Logf(lg.DebugLevel, "unsubscribe signal subscription for typename %s", ft.name)
}

func (ft *FunctionType) stopRequestSubscription() {
	if ft.requestSubscription == nil || !ft.requestSubscription.IsValid() {
		return
	}
	if wp := ft.sfWorkerPool.Load(); wp != nil {
		wp.Stop()
	}
	_ = ft.requestSubscription.Unsubscribe()
	lg.Logf(lg.DebugLevel, "unsubscribe request subscription for typename %s", ft.name)
}
