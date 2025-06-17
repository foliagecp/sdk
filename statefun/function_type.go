package statefun

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/foliagecp/easyjson"

	"github.com/foliagecp/sdk/statefun/logger"
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

	sfWorkerPool *SFWorkerPool
	tokens       system.TokenBucket
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
	}
	ft.sfWorkerPool = NewSFWorkerPool(ft, config.functionWorkerPoolConfig)
	runtime.registeredFunctionTypes[ft.name] = ft
	return ft
}

// --------------------------------------------------------------------------------------------------------------------

func (ft *FunctionType) SetExecutor(alias string, content string, constructor func(alias string, source string) sfPlugins.StatefunExecutor) error {
	ft.executor = sfPlugins.NewTypenameExecutor(alias, content, constructor)
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

	if !ft.TokenTryAcquire() {
		msg.RefusalCallback(true) // No redelivering cause system have no more scaling resources!
		logger.Logf(logger.ErrorLevel, sendMsgFuncErrorMsg, ft.name, id, "no tokens left")
		return
	}

	var msgChannel chan FunctionTypeMsg
	if value, ok := ft.idHandlersChannel.Load(id); ok {
		msgChannel = value.(chan FunctionTypeMsg)
	} else {
		msgChannel = make(chan FunctionTypeMsg, ft.config.idChannelSize)
		ft.idHandlersChannel.Store(id, msgChannel)
	}
	ft.prometricsMeasureIdChannels()

	select {
	case msgChannel <- msg:
		ft.sfWorkerPool.Notify()
	default:
		ft.TokenRelease()
		msg.RefusalCallback(false) // Can try to rediliver cause free tokens still exists, system have scaling resources
		logger.Logf(logger.WarnLevel, sendMsgFuncErrorMsg, ft.name, id, "queue for current id is full")
	}
}

func (ft *FunctionType) workerTaskExecutor(id string, msg FunctionTypeMsg) {
	id = ft.runtime.Domain.CreateObjectIDWithThisDomain(id, false)
	ft.idKeyMutex.Lock(id)

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
			GetObjectContext:          func() *easyjson.JSON { return ft.getContext(id) },
			SetObjectContext:          func(context *easyjson.JSON) { ft.setContext(id, context) },
			Domain:                    ft.runtime.Domain,
			Self:                      sfPlugins.StatefunAddress{Typename: ft.name, ID: id},
			Signal: func(signalProvider sfPlugins.SignalProvider, targetTypename string, targetID string, j *easyjson.JSON, o *easyjson.JSON, tc *easyjson.JSON) error {
				if tc == nil && msg.TraceContext != nil {
					lg.Logf(lg.TraceLevel, "=== Creating child trace from parent: %s", msg.TraceContext.ToString())
					currentTrace := TraceContextFromJSON(msg.TraceContext)
					if currentTrace != nil {
						newChildTrace := NewTraceContext(currentTrace.TraceID, currentTrace.SpanID)
						tc = newChildTrace.ToJSON()
						lg.Logf(lg.TraceLevel, "=== Child trace created: %s", tc.ToString())

					} else {
						lg.Logf(lg.TraceLevel, "=== Signal: tc=%v, msg.TraceContext exists=%v", tc != nil, msg.TraceContext != nil)

					}
				}
				return ft.runtime.signal(signalProvider, ft.name, id, targetTypename, targetID, j, o, tc)
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

	ft.handleMsgForID(id, msg, typenameIDContextProcessor)
	ft.idKeyMutex.Unlock(id)
}

func (ft *FunctionType) handleMsgForID(id string, msg FunctionTypeMsg, typenameIDContextProcessor *sfPlugins.StatefunContextProcessor) {
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
	typenameIDContextProcessor.Caller = *msg.Caller

	typenameIDContextProcessor.SetTraceContext(msg.TraceContext)

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
		ft.runtime.Domain.cache.DeleteValue(lockId, true, -1, "")
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

	if gaugeVec, err := system.GlobalPrometrics.EnsureGaugeVecSimple("ft_execution_time", "", []string{"typename", "id"}); err == nil {
		gaugeVec.With(prometheus.Labels{"typename": ft.name, "id": id}).Set(float64(time.Since(start).Microseconds()))
	}

	if msg.AckCallback != nil {
		msg.AckCallback(true)
	}
	if msgRequestCallback != nil {
		var replyData *easyjson.JSON = nil
		select {
		case replyData = <-replyDataChannel:
		case <-time.After(time.Duration(ft.runtime.config.requestTimeoutSec) * time.Second):
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
				ft.runtime.Domain.Cache().DeleteValue(funcCtxKey, true, -1, "")
			}
		}
	}
	// ------------------------------------------------------

	ft.idHandlersLastMsgTime.Range(func(key, value interface{}) bool {
		id := key.(string)
		lastMsgTime := value.(int64)

		if lastMsgTime+int64(typenameIDLifetimeMs)*int64(time.Millisecond) < now {
			ft.idKeyMutex.Lock(id)

			ft.idHandlersLastMsgTime.Delete(id)
			ft.idHandlersChannel.Delete(id)
			ft.contextProcessors.Delete(id)
			if ft.executor != nil {
				ft.executor.RemoveForID(id)
			}

			ft.prometricsMeasureIdChannels()

			garbageCollected++
			//lg.Logf(">>>>>>>>>>>>>> Garbage collected handler for %s:%s", ft.name, id)

			ft.idKeyMutex.Unlock(id)
		} else {
			handlersRunning++
		}
		return true
	})
	if garbageCollected > 0 && handlersRunning == 0 {
		lg.Logf(lg.TraceLevel, ">>>>>>>>>>>>>> Garbage collected for typename %s - no id handlers left", ft.name)
	}

	return
}

func (ft *FunctionType) getContext(keyValueID string) *easyjson.JSON {
	if j, err := ft.runtime.Domain.cache.GetValueAsJSON(keyValueID); err == nil {
		return j
	}
	j := easyjson.NewJSONObject()
	return &j
}

func (ft *FunctionType) setContext(keyValueID string, context *easyjson.JSON) {
	if context == nil {
		ft.runtime.Domain.cache.DeleteValue(keyValueID, true, -1, "")
	} else {
		ft.runtime.Domain.cache.SetValue(keyValueID, context.ToBytes(), true, -1, "")
	}
}

// Negative duration removes expiration
func (ft *FunctionType) setContextExpirationAfter(keyValueID string, after time.Duration) {
	if j, err := ft.runtime.Domain.cache.GetValueAsJSON(keyValueID); err == nil {
		if after < 0 {
			j.RemoveByPath(contextExpirationKey)
		} else {
			j.SetByPath(contextExpirationKey, easyjson.NewJSON(time.Now().Add(after).UnixNano()))
		}
		ft.runtime.Domain.cache.SetValue(keyValueID, j.ToBytes(), true, -1, "")
	}
}

func (ft *FunctionType) getStreamName() string {
	return fmt.Sprintf("%s_stream", system.GetHashStr(ft.subject))
}
