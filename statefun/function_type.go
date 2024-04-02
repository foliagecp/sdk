// Copyright 2023 NJWS Inc.

package statefun

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	lg "github.com/foliagecp/sdk/statefun/logger"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/foliagecp/easyjson"

	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
)

type FunctionLogicHandler func(sfPlugins.StatefunExecutor, *sfPlugins.StatefunContextProcessor)

type FunctionType struct {
	runtime                 *Runtime
	name                    string
	subject                 string
	config                  FunctionTypeConfig
	logicHandler            FunctionLogicHandler
	idKeyMutex              system.KeyMutex
	idHandlersChannel       sync.Map
	idHandlersLastMsgTime   sync.Map
	executor                *sfPlugins.TypenameExecutorPlugin
	instancesControlChannel chan struct{}
	resourceMutex           sync.Mutex
}

const (
	contextExpirationKey = "____ctx_expires_after_ms"
)

func NewFunctionType(runtime *Runtime, name string, logicHandler FunctionLogicHandler, config FunctionTypeConfig) *FunctionType {
	ft := &FunctionType{
		runtime:                 runtime,
		name:                    name,
		subject:                 fmt.Sprintf(DomainIngressSubjectsTmpl, runtime.Domain.name, fmt.Sprintf("%s.%s.%s.%s", SignalPrefix, runtime.Domain.name, name, "*")),
		logicHandler:            logicHandler,
		idKeyMutex:              system.NewKeyMutex(),
		config:                  config,
		instancesControlChannel: nil,
	}
	if config.maxIdHandlers > 0 {
		ft.instancesControlChannel = make(chan struct{}, config.maxIdHandlers)
	}
	runtime.registeredFunctionTypes[ft.name] = ft
	return ft
}

// --------------------------------------------------------------------------------------------------------------------

func (ft *FunctionType) SetExecutor(alias string, content string, constructor func(alias string, source string) sfPlugins.StatefunExecutor) error {
	ft.executor = sfPlugins.NewTypenameExecutor(alias, content, constructor)
	return nil
}

func (ft *FunctionType) sendMsg(originId string, msg FunctionTypeMsg) {
	id := ft.runtime.Domain.CreateObjectIDWithThisDomain(originId, false)

	ft.idKeyMutex.Lock(id)
	// Send msg to type id handler ------------------------------------------------------
	var msgChannel chan FunctionTypeMsg

	if value, ok := ft.idHandlersChannel.Load(id); ok {
		msgChannel = value.(chan FunctionTypeMsg)
	} else {
		// Limit typename's max id handlers running -------
		if ft.instancesControlChannel != nil {
			select {
			case ft.instancesControlChannel <- struct{}{}:
			default: // Limit is reached
				msg.RefusalCallback()
				return
			}
		}
		// ------------------------------------------------

		msgChannel = make(chan FunctionTypeMsg, ft.config.msgChannelSize)

		go ft.idHandlerRoutine(id, msgChannel)
		ft.idHandlersChannel.Store(id, msgChannel)
		if ft.executor != nil {
			ft.executor.AddForID(id)
		}
	}
	ft.idHandlersLastMsgTime.Store(id, time.Now().UnixNano())

	select {
	case msgChannel <- msg:
		// Debug values update ----------------------------
		gc := atomic.LoadInt64(&ft.runtime.gc)

		if gc == 0 {
			now := time.Now().UnixNano()
			atomic.StoreInt64(&ft.runtime.glce, now)
			atomic.StoreInt64(&ft.runtime.gt0, now)
		}
		atomic.AddInt64(&ft.runtime.gc, 1)
		// ------------------------------------------------
	default:
		if msg.RefusalCallback != nil {
			msg.RefusalCallback()
		}
	}
	// ----------------------------------------------------------------------------------
	ft.idKeyMutex.Unlock(id)
}

func (ft *FunctionType) idHandlerRoutine(id string, msgChannel chan FunctionTypeMsg) {
	system.GlobalPrometrics.GetRoutinesCounter().Started("functiontype-idHandlerRoutine")
	defer system.GlobalPrometrics.GetRoutinesCounter().Stopped("functiontype-idHandlerRoutine")
	typenameIDContextProcessor := sfPlugins.StatefunContextProcessor{
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
		Request: func(requestProvider sfPlugins.RequestProvider, targetTypename string, targetID string, j *easyjson.JSON, o *easyjson.JSON) (*easyjson.JSON, error) {
			return ft.runtime.request(requestProvider, ft.name, id, targetTypename, targetID, j, o)
		},
		Egress: func(egressProvider sfPlugins.EgressProvider, j *easyjson.JSON) error {
			return ft.runtime.egress(egressProvider, ft.name, id, j)
		},
		// To be assigned later:
		// Call: ...
		// Payload: ...
		// Options: ... // Otions from initial typename declaration will be merged and overwritten by the incoming one in message
		// Caller: ...
	}

	for msg := range msgChannel {
		ft.handleMsgForID(id, msg, &typenameIDContextProcessor)
	}
	if ft.instancesControlChannel != nil {
		<-ft.instancesControlChannel
	}
}

func (ft *FunctionType) handleMsgForID(id string, msg FunctionTypeMsg, typenameIDContextProcessor *sfPlugins.StatefunContextProcessor) {
	msgRequestCallback := msg.RequestCallback
	replyDataChannel := make(chan *easyjson.JSON, 1)
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

	typenameIDContextProcessor.ObjectMutexLock = func(objectId string, errorOnLocked bool) error {
		lockId := fmt.Sprintf("%s-lock", objectId)
		revId, err := KeyMutexLock(ft.runtime, lockId, errorOnLocked)
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

		err := KeyMutexUnlock(ft.runtime, lockId, revId)
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

	measureName := fmt.Sprintf("%s_execution_time", strings.ReplaceAll(ft.name, ".", ""))
	if gaugeVec, err := system.GlobalPrometrics.EnsureGaugeVecSimple(measureName, "", []string{"id"}); err == nil {
		gaugeVec.With(prometheus.Labels{"id": id}).Set(float64(time.Since(start).Microseconds()))
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

			v, _ := ft.idHandlersChannel.Load(id)
			msgChannel := v.(chan FunctionTypeMsg)
			close(msgChannel)
			ft.idHandlersChannel.Delete(id)
			ft.idHandlersLastMsgTime.Delete(id)
			if ft.executor != nil {
				ft.executor.RemoveForID(id)
			}

			garbageCollected++
			//lg.Logf(">>>>>>>>>>>>>> Garbage collected handler for %s:%s\n", ft.name, id)

			ft.idKeyMutex.Unlock(id)
		} else {
			handlersRunning++
		}
		return true
	})
	if garbageCollected > 0 && handlersRunning == 0 {
		lg.Logf(lg.TraceLevel, ">>>>>>>>>>>>>> Garbage collected for typename %s - no id handlers left\n", ft.name)
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
