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

func NewFunctionType(runtime *Runtime, name string, logicHandler FunctionLogicHandler, config FunctionTypeConfig) *FunctionType {
	ft := &FunctionType{
		runtime:                 runtime,
		name:                    name,
		subject:                 name + ".*",
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

func (ft *FunctionType) sendMsg(id string, msg FunctionTypeMsg) {
	/*// After message was received do typename balance if the one is needed and hasn't been done yet -------
	if ft.config.balanceNeeded {
		if !ft.config.balanced {
			var err error
			ft.typenameLockRevisionID, err = FunctionTypeMutexLock(ft, true)
			if err != nil {
				lg.Logf(lg.WarnLevel, "function with type %s has received a message, but this typename was already locked! Skipping message...\n", ft.name)
				// Preventing from rapidly calling this function over and over again if no function
				// in other runtime that can handle this message and kv mutex is already dead
				time.Sleep(time.Duration(ft.config.msgAckWaitMs/2) * time.Millisecond) // Sleep duration must be guarantee less than msgAckWaitMs, otherwise may miss doing Nak (via RefusalCallback) in time
				if msg.RefusalCallback != nil {
					msg.RefusalCallback()
				}
				return
			}
			ft.config.balanced = true
		}
	}
	// ----------------------------------------------------------------------------------------------------*/

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
		GlobalCache:        ft.runtime.cacheStore,
		GetFunctionContext: func() *easyjson.JSON { return ft.getContext(ft.name + "." + id) },
		SetFunctionContext: func(context *easyjson.JSON) { ft.setContext(ft.name+"."+id, context) },
		GetObjectContext:   func() *easyjson.JSON { return ft.getContext(id) },
		SetObjectContext:   func(context *easyjson.JSON) { ft.setContext(id, context) },
		Self:               sfPlugins.StatefunAddress{Typename: ft.name, ID: id},
		Signal: func(signalProvider sfPlugins.SignalProvider, targetTypename string, targetID string, j *easyjson.JSON, o *easyjson.JSON) error {
			return ft.runtime.signal(signalProvider, ft.name, id, targetTypename, targetID, j, o)
		},
		Request: func(requestProvider sfPlugins.RequestProvider, targetTypename string, targetID string, j *easyjson.JSON, o *easyjson.JSON) (*easyjson.JSON, error) {
			return ft.runtime.request(requestProvider, ft.name, id, targetTypename, targetID, j, o)
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
	/*var lockRevisionID uint64 = 0

	if !ft.config.balanceNeeded { // Use context mutex lock if function type is not typename balanced
		var err error
		lockRevisionID, err = ContextMutexLock(ft, id, false)
		if err != nil {
			if msg.RefusalCallback != nil {
				msg.RefusalCallback()
			}
			return
		}
	}*/

	replyDataChannel := make(chan *easyjson.JSON, 1)
	if msg.RequestCallback != nil {
		typenameIDContextProcessor.Reply = &sfPlugins.SyncReply{}

		replyDataChannel <- easyjson.NewJSONObject().GetPtr()
		cancelReplyIfExists := func() {
			select { // Remove old value if exists
			case <-replyDataChannel:
			default:
			}
		}
		typenameIDContextProcessor.Reply.CancelDefault = func() {
			cancelReplyIfExists()
		}
		typenameIDContextProcessor.Reply.With = func(data *easyjson.JSON) {
			cancelReplyIfExists()
			replyDataChannel <- data // Put new value
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

	typenameIDContextProcessor.ObjectMutexLock = func(errorOnLocked bool) error {
		lockId := fmt.Sprintf("%s-lock", id)
		revId, err := KeyMutexLock(ft.runtime, lockId, errorOnLocked)
		if err == nil {
			objCtx := ft.getContext(lockId)
			objCtx.SetByPath("__lock_rev_id", easyjson.NewJSON(revId))
			ft.setContext(lockId, objCtx)
			return nil
		}
		return err
	}
	typenameIDContextProcessor.ObjectMutexUnlock = func() error {
		lockId := fmt.Sprintf("%s-lock", id)

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
		ft.runtime.cacheStore.DeleteValue(lockId, true, -1, "")
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
	if msg.RequestCallback != nil {
		var replyData *easyjson.JSON = nil
		select {
		case replyData = <-replyDataChannel:
		case <-time.After(time.Duration(ft.runtime.config.requestTimeoutSec) * time.Second):
			replyData.SetByPath("status", easyjson.NewJSON("timeout"))
		}
		msg.RequestCallback(replyData)
	}

	/*if !ft.config.balanceNeeded { // Use context mutex lock if function type is not typename balanced
		system.MsgOnErrorReturn(ContextMutexUnlock(ft, id, lockRevisionID))
	}*/
	atomic.StoreInt64(&ft.runtime.glce, time.Now().UnixNano())
}

func (ft *FunctionType) gc(typenameIDLifetimeMs int) (garbageCollected int, handlersRunning int) {
	now := time.Now().UnixNano()

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
			// TODO: When to delete  function context??? function's context may be needed later!!!!
			// cacheStore.DeleteValue(ft.name+"."+id, true, -1, "") // Deleting function context
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
		/*if ft.config.balanced {
			ft.config.balanced = false
			system.MsgOnErrorReturn(FunctionTypeMutexUnlock(ft, ft.typenameLockRevisionID))
		}*/
	}

	return
}

func (ft *FunctionType) getContext(keyValueID string) *easyjson.JSON {
	if j, err := ft.runtime.cacheStore.GetValueAsJSON(keyValueID); err == nil {
		return j
	}
	j := easyjson.NewJSONObject()
	return &j
}

func (ft *FunctionType) setContext(keyValueID string, context *easyjson.JSON) {
	if context == nil {
		ft.runtime.cacheStore.SetValue(keyValueID, nil, true, -1, "")
	} else {
		ft.runtime.cacheStore.SetValue(keyValueID, context.ToBytes(), true, -1, "")
	}
}

func (ft *FunctionType) getStreamName() string {
	return fmt.Sprintf("%s_stream", system.GetHashStr(ft.subject))
}
