package statefun

import (
	"context"
	"fmt"
	"strings"
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

	idKeyMutex            system.KeyMutex
	idHandlersLastMsgTime sync.Map
	contextProcessors     sync.Map

	executor      *sfPlugins.TypenameExecutorPlugin
	resourceMutex sync.Mutex

	sfWorkerPool *SFWorkerPool
}

const (
	contextExpirationKey = "____ctx_expires_after_ms"
)

func NewFunctionType(runtime *Runtime, name string, logicHandler FunctionLogicHandler, config FunctionTypeConfig) *FunctionType {
	ft := &FunctionType{
		runtime:      runtime,
		name:         name,
		subject:      fmt.Sprintf(DomainIngressSubjectsTmpl, runtime.Domain.name, fmt.Sprintf("%s.%s.%s.%s", SignalPrefix, runtime.Domain.name, name, "*")),
		logicHandler: logicHandler,
		idKeyMutex:   system.NewKeyMutex(),
		config:       config,
	}
	ft.sfWorkerPool.ft = ft
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

	ft.idHandlersLastMsgTime.Store(id, time.Now().UnixNano())
	if ft.executor != nil {
		ft.executor.AddForID(id)
	}

	task := SFWorkerTask{
		Msg: SFWorkerMessage{
			ID:   id,
			Data: msg,
		},
	}

	if err := ft.sfWorkerPool.Submit(task, time.Duration(ft.config.msgAckWaitMs)*time.Millisecond); err != nil {
		logger.Logf(logger.ErrorLevel, "task refuse for statefun %s with id=%s, cannot accept message: %s", ft.name, id, err.Error())
		msg.RefusalCallback()
	}
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

			ft.idHandlersLastMsgTime.Delete(id)
			ft.contextProcessors.Delete(id)
			if ft.executor != nil {
				ft.executor.RemoveForID(id)
			}

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
