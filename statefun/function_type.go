// Copyright 2023 NJWS Inc.

package statefun

import (
	"fmt"
	"json_easy"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	sfPluginJS "github.com/foliagecp/sdk/statefun/plugins/js"
	"github.com/foliagecp/sdk/statefun/system"

	"github.com/nats-io/nats.go"
)

type GoMsg struct {
	ResultJSONChannel chan *json_easy.JSON
	Caller            *sfPlugins.StatefunAddress
	Payload           *json_easy.JSON
	Options           *json_easy.JSON
}

type FunctionHandler func(sfPlugins.StatefunExecutor, *sfPlugins.StatefunContextProcessor)

type FunctionType struct {
	runtime                *Runtime
	name                   string
	subject                string
	config                 *FunctionTypeConfig
	handler                FunctionHandler
	idHandlersChannel      sync.Map
	idHandlersLastMsgTime  sync.Map
	typenameLockRevisionID uint64
	executor               *sfPlugins.TypenameExecutorPlugin
}

func NewFunctionType(runtime *Runtime, name string, handler FunctionHandler, config *FunctionTypeConfig) *FunctionType {
	ft := &FunctionType{
		runtime: runtime,
		name:    name,
		subject: name + ".*",
		handler: handler,
		config:  config,
	}
	runtime.registeredFunctionTypes[ft.name] = ft
	return ft
}

func (ft *FunctionType) Start(streamName string) error {
	consumerName := strings.ReplaceAll(ft.name, ".", "")
	consumerGroup := consumerName + "-group"
	fmt.Printf("Handling function type %s\n", ft.name)

	// Create stream consumer if does not exist ---------------------
	consumerExists := false
	for info := range ft.runtime.js.Consumers(streamName, nats.MaxWait(10*time.Second)) {
		if info.Name == consumerName {
			consumerExists = true
		}
	}
	if !consumerExists {
		_, err := ft.runtime.js.AddConsumer(streamName, &nats.ConsumerConfig{
			Name:           consumerName,
			Durable:        "event-processor",
			DeliverSubject: ft.subject,
			DeliverGroup:   consumerGroup,
			AckPolicy:      nats.AckExplicitPolicy,
			AckWait:        time.Duration(ft.config.msgAckWaitMs) * time.Millisecond, // AckWait should be long due to async message Ack
		})
		system.MsgOnErrorReturn(err)
	}
	// --------------------------------------------------------------

	_, err := ft.runtime.js.QueueSubscribe(
		ft.subject,
		consumerGroup,
		func(msg *nats.Msg) {
			system.MsgOnErrorReturn(ft.handleMsg(msg))
		},
		//nats.Bind(STREAM_NAME, consumerName),
	)
	if err != nil {
		fmt.Printf("Invalid subscription for function type %s: %s\n", ft.name, err)
		return err
	}
	return nil
}

func (ft *FunctionType) SetExecutor(alias string, content string, constructor func(alias string, source string) sfPlugins.StatefunExecutor) error {
	ft.executor = sfPlugins.NewTypenameExecutor(alias, content, sfPluginJS.StatefunExecutorPluginJSContructor)
	return nil
}

func (ft *FunctionType) handleMsg(msg *nats.Msg) (err error) {
	// After message was received do typename balance if the one is needed and hasn't been done yet -------
	if ft.config.balanceNeeded {
		if !ft.config.balanced {
			ft.typenameLockRevisionID, err = FunctionTypeMutexLock(ft, true)
			if err != nil {
				system.MsgOnErrorReturn(msg.Nak())
				fmt.Printf("WARNING: function with type %s has received a message, but this typename was already locked! Skipping message...\n", ft.name)
				// Preventing from rapidly calling this function over and over again if no function
				// in other runtime that can handle this message and kv mutex is already dead
				time.Sleep(time.Duration(ft.config.msgAckWaitMs) * time.Millisecond)
				return
			}
			ft.config.balanced = true
		}
	}
	// ----------------------------------------------------------------------------------------------------

	gc := atomic.LoadInt64(&ft.runtime.gc)

	if gc == 0 {
		now := time.Now().UnixNano()
		atomic.StoreInt64(&ft.runtime.glce, now)
		atomic.StoreInt64(&ft.runtime.gt0, now)
	}
	atomic.AddInt64(&ft.runtime.gc, 1)

	tokens := strings.Split(msg.Subject, ".")
	id := tokens[len(tokens)-1]

	ft.sendMsgToIDHandler(id, msg, func() {
		atomic.AddInt64(&ft.runtime.gc, -1)
		system.MsgOnErrorReturn(msg.Nak()) // Typename id handler is full for current id, NAK message to contunue processing other ids for this typename
	})

	return
}

func (ft *FunctionType) sendMsgToIDHandler(id string, msg interface{}, onChannelFullCallback func()) {
	// Send msg to type id handler ------------------------------
	var msgChannel chan interface{}
	if value, ok := ft.idHandlersChannel.Load(id); ok {
		msgChannel = value.(chan interface{})
	} else {
		msgChannel = make(chan interface{}, ft.config.msgChannelSize)
		go ft.idHandler(id, msgChannel)
		ft.idHandlersChannel.Store(id, msgChannel)
		if ft.executor != nil {
			ft.executor.AddForID(id)
		}
	}
	ft.idHandlersLastMsgTime.Store(id, time.Now().UnixNano())

	if onChannelFullCallback == nil {
		msgChannel <- msg
	} else {
		select {
		case msgChannel <- msg:
			// Doing nothing
		default:
			onChannelFullCallback()
		}
	}
	// ----------------------------------------------------------
}

func (ft *FunctionType) idHandler(id string, msgChannel chan interface{}) {
	// For idHandlerNatsMsg msg ---------------------------
	msgAcker := func(msgChannel chan *nats.Msg) {
		for msg := range msgChannel {
			if msg == nil {
				return
			}
			system.MsgOnErrorReturn(msg.Ack())
		}
	}
	msgChannelFinal := make(chan *nats.Msg, ft.config.msgAckChannelSize)
	go msgAcker(msgChannelFinal)
	// ----------------------------------------------------

	functionTypeIDContextProcessor := sfPlugins.StatefunContextProcessor{
		GlobalCache:        ft.runtime.cacheStore,
		GetFunctionContext: func() *json_easy.JSON { return ft.getContext(ft.name + "." + id) },
		SetFunctionContext: func(context *json_easy.JSON) { ft.setContext(ft.name+"."+id, context) },
		GetObjectContext:   func() *json_easy.JSON { return ft.getContext(id) },
		SetObjectContext:   func(context *json_easy.JSON) { ft.setContext(id, context) },
		GolangCallSync: func(targetTypename string, targetID string, payload *json_easy.JSON, options *json_easy.JSON) *json_easy.JSON {
			return ft.runtime.callFunctionGolangSync(ft.name, id, targetTypename, targetID, payload, options)
		},
		Self:   sfPlugins.StatefunAddress{Typename: ft.name, ID: id},
		Egress: ft.egress,
		// To be assigned later:
		// Call: ...
		// Payload: ...
		// Options: ... // Otions from initial typename declaration will be merged and overwritten by the incoming one in message
		// Caller: ...
	}

	for msg := range msgChannel {
		if msg == nil {
			msgChannelFinal <- nil
			return
		}

		switch m := msg.(type) {
		case *nats.Msg:
			ft.idHandlerNatsMsg(id, m, &functionTypeIDContextProcessor, msgChannelFinal)
		case *GoMsg:
			ft.idHandlerGoMsg(id, m, &functionTypeIDContextProcessor)
		}
	}
}

func (ft *FunctionType) idHandlerNatsMsg(id string, msg *nats.Msg, functionTypeIDContextProcessor *sfPlugins.StatefunContextProcessor, msgChannelFinal chan *nats.Msg) {
	var lockRevisionID uint64 = 0
	if !ft.config.balanceNeeded { // Use context mutex lock if function type is not typename balanced
		var err error
		lockRevisionID, err = ContextMutexLock(ft, id, false)
		if err != nil {
			system.MsgOnErrorReturn(msg.Nak())
			return
		}
	}

	var data *json_easy.JSON
	if j, ok := json_easy.JSONFromBytes(msg.Data); ok {
		data = &j

		var payload *json_easy.JSON
		if data != nil && data.GetByPath("payload").IsObject() {
			j := data.GetByPath("payload")
			payload = &j
		} else {
			j := json_easy.NewJSONObject()
			payload = &j
		}

		var msgOptions *json_easy.JSON
		if data != nil && data.GetByPath("options").IsObject() {
			msgOptions = data.GetByPath("options").GetPtr()
		} else {
			msgOptions = json_easy.NewJSONObject().GetPtr()
		}

		caller := sfPlugins.StatefunAddress{}
		if data.GetByPath("caller_typename").IsString() && data.GetByPath("caller_id").IsString() {
			caller.Typename, _ = data.GetByPath("caller_typename").AsString()
			caller.ID, _ = data.GetByPath("caller_id").AsString()
		}

		functionTypeIDContextProcessor.Call = func(targetTypename string, targetID string, j *json_easy.JSON, o *json_easy.JSON) {
			ft.runtime.callFunction(ft.name, id, targetTypename, targetID, j, o)
		}
		functionTypeIDContextProcessor.Payload = payload
		functionTypeIDContextProcessor.Options = ft.config.options
		if msgOptions != nil {
			functionTypeIDContextProcessor.Options.DeepMerge(*msgOptions)
		}
		functionTypeIDContextProcessor.Caller = caller

		// Calling typename handler function --------------------
		if ft.executor != nil {
			ft.handler(ft.executor.GetForID(id), functionTypeIDContextProcessor)
		} else {
			ft.handler(nil, functionTypeIDContextProcessor)
		}
		// ------------------------------------------------------
	} else {
		fmt.Printf("Data for function %s with id=%s is not a JSON\n", ft.name, id)
	}

	msgChannelFinal <- msg

	if !ft.config.balanceNeeded { // Use context mutex lock if function type is not typename balanced
		system.MsgOnErrorReturn(ContextMutexUnlock(ft, id, lockRevisionID))
	}
	atomic.StoreInt64(&ft.runtime.glce, time.Now().UnixNano())
}

func (ft *FunctionType) idHandlerGoMsg(id string, msg *GoMsg, functionTypeIDContextProcessor *sfPlugins.StatefunContextProcessor) {
	functionTypeIDContextProcessor.Call = func(targetTypename string, targetID string, j *json_easy.JSON, o *json_easy.JSON) {
		if msg.Caller.Typename == targetTypename && msg.Caller.ID == targetID {
			msg.ResultJSONChannel <- j
		} else {
			ft.runtime.callFunction(ft.name, id, targetTypename, targetID, j, o)
		}
	}
	functionTypeIDContextProcessor.Payload = msg.Payload
	functionTypeIDContextProcessor.Options = ft.config.options
	if msg.Options != nil {
		functionTypeIDContextProcessor.Options.DeepMerge(*msg.Options)
	}
	functionTypeIDContextProcessor.Caller = *msg.Caller

	if ft.executor != nil {
		ft.handler(ft.executor.GetForID(id), functionTypeIDContextProcessor)
	} else {
		ft.handler(nil, functionTypeIDContextProcessor)
	}
}

func (ft *FunctionType) gc(functionTypeIDLifetimeMs int) (garbageCollected int, handlersRunning int) {
	now := time.Now().UnixNano()

	ft.idHandlersLastMsgTime.Range(func(key, value any) bool {
		id := key.(string)
		lastMsgTime := value.(int64)
		if lastMsgTime+int64(functionTypeIDLifetimeMs)*int64(time.Millisecond) < now {
			v, _ := ft.idHandlersChannel.Load(id)
			msgChannel := v.(chan interface{})
			msgChannel <- nil
			ft.idHandlersChannel.Delete(id)
			ft.idHandlersLastMsgTime.Delete(id)
			if ft.executor != nil {
				ft.executor.RemoveForID(id)
			}
			// TODO: When to delete  function context??? function's context may be needed later!!!!
			// cacheStore.DeleteValue(ft.name+"."+id, true, -1, "") // Deleting function context
			garbageCollected++
			//fmt.Printf(">>>>>>>>>>>>>> Garbage collected handler for %s:%s\n", ft.name, id)
		} else {
			handlersRunning++
		}
		return true
	})
	if garbageCollected > 0 && handlersRunning == 0 {
		fmt.Printf(">>>>>>>>>>>>>> Garbage collected for typename %s - no id handlers left\n", ft.name)
		if ft.config.balanced {
			ft.config.balanced = false
			system.MsgOnErrorReturn(FunctionTypeMutexUnlock(ft, ft.typenameLockRevisionID))
		}
	}

	return
}

func (ft *FunctionType) getContext(keyValueID string) *json_easy.JSON {
	if j, err := ft.runtime.cacheStore.GetValueAsJSON(keyValueID); err == nil {
		return j
	}
	j := json_easy.NewJSONObject()
	return &j
}

func (ft *FunctionType) setContext(keyValueID string, context *json_easy.JSON) {
	if context == nil {
		ft.runtime.cacheStore.SetValue(keyValueID, nil, true, -1, "")
	} else {
		ft.runtime.cacheStore.SetValue(keyValueID, context.ToBytes(), true, -1, "")
	}
}

func (ft *FunctionType) egress(natsTopic string, payload *json_easy.JSON) {
	go func() {
		system.MsgOnErrorReturn(ft.runtime.nc.Publish(natsTopic, payload.ToBytes()))
	}()
}
