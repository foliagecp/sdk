// Copyright 2023 NJWS Inc.

package js

import (
	"github.com/foliagecp/easyjson"
	lg "github.com/foliagecp/sdk/statefun/logger"

	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
	v8 "rogchap.com/v8go"
)

type StatefunExecutorPluginJS struct {
	vw            *v8.Isolate
	vmContect     *v8.Context
	copiledScript *v8.UnboundScript
	buildError    error

	contextProcessor *sfPlugins.StatefunContextProcessor
}

func StatefunExecutorPluginJSContructor(alias string, source string) sfPlugins.StatefunExecutor {
	sfejs := &StatefunExecutorPluginJS{}

	sfejs.vw = v8.NewIsolate() // creates a new JavaScript VM

	// () -> string
	statefunGetSelfTypenane := v8.NewFunctionTemplate(sfejs.vw, func(info *v8.FunctionCallbackInfo) *v8.Value {
		//lg.Logf("statefun_getSelfTypename: %v\n", info.Args()) // when the JS function is called this Go callback will execute
		if len(info.Args()) != 0 {
			lg.Logf(lg.ErrorLevel, "statefun_getSelfTypename requires no arguments but got %d\n", len(info.Args()))
			v, _ := v8.NewValue(sfejs.vw, nil)
			return v
		}
		v, _ := v8.NewValue(sfejs.vw, sfejs.contextProcessor.Self.Typename)
		return v // you can return a value back to the JS caller if required
	})
	// () -> string
	statefunGetSelfID := v8.NewFunctionTemplate(sfejs.vw, func(info *v8.FunctionCallbackInfo) *v8.Value {
		//lg.Logf("statefun_getSelfId: %v\n", info.Args())
		if len(info.Args()) != 0 {
			lg.Logf(lg.ErrorLevel, "statefun_getSelfId requires no arguments but got %d\n", len(info.Args()))
			v, _ := v8.NewValue(sfejs.vw, nil)
			return v
		}
		v, _ := v8.NewValue(sfejs.vw, sfejs.contextProcessor.Self.ID)
		return v
	})
	// () -> string
	statefunGetCallerTypenane := v8.NewFunctionTemplate(sfejs.vw, func(info *v8.FunctionCallbackInfo) *v8.Value {
		//lg.Logf("statefun_getCallerTypename: %v\n", info.Args()) // when the JS function is called this Go callback will execute
		if len(info.Args()) != 0 {
			lg.Logf(lg.ErrorLevel, "statefun_getCallerTypename requires no arguments but got %d\n", len(info.Args()))
			v, _ := v8.NewValue(sfejs.vw, nil)
			return v
		}
		v, _ := v8.NewValue(sfejs.vw, sfejs.contextProcessor.Caller.Typename)
		return v // you can return a value back to the JS caller if required
	})
	// () -> string
	statefunGetCallerID := v8.NewFunctionTemplate(sfejs.vw, func(info *v8.FunctionCallbackInfo) *v8.Value {
		//lg.Logf("statefun_getCallerId: %v\n", info.Args())
		if len(info.Args()) != 0 {
			lg.Logf(lg.ErrorLevel, "statefun_getCallerId requires no arguments but got %d\n", len(info.Args()))
			v, _ := v8.NewValue(sfejs.vw, nil)
			return v
		}
		v, _ := v8.NewValue(sfejs.vw, sfejs.contextProcessor.Caller.ID)
		return v
	})
	// () -> string
	statefunGetFunctionContext := v8.NewFunctionTemplate(sfejs.vw, func(info *v8.FunctionCallbackInfo) *v8.Value {
		//lg.Logf("statefun_getFunctionContext: %v\n", info.Args())
		if len(info.Args()) != 0 {
			lg.Logf(lg.ErrorLevel, "statefun_getFunctionContext requires no arguments but got %d\n", len(info.Args()))
			v, _ := v8.NewValue(sfejs.vw, nil)
			return v
		}
		v, _ := v8.NewValue(sfejs.vw, (*sfejs.contextProcessor.GetFunctionContext()).ToString())
		return v
	})
	// (string) -> int
	statefunSetFunctionContext := v8.NewFunctionTemplate(sfejs.vw, func(info *v8.FunctionCallbackInfo) *v8.Value {
		//lg.Logf("statefun_setFunctionContext: %v\n", info.Args())
		if len(info.Args()) != 1 {
			lg.Logf(lg.ErrorLevel, "statefun_setFunctionContext requires 1 argument but got %d\n", len(info.Args()))
			v, _ := v8.NewValue(sfejs.vw, int32(1))
			return v
		}
		if !info.Args()[0].IsString() {
			v, _ := v8.NewValue(sfejs.vw, int32(2))
			return v
		}

		newContextStr := info.Args()[0].String()
		newContext, ok := easyjson.JSONFromString(newContextStr)
		if !ok {
			v, _ := v8.NewValue(sfejs.vw, int32(3))
			return v
		}
		sfejs.contextProcessor.SetFunctionContext(&newContext)
		v, _ := v8.NewValue(sfejs.vw, int32(0))
		return v
	})
	// (string) -> int
	statefunSetRequestReplyData := v8.NewFunctionTemplate(sfejs.vw, func(info *v8.FunctionCallbackInfo) *v8.Value {
		//lg.Logf("statefun_setRequestReplyData: %v\n", info.Args())
		if len(info.Args()) != 1 {
			lg.Logf(lg.ErrorLevel, "statefun_setRequestReplyData requires 1 argument but got %d\n", len(info.Args()))
			v, _ := v8.NewValue(sfejs.vw, int32(1))
			return v
		}
		if !info.Args()[0].IsString() {
			v, _ := v8.NewValue(sfejs.vw, int32(2))
			return v
		}
		if sfejs.contextProcessor.Reply == nil {
			v, _ := v8.NewValue(sfejs.vw, int32(3))
			return v
		}
		requestReplyDataStr := info.Args()[0].String()
		requestReplyData, ok := easyjson.JSONFromString(requestReplyDataStr)
		if !ok {
			v, _ := v8.NewValue(sfejs.vw, int32(4))
			return v
		}
		sfejs.contextProcessor.Reply.With(&requestReplyData)
		v, _ := v8.NewValue(sfejs.vw, int32(0))
		return v
	})
	// () -> string
	statefunGetObjectContext := v8.NewFunctionTemplate(sfejs.vw, func(info *v8.FunctionCallbackInfo) *v8.Value {
		//lg.Logf("statefun_getObjectContext: %v\n", info.Args())
		if len(info.Args()) != 0 {
			lg.Logf(lg.ErrorLevel, "statefun_getObjectContext requires no arguments but got %d\n", len(info.Args()))
			v, _ := v8.NewValue(sfejs.vw, nil)
			return v
		}
		v, _ := v8.NewValue(sfejs.vw, (*sfejs.contextProcessor.GetObjectContext()).ToString())
		return v
	})
	// (string) -> int
	statefunSetObjectContext := v8.NewFunctionTemplate(sfejs.vw, func(info *v8.FunctionCallbackInfo) *v8.Value {
		//lg.Logf("statefun_setObjectContext: %v\n", info.Args())
		if len(info.Args()) != 1 {
			lg.Logf(lg.ErrorLevel, "statefun_setObjectContext requires 1 argument but got %d\n", len(info.Args()))
			v, _ := v8.NewValue(sfejs.vw, int32(1))
			return v
		}
		if !info.Args()[0].IsString() {
			v, _ := v8.NewValue(sfejs.vw, int32(2))
			return v
		}

		newContextStr := info.Args()[0].String()
		newContext, ok := easyjson.JSONFromString(newContextStr)
		if !ok {
			v, _ := v8.NewValue(sfejs.vw, int32(3))
			return v
		}
		sfejs.contextProcessor.SetObjectContext(&newContext)
		v, _ := v8.NewValue(sfejs.vw, int32(0))
		return v
	})
	// () -> string
	statefunGetPayload := v8.NewFunctionTemplate(sfejs.vw, func(info *v8.FunctionCallbackInfo) *v8.Value {
		//lg.Logf("statefun_getPayload: %v", info.Args())
		if len(info.Args()) != 0 {
			lg.Logf(lg.ErrorLevel, "statefun_getPayload requires no arguments but got %d\n", len(info.Args()))
			v, _ := v8.NewValue(sfejs.vw, nil)
			return v
		}
		v, _ := v8.NewValue(sfejs.vw, sfejs.contextProcessor.Payload.ToString())
		return v
	})
	// () -> string
	statefunGetOptions := v8.NewFunctionTemplate(sfejs.vw, func(info *v8.FunctionCallbackInfo) *v8.Value {
		//lg.Logf("statefun_getOptions: %v", info.Args())
		if len(info.Args()) != 0 {
			lg.Logf(lg.ErrorLevel, "statefun_getOptions requires no arguments but got %d\n", len(info.Args()))
			v, _ := v8.NewValue(sfejs.vw, nil)
			return v
		}
		v, _ := v8.NewValue(sfejs.vw, sfejs.contextProcessor.Options.ToString())
		return v
	})
	// (int, string, string, string, string) -> int
	statefunSignal := v8.NewFunctionTemplate(sfejs.vw, func(info *v8.FunctionCallbackInfo) *v8.Value {
		//lg.Logf("statefun_signal: %v\n", info.Args())
		if len(info.Args()) != 5 {
			lg.Logf(lg.ErrorLevel, "statefun_signal requires 5 argument but got %d\n", len(info.Args()))
			v, _ := v8.NewValue(sfejs.vw, int32(1))
			return v
		}
		if info.Args()[0].IsInt32() && info.Args()[1].IsString() && info.Args()[2].IsString() && info.Args()[3].IsString() && info.Args()[4].IsString() {
			if j, ok := easyjson.JSONFromString(info.Args()[3].String()); ok {
				var options *easyjson.JSON = nil
				if len(info.Args()[4].String()) > 0 {
					if o, ok := easyjson.JSONFromString(info.Args()[4].String()); ok {
						options = &o
					} else {
						lg.Logf(lg.ErrorLevel, "statefunSignal options is not empty and not a JSON: %s\n", info.Args()[4].String())
						v, _ := v8.NewValue(sfejs.vw, int32(4))
						return v
					}
				}
				system.MsgOnErrorReturn(sfejs.contextProcessor.Signal(
					sfPlugins.SignalProvider(info.Args()[0].Int32()),
					info.Args()[1].String(),
					info.Args()[2].String(),
					&j,
					options,
				))
				v, _ := v8.NewValue(sfejs.vw, int32(0))
				return v
			}
			lg.Logf(lg.ErrorLevel, "statefunSignal payload is not a JSON: %s\n", info.Args()[2].String())
			v, _ := v8.NewValue(sfejs.vw, int32(3))
			return v
		}
		v, _ := v8.NewValue(sfejs.vw, int32(2))
		return v
	})
	// (int, string, string, string, string) -> int|string
	statefunRequest := v8.NewFunctionTemplate(sfejs.vw, func(info *v8.FunctionCallbackInfo) *v8.Value {
		//lg.Logf("statefun_request: %v\n", info.Args())
		if len(info.Args()) != 5 {
			lg.Logf(lg.ErrorLevel, "statefun_request requires 5 argument but got %d\n", len(info.Args()))
			v, _ := v8.NewValue(sfejs.vw, int32(1))
			return v
		}
		if info.Args()[0].IsInt32() && info.Args()[1].IsString() && info.Args()[2].IsString() && info.Args()[3].IsString() && info.Args()[4].IsString() {
			if j, ok := easyjson.JSONFromString(info.Args()[3].String()); ok {
				var options *easyjson.JSON = nil
				if len(info.Args()[4].String()) > 0 {
					if o, ok := easyjson.JSONFromString(info.Args()[4].String()); ok {
						options = &o
					} else {
						lg.Logf(lg.ErrorLevel, "statefunRequest options is not empty and not a JSON: %s\n", info.Args()[4].String())
						v, _ := v8.NewValue(sfejs.vw, int32(4))
						return v
					}
				}
				j, err := sfejs.contextProcessor.Request(
					sfPlugins.RequestProvider(info.Args()[0].Int32()),
					info.Args()[1].String(),
					info.Args()[2].String(),
					&j,
					options,
				)
				if err != nil {
					v, _ := v8.NewValue(sfejs.vw, int32(5))
					return v
				}
				v, _ := v8.NewValue(sfejs.vw, j.ToString())
				return v
			}
			lg.Logf(lg.ErrorLevel, "statefunRequest payload is not a JSON: %s\n", info.Args()[2].String())
			v, _ := v8.NewValue(sfejs.vw, int32(3))
			return v
		}
		v, _ := v8.NewValue(sfejs.vw, int32(2))
		return v
	})
	// (string)
	print := v8.NewFunctionTemplate(sfejs.vw, func(info *v8.FunctionCallbackInfo) *v8.Value {
		lg.Logf(lg.InfoLevel, "%s: %v\n", alias, info.Args())
		return nil
	})

	global := v8.NewObjectTemplate(sfejs.vw)
	system.MsgOnErrorReturn(global.Set("statefun_getSelfTypename", statefunGetSelfTypenane))
	system.MsgOnErrorReturn(global.Set("statefun_getSelfId", statefunGetSelfID))
	system.MsgOnErrorReturn(global.Set("statefun_getCallerTypename", statefunGetCallerTypenane))
	system.MsgOnErrorReturn(global.Set("statefun_getCallerId", statefunGetCallerID))
	system.MsgOnErrorReturn(global.Set("statefun_getFunctionContext", statefunGetFunctionContext))
	system.MsgOnErrorReturn(global.Set("statefun_getObjectContext", statefunGetObjectContext))
	system.MsgOnErrorReturn(global.Set("statefun_getPayload", statefunGetPayload))
	system.MsgOnErrorReturn(global.Set("statefun_getOptions", statefunGetOptions))

	system.MsgOnErrorReturn(global.Set("statefun_setObjectContext", statefunSetObjectContext))
	system.MsgOnErrorReturn(global.Set("statefun_setFunctionContext", statefunSetFunctionContext))
	system.MsgOnErrorReturn(global.Set("statefun_setRequestReplyData", statefunSetRequestReplyData))

	system.MsgOnErrorReturn(global.Set("statefun_signal", statefunSignal))
	system.MsgOnErrorReturn(global.Set("statefun_request", statefunRequest))
	system.MsgOnErrorReturn(global.Set("print", print))

	sfejs.vmContect = v8.NewContext(sfejs.vw, global)                                                         // new context within the VM
	sfejs.copiledScript, sfejs.buildError = sfejs.vw.CompileUnboundScript(source, alias, v8.CompileOptions{}) // compile script to get cached data

	return sfejs
}

func (sfejs *StatefunExecutorPluginJS) Run(contextProcessor *sfPlugins.StatefunContextProcessor) error {
	sfejs.contextProcessor = contextProcessor
	_, err := sfejs.copiledScript.Run(sfejs.vmContect)
	return err
}

func (sfejs *StatefunExecutorPluginJS) BuildError() error {
	return sfejs.buildError
}
