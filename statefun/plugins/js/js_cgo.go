

//go:build cgo

package js

import (
	"github.com/foliagecp/easyjson"
	lg "github.com/foliagecp/sdk/statefun/logger"

	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
	v8 "rogchap.com/v8go"
)

type CustomJSError struct {
	*v8.JSError
}

func (e *CustomJSError) Error() string {
	return e.Message
}

func (e *CustomJSError) GetLocation() string {
	return e.Location
}

func (e *CustomJSError) GetStackTrace() string {
	return e.StackTrace
}

type StatefunExecutorPluginJS struct {
	vw            *v8.Isolate
	vmContect     *v8.Context
	copiledScript *v8.UnboundScript
	buildError    error

	ctx *sfPlugins.StatefunContextProcessor
}

func StatefunExecutorPluginJSContructor(alias string, source string) sfPlugins.StatefunExecutor {
	sfejs := &StatefunExecutorPluginJS{}

	sfejs.vw = v8.NewIsolate() // creates a new JavaScript VM

	// () -> string
	statefunGetSelfTypenane := v8.NewFunctionTemplate(sfejs.vw, func(info *v8.FunctionCallbackInfo) *v8.Value {
		//lg.Logf("statefun_getSelfTypename: %v", info.Args()) // when the JS function is called this Go callback will execute
		if len(info.Args()) != 0 {
			lg.Logf(lg.ErrorLevel, "statefun_getSelfTypename requires no arguments but got %d", len(info.Args()))
			v, _ := v8.NewValue(sfejs.vw, nil)
			return v
		}
		v, _ := v8.NewValue(sfejs.vw, sfejs.ctx.Self.Typename)
		return v // you can return a value back to the JS caller if required
	})
	// () -> string
	statefunGetSelfID := v8.NewFunctionTemplate(sfejs.vw, func(info *v8.FunctionCallbackInfo) *v8.Value {
		//lg.Logf("statefun_getSelfId: %v", info.Args())
		if len(info.Args()) != 0 {
			lg.Logf(lg.ErrorLevel, "statefun_getSelfId requires no arguments but got %d", len(info.Args()))
			v, _ := v8.NewValue(sfejs.vw, nil)
			return v
		}
		v, _ := v8.NewValue(sfejs.vw, sfejs.ctx.Self.ID)
		return v
	})
	// () -> string
	statefunGetCallerTypenane := v8.NewFunctionTemplate(sfejs.vw, func(info *v8.FunctionCallbackInfo) *v8.Value {
		//lg.Logf("statefun_getCallerTypename: %v", info.Args()) // when the JS function is called this Go callback will execute
		if len(info.Args()) != 0 {
			lg.Logf(lg.ErrorLevel, "statefun_getCallerTypename requires no arguments but got %d", len(info.Args()))
			v, _ := v8.NewValue(sfejs.vw, nil)
			return v
		}
		v, _ := v8.NewValue(sfejs.vw, sfejs.ctx.Caller.Typename)
		return v // you can return a value back to the JS caller if required
	})
	// () -> string
	statefunGetCallerID := v8.NewFunctionTemplate(sfejs.vw, func(info *v8.FunctionCallbackInfo) *v8.Value {
		//lg.Logf("statefun_getCallerId: %v", info.Args())
		if len(info.Args()) != 0 {
			lg.Logf(lg.ErrorLevel, "statefun_getCallerId requires no arguments but got %d", len(info.Args()))
			v, _ := v8.NewValue(sfejs.vw, nil)
			return v
		}
		v, _ := v8.NewValue(sfejs.vw, sfejs.ctx.Caller.ID)
		return v
	})
	// () -> string
	statefunGetFunctionContext := v8.NewFunctionTemplate(sfejs.vw, func(info *v8.FunctionCallbackInfo) *v8.Value {
		//lg.Logf("statefun_getFunctionContext: %v", info.Args())
		if len(info.Args()) != 0 {
			lg.Logf(lg.ErrorLevel, "statefun_getFunctionContext requires no arguments but got %d", len(info.Args()))
			v, _ := v8.NewValue(sfejs.vw, nil)
			return v
		}
		v, _ := v8.NewValue(sfejs.vw, (*sfejs.ctx.GetFunctionContext()).ToString())
		return v
	})
	// (string) -> int
	statefunSetFunctionContext := v8.NewFunctionTemplate(sfejs.vw, func(info *v8.FunctionCallbackInfo) *v8.Value {
		//lg.Logf("statefun_setFunctionContext: %v", info.Args())
		if len(info.Args()) != 1 {
			lg.Logf(lg.ErrorLevel, "statefun_setFunctionContext requires 1 argument but got %d", len(info.Args()))
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
		sfejs.ctx.SetFunctionContext(&newContext)
		v, _ := v8.NewValue(sfejs.vw, int32(0))
		return v
	})
	// (string) -> int
	statefunSetRequestReplyData := v8.NewFunctionTemplate(sfejs.vw, func(info *v8.FunctionCallbackInfo) *v8.Value {
		//lg.Logf("statefun_setRequestReplyData: %v", info.Args())
		if len(info.Args()) != 1 {
			lg.Logf(lg.ErrorLevel, "statefun_setRequestReplyData requires 1 argument but got %d", len(info.Args()))
			v, _ := v8.NewValue(sfejs.vw, int32(1))
			return v
		}
		if !info.Args()[0].IsString() {
			v, _ := v8.NewValue(sfejs.vw, int32(2))
			return v
		}
		if sfejs.ctx.Reply == nil {
			v, _ := v8.NewValue(sfejs.vw, int32(3))
			return v
		}
		requestReplyDataStr := info.Args()[0].String()
		requestReplyData, ok := easyjson.JSONFromString(requestReplyDataStr)
		if !ok {
			v, _ := v8.NewValue(sfejs.vw, int32(4))
			return v
		}
		sfejs.ctx.Reply.With(&requestReplyData)
		v, _ := v8.NewValue(sfejs.vw, int32(0))
		return v
	})
	// () -> string
	statefunGetObjectContext := v8.NewFunctionTemplate(sfejs.vw, func(info *v8.FunctionCallbackInfo) *v8.Value {
		//lg.Logf("statefun_getObjectContext: %v", info.Args())
		if len(info.Args()) != 0 {
			lg.Logf(lg.ErrorLevel, "statefun_getObjectContext requires no arguments but got %d", len(info.Args()))
			v, _ := v8.NewValue(sfejs.vw, nil)
			return v
		}
		v, _ := v8.NewValue(sfejs.vw, (*sfejs.ctx.GetObjectContext()).ToString())
		return v
	})
	// (string) -> int
	statefunSetObjectContext := v8.NewFunctionTemplate(sfejs.vw, func(info *v8.FunctionCallbackInfo) *v8.Value {
		//lg.Logf("statefun_setObjectContext: %v", info.Args())
		if len(info.Args()) != 1 {
			lg.Logf(lg.ErrorLevel, "statefun_setObjectContext requires 1 argument but got %d", len(info.Args()))
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
		sfejs.ctx.SetObjectContext(&newContext)
		v, _ := v8.NewValue(sfejs.vw, int32(0))
		return v
	})
	// () -> string
	statefunGetPayload := v8.NewFunctionTemplate(sfejs.vw, func(info *v8.FunctionCallbackInfo) *v8.Value {
		//lg.Logf("statefun_getPayload: %v", info.Args())
		if len(info.Args()) != 0 {
			lg.Logf(lg.ErrorLevel, "statefun_getPayload requires no arguments but got %d", len(info.Args()))
			v, _ := v8.NewValue(sfejs.vw, nil)
			return v
		}
		v, _ := v8.NewValue(sfejs.vw, sfejs.ctx.Payload.ToString())
		return v
	})
	// () -> string
	statefunGetOptions := v8.NewFunctionTemplate(sfejs.vw, func(info *v8.FunctionCallbackInfo) *v8.Value {
		//lg.Logf("statefun_getOptions: %v", info.Args())
		if len(info.Args()) != 0 {
			lg.Logf(lg.ErrorLevel, "statefun_getOptions requires no arguments but got %d", len(info.Args()))
			v, _ := v8.NewValue(sfejs.vw, nil)
			return v
		}
		v, _ := v8.NewValue(sfejs.vw, sfejs.ctx.Options.ToString())
		return v
	})
	// (int, string, string, string, string) -> int
	statefunSignal := v8.NewFunctionTemplate(sfejs.vw, func(info *v8.FunctionCallbackInfo) *v8.Value {
		//lg.Logf("statefun_signal: %v", info.Args())
		if len(info.Args()) != 5 {
			lg.Logf(lg.ErrorLevel, "statefun_signal requires 5 argument but got %d", len(info.Args()))
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
						lg.Logf(lg.ErrorLevel, "statefun_signal options is not empty and not a JSON: %s", info.Args()[4].String())
						v, _ := v8.NewValue(sfejs.vw, int32(4))
						return v
					}
				}
				system.MsgOnErrorReturn(sfejs.ctx.Signal(
					sfPlugins.SignalProvider(info.Args()[0].Int32()),
					info.Args()[1].String(),
					info.Args()[2].String(),
					&j,
					options,
				))
				v, _ := v8.NewValue(sfejs.vw, int32(0))
				return v
			}
			lg.Logf(lg.ErrorLevel, "statefun_signal payload is not a JSON: %s", info.Args()[2].String())
			v, _ := v8.NewValue(sfejs.vw, int32(3))
			return v
		}
		v, _ := v8.NewValue(sfejs.vw, int32(2))
		return v
	})
	// (int, string, string, string, string) -> int|string
	statefunRequest := v8.NewFunctionTemplate(sfejs.vw, func(info *v8.FunctionCallbackInfo) *v8.Value {
		//lg.Logf("statefun_request: %v", info.Args())
		if len(info.Args()) != 5 {
			lg.Logf(lg.ErrorLevel, "statefun_request requires 5 argument but got %d", len(info.Args()))
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
						lg.Logf(lg.ErrorLevel, "statefun_request options is not empty and not a JSON: %s", info.Args()[4].String())
						v, _ := v8.NewValue(sfejs.vw, int32(4))
						return v
					}
				}
				j, err := sfejs.ctx.Request(
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
			lg.Logf(lg.ErrorLevel, "statefun_request payload is not a JSON: %s", info.Args()[2].String())
			v, _ := v8.NewValue(sfejs.vw, int32(3))
			return v
		}
		v, _ := v8.NewValue(sfejs.vw, int32(2))
		return v
	})
	// (int, string) -> int
	statefunEgress := v8.NewFunctionTemplate(sfejs.vw, func(info *v8.FunctionCallbackInfo) *v8.Value {
		//lg.Logf("statefun_egress: %v", info.Args())
		if len(info.Args()) != 2 {
			lg.Logf(lg.ErrorLevel, "statefun_egress requires 2 argument but got %d", len(info.Args()))
			v, _ := v8.NewValue(sfejs.vw, int32(1))
			return v
		}
		if info.Args()[0].IsInt32() && info.Args()[1].IsString() {
			if j, ok := easyjson.JSONFromString(info.Args()[1].String()); ok {
				system.MsgOnErrorReturn(sfejs.ctx.Egress(
					sfPlugins.EgressProvider(info.Args()[0].Int32()),
					&j,
				))
				v, _ := v8.NewValue(sfejs.vw, int32(0))
				return v
			}
			lg.Logf(lg.ErrorLevel, "statefun_egress payload is not a JSON: %s", info.Args()[2].String())
			v, _ := v8.NewValue(sfejs.vw, int32(3))
			return v
		}
		v, _ := v8.NewValue(sfejs.vw, int32(2))
		return v
	})
	// (string)
	print := v8.NewFunctionTemplate(sfejs.vw, func(info *v8.FunctionCallbackInfo) *v8.Value {
		lg.Logf(lg.InfoLevel, "%s: %v", alias, info.Args())
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
	system.MsgOnErrorReturn(global.Set("statefun_egress", statefunEgress))
	system.MsgOnErrorReturn(global.Set("print", print))

	s, e := sfejs.vw.CompileUnboundScript(source, alias, v8.CompileOptions{}) // compile script to get cached data

	var err *CustomJSError
	if e != nil {
		err = &CustomJSError{e.(*v8.JSError)}
	}

	sfejs.vmContect = v8.NewContext(sfejs.vw, global) // new context within the VM
	sfejs.copiledScript = s
	sfejs.buildError = err

	return sfejs
}

func (sfejs *StatefunExecutorPluginJS) Run(ctx *sfPlugins.StatefunContextProcessor) error {
	sfejs.ctx = ctx
	_, e := sfejs.copiledScript.Run(sfejs.vmContect)
	var err *CustomJSError
	if e != nil {
		err = &CustomJSError{e.(*v8.JSError)}
	}

	return err
}

func (sfejs *StatefunExecutorPluginJS) BuildError() error {
	return sfejs.buildError
}
