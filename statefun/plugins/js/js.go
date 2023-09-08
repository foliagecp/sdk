// Copyright 2023 NJWS Inc.

package js

import (
	"fmt"
	"json_easy"

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
		//fmt.Printf("statefun_getSelfTypename: %v\n", info.Args()) // when the JS function is called this Go callback will execute
		if len(info.Args()) != 0 {
			fmt.Printf("statefun_getSelfTypename error: requies no arguments but got %d\n", len(info.Args()))
			v, _ := v8.NewValue(sfejs.vw, nil)
			return v
		}
		v, _ := v8.NewValue(sfejs.vw, sfejs.contextProcessor.Self.Typename)
		return v // you can return a value back to the JS caller if required
	})
	// () -> string
	statefunGetSelfID := v8.NewFunctionTemplate(sfejs.vw, func(info *v8.FunctionCallbackInfo) *v8.Value {
		//fmt.Printf("statefun_getSelfId: %v\n", info.Args())
		if len(info.Args()) != 0 {
			fmt.Printf("statefun_getSelfId error: requies no arguments but got %d\n", len(info.Args()))
			v, _ := v8.NewValue(sfejs.vw, nil)
			return v
		}
		v, _ := v8.NewValue(sfejs.vw, sfejs.contextProcessor.Self.ID)
		return v
	})
	// () -> string
	statefunGetCallerTypenane := v8.NewFunctionTemplate(sfejs.vw, func(info *v8.FunctionCallbackInfo) *v8.Value {
		//fmt.Printf("statefun_getCallerTypename: %v\n", info.Args()) // when the JS function is called this Go callback will execute
		if len(info.Args()) != 0 {
			fmt.Printf("statefun_getCallerTypename error: requies no arguments but got %d\n", len(info.Args()))
			v, _ := v8.NewValue(sfejs.vw, nil)
			return v
		}
		v, _ := v8.NewValue(sfejs.vw, sfejs.contextProcessor.Caller.Typename)
		return v // you can return a value back to the JS caller if required
	})
	// () -> string
	statefunGetCallerID := v8.NewFunctionTemplate(sfejs.vw, func(info *v8.FunctionCallbackInfo) *v8.Value {
		//fmt.Printf("statefun_getCallerId: %v\n", info.Args())
		if len(info.Args()) != 0 {
			fmt.Printf("statefun_getCallerId error: requies no arguments but got %d\n", len(info.Args()))
			v, _ := v8.NewValue(sfejs.vw, nil)
			return v
		}
		v, _ := v8.NewValue(sfejs.vw, sfejs.contextProcessor.Caller.ID)
		return v
	})
	// () -> string
	statefunGetFunctionContext := v8.NewFunctionTemplate(sfejs.vw, func(info *v8.FunctionCallbackInfo) *v8.Value {
		//fmt.Printf("statefun_getFunctionContext: %v\n", info.Args())
		if len(info.Args()) != 0 {
			fmt.Printf("statefun_getFunctionContext error: requies no arguments but got %d\n", len(info.Args()))
			v, _ := v8.NewValue(sfejs.vw, nil)
			return v
		}
		v, _ := v8.NewValue(sfejs.vw, (*sfejs.contextProcessor.GetFunctionContext()).ToString())
		return v
	})
	// (string) -> int
	statefunSetFunctionContext := v8.NewFunctionTemplate(sfejs.vw, func(info *v8.FunctionCallbackInfo) *v8.Value {
		//fmt.Printf("statefun_setFunctionContext: %v\n", info.Args())
		if len(info.Args()) != 1 {
			fmt.Printf("statefun_setFunctionContext error: requies 1 argument but got %d\n", len(info.Args()))
			v, _ := v8.NewValue(sfejs.vw, int32(1))
			return v
		}
		if !info.Args()[0].IsString() {
			v, _ := v8.NewValue(sfejs.vw, int32(2))
			return v
		}

		newContextStr := info.Args()[0].String()
		newContext, ok := json_easy.JSONFromString(newContextStr)
		if !ok {
			v, _ := v8.NewValue(sfejs.vw, int32(3))
			return v
		}
		sfejs.contextProcessor.SetFunctionContext(&newContext)
		v, _ := v8.NewValue(sfejs.vw, int32(0))
		return v
	})
	// () -> string
	statefunGetObjectContext := v8.NewFunctionTemplate(sfejs.vw, func(info *v8.FunctionCallbackInfo) *v8.Value {
		//fmt.Printf("statefun_getObjectContext: %v\n", info.Args())
		if len(info.Args()) != 0 {
			fmt.Printf("statefun_getObjectContext error: requies no arguments but got %d\n", len(info.Args()))
			v, _ := v8.NewValue(sfejs.vw, nil)
			return v
		}
		v, _ := v8.NewValue(sfejs.vw, (*sfejs.contextProcessor.GetObjectContext()).ToString())
		return v
	})
	// (string) -> int
	statefunSetObjectContext := v8.NewFunctionTemplate(sfejs.vw, func(info *v8.FunctionCallbackInfo) *v8.Value {
		//fmt.Printf("statefun_setObjectContext: %v\n", info.Args())
		if len(info.Args()) != 1 {
			fmt.Printf("statefun_setObjectContext error: requies 1 argument but got %d\n", len(info.Args()))
			v, _ := v8.NewValue(sfejs.vw, int32(1))
			return v
		}
		if !info.Args()[0].IsString() {
			v, _ := v8.NewValue(sfejs.vw, int32(2))
			return v
		}

		newContextStr := info.Args()[0].String()
		newContext, ok := json_easy.JSONFromString(newContextStr)
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
		//fmt.Printf("statefun_getPayload: %v", info.Args())
		if len(info.Args()) != 0 {
			fmt.Printf("statefun_getPayload error: requies no arguments but got %d\n", len(info.Args()))
			v, _ := v8.NewValue(sfejs.vw, nil)
			return v
		}
		v, _ := v8.NewValue(sfejs.vw, sfejs.contextProcessor.Payload.ToString())
		return v
	})
	// () -> string
	statefunGetOptions := v8.NewFunctionTemplate(sfejs.vw, func(info *v8.FunctionCallbackInfo) *v8.Value {
		//fmt.Printf("statefun_getOptions: %v", info.Args())
		if len(info.Args()) != 0 {
			fmt.Printf("statefun_getOptions error: requies no arguments but got %d\n", len(info.Args()))
			v, _ := v8.NewValue(sfejs.vw, nil)
			return v
		}
		v, _ := v8.NewValue(sfejs.vw, sfejs.contextProcessor.Options.ToString())
		return v
	})
	// (string, string, string, string) -> int
	statefunCall := v8.NewFunctionTemplate(sfejs.vw, func(info *v8.FunctionCallbackInfo) *v8.Value {
		//fmt.Printf("statefun_call: %v\n", info.Args())
		if len(info.Args()) != 4 {
			fmt.Printf("statefun_call error: requies 4 argument but got %d\n", len(info.Args()))
			v, _ := v8.NewValue(sfejs.vw, int32(1))
			return v
		}
		if info.Args()[0].IsString() && info.Args()[1].IsString() && info.Args()[2].IsString() && info.Args()[3].IsString() {
			if j, ok := json_easy.JSONFromString(info.Args()[2].String()); ok {
				var options *json_easy.JSON = nil
				if len(info.Args()[3].String()) > 0 {
					if o, ok := json_easy.JSONFromString(info.Args()[3].String()); ok {
						options = &o
					} else {
						fmt.Printf("statefunCall options is not empty and not a JSON: %s\n", info.Args()[3].String())
						v, _ := v8.NewValue(sfejs.vw, int32(3))
						return v
					}
				}
				sfejs.contextProcessor.Call(info.Args()[0].String(), info.Args()[1].String(), &j, options)
				v, _ := v8.NewValue(sfejs.vw, int32(0))
				return v
			}
			fmt.Printf("statefunCall payload is not a JSON: %s\n", info.Args()[2].String())
			v, _ := v8.NewValue(sfejs.vw, int32(3))
			return v
		}
		v, _ := v8.NewValue(sfejs.vw, int32(2))
		return v
	})
	// (string, string) -> int
	statefunEgress := v8.NewFunctionTemplate(sfejs.vw, func(info *v8.FunctionCallbackInfo) *v8.Value {
		//fmt.Printf("statefun_egress: %v\n", info.Args())
		if len(info.Args()) != 2 {
			fmt.Printf("statefun_egress error: requies 2 argument but got %d\n", len(info.Args()))
			v, _ := v8.NewValue(sfejs.vw, int32(1))
			return v
		}
		if info.Args()[0].IsString() && info.Args()[1].IsString() {
			if j, ok := json_easy.JSONFromString(info.Args()[1].String()); ok {
				sfejs.contextProcessor.Egress(info.Args()[0].String(), &j)
				v, _ := v8.NewValue(sfejs.vw, int32(0))
				return v
			}
			fmt.Printf("statefunEgress payload is not a JSON: %s\n", info.Args()[1].String())
			v, _ := v8.NewValue(sfejs.vw, int32(3))
			return v
		}
		v, _ := v8.NewValue(sfejs.vw, int32(2))
		return v
	})
	// (string)
	print := v8.NewFunctionTemplate(sfejs.vw, func(info *v8.FunctionCallbackInfo) *v8.Value {
		fmt.Printf("%s: %v\n", alias, info.Args())
		return nil
	})

	global := v8.NewObjectTemplate(sfejs.vw)
	system.MsgOnErrorReturn(global.Set("statefun_getSelfTypename", statefunGetSelfTypenane))
	system.MsgOnErrorReturn(global.Set("statefun_getSelfId", statefunGetSelfID))
	system.MsgOnErrorReturn(global.Set("statefun_getCallerTypename", statefunGetCallerTypenane))
	system.MsgOnErrorReturn(global.Set("statefun_getCallerId", statefunGetCallerID))
	system.MsgOnErrorReturn(global.Set("statefun_getFunctionContext", statefunGetFunctionContext))
	system.MsgOnErrorReturn(global.Set("statefun_setFunctionContext", statefunSetFunctionContext))
	system.MsgOnErrorReturn(global.Set("statefun_getObjectContext", statefunGetObjectContext))
	system.MsgOnErrorReturn(global.Set("statefun_setObjectnContext", statefunSetObjectContext))
	system.MsgOnErrorReturn(global.Set("statefun_getPayload", statefunGetPayload))
	system.MsgOnErrorReturn(global.Set("statefun_getOptions", statefunGetOptions))
	system.MsgOnErrorReturn(global.Set("statefun_call", statefunCall))
	system.MsgOnErrorReturn(global.Set("statefun_egress", statefunEgress))
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
