package js

import (
	"fmt"

	"github.com/dop251/goja"
	"github.com/foliagecp/easyjson"
	lg "github.com/foliagecp/sdk/statefun/logger"

	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
)

// -------------------- Errors --------------------

// CustomJSError wraps JavaScript errors with additional info.
type CustomJSError struct {
	Message    string
	Location   string
	StackTrace string
}

func (e *CustomJSError) Error() string       { return e.Message }
func (e *CustomJSError) GetLocation() string { return e.Location }
func (e *CustomJSError) GetStackTrace() string {
	return e.StackTrace
}

// Universal conversion of goja error -> CustomJSError
func newCustomJSErrorFromErr(vm *goja.Runtime, err error) error {
	if err == nil {
		return nil
	}
	if ex, ok := err.(*goja.Exception); ok {
		msg := ex.Error()
		stack := ""

		// Get stack trace from JS "stack" property (works in all goja versions).
		if v := ex.Value(); !goja.IsUndefined(v) && !goja.IsNull(v) {
			if obj := v.ToObject(vm); obj != nil {
				if s := obj.Get("stack"); s != nil && !goja.IsUndefined(s) && !goja.IsNull(s) {
					stack = s.String()
				}
			}
		}
		return &CustomJSError{Message: msg, Location: "", StackTrace: stack}
	}
	return &CustomJSError{Message: err.Error(), Location: "", StackTrace: ""}
}

// -------------------- Executor --------------------

// StatefunExecutorPluginJS is a JS executor backed by goja.
type StatefunExecutorPluginJS struct {
	vm         *goja.Runtime
	prog       *goja.Program
	buildError error

	ctx *sfPlugins.StatefunContextProcessor
}

// Executor constructor (same interface as v8 backend)
func StatefunExecutorPluginJSContructor(alias string, source string) sfPlugins.StatefunExecutor {
	sfejs := &StatefunExecutorPluginJS{
		vm: goja.New(),
	}
	vm := sfejs.vm

	// Helper for returning integer codes as JS values
	retInt := func(code int32) goja.Value { return vm.ToValue(code) }

	// -------------------- JS API (global functions) --------------------

	// () -> string
	statefunGetSelfTypenane := func(call goja.FunctionCall) goja.Value {
		if len(call.Arguments) != 0 {
			lg.Logf(lg.ErrorLevel, "statefun_getSelfTypename requires no arguments but got %d", len(call.Arguments))
			return vm.ToValue(nil)
		}
		return vm.ToValue(sfejs.ctx.Self.Typename)
	}
	// () -> string
	statefunGetSelfID := func(call goja.FunctionCall) goja.Value {
		if len(call.Arguments) != 0 {
			lg.Logf(lg.ErrorLevel, "statefun_getSelfId requires no arguments but got %d", len(call.Arguments))
			return vm.ToValue(nil)
		}
		return vm.ToValue(sfejs.ctx.Self.ID)
	}
	// () -> string
	statefunGetCallerTypenane := func(call goja.FunctionCall) goja.Value {
		if len(call.Arguments) != 0 {
			lg.Logf(lg.ErrorLevel, "statefun_getCallerTypename requires no arguments but got %d", len(call.Arguments))
			return vm.ToValue(nil)
		}
		return vm.ToValue(sfejs.ctx.Caller.Typename)
	}
	// () -> string
	statefunGetCallerID := func(call goja.FunctionCall) goja.Value {
		if len(call.Arguments) != 0 {
			lg.Logf(lg.ErrorLevel, "statefun_getCallerId requires no arguments but got %d", len(call.Arguments))
			return vm.ToValue(nil)
		}
		return vm.ToValue(sfejs.ctx.Caller.ID)
	}
	// () -> string
	statefunGetFunctionContext := func(call goja.FunctionCall) goja.Value {
		if len(call.Arguments) != 0 {
			lg.Logf(lg.ErrorLevel, "statefun_getFunctionContext requires no arguments but got %d", len(call.Arguments))
			return vm.ToValue(nil)
		}
		return vm.ToValue((*sfejs.ctx.GetFunctionContext()).ToString())
	}
	// (string) -> int
	statefunSetFunctionContext := func(call goja.FunctionCall) goja.Value {
		if len(call.Arguments) != 1 {
			lg.Logf(lg.ErrorLevel, "statefun_setFunctionContext requires 1 argument but got %d", len(call.Arguments))
			return retInt(1)
		}
		if _, ok := call.Argument(0).Export().(string); !ok {
			return retInt(2)
		}
		str := call.Argument(0).String()
		newContext, ok := easyjson.JSONFromString(str)
		if !ok {
			return retInt(3)
		}
		sfejs.ctx.SetFunctionContext(&newContext)
		return retInt(0)
	}
	// (string) -> int
	statefunSetRequestReplyData := func(call goja.FunctionCall) goja.Value {
		if len(call.Arguments) != 1 {
			lg.Logf(lg.ErrorLevel, "statefun_setRequestReplyData requires 1 argument but got %d", len(call.Arguments))
			return retInt(1)
		}
		if sfejs.ctx.Reply == nil {
			return retInt(3)
		}
		if _, ok := call.Argument(0).Export().(string); !ok {
			return retInt(2)
		}
		str := call.Argument(0).String()
		j, ok := easyjson.JSONFromString(str)
		if !ok {
			return retInt(4)
		}
		sfejs.ctx.Reply.With(&j)
		return retInt(0)
	}
	// () -> string
	statefunGetObjectContext := func(call goja.FunctionCall) goja.Value {
		if len(call.Arguments) != 0 {
			lg.Logf(lg.ErrorLevel, "statefun_getObjectContext requires no arguments but got %d", len(call.Arguments))
			return vm.ToValue(nil)
		}
		return vm.ToValue((*sfejs.ctx.GetObjectContext()).ToString())
	}
	// (string) -> int
	statefunSetObjectContext := func(call goja.FunctionCall) goja.Value {
		if len(call.Arguments) != 1 {
			lg.Logf(lg.ErrorLevel, "statefun_setObjectContext requires 1 argument but got %d", len(call.Arguments))
			return retInt(1)
		}
		if _, ok := call.Argument(0).Export().(string); !ok {
			return retInt(2)
		}
		str := call.Argument(0).String()
		newContext, ok := easyjson.JSONFromString(str)
		if !ok {
			return retInt(3)
		}
		sfejs.ctx.SetObjectContext(&newContext)
		return retInt(0)
	}
	// () -> string
	statefunGetPayload := func(call goja.FunctionCall) goja.Value {
		if len(call.Arguments) != 0 {
			lg.Logf(lg.ErrorLevel, "statefun_getPayload requires no arguments but got %d", len(call.Arguments))
			return vm.ToValue(nil)
		}
		return vm.ToValue(sfejs.ctx.Payload.ToString())
	}
	// () -> string
	statefunGetOptions := func(call goja.FunctionCall) goja.Value {
		if len(call.Arguments) != 0 {
			lg.Logf(lg.ErrorLevel, "statefun_getOptions requires no arguments but got %d", len(call.Arguments))
			return vm.ToValue(nil)
		}
		return vm.ToValue(sfejs.ctx.Options.ToString())
	}
	// (int, string, string, string, string) -> int
	statefunSignal := func(call goja.FunctionCall) goja.Value {
		if len(call.Arguments) != 5 {
			lg.Logf(lg.ErrorLevel, "statefun_signal requires 5 arguments but got %d", len(call.Arguments))
			return retInt(1)
		}
		if _, ok := call.Argument(0).Export().(int64); !ok {
			return retInt(2)
		}
		tn := call.Argument(1).String()
		id := call.Argument(2).String()
		payloadStr := call.Argument(3).String()
		optionsStr := call.Argument(4).String()

		j, ok := easyjson.JSONFromString(payloadStr)
		if !ok {
			lg.Logf(lg.ErrorLevel, "statefun_signal payload is not a JSON: %s", payloadStr)
			return retInt(3)
		}
		var options *easyjson.JSON
		if len(optionsStr) > 0 {
			if o, ok := easyjson.JSONFromString(optionsStr); ok {
				options = &o
			} else {
				lg.Logf(lg.ErrorLevel, "statefun_signal options is not empty and not a JSON: %s", optionsStr)
				return retInt(4)
			}
		}
		system.MsgOnErrorReturn(sfejs.ctx.Signal(
			sfPlugins.SignalProvider(int32(call.Argument(0).ToInteger())),
			tn, id, &j, options,
		))
		return retInt(0)
	}
	// (int, string, string, string, string) -> int|string
	statefunRequest := func(call goja.FunctionCall) goja.Value {
		if len(call.Arguments) != 5 {
			lg.Logf(lg.ErrorLevel, "statefun_request requires 5 arguments but got %d", len(call.Arguments))
			return retInt(1)
		}
		if _, ok := call.Argument(0).Export().(int64); !ok {
			return retInt(2)
		}
		tn := call.Argument(1).String()
		id := call.Argument(2).String()
		payloadStr := call.Argument(3).String()
		optionsStr := call.Argument(4).String()

		j, ok := easyjson.JSONFromString(payloadStr)
		if !ok {
			lg.Logf(lg.ErrorLevel, "statefun_request payload is not a JSON: %s", payloadStr)
			return retInt(3)
		}
		var options *easyjson.JSON
		if len(optionsStr) > 0 {
			if o, ok := easyjson.JSONFromString(optionsStr); ok {
				options = &o
			} else {
				lg.Logf(lg.ErrorLevel, "statefun_request options is not empty and not a JSON: %s", optionsStr)
				return retInt(4)
			}
		}
		resp, err := sfejs.ctx.Request(
			sfPlugins.RequestProvider(int32(call.Argument(0).ToInteger())),
			tn, id, &j, options,
		)
		if err != nil {
			return retInt(5)
		}
		return vm.ToValue(resp.ToString())
	}
	// (int, string) -> int
	statefunEgress := func(call goja.FunctionCall) goja.Value {
		if len(call.Arguments) != 2 {
			lg.Logf(lg.ErrorLevel, "statefun_egress requires 2 arguments but got %d", len(call.Arguments))
			return retInt(1)
		}
		if _, ok := call.Argument(0).Export().(int64); !ok {
			return retInt(2)
		}
		if _, ok := call.Argument(1).Export().(string); !ok {
			return retInt(2)
		}

		provider := int32(call.Argument(0).ToInteger())
		payloadStr := call.Argument(1).String()

		if j, ok := easyjson.JSONFromString(payloadStr); ok {
			system.MsgOnErrorReturn(sfejs.ctx.Egress(
				sfPlugins.EgressProvider(provider),
				&j,
			))
			return retInt(0)
		}
		lg.Logf(lg.ErrorLevel, "statefun_egress payload is not a JSON: %s", payloadStr)
		return retInt(3)
	}
	// (string...) -> void
	print := func(call goja.FunctionCall) goja.Value {
		lg.Logf(lg.InfoLevel, "%s: %v", alias, call.Arguments)
		return goja.Undefined()
	}

	// Register global functions (equivalent to v8 ObjectTemplate)
	_ = vm.Set("statefun_getSelfTypename", statefunGetSelfTypenane)
	_ = vm.Set("statefun_getSelfId", statefunGetSelfID)
	_ = vm.Set("statefun_getCallerTypename", statefunGetCallerTypenane)
	_ = vm.Set("statefun_getCallerId", statefunGetCallerID)
	_ = vm.Set("statefun_getFunctionContext", statefunGetFunctionContext)
	_ = vm.Set("statefun_getObjectContext", statefunGetObjectContext)
	_ = vm.Set("statefun_getPayload", statefunGetPayload)
	_ = vm.Set("statefun_getOptions", statefunGetOptions)

	_ = vm.Set("statefun_setObjectContext", statefunSetObjectContext)
	_ = vm.Set("statefun_setFunctionContext", statefunSetFunctionContext)
	_ = vm.Set("statefun_setRequestReplyData", statefunSetRequestReplyData)

	_ = vm.Set("statefun_signal", statefunSignal)
	_ = vm.Set("statefun_request", statefunRequest)
	_ = vm.Set("statefun_egress", statefunEgress)
	_ = vm.Set("print", print)

	// Compile code once (equivalent to v8 CompileUnboundScript).
	// Wrap in braces "{...}" for compatibility with original logic.
	srcWrapped := fmt.Sprintf("{\n%s\n}", source)
	prog, cerr := goja.Compile(alias, srcWrapped, false)
	if cerr != nil {
		sfejs.buildError = newCustomJSErrorFromErr(vm, cerr)
	}
	sfejs.prog = prog

	return sfejs
}

// Execute script (Run executes already compiled program)
func (sfejs *StatefunExecutorPluginJS) Run(ctx *sfPlugins.StatefunContextProcessor) error {
	sfejs.ctx = ctx
	if sfejs.prog == nil {
		return sfejs.buildError
	}
	_, err := sfejs.vm.RunProgram(sfejs.prog)
	if err != nil {
		return newCustomJSErrorFromErr(sfejs.vm, err)
	}
	return nil
}

// Return build-time error if present
func (sfejs *StatefunExecutorPluginJS) BuildError() error {
	return sfejs.buildError
}
