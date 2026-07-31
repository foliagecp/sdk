package triggerfunc

import (
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/clients/go/db"
	"github.com/foliagecp/sdk/statefun/logger"
	lg "github.com/foliagecp/sdk/statefun/logger"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
)

/* Example
	script := `
print("MY SCRIPT !!!!!!'");
result_name = object_data.body.hostname;
	`

	typeBody := easyjson.NewJSONObject()
	typeBody.SetByPath("js.namegen", easyjson.NewJSON(script))

	system.MsgOnErrorReturn(dbClient.CMDB.TypeCreate("typew", typeBody))
	system.MsgOnErrorReturn(dbClient.CMDB.TriggerObjectSet("typew", "create", "functions.triggers.object.namegen"))

	objectBody := easyjson.NewJSONObject()
	objectBody.SetByPath("hostname", easyjson.NewJSON("cluster-0"))
	dbClient.CMDB.ObjectCreate("test1", "typew", objectBody)
*/

var (
	objectNameGeneratorJSCode = `
var context = JSON.parse(statefun_getFunctionContext());
let code_exists = true;
try {
	if (context.type_data.body.js.namegen.length == 0) {
		print("name_generator.js: found no JS code in function's context by path 'context.type_data.body.js.namegen'");
		code_exists = false;
	}
} catch (error) {
    print("name_generator.js: error on access JS code by path 'context.type_data.body.js.namegen':", error.message);
	code_exists = false;
}
let type_data = context.type_data
let object_data = context.object_data
let result_name_path = ""
let result_name = ""
if (code_exists) {
	try {
		eval(type_data.body.js.namegen);
	} catch (error) {
		print("name_generator.js: error on executing script stored in type", type_data.name + ":", error.message);
	}
}
if (result_name && result_name.length > 0) {
	if (result_name_path && result_name_path.length > 0) {
		context.result_name_path = result_name_path
	}
	context.result_name = result_name
	var contextStr = JSON.stringify(context)
	statefun_setFunctionContext(contextStr)
}
`
)

// functionContextTTL bounds how long a per-object name-generator function
// context lives in the cache after its single, synchronous use.
const functionContextTTL = time.Minute

func ObjectNameGenerator(executor sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	// A link trigger fires on the FROM object, but the generated name belongs
	// to the TO object — forward the trigger to TO immediately, BEFORE reading
	// anything. The previous order read the FROM object + its type and built
	// the whole function context first, then discarded all of it on every link
	// trigger; that hammered high-out-degree vertices and the shared type
	// vertex with reads that were thrown away.
	//
	// Forward with ctx.Request (not ctx.Signal): AutoRequestSelect routes the
	// call in-process when TO lives in this runtime, sparing an extra NATS
	// publish/redelivery hop on the bus.
	if ctx.Payload.PathExists("trigger.link") {
		link := ctx.Payload.GetByPath("trigger.link")
		for _, k := range link.ObjectKeys() {
			toId := link.GetByPath(k + ".to").AsStringDefault("")
			if len(toId) > 0 {
				system.MsgOnErrorReturn(ctx.Request(sfPlugins.AutoRequestSelect, ctx.Self.Typename, toId, nil, nil))
				return // The name belongs to TO; it regenerates there.
			}
		}
	}

	dbc, err := db.NewDBSyncClientFromRequestFunction(ctx.Request)
	if err != nil {
		logger.Logf(logger.ErrorLevel, "ObjectNameGenerator cannot create db client")
		return
	}

	var objectData *easyjson.JSON
	if d, err := dbc.CMDB.ObjectRead(ctx.Self.ID); err != nil {
		logger.Logf(logger.ErrorLevel, "ObjectNameGenerator cannot read object with id=%s: %s", ctx.Self.ID, err.Error())
		return
	} else {
		objectData = &d
	}
	objectData.SetByPath("uuid", easyjson.NewJSON(ctx.Self.ID))

	typeName := objectData.GetByPath("type").AsStringDefault("")
	if len(typeName) == 0 {
		logger.Logf(logger.ErrorLevel, "ObjectNameGenerator vertex with id=%s is not an object", ctx.Self.ID)
		return
	}

	var typeData *easyjson.JSON
	if d, err := dbc.CMDB.TypeRead(typeName); err != nil {
		logger.Logf(logger.ErrorLevel, "ObjectNameGenerator cannot read type %s data of object with id=%s: %s", typeName, ctx.Self.ID, err.Error())
		return
	} else {
		typeData = &d
	}
	typeData.SetByPath("name", easyjson.NewJSON(typeName))

	functionContext := easyjson.NewJSONObject().GetPtr()
	functionContext.SetByPath("object_data", *objectData)
	functionContext.SetByPath("type_data", *typeData)
	ctx.SetFunctionContext(functionContext)
	// Mark the context to expire IMMEDIATELY after storing it: every early
	// return below (executor build error, script run error) must not leave a
	// permanent, GC-invisible context carrying the whole object+type bodies —
	// one per touched object. The closing SetContextExpirationAfter at the end
	// stays: the JS side may overwrite the context (statefun_setFunctionContext),
	// which drops this mark, and the final call restores it on the happy path.
	ctx.SetContextExpirationAfter(functionContextTTL)

	if executor != nil {
		if err := executor.BuildError(); err != nil {
			logger.Logf(logger.ErrorLevel, "ObjectNameGenerator build script for object of type=%s with id=%s: %s", typeName, ctx.Self.ID, err.Error())
			return
		} else {
			if err := executor.Run(ctx); err != nil {
				e := err.(sfPlugins.PluginError)
				lg.Logf(lg.ErrorLevel, "ObjectNameGenerator run script for object of type=%s with id=%s: %s [%s]", typeName, ctx.Self.ID, e.Error(), e.GetLocation())
			}
		}
	}
	functionContext = ctx.GetFunctionContext()

	resultName := functionContext.GetByPath("result_name").AsStringDefault("")
	if len(resultName) == 0 {
		logger.Logf(logger.ErrorLevel, "ObjectNameGenerator' execute script for object with id=%s cannot calculate its name", ctx.Self.ID)
	}

	bodyWithName := easyjson.NewJSONObject()
	path := functionContext.GetByPath("result_name_path").AsStringDefault("__meta.name")
	bodyWithName.SetByPath(path, easyjson.NewJSON(resultName))
	system.MsgOnErrorReturn(dbc.CMDB.ObjectUpdate(ctx.Self.ID, bodyWithName, false))

	// The function context set above is per-(function, object id); without an
	// expiry it lingers in the cache for every named object, accumulating
	// across a bulk burst. It is only the Go<->JS IPC channel for this single
	// synchronous invocation, so mark it to expire and let the cache GC reclaim
	// it.
	ctx.SetContextExpirationAfter(functionContextTTL)
}
