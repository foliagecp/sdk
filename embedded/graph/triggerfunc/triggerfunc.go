package triggerfunc

import (
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
let result_name = ""
if (code_exists) {
	try {
		eval(type_data.body.js.namegen);
	} catch (error) {
		print("name_generator.js: error on executing script stored in type", type_data.name + ":", error.message);
	}
}
if (result_name && result_name.length > 0) {
	context.result_name = result_name
	var contextStr = JSON.stringify(context)
	statefun_setFunctionContext(contextStr)
}
`
)

func ObjectNameGenerator(executor sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
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

	if executor != nil {
		if err := executor.BuildError(); err != nil {
			lg.Logln(lg.ErrorLevel, err.Error())
			logger.Logf(logger.ErrorLevel, "ObjectNameGenerator cannot execute script, object with id=%s: %s", ctx.Self.ID, err.Error())
			return
		} else {
			if err := executor.Run(ctx); err != nil {
				lg.Logln(lg.ErrorLevel, err.Error())
			}
		}
	}
	functionContext = ctx.GetFunctionContext()

	resultName := functionContext.GetByPath("result_name").AsStringDefault("")
	if len(resultName) == 0 {
		resultDetails := functionContext.GetByPath("result_name").AsStringDefault("")
		logger.Logf(logger.ErrorLevel, "ObjectNameGenerator' execute script for object with id=%s cannot calculate its name: %s", ctx.Self.ID, resultDetails)
	}

	bodyWithName := easyjson.NewJSONObject()
	path := ctx.Payload.GetByPath("result_name_path").AsStringDefault("__meta.name")
	bodyWithName.SetByPath(path, easyjson.NewJSON(resultName))
	system.MsgOnErrorReturn(dbc.CMDB.ObjectUpdate(ctx.Self.ID, bodyWithName, false))
}
