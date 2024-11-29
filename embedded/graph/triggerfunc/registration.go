package triggerfunc

import (
	"github.com/foliagecp/sdk/statefun"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	sfPluginJS "github.com/foliagecp/sdk/statefun/plugins/js"
	"github.com/foliagecp/sdk/statefun/system"
)

func RegisterObjectNameGenerator(runtime *statefun.Runtime) {
	ft := statefun.NewFunctionType(runtime, "functions.triggers.object.namegen", ObjectNameGenerator, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).SetMaxIdHandlers(-1))
	system.MsgOnErrorReturn(ft.SetExecutor("name_generator.js", string(objectNameGeneratorJSCode), sfPluginJS.StatefunExecutorPluginJSContructor))
}
