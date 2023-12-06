package crud

import (
	"github.com/foliagecp/sdk/statefun"
)

func RegisterAllFunctionTypes(runtime *statefun.Runtime) {
	// High-Level API Registration
	statefun.NewFunctionType(runtime, "functions.cmdb.api.object.create", CreateObject, *statefun.NewFunctionTypeConfig().SetPrometricsEnabled(true))
	statefun.NewFunctionType(runtime, "functions.cmdb.api.object.update", UpdateObject, *statefun.NewFunctionTypeConfig().SetPrometricsEnabled(true))
	statefun.NewFunctionType(runtime, "functions.cmdb.api.object.delete", DeleteObject, *statefun.NewFunctionTypeConfig().SetPrometricsEnabled(true))

	statefun.NewFunctionType(runtime, "functions.cmdb.api.type.create", CreateType, *statefun.NewFunctionTypeConfig().SetPrometricsEnabled(true))
	statefun.NewFunctionType(runtime, "functions.cmdb.api.type.update", UpdateType, *statefun.NewFunctionTypeConfig().SetPrometricsEnabled(true))
	statefun.NewFunctionType(runtime, "functions.cmdb.api.type.delete", DeleteType, *statefun.NewFunctionTypeConfig().SetPrometricsEnabled(true))

	statefun.NewFunctionType(runtime, "functions.cmdb.api.types.link.create", CreateTypesLink, *statefun.NewFunctionTypeConfig().SetPrometricsEnabled(true))
	statefun.NewFunctionType(runtime, "functions.cmdb.api.types.link.update", UpdateTypesLink, *statefun.NewFunctionTypeConfig().SetPrometricsEnabled(true))
	statefun.NewFunctionType(runtime, "functions.cmdb.api.types.link.delete", DeleteTypesLink, *statefun.NewFunctionTypeConfig().SetPrometricsEnabled(true))

	statefun.NewFunctionType(runtime, "functions.cmdb.api.objects.link.create", CreateObjectsLink, *statefun.NewFunctionTypeConfig().SetPrometricsEnabled(true))
	statefun.NewFunctionType(runtime, "functions.cmdb.api.objects.link.update", UpdateObjectsLink, *statefun.NewFunctionTypeConfig().SetPrometricsEnabled(true))
	statefun.NewFunctionType(runtime, "functions.cmdb.api.objects.link.delete", DeleteObejectsLink, *statefun.NewFunctionTypeConfig().SetPrometricsEnabled(true))
	// High-Level API End Registration

	// Low-Level API
	statefun.NewFunctionType(runtime, "functions.graph.api.vertex.create", LLAPIObjectCreate, *statefun.NewFunctionTypeConfig().SetPrometricsEnabled(true))
	statefun.NewFunctionType(runtime, "functions.graph.api.vertex.update", LLAPIObjectUpdate, *statefun.NewFunctionTypeConfig().SetPrometricsEnabled(true))
	statefun.NewFunctionType(runtime, "functions.graph.api.vertex.delete", LLAPIObjectDelete, *statefun.NewFunctionTypeConfig().SetPrometricsEnabled(true))

	statefun.NewFunctionType(runtime, "functions.graph.api.link.create", LLAPILinkCreate, *statefun.NewFunctionTypeConfig().SetPrometricsEnabled(true))
	statefun.NewFunctionType(runtime, "functions.graph.api.link.update", LLAPILinkUpdate, *statefun.NewFunctionTypeConfig().SetPrometricsEnabled(true))
	statefun.NewFunctionType(runtime, "functions.graph.api.link.delete", LLAPILinkDelete, *statefun.NewFunctionTypeConfig().SetPrometricsEnabled(true))
}
