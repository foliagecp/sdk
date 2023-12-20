package crud

import (
	"github.com/foliagecp/sdk/statefun"
)

func RegisterAllFunctionTypes(runtime *statefun.Runtime) {
	// High-Level API Registration
	statefun.NewFunctionType(runtime, "functions.cmdb.api.object.create", CreateObject, *statefun.NewFunctionTypeConfig().SetServiceState(true).SetMaxIdHandlers(0))
	statefun.NewFunctionType(runtime, "functions.cmdb.api.object.update", UpdateObject, *statefun.NewFunctionTypeConfig().SetServiceState(true).SetMaxIdHandlers(0))
	statefun.NewFunctionType(runtime, "functions.cmdb.api.object.delete", DeleteObject, *statefun.NewFunctionTypeConfig().SetServiceState(true).SetMaxIdHandlers(0))

	statefun.NewFunctionType(runtime, "functions.cmdb.api.type.create", CreateType, *statefun.NewFunctionTypeConfig().SetServiceState(true).SetMaxIdHandlers(0))
	statefun.NewFunctionType(runtime, "functions.cmdb.api.type.update", UpdateType, *statefun.NewFunctionTypeConfig().SetServiceState(true).SetMaxIdHandlers(0))
	statefun.NewFunctionType(runtime, "functions.cmdb.api.type.delete", DeleteType, *statefun.NewFunctionTypeConfig().SetServiceState(true).SetMaxIdHandlers(0))

	statefun.NewFunctionType(runtime, "functions.cmdb.api.types.link.create", CreateTypesLink, *statefun.NewFunctionTypeConfig().SetServiceState(true).SetMaxIdHandlers(0))
	statefun.NewFunctionType(runtime, "functions.cmdb.api.types.link.update", UpdateTypesLink, *statefun.NewFunctionTypeConfig().SetServiceState(true).SetMaxIdHandlers(0))
	statefun.NewFunctionType(runtime, "functions.cmdb.api.types.link.delete", DeleteTypesLink, *statefun.NewFunctionTypeConfig().SetServiceState(true).SetMaxIdHandlers(0))

	statefun.NewFunctionType(runtime, "functions.cmdb.api.objects.link.create", CreateObjectsLink, *statefun.NewFunctionTypeConfig().SetServiceState(true).SetMaxIdHandlers(0))
	statefun.NewFunctionType(runtime, "functions.cmdb.api.objects.link.update", UpdateObjectsLink, *statefun.NewFunctionTypeConfig().SetServiceState(true).SetMaxIdHandlers(0))
	statefun.NewFunctionType(runtime, "functions.cmdb.api.objects.link.delete", DeleteObejectsLink, *statefun.NewFunctionTypeConfig().SetServiceState(true).SetMaxIdHandlers(0))
	// High-Level API End Registration

	// Low-Level API
	statefun.NewFunctionType(runtime, "functions.graph.api.vertex.create", LLAPIObjectCreate, *statefun.NewFunctionTypeConfig().SetServiceState(true).SetMaxIdHandlers(0))
	statefun.NewFunctionType(runtime, "functions.graph.api.vertex.update", LLAPIObjectUpdate, *statefun.NewFunctionTypeConfig().SetServiceState(true).SetMaxIdHandlers(0))
	statefun.NewFunctionType(runtime, "functions.graph.api.vertex.delete", LLAPIObjectDelete, *statefun.NewFunctionTypeConfig().SetServiceState(true).SetMaxIdHandlers(0))

	statefun.NewFunctionType(runtime, "functions.graph.api.link.create", LLAPILinkCreate, *statefun.NewFunctionTypeConfig().SetServiceState(true).SetMaxIdHandlers(0))
	statefun.NewFunctionType(runtime, "functions.graph.api.link.update", LLAPILinkUpdate, *statefun.NewFunctionTypeConfig().SetServiceState(true).SetMaxIdHandlers(0))
	statefun.NewFunctionType(runtime, "functions.graph.api.link.delete", LLAPILinkDelete, *statefun.NewFunctionTypeConfig().SetServiceState(true).SetMaxIdHandlers(0))
}
