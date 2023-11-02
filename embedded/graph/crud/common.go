package crud

import (
	"github.com/foliagecp/sdk/statefun"
)

func RegisterAllFunctionTypes(runtime *statefun.Runtime) {
	// High-Level API Registration
	statefun.NewFunctionType(runtime, "functions.graph.api.object.create", CreateObject, *statefun.NewFunctionTypeConfig())
	statefun.NewFunctionType(runtime, "functions.graph.api.object.update", UpdateObject, *statefun.NewFunctionTypeConfig())
	statefun.NewFunctionType(runtime, "functions.graph.api.object.delete", DeleteObject, *statefun.NewFunctionTypeConfig())

	statefun.NewFunctionType(runtime, "functions.graph.api.type.create", CreateType, *statefun.NewFunctionTypeConfig())
	statefun.NewFunctionType(runtime, "functions.graph.api.type.update", UpdateType, *statefun.NewFunctionTypeConfig())
	statefun.NewFunctionType(runtime, "functions.graph.api.type.delete", DeleteType, *statefun.NewFunctionTypeConfig())

	statefun.NewFunctionType(runtime, "functions.graph.api.types.link.create", CreateTypesLink, *statefun.NewFunctionTypeConfig())
	statefun.NewFunctionType(runtime, "functions.graph.api.types.link.update", UpdateTypesLink, *statefun.NewFunctionTypeConfig())
	statefun.NewFunctionType(runtime, "functions.graph.api.types.link.delete", DeleteTypesLink, *statefun.NewFunctionTypeConfig())

	statefun.NewFunctionType(runtime, "functions.graph.api.objects.link.create", CreateObjectsLink, *statefun.NewFunctionTypeConfig())
	statefun.NewFunctionType(runtime, "functions.graph.api.objects.link.update", UpdateObjectsLink, *statefun.NewFunctionTypeConfig())
	statefun.NewFunctionType(runtime, "functions.graph.api.objects.link.delete", DeleteObejectsLink, *statefun.NewFunctionTypeConfig())
	// High-Level API End Registration

	// Low-Level API
	statefun.NewFunctionType(runtime, "functions.graph.ll.api.object.create", LLAPIObjectCreate, *statefun.NewFunctionTypeConfig())
	statefun.NewFunctionType(runtime, "functions.graph.ll.api.object.update", LLAPIObjectUpdate, *statefun.NewFunctionTypeConfig())
	statefun.NewFunctionType(runtime, "functions.graph.ll.api.object.delete", LLAPIObjectDelete, *statefun.NewFunctionTypeConfig())

	statefun.NewFunctionType(runtime, "functions.graph.ll.api.link.create", LLAPILinkCreate, *statefun.NewFunctionTypeConfig())
	statefun.NewFunctionType(runtime, "functions.graph.ll.api.link.update", LLAPILinkUpdate, *statefun.NewFunctionTypeConfig())
	statefun.NewFunctionType(runtime, "functions.graph.ll.api.link.delete", LLAPILinkDelete, *statefun.NewFunctionTypeConfig())
}
