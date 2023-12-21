package crud

import (
	"github.com/foliagecp/sdk/statefun"
)

func RegisterAllFunctionTypes(runtime *statefun.Runtime) {
	// High-Level API Registration

	// TODO: type_body: {"triggers":{"create":["functions.a.b.c", "functions.a.b.d", ...],"update":[...],"delete":[...]}}
	// TODO: functions.a.b.c is being called on "id" of created object
	// TODO: Create payload: {"trigger.object.create": {"new_body": {}}}
	statefun.NewFunctionType(runtime, "functions.cmdb.api.type.create", CreateType, *statefun.NewFunctionTypeConfig().SetServiceState(true))
	// TODO: Update payload: {"trigger.object.update": {"old_body": {}, "new_body": {}}}
	statefun.NewFunctionType(runtime, "functions.cmdb.api.type.update", UpdateType, *statefun.NewFunctionTypeConfig().SetServiceState(true))
	// TODO: Delete payload: {"trigger.object.delete": {"old_body": {}}} ! Object and data in cache will be erased by the moment trigger is called
	// objects which had links to a deleted object can be notified via link deletion trigger
	statefun.NewFunctionType(runtime, "functions.cmdb.api.type.delete", DeleteType, *statefun.NewFunctionTypeConfig().SetServiceState(true))

	// TODO: type_body: {"triggers":{"create":["functions.a.b.c", "functions.a.b.d", ...],"update":[...],"delete":[...]}}
	// TODO: functions.a.b.c is being called on "from_id" (NOT ON "to_id") object link goes out from
	// TODO: Create payload: {"trigger.link.create": {"to": id, "type": typename, "new_body": {}}}
	statefun.NewFunctionType(runtime, "functions.cmdb.api.types.link.create", CreateTypesLink, *statefun.NewFunctionTypeConfig().SetServiceState(true))
	// TODO: Update payload: {"trigger.link.update": {"to": id, "type": typename, "old_body": {}, "new_body": {}}}
	statefun.NewFunctionType(runtime, "functions.cmdb.api.types.link.update", UpdateTypesLink, *statefun.NewFunctionTypeConfig().SetServiceState(true))
	// TODO: Delete payload: {"trigger.link.delete": {"to": id, "type": typename, "old_body": {}}}
	statefun.NewFunctionType(runtime, "functions.cmdb.api.types.link.delete", DeleteTypesLink, *statefun.NewFunctionTypeConfig().SetServiceState(true))

	// TODO: Put triggers onto these functions
	// TODO: Low level API must return list of all operations it did, so high level api would iterate them and apply triggers when needed
	statefun.NewFunctionType(runtime, "functions.cmdb.api.object.create", CreateObject, *statefun.NewFunctionTypeConfig().SetServiceState(true))
	statefun.NewFunctionType(runtime, "functions.cmdb.api.object.update", UpdateObject, *statefun.NewFunctionTypeConfig().SetServiceState(true))
	statefun.NewFunctionType(runtime, "functions.cmdb.api.object.delete", DeleteObject, *statefun.NewFunctionTypeConfig().SetServiceState(true))

	statefun.NewFunctionType(runtime, "functions.cmdb.api.objects.link.create", CreateObjectsLink, *statefun.NewFunctionTypeConfig().SetServiceState(true))
	statefun.NewFunctionType(runtime, "functions.cmdb.api.objects.link.update", UpdateObjectsLink, *statefun.NewFunctionTypeConfig().SetServiceState(true))
	statefun.NewFunctionType(runtime, "functions.cmdb.api.objects.link.delete", DeleteObejectsLink, *statefun.NewFunctionTypeConfig().SetServiceState(true))
	// ---------------------------------------
	// High-Level API End Registration

	// Low-Level API
	statefun.NewFunctionType(runtime, "functions.graph.api.vertex.create", LLAPIVertexCreate, *statefun.NewFunctionTypeConfig().SetServiceState(true))
	statefun.NewFunctionType(runtime, "functions.graph.api.vertex.update", LLAPIVertexUpdate, *statefun.NewFunctionTypeConfig().SetServiceState(true))
	statefun.NewFunctionType(runtime, "functions.graph.api.vertex.delete", LLAPIVertexDelete, *statefun.NewFunctionTypeConfig().SetServiceState(true))

	statefun.NewFunctionType(runtime, "functions.graph.api.link.create", LLAPILinkCreate, *statefun.NewFunctionTypeConfig().SetServiceState(true))
	statefun.NewFunctionType(runtime, "functions.graph.api.link.update", LLAPILinkUpdate, *statefun.NewFunctionTypeConfig().SetServiceState(true))
	statefun.NewFunctionType(runtime, "functions.graph.api.link.delete", LLAPILinkDelete, *statefun.NewFunctionTypeConfig().SetServiceState(true))
}
