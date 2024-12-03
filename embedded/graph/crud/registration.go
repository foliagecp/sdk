package crud

import (
	"github.com/foliagecp/sdk/statefun"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
)

func RegisterAllFunctionTypes(runtime *statefun.Runtime) {
	// Deprecated -----------------------------------------
	statefun.NewFunctionType(runtime, "functions.graph.api.vertex.create", LLAPIVertexCreate, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).SetMaxIdHandlers(-1))
	statefun.NewFunctionType(runtime, "functions.graph.api.vertex.update", LLAPIVertexUpdate, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).SetMaxIdHandlers(-1))
	statefun.NewFunctionType(runtime, "functions.graph.api.vertex.delete", LLAPIVertexDelete, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).SetMaxIdHandlers(-1))
	statefun.NewFunctionType(runtime, "functions.graph.api.vertex.read", LLAPIVertexRead, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).SetMaxIdHandlers(-1))

	statefun.NewFunctionType(runtime, "functions.graph.api.link.create", LLAPILinkCreate, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).SetMaxIdHandlers(-1))
	statefun.NewFunctionType(runtime, "functions.graph.api.link.update", LLAPILinkUpdate, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).SetMaxIdHandlers(-1))
	statefun.NewFunctionType(runtime, "functions.graph.api.link.delete", LLAPILinkDelete, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).SetMaxIdHandlers(-1))
	statefun.NewFunctionType(runtime, "functions.graph.api.link.read", LLAPILinkRead, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).SetMaxIdHandlers(-1))
	// ----------------------------------------------------

	// Graph level API registration
	statefun.NewFunctionType(runtime, "functions.graph.api.crud", GraphCRUDGateway, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).SetMaxIdHandlers(-1))
	statefun.NewFunctionType(runtime, "functions.graph.api.dirty.vertex.read", GraphDirtyVertexRead, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).SetMaxIdHandlers(-1))
	statefun.NewFunctionType(runtime, "functions.graph.api.dirty.vertex.link.read", GraphDirtyVertexLinkRead, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).SetMaxIdHandlers(-1))

	// CMDB level API registration
	statefun.NewFunctionType(runtime, "functions.cmdb.api.crud.isolated", CMDB_CRUDIsolated, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).SetMaxIdHandlers(-1))
	statefun.NewFunctionType(runtime, "functions.cmdb.api.crud", CMDB_CRUDGateway, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).SetMaxIdHandlers(-1))
	statefun.NewFunctionType(runtime, "functions.cmdb.api.dirty.type.read", CMDBDirtyTypeRead, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).SetMaxIdHandlers(-1))
	statefun.NewFunctionType(runtime, "functions.cmdb.api.dirty.type.relation.read", CMDBDirtyTypeRelationRead, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).SetMaxIdHandlers(-1))
	statefun.NewFunctionType(runtime, "functions.cmdb.api.dirty.object.read", CMDBDirtyObjectRead, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).SetMaxIdHandlers(-1))
	statefun.NewFunctionType(runtime, "functions.cmdb.api.dirty.object.relation.read", CMDBDirtyObjectRelationRead, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).SetMaxIdHandlers(-1))

	if runtime.Domain.Name() == runtime.Domain.HubDomainName() {
		runtime.RegisterOnAfterStartFunction(cmdbSchemaPrepare, false)
	}
}
