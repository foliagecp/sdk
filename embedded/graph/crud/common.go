package crud

import (
	"github.com/foliagecp/sdk/statefun"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
)

const (
	LinkKeySuff1Pattern = "%s"
	LinkKeySuff2Pattern = "%s.%s"
	LinkKeySuff3Pattern = "%s.%s.%s"
	LinkKeySuff4Pattern = "%s.%s.%s.%s"

	// key=fmt.Sprintf(OutLinkBodyKeyPrefPattern+LinkKeySuff1Pattern, <fromVertexId>, <linkName>), value=<linkType.toVertexId>
	OutLinkTargetKeyPrefPattern = "%s.out.to."

	// key=fmt.Sprintf(OutLinkBodyKeyPrefPattern+LinkKeySuff2Pattern, <fromVertexId>, <linkName>), value=<linkBody>
	OutLinkBodyKeyPrefPattern = "%s.out.body."

	// key=fmt.Sprintf(OutLinkBodyKeyPrefPattern+LinkKeySuff2Pattern, <fromVertexId>, <linkType>, <toVertexId>), value=<linkName>
	OutLinkTypeKeyPrefPattern = "%s.ltype."

	// key=fmt.Sprintf(OutLinkIndexPrefPattern+LinkKeySuff3Pattern, <fromVertexId>, <linkName>, <index_name>, <value>), value=nil
	OutLinkIndexPrefPattern = "%s.out.index."
	// key=fmt.Sprintf(InLinkKeyPrefPattern+LinkKeySuff2Pattern, <toVertexId>, <fromVertexId>, <linkName>), value=nil
	InLinkKeyPrefPattern = "%s.in."
)

func RegisterAllFunctionTypes(runtime *statefun.Runtime) {
	// High-Level API Helpers
	statefun.NewFunctionType(runtime, "functions.cmdb.api.delete_object_filtered_out_links", DeleteObjectFilteredOutLinksStatefun, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).SetAllowedSignalProviders().SetMaxIdHandlers(-1))

	// High-Level API Registration
	statefun.NewFunctionType(runtime, "functions.cmdb.api.type.create", CreateType, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).SetMaxIdHandlers(-1))
	statefun.NewFunctionType(runtime, "functions.cmdb.api.type.update", UpdateType, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).SetMaxIdHandlers(-1))
	statefun.NewFunctionType(runtime, "functions.cmdb.api.type.delete", DeleteType, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).SetMaxIdHandlers(-1))
	statefun.NewFunctionType(runtime, "functions.cmdb.api.type.read", ReadType, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).SetMaxIdHandlers(-1))

	statefun.NewFunctionType(runtime, "functions.cmdb.api.types.link.create", CreateTypesLink, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).SetMaxIdHandlers(-1))
	statefun.NewFunctionType(runtime, "functions.cmdb.api.types.link.update", UpdateTypesLink, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).SetMaxIdHandlers(-1))
	statefun.NewFunctionType(runtime, "functions.cmdb.api.types.link.delete", DeleteTypesLink, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).SetMaxIdHandlers(-1))
	statefun.NewFunctionType(runtime, "functions.cmdb.api.types.link.read", ReadTypesLink, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).SetMaxIdHandlers(-1))

	statefun.NewFunctionType(runtime, "functions.cmdb.api.object.create", CreateObject, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).SetMaxIdHandlers(-1))
	statefun.NewFunctionType(runtime, "functions.cmdb.api.object.update", UpdateObject, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).SetMaxIdHandlers(-1))
	statefun.NewFunctionType(runtime, "functions.cmdb.api.object.delete", DeleteObject, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).SetMaxIdHandlers(-1))
	statefun.NewFunctionType(runtime, "functions.cmdb.api.object.read", ReadObject, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).SetMaxIdHandlers(-1))

	statefun.NewFunctionType(runtime, "functions.cmdb.api.objects.link.create", CreateObjectsLink, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).SetMaxIdHandlers(-1))
	statefun.NewFunctionType(runtime, "functions.cmdb.api.objects.link.update", UpdateObjectsLink, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).SetMaxIdHandlers(-1))
	statefun.NewFunctionType(runtime, "functions.cmdb.api.objects.link.delete", DeleteObjectsLink, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).SetMaxIdHandlers(-1))
	statefun.NewFunctionType(runtime, "functions.cmdb.api.objects.link.read", ReadObjectsLink, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).SetMaxIdHandlers(-1))

	// Low-Level API Registration
	statefun.NewFunctionType(runtime, "functions.graph.api.vertex.create", LLAPIVertexCreate, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).SetMaxIdHandlers(-1))
	statefun.NewFunctionType(runtime, "functions.graph.api.vertex.update", LLAPIVertexUpdate, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).SetMaxIdHandlers(-1))
	statefun.NewFunctionType(runtime, "functions.graph.api.vertex.delete", LLAPIVertexDelete, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).SetMaxIdHandlers(-1))
	statefun.NewFunctionType(runtime, "functions.graph.api.vertex.read", LLAPIVertexRead, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).SetMaxIdHandlers(-1))

	statefun.NewFunctionType(runtime, "functions.graph.api.link.create", LLAPILinkCreate, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).SetMaxIdHandlers(-1))
	statefun.NewFunctionType(runtime, "functions.graph.api.link.update", LLAPILinkUpdate, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).SetMaxIdHandlers(-1))
	statefun.NewFunctionType(runtime, "functions.graph.api.link.delete", LLAPILinkDelete, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).SetMaxIdHandlers(-1))
	statefun.NewFunctionType(runtime, "functions.graph.api.link.read", LLAPILinkRead, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).SetMaxIdHandlers(-1))

	if runtime.Domain.Name() == runtime.Domain.HubDomainName() {
		runtime.RegisterOnAfterStartFunction(cmdbSchemaPrepare, false)
	}
}
