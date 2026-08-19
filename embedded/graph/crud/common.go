package crud

import (
	"github.com/foliagecp/sdk/statefun"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
)

const (
	KeySuff1Pattern = "%s"
	KeySuff2Pattern = "%s.%s"
	KeySuff3Pattern = "%s.%s.%s"
	KeySuff4Pattern = "%s.%s.%s.%s"

	// Link related keys ----------------------------------

	// key=fmt.Sprintf(OutLinkBodyKeyPrefPattern+KeySuff1Pattern, <fromVertexId>, <linkName>), value=<linkType.toVertexId>
	OutLinkTargetKeyPrefPattern = "%s.out.to."

	// key=fmt.Sprintf(OutLinkBodyKeyPrefPattern+KeySuff2Pattern, <fromVertexId>, <linkName>), value=<linkBody>
	OutLinkBodyKeyPrefPattern = "%s.out.body."

	// key=fmt.Sprintf(OutLinkBodyKeyPrefPattern+KeySuff2Pattern, <fromVertexId>, <linkType>, <toVertexId>), value=<linkName>
	OutLinkTypeKeyPrefPattern = "%s.ltype."

	// key=fmt.Sprintf(OutLinkIndexPrefPattern+KeySuff3Pattern, <fromVertexId>, <linkName>, <index_name>, <value>), value=nil
	OutLinkIndexPrefPattern = "%s.out.index."
	// key=fmt.Sprintf(InLinkKeyPrefPattern+KeySuff2Pattern, <toVertexId>, <fromVertexId>, <linkName>), value=linkType
	InLinkKeyPrefPattern = "%s.in."
)

// RegisterAllFunctionTypes registers the CRUD API on the runtime.
//
// protectedBodyFields, when given, declares the protected top-level body keys
// for the whole graph: they are published into the built-in `objects` vertex at
// startup, and every application working on that graph — including ones
// configured differently — reads them from there. The caller decides where the
// list comes from (its own environment, a config file, a constant). Passing
// nothing keeps this runtime a follower: it publishes no policy and enforces
// whatever the graph already declares.
func RegisterAllFunctionTypes(runtime *statefun.Runtime, protectedBodyFields ...string) {
	RegisterPolyTypeFunctions(runtime)

	// High-Level API Helpers
	statefun.NewFunctionType(runtime, "functions.cmdb.api.delete_object_filtered_out_links", DeleteObjectFilteredOutLinksStatefun, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).SetAllowedSignalProviders())

	// High-Level API Registration
	statefun.NewFunctionType(runtime, "functions.cmdb.api.type.create", CreateType, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect))
	statefun.NewFunctionType(runtime, "functions.cmdb.api.type.update", UpdateType, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect))
	statefun.NewFunctionType(runtime, "functions.cmdb.api.type.delete", DeleteType, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect))
	statefun.NewFunctionType(runtime, "functions.cmdb.api.type.read", ReadType, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect))

	statefun.NewFunctionType(runtime, "functions.cmdb.api.types.link.create", CreateTypesLink, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect))
	statefun.NewFunctionType(runtime, "functions.cmdb.api.types.link.update", UpdateTypesLink, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect))
	statefun.NewFunctionType(runtime, "functions.cmdb.api.types.link.delete", DeleteTypesLink, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect))
	statefun.NewFunctionType(runtime, "functions.cmdb.api.types.link.read", ReadTypesLink, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect))

	statefun.NewFunctionType(runtime, "functions.cmdb.api.object.create", CreateObject, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect))
	statefun.NewFunctionType(runtime, "functions.cmdb.api.object.update", UpdateObject, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect))
	statefun.NewFunctionType(runtime, "functions.cmdb.api.object.delete", DeleteObject, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect))
	statefun.NewFunctionType(runtime, "functions.cmdb.api.object.read", ReadObject, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect))

	statefun.NewFunctionType(runtime, "functions.cmdb.api.objects.link.create", CreateObjectsLink, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect))
	statefun.NewFunctionType(runtime, "functions.cmdb.api.objects.link.update", UpdateObjectsLink, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect))
	statefun.NewFunctionType(runtime, "functions.cmdb.api.objects.link.delete", DeleteObjectsLink, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect))
	statefun.NewFunctionType(runtime, "functions.cmdb.api.objects.link.read", ReadObjectsLink, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect))

	statefun.NewFunctionType(runtime, "functions.cmdb.api.schema.ensure", EnsureBuiltInSchemaFunction, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect))

	// Low-Level API Registration
	statefun.NewFunctionType(runtime, "functions.graph.api.vertex.create", LLAPIVertexCreate, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect))
	statefun.NewFunctionType(runtime, "functions.graph.api.vertex.update", LLAPIVertexUpdate, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect))
	statefun.NewFunctionType(runtime, "functions.graph.api.vertex.delete", LLAPIVertexDelete, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect))
	statefun.NewFunctionType(runtime, "functions.graph.api.vertex.read", LLAPIVertexRead, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect))

	statefun.NewFunctionType(runtime, "functions.graph.api.link.create", LLAPILinkCreate, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect))
	statefun.NewFunctionType(runtime, "functions.graph.api.link.update", LLAPILinkUpdate, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect))
	statefun.NewFunctionType(runtime, "functions.graph.api.link.delete", LLAPILinkDelete, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect))
	statefun.NewFunctionType(runtime, "functions.graph.api.link.read", LLAPILinkRead, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect))

	// The graph belongs to the hub domain, so that is where the built-in schema
	// is created and repaired — and where a declared protection policy is
	// published.
	if runtime.Domain.Name() == runtime.Domain.HubDomainName() {
		runtime.RegisterOnAfterStartFunction(cmdbSchemaPrepare(protectedBodyFields), false)
	}
	// Nothing to register for READING the policy back: the runtime itself pulls
	// what the graph declares protected once its afterStart functions are done
	// (statefun.Domain.PullProtectedBodyFields). Several applications routinely
	// share one graph — and one domain: one provides the CRUD layer, creates the
	// schema and declares what is protected, the others merely attach. Making
	// that pull a property of the runtime rather than of this package is what
	// lets an application which never registers CRUD still enforce the policy.
	// Trash can retention sweep — in EVERY domain: a domain's parked objects
	// live in its own cache, so each domain sweeps its own bin.
	runtime.RegisterOnAfterStartFunction(trashCanRetentionSweep, false)
}
