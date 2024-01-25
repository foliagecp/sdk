package crud

import (
	"github.com/foliagecp/sdk/statefun"
)

const (
	LinkKeySuff1Pattern = "%s"
	LinkKeySuff2Pattern = "%s.%s"
	LinkKeySuff3Pattern = "%s.%s.%s"
	LinkKeySuff4Pattern = "%s.%s.%s.%s"

	// key=fmt.Sprintf(OutLinkBodyKeyPrefPattern+LinkKeySuff2Pattern, <fromVertexId>, <linkType>, <toVertexId>), value=<linkBody>
	OutLinkBodyKeyPrefPattern = "%s.out.body."
	// key=fmt.Sprintf(OutLinkIndexPrefPattern+LinkKeySuff3Pattern, <fromVertexId>, <linkType>, <toVertexId>, <index_name>, <tag>), value=nil
	OutLinkIndexPrefPattern = "%s.out.index."
	// key=fmt.Sprintf(OutLinkLinkNamePrefPattern+LinkKeySuff1Pattern, <fromVertexId>, <link_name>), value=nil
	OutLinkLinkNamePrefPattern = "%s.out.name."
	// key=fmt.Sprintf(OutLinkNameGenKeyPattern, <fromVertexId>), value=counter[int64]
	OutLinkNameGenKeyPattern = "%s.out.namegen"
	// key=fmt.Sprintf(InLinkKeyPrefPattern+LinkKeySuff2Pattern, <toVertexId>, <fromVertexId>, <linkType>), value=nil
	InLinkKeyPrefPattern = "%s.in."
)

const (
	Types   = "types"
	Objects = "objects"

	TypeLink   = "__type"
	ObjectLink = "__object"

	TypeTag = "TYPE_"
)

var (
	llAPIVertexCUDNames = []string{"functions.graph.api.vertex.create", "functions.graph.api.vertex.update", "functions.graph.api.vertex.delete"}
	llAPILinkCUDNames   = []string{"functions.graph.api.link.create", "functions.graph.api.link.update", "functions.graph.api.link.delete"}
)

func RegisterAllFunctionTypes(runtime *statefun.Runtime) {
	// High-Level API Registration
	statefun.NewFunctionType(runtime, "functions.cmdb.api.type.create", CreateType, *statefun.NewFunctionTypeConfig().SetServiceState(true).SetMaxIdHandlers(-1))
	statefun.NewFunctionType(runtime, "functions.cmdb.api.type.update", UpdateType, *statefun.NewFunctionTypeConfig().SetServiceState(true).SetMaxIdHandlers(-1))
	statefun.NewFunctionType(runtime, "functions.cmdb.api.type.delete", DeleteType, *statefun.NewFunctionTypeConfig().SetServiceState(true).SetMaxIdHandlers(-1))

	statefun.NewFunctionType(runtime, "functions.cmdb.api.types.link.create", CreateTypesLink, *statefun.NewFunctionTypeConfig().SetServiceState(true).SetMaxIdHandlers(-1))
	statefun.NewFunctionType(runtime, "functions.cmdb.api.types.link.update", UpdateTypesLink, *statefun.NewFunctionTypeConfig().SetServiceState(true).SetMaxIdHandlers(-1))
	statefun.NewFunctionType(runtime, "functions.cmdb.api.types.link.delete", DeleteTypesLink, *statefun.NewFunctionTypeConfig().SetServiceState(true).SetMaxIdHandlers(-1))

	statefun.NewFunctionType(runtime, "functions.cmdb.api.object.create", CreateObject, *statefun.NewFunctionTypeConfig().SetServiceState(true).SetMaxIdHandlers(-1))
	statefun.NewFunctionType(runtime, "functions.cmdb.api.object.update", UpdateObject, *statefun.NewFunctionTypeConfig().SetServiceState(true).SetMaxIdHandlers(-1))
	statefun.NewFunctionType(runtime, "functions.cmdb.api.object.delete", DeleteObject, *statefun.NewFunctionTypeConfig().SetServiceState(true).SetMaxIdHandlers(-1))

	statefun.NewFunctionType(runtime, "functions.cmdb.api.objects.link.create", CreateObjectsLink, *statefun.NewFunctionTypeConfig().SetServiceState(true).SetMaxIdHandlers(-1))
	statefun.NewFunctionType(runtime, "functions.cmdb.api.objects.link.update", UpdateObjectsLink, *statefun.NewFunctionTypeConfig().SetServiceState(true).SetMaxIdHandlers(-1))
	statefun.NewFunctionType(runtime, "functions.cmdb.api.objects.link.delete", DeleteObjectsLink, *statefun.NewFunctionTypeConfig().SetServiceState(true).SetMaxIdHandlers(-1))

	// Low-Level API Registration
	statefun.NewFunctionType(runtime, llAPIVertexCUDNames[0], LLAPIVertexCreate, *statefun.NewFunctionTypeConfig().SetServiceState(true).SetMaxIdHandlers(-1))
	statefun.NewFunctionType(runtime, llAPIVertexCUDNames[1], LLAPIVertexUpdate, *statefun.NewFunctionTypeConfig().SetServiceState(true).SetMaxIdHandlers(-1))
	statefun.NewFunctionType(runtime, llAPIVertexCUDNames[2], LLAPIVertexDelete, *statefun.NewFunctionTypeConfig().SetServiceState(true).SetMaxIdHandlers(-1))

	statefun.NewFunctionType(runtime, llAPILinkCUDNames[0], LLAPILinkCreate, *statefun.NewFunctionTypeConfig().SetServiceState(true).SetMaxIdHandlers(-1))
	statefun.NewFunctionType(runtime, llAPILinkCUDNames[1], LLAPILinkUpdate, *statefun.NewFunctionTypeConfig().SetServiceState(true).SetMaxIdHandlers(-1))
	statefun.NewFunctionType(runtime, llAPILinkCUDNames[2], LLAPILinkDelete, *statefun.NewFunctionTypeConfig().SetServiceState(true).SetMaxIdHandlers(-1))
}
