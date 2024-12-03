package crud

import (
	"context"
	"regexp"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/statefun"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
)

const (
	KeySuff1Pattern = "%s"
	KeySuff2Pattern = "%s.%s"
	KeySuff3Pattern = "%s.%s.%s"
	KeySuff4Pattern = "%s.%s.%s.%s"

	// Vertex related keys --------------------------------

	// key=fmt.Sprintf(BodyValueIndexPrefPattern+KeySuff2Pattern, <vertexId>, <bodyKeyName>, <valueType>), value=<bodyKeyValue>
	VertexBodyValueIndexPrefPattern = "%s.body.index."

	// Link related keys ----------------------------------

	// key=fmt.Sprintf(BodyValueIndexPrefPattern+KeySuff3Pattern, <fromVertexId>, <linkName>, <bodyKeyName>, <valueType>), value=<bodyKeyValue>
	LinkBodyValueIndexPrefPattern = "%s.out.body.index."

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

const (
	// crud_op.<op_target>.<op_id>
	opRegisterPrefixTemplate = "crud_op.%s.%s"
	// crud_op.<op_target>.<op_id>.<op_type>.<op_time>
	opRegisterTemplate = opRegisterPrefixTemplate + ".%s.%d"
)

const (
	ROOT_TYPES_LINKTYPE   = "__types"
	ROOT_OBJECTS_LINKTYPE = "__objects"

	TYPES_TYPE_LINKTYPE     = "__types_type"
	OBJECTS_OBJECT_TYPELINK = "__objects_object"

	TYPE_TYPE_LINKTYPE   = "__type_type"
	TYPE_OBJECT_LINKTYPE = "__type_object"

	GROUP_TYPELINK      = "group"
	BUILT_IN_TYPES      = "types"
	BUILT_IN_OBJECTS    = "objects"
	BUILT_IN_ROOT       = "root"
	BUILT_IN_TYPE_GROUP = "group"
	BUILT_IN_OBJECT_NAV = "nav"

	OBJECT_TYPE_LINKTYPE = "__object_type"
)

var (
	CRUDValidTypes = map[string]struct{}{
		"create": {},
		"update": {},
		"delete": {},
		"read":   {},
	}
	validLinkName = regexp.MustCompile(`\A[a-zA-Z0-9\/_-]+\z`)
)

func unifiedCRUDDataAggregator(om *sfMediators.OpMediator) easyjson.JSON {
	aggregatedData := easyjson.NewJSONNull()
	for _, opMsg := range om.GetAggregatedOpMsgs() {
		if opMsg.Data.IsNonEmptyObject() {
			if aggregatedData.IsNull() {
				aggregatedData = opMsg.Data.Clone()
			} else {
				aggregatedData.DeepMerge(opMsg.Data)
			}
		}
	}
	return aggregatedData
}

func cmdbSchemaPrepare(ctx context.Context, runtime *statefun.Runtime) error {
	// ----------------------------------------------------
	{
		payload := easyjson.NewJSONObject()
		payload.SetByPath("operation.target", easyjson.NewJSON("vertex"))
		payload.SetByPath("operation.type", easyjson.NewJSON("create"))
		system.MsgOnErrorReturn(runtime.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.crud", BUILT_IN_ROOT, &payload, nil))
		system.MsgOnErrorReturn(runtime.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.crud", BUILT_IN_TYPES, &payload, nil))
		system.MsgOnErrorReturn(runtime.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.crud", BUILT_IN_OBJECTS, &payload, nil))
	}
	{
		payload := easyjson.NewJSONObject()
		payload.SetByPath("operation.target", easyjson.NewJSON("vertex.link"))
		payload.SetByPath("operation.type", easyjson.NewJSON("create"))
		payload.SetByPath("data.to", easyjson.NewJSON(BUILT_IN_TYPES))
		payload.SetByPath("data.type", easyjson.NewJSON(ROOT_TYPES_LINKTYPE))
		payload.SetByPath("data.name", easyjson.NewJSON(runtime.Domain.CreateObjectIDWithHubDomain(BUILT_IN_TYPES, false)))
		system.MsgOnErrorReturn(runtime.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.crud", BUILT_IN_ROOT, &payload, nil))
	}
	{
		payload := easyjson.NewJSONObject()
		payload.SetByPath("operation.target", easyjson.NewJSON("vertex.link"))
		payload.SetByPath("operation.type", easyjson.NewJSON("create"))
		payload.SetByPath("data.to", easyjson.NewJSON(BUILT_IN_OBJECTS))
		payload.SetByPath("data.type", easyjson.NewJSON(ROOT_OBJECTS_LINKTYPE))
		payload.SetByPath("data.name", easyjson.NewJSON(runtime.Domain.CreateObjectIDWithHubDomain(BUILT_IN_OBJECTS, false)))
		system.MsgOnErrorReturn(runtime.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.crud", BUILT_IN_ROOT, &payload, nil))
	}
	// ----------------------------------------------------
	// ----------------------------------------------------
	{
		payload := easyjson.NewJSONObject()
		payload.SetByPath("operation.target", easyjson.NewJSON("type"))
		payload.SetByPath("operation.type", easyjson.NewJSON("create"))
		system.MsgOnErrorReturn(runtime.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.crud", BUILT_IN_TYPE_GROUP, nil, nil))
	}
	{
		payload := easyjson.NewJSONObject()
		payload.SetByPath("operation.target", easyjson.NewJSON("type.relation"))
		payload.SetByPath("operation.type", easyjson.NewJSON("create"))
		payload.SetByPath("data.to", easyjson.NewJSON(BUILT_IN_TYPE_GROUP))
		payload.SetByPath("data.object_relation_type", easyjson.NewJSON(GROUP_TYPELINK))
		system.MsgOnErrorReturn(runtime.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.crud", BUILT_IN_TYPE_GROUP, &payload, nil))
	}
	{
		payload := easyjson.NewJSONObject()
		payload.SetByPath("operation.target", easyjson.NewJSON("object"))
		payload.SetByPath("operation.type", easyjson.NewJSON("create"))
		system.MsgOnErrorReturn(runtime.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.crud", BUILT_IN_OBJECT_NAV, &payload, nil))
	}
	// ----------------------------------------------------
	return nil
}
