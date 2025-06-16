package crud

import (
	"github.com/foliagecp/easyjson"
	lg "github.com/foliagecp/sdk/statefun/logger"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
)

type InheritanceCascadeDeleteReasonType int

const (
	ParentTypeDeleteType InheritanceCascadeDeleteReasonType = iota
	ParentTypeDeleteChild
	ParentTypeDeleteOutTypeObjectLink
)

type InheritanceCascadeDeleteGoalType struct {
	reason InheritanceCascadeDeleteReasonType
	target string
}

func gatherTypes4CascadeObjectLinkRefreshStartingFromTypeWithID(ctx *sfPlugins.StatefunContextProcessor, typeId string, includeTypeId bool) []string {
	foundChildTypes := map[string]struct{}{}

	queue := []string{ctx.Self.ID}
	for len(queue) > 0 {
		currentType := queue[0]
		queue = queue[1:]

		parentTypes := getTypeChildren(ctx, currentType)

		for _, childType := range parentTypes {
			if _, ok := foundChildTypes[childType]; !ok {
				foundChildTypes[childType] = struct{}{}
				queue = append(queue, childType)
			}
		}
	}

	if includeTypeId {
		foundChildTypes[typeId] = struct{}{}
	}

	keys := make([]string, 0, len(foundChildTypes))
	for k := range foundChildTypes {
		keys = append(keys, k)
	}

	return keys
}

func deleteObjectOutLinkIfInvalidByInheritance(ctx *sfPlugins.StatefunContextProcessor, objectId string, outLinkName string) {

}

func runInvalidateObjectLinks(ctx *sfPlugins.StatefunContextProcessor, objectId string) {
	var outLinks *easyjson.JSON

	payload := easyjson.NewJSONObjectWithKeyValue("details", easyjson.NewJSON(true))
	som := sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.read", makeSequenceFreeParentBasedID(ctx, typeName), injectParentHoldsLocks(ctx, &payload), nil))
	if som.Status == sfMediators.SYNC_OP_STATUS_OK {
		outLinks = som.Data.GetByPath("links.out").GetPtr()
	}
	if outLinks != nil {
		for i := 0; i < outLinks.GetByPath("names").ArraySize(); i++ {
			linkName := outLinks.GetByPath("names").ArrayElement(i).AsStringDefault("")
			deleteObjectOutLinkIfInvalidByInheritance(ctx, objectId, linkName)
		}
	}
}

func runCascadeObjectLinkRefreshStartingForTypeWithID(ctx *sfPlugins.StatefunContextProcessor, typeId string) {
	typeObjects, err := findTypeObjects(ctx, typeId)
	if err != nil {
		lg.Logln(lg.ErrorLevel, "runCascadeObjectLinkRefreshStartingForTypeWithID: %s", err.Error())
		return
	}

	for _, objectId := range typeObjects {
		runInvalidateObjectLinks(ctx, objectId)
	}
}

func CascadeDeleteInvalidObjectLinksPermittedByInheritanceFromTypeAtSelfIDIfNeeded(ctx *sfPlugins.StatefunContextProcessor, goal InheritanceCascadeDeleteGoalType) {
	/*
		func gatherTypes4CascadeObjectLinkRefreshStartingFromTypeWithID(ctx *sfPlugins.StatefunContextProcessor, typeId string, includeTypeId bool) []string {
		}
	*/
	/*
		func runCascadeObjectLinkRefreshStartingForTypeWithID(ctx *sfPlugins.StatefunContextProcessor, typeId string) {
		}
	*/
	/*
		typesToRefresh := []string{}
		if goal.reason == ParentTypeDeleteOutTypeObjectLink {
			delete_out_object_link_2_type(goal.target)

			typesToRefresh = gatherTypes4CascadeObjectLinkRefreshStartingFromTypeWithID(ctx, self.ID, false)
		}

		if goal.reason == ParentTypeDeleteChild {
			delete_child_type(goal.target)

			typesToRefresh = gatherTypes4CascadeObjectLinkRefreshStartingFromTypeWithID(ctx, goal.target, true)
		}

		if goal.reason == ParentTypeDeleteType {
			typesToRefresh := gatherTypes4CascadeObjectLinkRefreshStartingFromTypeWithID(ctx, self.ID, false)

			delete_type(self.ID)
		}

		for typeId := range typesToRefresh {
			runCascadeObjectLinkRefreshStartingForTypeWithID(ctx, typeId)
		}
	*/

	typesToRefresh := []string{}
	switch goal.reason {
	case ParentTypeDeleteType:
		{
			typesToRefresh = gatherTypes4CascadeObjectLinkRefreshStartingFromTypeWithID(ctx, ctx.Self.ID, false) // Parent type ctx.Self.ID is not needed
			ctx.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.type.delete", ctx.Self.ID, nil, nil)
		}
	case ParentTypeDeleteChild:
		{
			payload := easyjson.NewJSONObjectWithKeyValue("child_type", easyjson.NewJSON(goal.target))
			ctx.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.inherit.type.remove.child", ctx.Self.ID, &payload, nil)

			typesToRefresh = gatherTypes4CascadeObjectLinkRefreshStartingFromTypeWithID(ctx, goal.target, true) // Child type ctx.Self.ID is needed
		}
	case ParentTypeDeleteOutTypeObjectLink:
		{
			payload := easyjson.NewJSONObject()
			payload.SetByPath("to", easyjson.NewJSON(goal.target))
			ctx.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.objects.link.delete", ctx.Self.ID, &payload, nil)

			typesToRefresh = gatherTypes4CascadeObjectLinkRefreshStartingFromTypeWithID(ctx, goal.target, false) // Parent type ctx.Self.ID is not needed
		}
	}

	for typeId := range typesToRefresh {
		runCascadeObjectLinkRefreshStartingForTypeWithID(ctx, typeId)
	}
}

func UpdateTypeModelVersion(ctx *sfPlugins.StatefunContextProcessor) {
	typesVertexId := ctx.Domain.CreateObjectIDWithHubDomain(BUILT_IN_TYPES, false)

	payload := easyjson.NewJSONObject()
	payload.SetByPath("body.version", easyjson.NewJSON(system.GetUniqueStrID()))
	ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.update", makeSequenceFreeParentBasedID(ctx, typesVertexId), injectParentHoldsLocks(ctx, &payload), nil)
}

func RecalculateInheritanceCacheForTypeAtSelfIDIfNeeded(ctx *sfPlugins.StatefunContextProcessor) {
	typeCacheVersion, typeModelVersion, typeBody := getTypeCacheVersionAndGlobalVersion(ctx, ctx.Self.ID)

	if typeCacheVersion != typeModelVersion && typeModelVersion != "" {
		foundParentTypes := map[string]struct{}{}

		queue := []string{ctx.Self.ID}
		for len(queue) > 0 {
			currentType := queue[0]
			queue = queue[1:]

			parentTypes := getTypeParents(ctx, currentType)

			for _, parentType := range parentTypes {
				if _, ok := foundParentTypes[parentType]; !ok {
					foundParentTypes[parentType] = struct{}{}
					queue = append(queue, parentType)
				}
			}
		}

		keys := make([]string, 0, len(foundParentTypes))
		for k := range foundParentTypes {
			keys = append(keys, k)
		}

		newTypeBody := typeBody.Clone()
		newTypeBody.RemoveByPath("cache")
		newTypeBody.SetByPath("cache.parent_types", easyjson.JSONFromArray(keys))
		newTypeBody.SetByPath("cache.version", easyjson.NewJSON(typeModelVersion))

		payload := easyjson.NewJSONObject()
		payload.SetByPath("body", newTypeBody)
		payload.SetByPath("replace", easyjson.NewJSON(true))
		ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.update", makeSequenceFreeParentBasedID(ctx, ctx.Self.ID), injectParentHoldsLocks(ctx, &payload), nil)
	}
}

func getTypeCacheVersionAndGlobalVersion(ctx *sfPlugins.StatefunContextProcessor, typeName string) (typeCacheVersion, typeModelVersion string, typeBody *easyjson.JSON) {
	typeCacheVersion = ""
	typeModelVersion = ""
	typeBody = nil

	som1 := sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.read", makeSequenceFreeParentBasedID(ctx, typeName), injectParentHoldsLocks(ctx, nil), nil))
	if som1.Status == sfMediators.SYNC_OP_STATUS_OK {
		typeBody = som1.Data.GetByPathPtr("body")
		typeCacheVersion = som1.Data.GetByPath("body.cache.version").AsStringDefault("")
	}

	typesVertexId := ctx.Domain.CreateObjectIDWithHubDomain(BUILT_IN_TYPES, false)
	som2 := sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.read", makeSequenceFreeParentBasedID(ctx, typesVertexId), injectParentHoldsLocks(ctx, nil), nil))
	if som2.Status == sfMediators.SYNC_OP_STATUS_OK {
		typeModelVersion = som2.Data.GetByPath("body.version").AsStringDefault("")
	}

	return
}

func getTypeParents(ctx *sfPlugins.StatefunContextProcessor, typeName string) []string {
	result := []string{}

	var inLinks *easyjson.JSON

	payload := easyjson.NewJSONObjectWithKeyValue("details", easyjson.NewJSON(true))
	som := sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.read", makeSequenceFreeParentBasedID(ctx, typeName), injectParentHoldsLocks(ctx, &payload), nil))
	if som.Status == sfMediators.SYNC_OP_STATUS_OK {
		inLinks = som.Data.GetByPath("links.in").GetPtr()
	}
	if inLinks == nil {
		return result
	}

	for i := 0; i < inLinks.ArraySize(); i++ {
		inLink := inLinks.ArrayElement(i)
		from := inLink.GetByPath("from").AsStringDefault("")
		linkType := inLink.GetByPath("type").AsStringDefault("")
		if linkType == TYPES_CHILDLINK {
			result = append(result, from)
		}
	}

	return result
}

func getTypeChildren(ctx *sfPlugins.StatefunContextProcessor, typeName string) []string {
	result := []string{}

	var outLinks *easyjson.JSON

	payload := easyjson.NewJSONObjectWithKeyValue("details", easyjson.NewJSON(true))
	som := sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.read", makeSequenceFreeParentBasedID(ctx, typeName), injectParentHoldsLocks(ctx, &payload), nil))
	if som.Status == sfMediators.SYNC_OP_STATUS_OK {
		outLinks = som.Data.GetByPath("links.out").GetPtr()
	}
	if outLinks == nil {
		return result
	}

	for i := 0; i < outLinks.GetByPath("names").ArraySize(); i++ {
		linkType := outLinks.GetByPath("types").ArrayElement(i).AsStringDefault("")
		to := outLinks.GetByPath("ids").ArrayElement(i).AsStringDefault("")
		if linkType == TYPES_CHILDLINK {
			result = append(result, to)
		}
	}

	return result
}
