package crud

import (
	"fmt"
	"strings"

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
		fmt.Println(">>>>>>>>>> \"", k, "\"")
		keys = append(keys, k)
	}

	return keys
}

func getObjectAllTypesBaseAndParents(ctx *sfPlugins.StatefunContextProcessor, objectId string) (result map[string]struct{}) {
	result = map[string]struct{}{}

	targetObjectType, err := findObjectType(ctx, objectId)
	if err != nil {
		lg.Logln(lg.ErrorLevel, "getObjectAllTypesBaseAndParents findObjectType for id=%s: %s", objectId, err.Error())
		return
	}

	targetObjectType = ctx.Domain.CreateObjectIDWithHubDomain(targetObjectType, true)
	result[targetObjectType] = struct{}{}

	om := sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.type.read", makeSequenceFreeParentBasedID(ctx, targetObjectType), injectParentHoldsLocks(ctx, nil), nil))
	if om.Data.PathExists("body.cache.parent_types") {
		parentTypes := om.Data.GetByPath("body.cache.parent_types")
		for i := 0; i < parentTypes.ArraySize(); i++ {
			parentType := parentTypes.ArrayElement(i).AsStringDefault("")
			parentType = ctx.Domain.CreateObjectIDWithHubDomain(parentType, true)
			if len(parentType) > 0 {
				result[parentType] = struct{}{}
			}
		}
	}

	return
}

func isObjectLinkPermittedForClaimedTypes(ctx *sfPlugins.StatefunContextProcessor, fromObjectId, toObjectId, fromObjectClaimType, toObjectClaimType string) string {
	fromObjectAllTypes := getObjectAllTypesBaseAndParents(ctx, fromObjectId)
	toObjectAllTypes := getObjectAllTypesBaseAndParents(ctx, toObjectId)

	if _, ok := fromObjectAllTypes[fromObjectClaimType]; !ok {
		return ""
	}
	if _, ok := toObjectAllTypes[toObjectClaimType]; !ok {
		return ""
	}
	s, err := getObjectsLinkTypeFromTypesLink(ctx, fromObjectClaimType, toObjectClaimType)
	if len(s) == 0 || err != nil {
		return ""
	}

	return s
}

func deleteObjectOutLinkIfInvalidByInheritance(ctx *sfPlugins.StatefunContextProcessor, fromObjectId, outLinkType, toObjectId string) {
	tokens := strings.Split(outLinkType, "#")
	if len(tokens) == 3 {
		fromParentType := tokens[0]
		toParentType := tokens[1]
		fmt.Println("--- 4.3.2.1", fromObjectId, toObjectId, fromParentType, toParentType)
		if len(isObjectLinkPermittedForClaimedTypes(ctx, fromObjectId, toObjectId, fromParentType, toParentType)) == 0 {
			fmt.Println("--- 4.3.2.2")
			objectLink := easyjson.NewJSONObject()
			objectLink.SetByPath("to", easyjson.NewJSON(toObjectId))
			objectLink.SetByPath("type", easyjson.NewJSON(outLinkType))
			ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.delete", makeSequenceFreeParentBasedID(ctx, fromObjectId), injectParentHoldsLocks(ctx, &objectLink), ctx.Options)
			fmt.Println("--- 4.3.2.3")
		}
		fmt.Println("--- 4.3.2.4")
	}
}

func runInvalidateObjectLinks(ctx *sfPlugins.StatefunContextProcessor, objectId string) {
	fmt.Println("--- 4.3.1", objectId)

	var outLinks *easyjson.JSON

	payload := easyjson.NewJSONObjectWithKeyValue("details", easyjson.NewJSON(true))
	som := sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.read", makeSequenceFreeParentBasedID(ctx, objectId), injectParentHoldsLocks(ctx, &payload), nil))
	if som.Status == sfMediators.SYNC_OP_STATUS_OK {
		outLinks = som.Data.GetByPath("links.out").GetPtr()
	}
	if outLinks != nil {
		for i := 0; i < outLinks.GetByPath("names").ArraySize(); i++ {
			linkType := outLinks.GetByPath("types").ArrayElement(i).AsStringDefault("")
			targetObjectId := outLinks.GetByPath("ids").ArrayElement(i).AsStringDefault("")
			fmt.Println("--- 4.3.2", objectId, linkType, targetObjectId)
			deleteObjectOutLinkIfInvalidByInheritance(ctx, objectId, linkType, targetObjectId)
		}
	}
}

func runCascadeObjectLinkRefreshStartingForTypeWithID(ctx *sfPlugins.StatefunContextProcessor, typeId string) {
	fmt.Println("--- 4.1", typeId)
	typeObjects, err := findTypeObjects(ctx, typeId)
	fmt.Println("--- 4.2")
	if err != nil {
		lg.Logln(lg.ErrorLevel, "runCascadeObjectLinkRefreshStartingForTypeWithID: %s", err.Error())
		return
	}

	fmt.Println("--- 4.3")
	for _, objectId := range typeObjects {
		runInvalidateObjectLinks(ctx, objectId)
	}

	fmt.Println("--- 4.4")
}

func InheritaceGoalPrepare(ctx *sfPlugins.StatefunContextProcessor, goal InheritanceCascadeDeleteGoalType) []string {
	fmt.Println("--- 1")
	typesToRefresh := []string{}
	switch goal.reason {
	case ParentTypeDeleteType:
		{
			typesToRefresh = gatherTypes4CascadeObjectLinkRefreshStartingFromTypeWithID(ctx, ctx.Self.ID, false) // ctx.Self.ID is parent type
			// Delete type after
		}
	case ParentTypeDeleteChild:
		{
			// Unlink child type first
			typesToRefresh = gatherTypes4CascadeObjectLinkRefreshStartingFromTypeWithID(ctx, goal.target, true) // goal.target is child type
		}
	case ParentTypeDeleteOutTypeObjectLink:
		{
			// Delete types' object link first
			typesToRefresh = gatherTypes4CascadeObjectLinkRefreshStartingFromTypeWithID(ctx, ctx.Self.ID, false) // ctx.Self.ID is parent type
		}
	}
	fmt.Println("--- 2", typesToRefresh)
	return typesToRefresh
}

func InheritaceGoalFinalize(ctx *sfPlugins.StatefunContextProcessor, typesToRefresh []string) {
	fmt.Println("--- 3")
	UpdateTypeModelVersion(ctx)
	fmt.Println("--- 4")
	for _, typeId := range typesToRefresh {
		runCascadeObjectLinkRefreshStartingForTypeWithID(ctx, typeId)
	}
	fmt.Println("--- 5")
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

	fmt.Println("^^^^^^^^^", result)
	return result
}
