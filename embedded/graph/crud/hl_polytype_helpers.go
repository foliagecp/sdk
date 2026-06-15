package crud

import (
	"strings"

	"github.com/foliagecp/easyjson"
	lg "github.com/foliagecp/sdk/statefun/logger"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
)

type PolyTypeCascadeDeleteReasonType int

const (
	SuperTypeDelete PolyTypeCascadeDeleteReasonType = iota
	SuperTypeDeleteSubType
	SuperTypeDeleteOutTypeObjectLink
)

type PolyTypeCascadeDeleteGoalType struct {
	reason PolyTypeCascadeDeleteReasonType
	target string
}

func gatherTypes4CascadeObjectLinkRefreshStartingFromTypeID(ctx *sfPlugins.StatefunContextProcessor, typeId string, includeTypeId bool) []string {
	foundChildTypes := map[string]struct{}{}

	queue := []string{typeId}
	for len(queue) > 0 {
		currentType := queue[0]
		queue = queue[1:]

		childrenTypes := getTypeChildren(ctx, currentType)

		for _, childType := range childrenTypes {
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

func getObjectAllTypesBaseAndParents(ctx *sfPlugins.StatefunContextProcessor, objectId string) (result map[string]struct{}) {
	result = map[string]struct{}{}

	targetObjectType, err := findObjectType(ctx, objectId)
	if err != nil {
		lg.Logln(lg.ErrorLevel, "getObjectAllTypesBaseAndParents findObjectType for id=%s: %s", objectId, err.Error())
		return
	}

	targetObjectType = ctx.Domain.CreateObjectIDWithHubDomain(targetObjectType, true)
	result[targetObjectType] = struct{}{}

	for _, parentType := range getTypeResolvedParentTypes(ctx, targetObjectType) {
		parentType = ctx.Domain.CreateObjectIDWithHubDomain(parentType, true)
		if len(parentType) > 0 {
			result[parentType] = struct{}{}
		}
	}

	return
}

// getTypeResolvedParentTypes returns a type's resolved parent types
// (body.cache.parent_types). It reads them STRAIGHT FROM THE IN-MEMORY CACHE
// whenever the type's inheritance cache is in sync with the global type-model
// version — no mediator round-trip.
//
// This is the hot path of bulk cross-pack (supertype) link creation. Resolving
// each endpoint's inheritance through a mediator type.read made every link
// issue a nested request per endpoint, and because statefun processes one
// message per id at a time, all links re-reading the SAME few endpoint types
// serialized on those types' single-threaded per-id workers — so the per-link
// cost grew with fan-out (a hub object emitting thousands of cross-pack links
// dropped to ~150 links/s regardless of concurrency). The cached value is
// identical to what type.read returns once warm. Only when the cache is cold or
// stale (the type's version != the global version, e.g. while the type model is
// still moving during a first multi-pack build) does it fall back to the
// authoritative mediator type.read, which also recomputes and warms the cache.
func getTypeResolvedParentTypes(ctx *sfPlugins.StatefunContextProcessor, typeId string) []string {
	if parents, ok := freshTypeParentTypesFromCache(ctx, typeId); ok {
		return parents
	}
	// Cold/stale: authoritative path — recomputes and warms the inheritance cache.
	om := sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.type.read", makeSequenceFreeParentBasedID(ctx, typeId), injectParentHoldsLocks(ctx, nil), nil))
	if !om.Data.PathExists("body.cache.parent_types") {
		return nil
	}
	return parentTypesArrayToSlice(om.Data.GetByPath("body.cache.parent_types"))
}

// freshTypeParentTypesFromCache returns the type's parent types from the
// in-memory cache, but ONLY when it can confirm the type's cached inheritance
// version equals the current global type-model version (the same freshness
// guard typeInheritanceCacheMayBeStale uses). It returns ok=false in every
// uncertain case (BUILT_IN_TYPES or the type vertex not cached yet, empty or
// mismatched version) so the caller falls back to the mediator path that
// recomputes and warms the cache. A momentarily stale read is impossible: a
// version mismatch always defers to the authoritative path.
func freshTypeParentTypesFromCache(ctx *sfPlugins.StatefunContextProcessor, typeId string) (parents []string, ok bool) {
	typesVertexId := ctx.Domain.CreateObjectIDWithHubDomain(BUILT_IN_TYPES, false)
	modelBody, err := ctx.Domain.Cache().GetValueJSON(typesVertexId)
	if err != nil {
		return nil, false
	}
	modelVersion := modelBody.GetByPath("version").AsStringDefault("")
	if modelVersion == "" {
		return nil, false
	}
	typeBody, err := ctx.Domain.Cache().GetValueJSON(typeId)
	if err != nil {
		return nil, false
	}
	if typeBody.GetByPath("cache.version").AsStringDefault("") != modelVersion {
		return nil, false
	}
	return parentTypesArrayToSlice(typeBody.GetByPath("cache.parent_types")), true
}

func parentTypesArrayToSlice(arr easyjson.JSON) []string {
	parents := make([]string, 0, arr.ArraySize())
	for i := 0; i < arr.ArraySize(); i++ {
		parents = append(parents, arr.ArrayElement(i).AsStringDefault(""))
	}
	return parents
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
		if len(isObjectLinkPermittedForClaimedTypes(ctx, fromObjectId, toObjectId, fromParentType, toParentType)) == 0 {
			objectLink := easyjson.NewJSONObject()
			objectLink.SetByPath("to", easyjson.NewJSON(toObjectId))
			objectLink.SetByPath("type", easyjson.NewJSON(outLinkType))
			_, err := ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.delete", makeSequenceFreeParentBasedID(ctx, fromObjectId), injectParentHoldsLocks(ctx, &objectLink), ctx.Options)
			system.MsgOnErrorReturn(err)
		}
	}
}

func runInvalidateObjectLinks(ctx *sfPlugins.StatefunContextProcessor, objectId string) {
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
			deleteObjectOutLinkIfInvalidByInheritance(ctx, objectId, linkType, targetObjectId)
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

func PolyTypeGoalPrepare(ctx *sfPlugins.StatefunContextProcessor, goal PolyTypeCascadeDeleteGoalType) []string {
	typesToRefresh := []string{}
	switch goal.reason {
	case SuperTypeDelete:
		{
			typesToRefresh = gatherTypes4CascadeObjectLinkRefreshStartingFromTypeID(ctx, ctx.Self.ID, false) // ctx.Self.ID is parent type
			// Delete type after
		}
	case SuperTypeDeleteSubType:
		{
			// Unlink child type first
			typesToRefresh = gatherTypes4CascadeObjectLinkRefreshStartingFromTypeID(ctx, goal.target, true) // goal.target is child type
		}
	case SuperTypeDeleteOutTypeObjectLink:
		{
			// Delete types' object link first
			typesToRefresh = gatherTypes4CascadeObjectLinkRefreshStartingFromTypeID(ctx, ctx.Self.ID, false) // ctx.Self.ID is parent type
		}
	}
	return typesToRefresh
}

func PolyTypeGoalFinalize(ctx *sfPlugins.StatefunContextProcessor, typesToRefresh []string) {
	UpdateTypeModelVersion(ctx)
	for _, typeId := range typesToRefresh {
		runCascadeObjectLinkRefreshStartingForTypeWithID(ctx, typeId)
	}
}

func UpdateTypeModelVersion(ctx *sfPlugins.StatefunContextProcessor) {
	typesVertexId := ctx.Domain.CreateObjectIDWithHubDomain(BUILT_IN_TYPES, false)

	payload := easyjson.NewJSONObject()
	payload.SetByPath("body.version", easyjson.NewJSON(system.GetUniqueStrID()))
	_, err := ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.update", makeSequenceFreeParentBasedID(ctx, typesVertexId), injectParentHoldsLocks(ctx, &payload), nil)
	system.MsgOnErrorReturn(err)
}

func RecalculateInheritanceCacheForTypeAtSelfIDIfNeeded(ctx *sfPlugins.StatefunContextProcessor) {
	// Fast path: the inheritance cache only needs recomputing when the global
	// type-model version has moved past the version this type last synced to.
	// Check that by reading BOTH versions straight from the in-memory cache
	// (the source of truth) instead of through functions.graph.api.vertex.read.
	//
	// This guard runs on every ReadType, and the global version lives on the
	// single shared BUILT_IN_TYPES vertex — routing the check through the
	// mediator (payload build + operation-key lock + reply parse) on each call
	// turned that vertex into a hotspot under bulk type reads (e.g. naming
	// triggers). Direct cache reads return the same committed values with none
	// of that overhead; a momentarily stale read can at worst cause one extra
	// idempotent recompute on a later call, never a missed one — the
	// authoritative slow path below still re-reads through the mediator before
	// it writes.
	if !typeInheritanceCacheMayBeStale(ctx) {
		return
	}

	typeCacheVersion, typeModelVersion, typeBody := getTypeCacheVersionAndGlobalVersion(ctx, ctx.Self.ID)

	// The type vertex does not exist (or carries no body) — e.g. ReadType for a
	// non-existent type. There is no inheritance to recompute, and proceeding
	// would dereference a nil body (typeBody.Clone) and panic the worker, leaving
	// the request unanswered (client times out). Nothing to do.
	if typeBody == nil {
		return
	}

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
		newTypeBody.SetByPath("cache.parent_types", easyjson.NewJSON(keys))
		newTypeBody.SetByPath("cache.version", easyjson.NewJSON(typeModelVersion))

		payload := easyjson.NewJSONObject()
		payload.SetByPath("body", newTypeBody)
		payload.SetByPath("replace", easyjson.NewJSON(true))
		_, err := ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.update", makeSequenceFreeParentBasedID(ctx, ctx.Self.ID), injectParentHoldsLocks(ctx, &payload), nil)
		system.MsgOnErrorReturn(err)
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

// typeInheritanceCacheMayBeStale reports — using only direct in-memory cache
// reads, no mediator and no locks — whether this type's inheritance cache
// might be out of date and so warrants the authoritative slow path.
//
// It returns false ONLY when it can positively confirm that the type's cached
// version equals the current global model version (the steady-state common
// case, where nothing needs doing). In every uncertain case — vertex body not
// in cache yet, or an empty global version — it returns true and defers to the
// existing mediator-based path, which preserves the original semantics
// (including its "do nothing when the global version is empty" behaviour).
func typeInheritanceCacheMayBeStale(ctx *sfPlugins.StatefunContextProcessor) bool {
	selfID := getOriginalID(ctx.Self.ID)
	typesVertexId := ctx.Domain.CreateObjectIDWithHubDomain(BUILT_IN_TYPES, false)

	modelBody, err := ctx.Domain.Cache().GetValueJSON(typesVertexId)
	if err != nil {
		return true
	}
	typeModelVersion := modelBody.GetByPath("version").AsStringDefault("")
	if typeModelVersion == "" {
		return true
	}

	typeBody, err := ctx.Domain.Cache().GetValueJSON(selfID)
	if err != nil {
		return true
	}
	typeCacheVersion := typeBody.GetByPath("cache.version").AsStringDefault("")

	return typeCacheVersion != typeModelVersion
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
		if linkType == TYPES_SUBTYPELINK {
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
		if linkType == TYPES_SUBTYPELINK {
			result = append(result, to)
		}
	}

	return result
}
