// Copyright 2023 NJWS Inc.

// Foliage graph store crud package.
// Provides stateful functions of low-level crud operations for the graph store
package crud

import (
	"fmt"
	"strings"

	"github.com/foliagecp/easyjson"

	"github.com/foliagecp/sdk/embedded/graph/common"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
)

func getOpStackFromOptions(options *easyjson.JSON) *easyjson.JSON {
	returnOpStack := false
	if options != nil {
		returnOpStack = options.GetByPath("return_op_stack").AsBoolDefault(false)
	}
	var opStack *easyjson.JSON = nil
	if returnOpStack {
		opStack = easyjson.NewJSONArray().GetPtr()
	}
	return opStack
}

func addVertexOpToOpStack(opStack *easyjson.JSON, opName string, vertexId string, oldBody *easyjson.JSON, newBody *easyjson.JSON) bool {
	if opStack != nil && opStack.IsArray() {
		op := easyjson.NewJSONObjectWithKeyValue("op", easyjson.NewJSON(opName))
		op.SetByPath("id", easyjson.NewJSON(vertexId))
		if oldBody != nil {
			op.SetByPath("old_body", *oldBody)
		}
		if newBody != nil {
			op.SetByPath("new_body", *newBody)
		}
		opStack.AddToArray(op)
		return true
	}
	return false
}

func addLinkOpToOpStack(opStack *easyjson.JSON, opName string, fromVertexId string, toVertexId string, linkType string, oldBody *easyjson.JSON, newBody *easyjson.JSON) bool {
	if opStack != nil && opStack.IsArray() {
		op := easyjson.NewJSONObjectWithKeyValue("op", easyjson.NewJSON(opName))
		op.SetByPath("from", easyjson.NewJSON(fromVertexId))
		op.SetByPath("to", easyjson.NewJSON(toVertexId))
		op.SetByPath("type", easyjson.NewJSON(linkType))
		if oldBody != nil {
			op.SetByPath("old_body", *oldBody)
		}
		if newBody != nil {
			op.SetByPath("new_body", *newBody)
		}
		opStack.AddToArray(op)
		return true
	}
	return false
}

func mergeOpStack(opStackRecepient *easyjson.JSON, opStackDonor *easyjson.JSON) bool {
	if opStackRecepient != nil && opStackRecepient.IsArray() && opStackDonor != nil && opStackDonor.IsArray() {
		for i := 0; i < opStackDonor.ArraySize(); i++ {
			opStackRecepient.AddToArray(opStackDonor.ArrayElement(i))
		}
	}
	return false
}

func resultWithOpStack(opStack *easyjson.JSON) easyjson.JSON {
	if opStack == nil {
		return easyjson.NewJSONNull()
	}
	return easyjson.NewJSONObjectWithKeyValue("op_stack", *opStack)
}

/*
Creates a vertex in the graph with an id the function being called with.

Request:

	payload: json - optional
		// Initial request from caller:
		body: json - optional // Body for object to be created with.
			<key>: <type> - optional // Any additional key and value to be stored in objects's body.

	options: json - optional
		return_op_stack: bool - optional

Reply:

	payload: json
		status: string
		result: any
		op_stack: json array - optional
*/
func LLAPIVertexCreate(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	sosc := common.NewSyncOpStatusController(ctx)

	_, err := ctx.Domain.Cache().GetValue(ctx.Self.ID)
	if err == nil { // If vertex already exists
		sosc.Integreate(common.SyncOpFailed(fmt.Sprintf("vertex with id=%s already exists", ctx.Self.ID))).Reply()
		return
	}

	payload := ctx.Payload
	opStack := getOpStackFromOptions(ctx.Options)

	var objectBody easyjson.JSON
	if payload.GetByPath("body").IsObject() {
		objectBody = payload.GetByPath("body")
	} else {
		objectBody = easyjson.NewJSONObject()
	}

	ctx.SetObjectContext(&objectBody)
	addVertexOpToOpStack(opStack, ctx.Self.Typename, ctx.Self.ID, nil, &objectBody)

	sosc.Integreate(common.SyncOpOk(resultWithOpStack(opStack))).Reply()
}

/*
Updates a vertex in the graph with an id the function being called with. Merges or replaces the old vertice's body with the new one.

Request:

	payload: json - optional
		// Initial request from caller:
		body: json - optional // Body for object to be created with.
			<key>: <type> - optional // Any additional key and value to be stored in objects's body.
		mode: string - optional // "merge" (default) - deep merge old and new bodies, "replace" - replace old body with the new one, <other> is interpreted as "merge" without any notification

	options: json - optional
		return_op_stack: bool - optional

Reply:

	payload: json
		status: string
		result: any
		op_stack: json array - optional
*/
func LLAPIVertexUpdate(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	sosc := common.NewSyncOpStatusController(ctx)

	_, err := ctx.Domain.Cache().GetValue(ctx.Self.ID)
	if err != nil { // If vertex does not exist
		sosc.Integreate(common.SyncOpIdle(fmt.Sprintf("vertex with id=%s does not exist", ctx.Self.ID))).Reply()
		return
	}

	payload := ctx.Payload
	opStack := getOpStackFromOptions(ctx.Options)

	var objectBody easyjson.JSON
	if payload.GetByPath("body").IsObject() {
		objectBody = payload.GetByPath("body")
	} else {
		objectBody = easyjson.NewJSONObject()
	}

	fixedOldBody := ctx.GetObjectContext()
	newBody := fixedOldBody

	mode := payload.GetByPath("mode").AsStringDefault("merge")
	switch mode {
	case "replace":
		newBody = &objectBody
		ctx.SetObjectContext(newBody) // Update an object
	case "merge":
		fallthrough
	default:
		newBody = ctx.GetObjectContext()
		newBody.DeepMerge(objectBody)
		ctx.SetObjectContext(newBody) // Update an object
	}

	addVertexOpToOpStack(opStack, ctx.Self.Typename, ctx.Self.ID, fixedOldBody, newBody)

	sosc.Integreate(common.SyncOpOk(resultWithOpStack(opStack))).Reply()
}

/*
Deletes a vartex with an id the function being called with from the graph and deletes all links related to it.

Request:

	options: json - optional
		return_op_stack: bool - optional

Reply:

	payload: json
		status: string
		result: any
		op_stack: json array - optional
*/
func LLAPIVertexDelete(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	sosc := common.NewSyncOpStatusController(ctx)

	_, err := ctx.Domain.Cache().GetValue(ctx.Self.ID)
	if err != nil { // If vertex does not exist
		sosc.Integreate(common.SyncOpIdle(fmt.Sprintf("vertex with id=%s does not exist", ctx.Self.ID))).Reply()
		return
	}

	opStack := getOpStackFromOptions(ctx.Options)

	// Delete all out links -------------------------------
	outLinkKeys := ctx.Domain.Cache().GetKeysByPattern(fmt.Sprintf(OutLinkBodyKeyPrefPattern+LinkKeySuff1Pattern, ctx.Self.ID, ">"))
	for _, outLinkKey := range outLinkKeys {
		inLinkKeyTokens := strings.Split(outLinkKey, ".")
		toObjectID := inLinkKeyTokens[len(inLinkKeyTokens)-1]
		linkType := inLinkKeyTokens[len(inLinkKeyTokens)-2]

		deleteLinkPayload := easyjson.NewJSONObject()
		deleteLinkPayload.SetByPath("to", easyjson.NewJSON(toObjectID))
		deleteLinkPayload.SetByPath("link_type", easyjson.NewJSON(linkType))
		sosc.Integreate(common.SyncOpMsgFromSfReply(ctx.Request(sfPlugins.AutoSelect, "functions.graph.api.link.delete", ctx.Self.ID, &deleteLinkPayload, ctx.Options)))
		mergeOpStack(opStack, sosc.GetLastSyncOp().Data.GetByPath("op_stack").GetPtr())
		if sosc.GetLastSyncOp().Status == common.SYNC_OP_STATUS_FAILED {
			sosc.ReplyWithData(resultWithOpStack(opStack).GetPtr())
			return
		}
	}
	// ----------------------------------------------------

	// Delete all in links --------------------------------
	inLinkKeys := ctx.Domain.Cache().GetKeysByPattern(fmt.Sprintf(InLinkKeyPrefPattern+LinkKeySuff1Pattern, ctx.Self.ID, ">"))
	for _, inLinkKey := range inLinkKeys {
		inLinkKeyTokens := strings.Split(inLinkKey, ".")
		fromObjectID := inLinkKeyTokens[len(inLinkKeyTokens)-2]
		linkType := inLinkKeyTokens[len(inLinkKeyTokens)-1]

		deleteLinkPayload := easyjson.NewJSONObject()
		deleteLinkPayload.SetByPath("to", easyjson.NewJSON(ctx.Self.ID))
		deleteLinkPayload.SetByPath("link_type", easyjson.NewJSON(linkType))
		sosc.Integreate(common.SyncOpMsgFromSfReply(ctx.Request(sfPlugins.AutoSelect, "functions.graph.api.link.delete", fromObjectID, &deleteLinkPayload, ctx.Options)))
		mergeOpStack(opStack, sosc.GetLastSyncOp().Data.GetByPath("op_stack").GetPtr())
		if sosc.GetLastSyncOp().Status == common.SYNC_OP_STATUS_FAILED {
			sosc.ReplyWithData(resultWithOpStack(opStack).GetPtr())
			return
		}
	}
	// ----------------------------------------------------

	// Delete link name generator -------------------------
	ctx.Domain.Cache().DeleteValue(fmt.Sprintf(OutLinkNameGenKeyPattern, ctx.Self.ID), true, -1, "")
	// ----------------------------------------------------

	var oldBody *easyjson.JSON = nil
	if opStack != nil {
		oldBody = ctx.GetObjectContext()
	}
	ctx.Domain.Cache().DeleteValue(ctx.Self.ID, true, -1, "") // Delete object's body
	addVertexOpToOpStack(opStack, ctx.Self.Typename, ctx.Self.ID, oldBody, nil)

	sosc.Integreate(common.SyncOpOk(resultWithOpStack(opStack))).Reply()
}

/*
Reads and returns vertice's body.

Request:

	options: json - optional
		return_op_stack: bool - optional

Reply:

	payload: json
		status: string
		result: json // Body for object.
		op_stack: json array - optional
*/
func LLAPIVertexRead(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	sosc := common.NewSyncOpStatusController(ctx)

	_, err := ctx.Domain.Cache().GetValue(ctx.Self.ID)
	if err != nil { // If vertex does not exist
		sosc.Integreate(common.SyncOpIdle(fmt.Sprintf("vertex with id=%s does not exist", ctx.Self.ID))).Reply()
		return
	}

	opStack := getOpStackFromOptions(ctx.Options)

	body := ctx.GetObjectContext()
	addVertexOpToOpStack(opStack, ctx.Self.Typename, ctx.Self.ID, nil, nil)

	result := resultWithOpStack(opStack).GetPtr()
	result.SetByPath("body", *body)
	sosc.Integreate(common.SyncOpOk(*result)).Reply()
}

/*
Creates a link of type="link_type" from a vertex with id the funcion being called with to a vartex with id="to".

Request:

	payload: json - required
		// Initial request from caller:
		to: string - required // ID for descendant object.
		link_type: string - required // Type of link leading to descendant.
		body: json - optional // Body for link leading to descendant.
			name: string - optional // Defines link's name which is unique among all object's output links. Will be generated automatically if not defined or if same named out link already exists.
			tags: []string - optional // Defines link tags.
			<key>: <type> - optional // Any additional key and value to be stored in link's body.

		// Self-requests to descendants (RequestReply): // ID can be composite: <object_id>===self_link - for non-blocking execution on the same object
			in_link_type: string - required // Type of input link to create

	options: json - optional
		return_op_stack: bool - optional

Reply:

	payload: json
		status: string
		result: any
		op_stack: json array - optional
*/
func LLAPILinkCreate(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	sosc := common.NewSyncOpStatusController(ctx)

	selfId := strings.Split(ctx.Self.ID, "===")[0]
	_, err := ctx.Domain.Cache().GetValue(selfId)
	if err != nil { // If vertex does not exist
		sosc.Integreate(common.SyncOpFailed(fmt.Sprintf("vertex with id=%s does not exist", selfId))).Reply()
		return
	}

	payload := ctx.Payload
	opStack := getOpStackFromOptions(ctx.Options)

	if payload.PathExists("in_link_type") {
		if inLinkType, ok := payload.GetByPath("in_link_type").AsString(); ok && len(inLinkType) > 0 {
			if linkFromObjectUUID := ctx.Caller.ID; len(linkFromObjectUUID) > 0 {
				ctx.Domain.Cache().SetValue(fmt.Sprintf(InLinkKeyPrefPattern+LinkKeySuff2Pattern, selfId, linkFromObjectUUID, inLinkType), nil, true, -1, "")
				sosc.Integreate(common.SyncOpOk(easyjson.NewJSONNull())).Reply()
				return
			} else {
				sosc.Integreate(common.SyncOpFailed(fmt.Sprintf("caller id is not defined, no source vertex id"))).Reply()
				return
			}
		} else {
			sosc.Integreate(common.SyncOpFailed(fmt.Sprintf("in_link_type is not defined"))).Reply()
			return
		}
	} else {
		var linkBody easyjson.JSON
		if payload.GetByPath("body").IsObject() {
			linkBody = payload.GetByPath("body")
		} else {
			linkBody = easyjson.NewJSONObject()
		}

		var linkType string
		if s, ok := payload.GetByPath("link_type").AsString(); ok {
			linkType = s
		} else {
			sosc.Integreate(common.SyncOpFailed(fmt.Sprintf("link_type is not defined"))).Reply()
			return
		}

		var toId string
		if s, ok := payload.GetByPath("to").AsString(); ok {
			toId = s
		} else {
			sosc.Integreate(common.SyncOpFailed(fmt.Sprintf("to is not defined"))).Reply()
			return
		}
		toId = ctx.Domain.CreateObjectIDWithThisDomainIfndef(toId)

		_, err := ctx.Domain.Cache().GetValue(fmt.Sprintf(OutLinkBodyKeyPrefPattern+LinkKeySuff2Pattern, selfId, linkType, toId))
		if err == nil { // If link already exists
			sosc.Integreate(common.SyncOpFailed(fmt.Sprintf("link from=%s, to=%s, link_type=%s already exists", selfId, toId, linkType))).Reply()
			return
		}

		// Create out link on this object -------------------------
		// Create unique link name ----------
		linkName, linkNameDefined := linkBody.GetByPath("name").AsString()
		if len(linkName) == 0 {
			linkNameDefined = false
		}
		sameNamedLinkExists := false
		if linkNameDefined {
			if _, err := ctx.Domain.Cache().GetValue(fmt.Sprintf(OutLinkLinkNamePrefPattern+LinkKeySuff1Pattern, selfId, linkName)); err == nil {
				sameNamedLinkExists = true
			}
		}
		if !linkNameDefined || sameNamedLinkExists {
			var namegen int64 = 0
			if v, err := ctx.Domain.Cache().GetValue(fmt.Sprintf(OutLinkNameGenKeyPattern, selfId)); err == nil {
				namegen = system.BytesToInt64(v)
			}
			linkName = fmt.Sprintf("name%d", namegen)
			namegen++
			ctx.Domain.Cache().SetValue(fmt.Sprintf(OutLinkNameGenKeyPattern, selfId), system.Int64ToBytes(namegen), true, -1, "")
			linkBody.SetByPath("name", easyjson.NewJSON(linkName))
		}
		// ----------------------------------
		// Create link name -----------------
		ctx.Domain.Cache().SetValue(fmt.Sprintf(OutLinkLinkNamePrefPattern+LinkKeySuff1Pattern, selfId, linkName), nil, true, -1, "")
		// ----------------------------------
		// Index link name ------------------
		ctx.Domain.Cache().SetValue(fmt.Sprintf(OutLinkIndexPrefPattern+LinkKeySuff4Pattern, selfId, linkType, toId, "name", linkName), nil, true, -1, "")
		// ----------------------------------
		// Set link body --------------------
		ctx.Domain.Cache().SetValue(fmt.Sprintf(OutLinkBodyKeyPrefPattern+LinkKeySuff2Pattern, selfId, linkType, toId), linkBody.ToBytes(), true, -1, "") // Store link body in KV
		// ----------------------------------
		// Store tags -----------------------
		if linkBody.GetByPath("tags").IsNonEmptyArray() {
			if linkTags, ok := linkBody.GetByPath("tags").AsArrayString(); ok {
				for _, linkTag := range linkTags {
					ctx.Domain.Cache().SetValue(fmt.Sprintf(OutLinkIndexPrefPattern+LinkKeySuff4Pattern, selfId, linkType, toId, "tag", linkTag), nil, true, -1, "")
				}
			}
		}
		// ----------------------------------
		// --------------------------------------------------------

		addLinkOpToOpStack(opStack, ctx.Self.Typename, ctx.Self.ID, toId, linkType, nil, &linkBody)

		// Create in link on descendant object --------------------
		nextCallPayload := easyjson.NewJSONObject()
		nextCallPayload.SetByPath("in_link_type", easyjson.NewJSON(linkType))
		targetId := toId
		if toId == ctx.Self.ID {
			targetId = targetId + "===self_link"
		}
		sosc.Integreate(common.SyncOpMsgFromSfReply(ctx.Request(sfPlugins.AutoSelect, ctx.Self.Typename, targetId, &nextCallPayload, nil)))
		if sosc.GetLastSyncOp().Status == common.SYNC_OP_STATUS_FAILED {
			sosc.ReplyWithData(resultWithOpStack(opStack).GetPtr())
			return
		}
		// --------------------------------------------------------

		sosc.Integreate(common.SyncOpOk(resultWithOpStack(opStack))).Reply()
	}
}

/*
Updates a link of type="link_type" from a vertex with id the funcion being called with to a vertex with id="to".

Request:

	payload: json - required
		// Initial request from caller:
		to: string - required // ID for descendant object. If a descandant with the specified uuid does not exist - will be created with empty body.
		link_type: string - required // Type of link leading to descendant.
		body: json - optional // Body for link leading to descendant.
			tags: []string - optional // Defines link tags.
			<key>: <type> - optional // Any additional key and value to be stored in link's body.
		mode: string - optional // "merge" (default) - deep merge old and new bodies, "replace" - replace old body with the new one, <other> is interpreted as "merge" without any notification

	options: json - optional
		return_op_stack: bool - optional

Reply:

	payload: json
		status: string
		result: any
		op_stack: json array - optional
*/
func LLAPILinkUpdate(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	sosc := common.NewSyncOpStatusController(ctx)

	payload := ctx.Payload

	opStack := getOpStackFromOptions(ctx.Options)

	var linkType string
	if s, ok := payload.GetByPath("link_type").AsString(); ok {
		linkType = s
	} else {
		sosc.Integreate(common.SyncOpFailed(fmt.Sprintf("link type is not defined"))).Reply()
		return
	}

	var toId string
	if s, ok := payload.GetByPath("to").AsString(); ok {
		toId = s
	} else {
		sosc.Integreate(common.SyncOpFailed(fmt.Sprintf("to is not defined"))).Reply()
		return
	}
	toId = ctx.Domain.CreateObjectIDWithThisDomainIfndef(toId)

	var linkBody easyjson.JSON
	if payload.GetByPath("body").IsObject() {
		linkBody = payload.GetByPath("body")
	} else {
		linkBody = easyjson.NewJSONObject()
	}

	fixedOldLinkBody, err := ctx.Domain.Cache().GetValueAsJSON(fmt.Sprintf(OutLinkBodyKeyPrefPattern+LinkKeySuff2Pattern, ctx.Self.ID, linkType, toId))
	if err != nil {
		sosc.Integreate(common.SyncOpIdle(fmt.Sprintf("link from=%s, to=%s, link_type=%s does not exist", ctx.Self.ID, toId, linkType))).Reply()
		return
	}

	// Delete old indices -----------------------------------------
	// Link name ------------------------
	if linkName, ok := fixedOldLinkBody.GetByPath("name").AsString(); ok {
		ctx.Domain.Cache().DeleteValue(fmt.Sprintf(OutLinkLinkNamePrefPattern+LinkKeySuff1Pattern, ctx.Self.ID, linkName), true, -1, "")
		ctx.Domain.Cache().DeleteValue(fmt.Sprintf(OutLinkIndexPrefPattern+LinkKeySuff4Pattern, ctx.Self.ID, linkType, toId, "name", linkName), true, -1, "")
	}
	// ----------------------------------
	// Link tags ------------------------
	if fixedOldLinkBody.GetByPath("tags").IsNonEmptyArray() {
		if linkTags, ok := fixedOldLinkBody.GetByPath("tags").AsArrayString(); ok {
			for _, linkTag := range linkTags {
				ctx.Domain.Cache().DeleteValue(fmt.Sprintf(OutLinkIndexPrefPattern+LinkKeySuff4Pattern, ctx.Self.ID, linkType, toId, "tag", linkTag), true, -1, "")
			}
		}
	}
	// ----------------------------------
	// ------------------------------------------------------------
	// Generate new link body -------------------------------------
	mode := payload.GetByPath("mode").AsStringDefault("merge")
	newBody := fixedOldLinkBody
	switch mode {
	case "replace":
		newBody = &linkBody
	case "merge":
		fallthrough
	default:
		newBody = fixedOldLinkBody.Clone().GetPtr()
		newBody.DeepMerge(linkBody)
	}
	// ------------------------------------------------------------
	// Create unique link name ------------------------------------
	linkName, linkNameDefined := newBody.GetByPath("name").AsString()
	if len(linkName) == 0 {
		linkNameDefined = false
	}
	sameNamedLinkExists := false
	if linkNameDefined {
		if _, err := ctx.Domain.Cache().GetValue(fmt.Sprintf(OutLinkLinkNamePrefPattern+LinkKeySuff1Pattern, ctx.Self.ID, linkName)); err == nil {
			sameNamedLinkExists = true
		}
	}
	if !linkNameDefined || sameNamedLinkExists {
		var namegen int64 = 0
		if v, err := ctx.Domain.Cache().GetValue(fmt.Sprintf(OutLinkNameGenKeyPattern, ctx.Self.ID)); err == nil {
			namegen = system.BytesToInt64(v)
		}
		linkName = fmt.Sprintf("name%d", namegen)
		namegen++
		ctx.Domain.Cache().SetValue(fmt.Sprintf(OutLinkNameGenKeyPattern, ctx.Self.ID), system.Int64ToBytes(namegen), true, -1, "")
		newBody.SetByPath("name", easyjson.NewJSON(linkName))
	}
	// ------------------------------------------------------------
	// Create link name -------------------------------------------
	ctx.Domain.Cache().SetValue(fmt.Sprintf(OutLinkLinkNamePrefPattern+LinkKeySuff1Pattern, ctx.Self.ID, linkName), nil, true, -1, "")
	// ------------------------------------------------------------
	// Index link name --------------------------------------------
	ctx.Domain.Cache().SetValue(fmt.Sprintf(OutLinkIndexPrefPattern+LinkKeySuff4Pattern, ctx.Self.ID, linkType, toId, "name", linkName), nil, true, -1, "")
	// ------------------------------------------------------------
	// Update link body -------------------------------------------
	ctx.Domain.Cache().SetValue(fmt.Sprintf(OutLinkBodyKeyPrefPattern+LinkKeySuff2Pattern, ctx.Self.ID, linkType, toId), newBody.ToBytes(), true, -1, "") // Store link body in KV
	// ------------------------------------------------------------
	// Create new indices -----------------------------------------
	if newBody.GetByPath("tags").IsNonEmptyArray() {
		if linkTags, ok := newBody.GetByPath("tags").AsArrayString(); ok {
			for _, linkTag := range linkTags {
				ctx.Domain.Cache().SetValue(fmt.Sprintf(OutLinkIndexPrefPattern+LinkKeySuff4Pattern, ctx.Self.ID, linkType, toId, "tag", linkTag), nil, true, -1, "")
			}
		}
	}
	// ------------------------------------------------------------
	addLinkOpToOpStack(opStack, ctx.Self.Typename, ctx.Self.ID, toId, linkType, fixedOldLinkBody, newBody)

	sosc.Integreate(common.SyncOpOk(resultWithOpStack(opStack))).Reply()
}

/*
Delete a link of type="link_type" from a vertex with id the funcion being called with to a vertex with id="to".

Request:

	payload: json - required
		// Initial request from caller:
		to: string - required // ID for descendant object.
		link_type: string - required // Type of link leading to descendant.

		// Self-requests to descendants (RequestReply): // ID can be composite: <object_id>===self_link - for non-blocking execution on the same object
		in_link_type: string - required // Type of input link to delete

	options: json - optional
		return_op_stack: bool - optional

Reply:

	payload: json
		status: string
		result: any
		op_stack: json array - optional
*/
func LLAPILinkDelete(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	sosc := common.NewSyncOpStatusController(ctx)

	selfId := strings.Split(ctx.Self.ID, "===")[0]

	payload := ctx.Payload
	opStack := getOpStackFromOptions(ctx.Options)

	if payload.PathExists("in_link_type") {
		if inLinkType, ok := payload.GetByPath("in_link_type").AsString(); ok && len(inLinkType) > 0 {
			if linkFromObjectUUID := ctx.Caller.ID; len(linkFromObjectUUID) > 0 {
				ctx.Domain.Cache().DeleteValue(fmt.Sprintf(InLinkKeyPrefPattern+LinkKeySuff2Pattern, selfId, linkFromObjectUUID, inLinkType), true, -1, "")
				sosc.Integreate(common.SyncOpOk(easyjson.NewJSONNull())).Reply()
				return
			} else {
				sosc.Integreate(common.SyncOpFailed(fmt.Sprintf("caller id is not defined, no source vertex id"))).Reply()
				return
			}
		} else {
			sosc.Integreate(common.SyncOpFailed(fmt.Sprintf("in_link_type is not defined"))).Reply()
			return
		}
	} else {
		var linkType string
		if s, ok := payload.GetByPath("link_type").AsString(); ok {
			linkType = s
		} else {
			sosc.Integreate(common.SyncOpFailed(fmt.Sprintf("link type is not defined"))).Reply()
			return
		}
		var toId string
		if s, ok := payload.GetByPath("to").AsString(); ok {
			toId = s
		} else {
			sosc.Integreate(common.SyncOpFailed(fmt.Sprintf("to is not defined"))).Reply()
			return
		}
		toId = ctx.Domain.CreateObjectIDWithThisDomainIfndef(toId)

		lbk := fmt.Sprintf(OutLinkBodyKeyPrefPattern+LinkKeySuff2Pattern, selfId, linkType, toId)
		linkBody, err := ctx.Domain.Cache().GetValueAsJSON(lbk)
		if err != nil { // If does no exist
			sosc.Integreate(common.SyncOpIdle(fmt.Sprintf("link from=%s, to=%s, link_type=%s does not exist", selfId, toId, linkType))).Reply()
			return
		}

		ctx.Domain.Cache().DeleteValue(lbk, true, -1, "")

		if linkBody != nil {
			// Delete link name -------------------
			if linkName, ok := linkBody.GetByPath("name").AsString(); ok {
				ctx.Domain.Cache().DeleteValue(fmt.Sprintf(OutLinkLinkNamePrefPattern+LinkKeySuff1Pattern, selfId, linkName), true, -1, "")
				ctx.Domain.Cache().DeleteValue(fmt.Sprintf(OutLinkIndexPrefPattern+LinkKeySuff4Pattern, selfId, linkType, toId, "name", linkName), true, -1, "")
			}
			// -----------------------------------
			// Delete tags -----------------------
			if linkBody.GetByPath("tags").IsNonEmptyArray() {
				if linkTags, ok := linkBody.GetByPath("tags").AsArrayString(); ok {
					for _, linkTag := range linkTags {
						ctx.Domain.Cache().DeleteValue(fmt.Sprintf(OutLinkIndexPrefPattern+LinkKeySuff4Pattern, selfId, linkType, toId, "tag", linkTag), true, -1, "")
					}
				}
			}
			// ------------------------------------
		}

		addLinkOpToOpStack(opStack, ctx.Self.Typename, selfId, toId, linkType, linkBody, nil)

		// Create in link on descendant object --------------------
		nextCallPayload := easyjson.NewJSONObject()
		nextCallPayload.SetByPath("in_link_type", easyjson.NewJSON(linkType))

		targetId := toId
		if toId == ctx.Self.ID {
			targetId = targetId + "===self_link"
		}
		sosc.Integreate(common.SyncOpMsgFromSfReply(ctx.Request(sfPlugins.AutoSelect, ctx.Self.Typename, targetId, &nextCallPayload, nil)))
		if sosc.GetLastSyncOp().Status == common.SYNC_OP_STATUS_FAILED {
			sosc.ReplyWithData(resultWithOpStack(opStack).GetPtr())
			return
		}
		// --------------------------------------------------------

		sosc.Integreate(common.SyncOpOk(resultWithOpStack(opStack))).Reply()
	}
}

/*
Reads and returns link's body.

Request:

	payload: json - required
		// Initial request from caller:
		to: string - required // ID for descendant object. If not defined random UUID will be generated. If a descandant with the specified uuid does not exist - will be created with empty body.
		link_type: string - required // Type of link leading to descendant. If not defined random UUID will be used.

	options: json - optional
		return_op_stack: bool - optional

Reply:

	payload: json
		status: string
		result: json // Body for link.
		op_stack: json array - optional
*/
func LLAPILinkRead(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	sosc := common.NewSyncOpStatusController(ctx)

	payload := ctx.Payload
	opStack := getOpStackFromOptions(ctx.Options)

	var linkType string
	if s, ok := payload.GetByPath("link_type").AsString(); ok {
		linkType = s
	} else {
		sosc.Integreate(common.SyncOpFailed(fmt.Sprintf("link type is not defined"))).Reply()
		return
	}
	var toId string
	if s, ok := payload.GetByPath("to").AsString(); ok {
		toId = s
	} else {
		sosc.Integreate(common.SyncOpFailed(fmt.Sprintf("to is not defined"))).Reply()
		return
	}
	toId = ctx.Domain.CreateObjectIDWithThisDomainIfndef(toId)

	lbk := fmt.Sprintf(OutLinkBodyKeyPrefPattern+LinkKeySuff2Pattern, ctx.Self.ID, linkType, toId)
	linkBody, err := ctx.Domain.Cache().GetValueAsJSON(lbk)
	if err != nil { // If does no exist
		sosc.Integreate(common.SyncOpIdle(fmt.Sprintf("link from=%s, to=%s, link_type=%s does not exist", ctx.Self.ID, toId, linkType))).Reply()
		return
	}

	addLinkOpToOpStack(opStack, ctx.Self.Typename, ctx.Self.ID, toId, linkType, nil, nil)

	result := resultWithOpStack(opStack).GetPtr()
	result.SetByPath("body", *linkBody)
	sosc.Integreate(common.SyncOpOk(*result)).Reply()
}
