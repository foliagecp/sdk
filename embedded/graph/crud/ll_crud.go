

// Foliage graph store crud package.
// Provides stateful functions of low-level crud operations for the graph store
package crud

import (
	"fmt"
	"strings"

	"github.com/foliagecp/easyjson"

	"github.com/foliagecp/sdk/embedded/graph/common"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
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

func resultWithOpStack(existingResult *easyjson.JSON, opStack *easyjson.JSON) easyjson.JSON {
	if existingResult == nil {
		if opStack == nil {
			return easyjson.NewJSONNull()
		}
		return easyjson.NewJSONObjectWithKeyValue("op_stack", *opStack)
	} else {
		if opStack == nil {
			return *existingResult
		}
		existingResult.SetByPath("op_stack", *opStack)
		return *existingResult
	}
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

	sosc.Integreate(common.SyncOpOk(resultWithOpStack(nil, opStack))).Reply()
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

	sosc.Integreate(common.SyncOpOk(resultWithOpStack(nil, opStack))).Reply()
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
	outLinkKeys := ctx.Domain.Cache().GetKeysByPattern(fmt.Sprintf(OutLinkBodyKeyPrefPatternNEW+LinkKeySuff1Pattern, ctx.Self.ID, ">"))
	for _, outLinkKey := range outLinkKeys {
		inLinkKeyTokens := strings.Split(outLinkKey, ".")
		toObjectID := inLinkKeyTokens[len(inLinkKeyTokens)-1]
		linkType := inLinkKeyTokens[len(inLinkKeyTokens)-2]

		deleteLinkPayload := easyjson.NewJSONObject()
		deleteLinkPayload.SetByPath("to", easyjson.NewJSON(toObjectID))
		deleteLinkPayload.SetByPath("type", easyjson.NewJSON(linkType))
		sosc.Integreate(common.SyncOpMsgFromSfReply(ctx.Request(sfPlugins.AutoSelect, "functions.graph.api.link.delete", ctx.Self.ID, &deleteLinkPayload, ctx.Options)))
		mergeOpStack(opStack, sosc.GetLastSyncOp().Data.GetByPath("op_stack").GetPtr())
		if sosc.GetLastSyncOp().Status == common.SYNC_OP_STATUS_FAILED {
			sosc.ReplyWithData(resultWithOpStack(nil, opStack).GetPtr())
			return
		}
	}
	// ----------------------------------------------------

	// Delete all in links --------------------------------
	inLinkKeys := ctx.Domain.Cache().GetKeysByPattern(fmt.Sprintf(OutLinkBodyKeyPrefPatternNEW+LinkKeySuff1Pattern, ctx.Self.ID, ">"))
	for _, inLinkKey := range inLinkKeys {
		inLinkKeyTokens := strings.Split(inLinkKey, ".")
		fromObjectID := inLinkKeyTokens[len(inLinkKeyTokens)-2]
		linkType := inLinkKeyTokens[len(inLinkKeyTokens)-1]

		deleteLinkPayload := easyjson.NewJSONObject()
		deleteLinkPayload.SetByPath("to", easyjson.NewJSON(ctx.Self.ID))
		deleteLinkPayload.SetByPath("type", easyjson.NewJSON(linkType))
		sosc.Integreate(common.SyncOpMsgFromSfReply(ctx.Request(sfPlugins.AutoSelect, "functions.graph.api.link.delete", fromObjectID, &deleteLinkPayload, ctx.Options)))
		mergeOpStack(opStack, sosc.GetLastSyncOp().Data.GetByPath("op_stack").GetPtr())
		if sosc.GetLastSyncOp().Status == common.SYNC_OP_STATUS_FAILED {
			sosc.ReplyWithData(resultWithOpStack(nil, opStack).GetPtr())
			return
		}
	}
	// ----------------------------------------------------

	// Delete link name generator -------------------------
	ctx.Domain.Cache().DeleteValue(fmt.Sprintf(OutLinkBodyKeyPrefPatternNEW, ctx.Self.ID), true, -1, "")
	// ----------------------------------------------------

	var oldBody *easyjson.JSON = nil
	if opStack != nil {
		oldBody = ctx.GetObjectContext()
	}
	ctx.Domain.Cache().DeleteValue(ctx.Self.ID, true, -1, "") // Delete object's body
	addVertexOpToOpStack(opStack, ctx.Self.Typename, ctx.Self.ID, oldBody, nil)

	sosc.Integreate(common.SyncOpOk(resultWithOpStack(nil, opStack))).Reply()
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

	sosc.Integreate(common.SyncOpOk(resultWithOpStack(easyjson.NewJSONObjectWithKeyValue("body", *body).GetPtr(), opStack))).Reply()
}

/*
Creates a link of type="type" from a vertex with id the funcion being called with to a vartex with id="to".

Request:

	payload: json - required
		// Initial request from caller:
		to: string - required // ID for descendant object.
		name: string - required // Defines link's name which is unique among all object's output links.
		type: string - required // Type of link leading to descendant.
		tags: []string - optional // Defines link tags.
		body: json - optional // Body for link leading to descendant.
			<key>: <type> - optional // Any additional key and value to be stored in link's body.

		// Self-requests to descendants (RequestReply): // ID can be composite: <object_id>===self_link - for non-blocking execution on the same object
			in_name: string - required // Creating input link's name

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

	if payload.PathExists("in_name") {
		if inLinkName, ok := payload.GetByPath("in_name").AsString(); ok && len(inLinkName) > 0 {
			if linkFromObjectUUID := ctx.Caller.ID; len(linkFromObjectUUID) > 0 {
				ctx.Domain.Cache().SetValue(fmt.Sprintf(InLinkKeyPrefPatternNEW+LinkKeySuff2Pattern, selfId, linkFromObjectUUID, inLinkName), nil, true, -1, "")
				sosc.Integreate(common.SyncOpOk(easyjson.NewJSONNull())).Reply()
				return
			} else {
				sosc.Integreate(common.SyncOpFailed(fmt.Sprintf("caller id is not defined, no source vertex id"))).Reply()
				return
			}
		} else {
			sosc.Integreate(common.SyncOpFailed(fmt.Sprintf("in_name is not defined"))).Reply()
			return
		}
	} else {
		var linkBody easyjson.JSON
		if payload.GetByPath("body").IsObject() {
			linkBody = payload.GetByPath("body")
		} else {
			linkBody = easyjson.NewJSONObject()
		}

		var toId string
		if s, ok := payload.GetByPath("to").AsString(); ok {
			toId = s
		} else {
			sosc.Integreate(common.SyncOpFailed(fmt.Sprintf("to is not defined"))).Reply()
			return
		}
		toId = ctx.Domain.CreateObjectIDWithThisDomainIfndef(toId)

		var linkName string
		if s, ok := payload.GetByPath("name").AsString(); ok {
			linkName = s
		} else {
			sosc.Integreate(common.SyncOpFailed(fmt.Sprintf("name is not defined"))).Reply()
			return
		}

		var linkType string
		if s, ok := payload.GetByPath("type").AsString(); ok {
			linkType = s
		} else {
			sosc.Integreate(common.SyncOpFailed(fmt.Sprintf("type is not defined"))).Reply()
			return
		}

		// Check if link with this name already exists --------------
		_, err := ctx.Domain.Cache().GetValue(fmt.Sprintf(OutLinkBodyKeyPrefPatternNEW+LinkKeySuff1Pattern, selfId, linkName))
		if err == nil {
			sosc.Integreate(common.SyncOpFailed(fmt.Sprintf("link from=%s with name=%s already exists", selfId, linkName))).Reply()
			return
		}
		// ----------------------------------------------------------
		// Check if link with this type "type" to "to" already exists
		_, err = ctx.Domain.Cache().GetValue(fmt.Sprintf(OutLinkTypeKeyPrefPatternNEW+LinkKeySuff2Pattern, selfId, linkType, toId))
		if err == nil {
			sosc.Integreate(common.SyncOpFailed(fmt.Sprintf("link from=%s with name=%s to=%s with type=%s already exists", selfId, linkName, toId, linkType))).Reply()
			return
		}
		// -----------------------------------------------------------

		// Create out link on this object -------------------------
		// Set link target ------------------
		ctx.Domain.Cache().SetValue(fmt.Sprintf(OutLinkTargetKeyPrefPattern+LinkKeySuff1Pattern, selfId, linkName), []byte(fmt.Sprintf("%s.%s", linkType, toId)), true, -1, "") // Store link body in KV
		// ----------------------------------
		// Set link body --------------------
		ctx.Domain.Cache().SetValue(fmt.Sprintf(OutLinkBodyKeyPrefPatternNEW+LinkKeySuff1Pattern, selfId, linkName), linkBody.ToBytes(), true, -1, "") // Store link body in KV
		// ----------------------------------
		// Index link type ------------------
		ctx.Domain.Cache().SetValue(fmt.Sprintf(OutLinkIndexPrefPattern+LinkKeySuff3Pattern, selfId, linkName, "type", linkType), nil, true, -1, "")
		// ----------------------------------
		// Index link tags ------------------
		if payload.GetByPath("tags").IsNonEmptyArray() {
			if linkTags, ok := payload.GetByPath("tags").AsArrayString(); ok {
				for _, linkTag := range linkTags {
					ctx.Domain.Cache().SetValue(fmt.Sprintf(OutLinkIndexPrefPattern+LinkKeySuff3Pattern, selfId, linkName, "tag", linkTag), nil, true, -1, "")
				}
			}
		}
		// ----------------------------------
		// --------------------------------------------------------

		addLinkOpToOpStack(opStack, ctx.Self.Typename, ctx.Self.ID, toId, linkType, nil, &linkBody)

		// Create in link on descendant object --------------------
		nextCallPayload := easyjson.NewJSONObject()
		nextCallPayload.SetByPath("in_name", easyjson.NewJSON(linkType))
		targetId := toId
		if toId == ctx.Self.ID {
			targetId = targetId + "===self_link"
		}
		sosc.Integreate(common.SyncOpMsgFromSfReply(ctx.Request(sfPlugins.AutoSelect, ctx.Self.Typename, targetId, &nextCallPayload, nil)))
		if sosc.GetLastSyncOp().Status == common.SYNC_OP_STATUS_FAILED {
			sosc.ReplyWithData(resultWithOpStack(nil, opStack).GetPtr())
			return
		}
		// --------------------------------------------------------

		sosc.Integreate(common.SyncOpOk(resultWithOpStack(nil, opStack))).Reply()
	}
}

/*
Updates a link of type="type" from a vertex with id the funcion being called with to a vertex with id="to".

Request:

	payload: json - required
		// Initial request from caller:
		name: string - required // Defines link's name which is unique among all object's output links.
		tags: []string - optional // Defines link tags.
		replace: bool - optional // "false" - default, body and tags will be merged, "true" - body and tags will be replaced,
		body: json - optional // Body for link leading to descendant.
			<key>: <type> - optional // Any additional key and value to be stored in link's body.

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

	var linkName string
	if s, ok := payload.GetByPath("name").AsString(); ok {
		linkName = s
	} else {
		sosc.Integreate(common.SyncOpFailed(fmt.Sprintf("name is not defined"))).Reply()
		return
	}

	linkTargetBytes, err := ctx.Domain.Cache().GetValue(fmt.Sprintf(OutLinkTargetKeyPrefPattern+LinkKeySuff1Pattern, ctx.Self.ID, linkName))
	if err != nil {
		sosc.Integreate(common.SyncOpIdle(fmt.Sprintf("link from=%s with name=%s does not exist", ctx.Self.ID, linkName))).Reply()
		return
	}
	oldLinkBody, err := ctx.Domain.Cache().GetValueAsJSON(fmt.Sprintf(OutLinkBodyKeyPrefPatternNEW+LinkKeySuff1Pattern, ctx.Self.ID, linkName))
	if err != nil {
		sosc.Integreate(common.SyncOpFailed(fmt.Sprintf("link from=%s with name=%s", ctx.Self.ID, linkName))).Reply()
		return
	}

	linkTargetStr := string(linkTargetBytes)
	linkTargetTokens := strings.Split(linkTargetStr, ".")
	if len(linkTargetTokens) != 2 || len(linkTargetTokens[0]) == 0 || len(linkTargetTokens[1]) == 0 {
		sosc.Integreate(common.SyncOpFailed(fmt.Sprintf("link from=%s with name=%s, has invalid target: %s", ctx.Self.ID, linkName, linkTargetStr))).Reply()
	}
	linkType := linkTargetTokens[0]
	toId := linkTargetTokens[1]

	var replace bool = payload.GetByPath("mode").AsBoolDefault(false)

	var linkBody easyjson.JSON
	if payload.GetByPath("body").IsObject() {
		linkBody = payload.GetByPath("body")
	} else {
		linkBody = easyjson.NewJSONObject()
	}

	if replace {
		// Remove all indices -----------------------------
		indexKeys := ctx.Domain.Cache().GetKeysByPattern(fmt.Sprintf(OutLinkIndexPrefPattern+LinkKeySuff2Pattern, ctx.Self.ID, linkName, ">"))
		for _, indexKey := range indexKeys {
			ctx.Domain.Cache().DeleteValue(indexKey, true, -1, "")
		}
		// ------------------------------------------------
	} else { // merge
		newBody := oldLinkBody.Clone().GetPtr()
		newBody.DeepMerge(linkBody)
		linkBody = *newBody
	}

	// Create out link on this object -------------------------
	// Set link body --------------------
	ctx.Domain.Cache().SetValue(fmt.Sprintf(OutLinkBodyKeyPrefPatternNEW+LinkKeySuff1Pattern, ctx.Self.ID, linkName), linkBody.ToBytes(), true, -1, "") // Store link body in KV
	// ----------------------------------
	// Index link tags ------------------
	if linkBody.GetByPath("tags").IsNonEmptyArray() {
		if linkTags, ok := linkBody.GetByPath("tags").AsArrayString(); ok {
			for _, linkTag := range linkTags {
				ctx.Domain.Cache().SetValue(fmt.Sprintf(OutLinkIndexPrefPattern+LinkKeySuff3Pattern, ctx.Self.ID, linkName, "tag", linkTag), nil, true, -1, "")
			}
		}
	}
	// ----------------------------------
	// --------------------------------------------------------

	addLinkOpToOpStack(opStack, ctx.Self.Typename, ctx.Self.ID, toId, linkType, oldLinkBody, &linkBody)

	sosc.Integreate(common.SyncOpOk(resultWithOpStack(nil, opStack))).Reply()
}

/*
Delete a link of type="type" from a vertex with id the funcion being called with to a vertex with id="to".

Request:

	payload: json - required
		// Initial request from caller:
		name: string - required // Defines link's name which is unique among all object's output links.

		// Self-requests to descendants (RequestReply): // ID can be composite: <object_id>===self_link - for non-blocking execution on the same object
		in_name: string - required // Deleting input link's name

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

	if payload.PathExists("in_name") {
		var linkName string
		if s, ok := payload.GetByPath("name").AsString(); ok {
			linkName = s
		} else {
			sosc.Integreate(common.SyncOpFailed(fmt.Sprintf("name is not defined"))).Reply()
			return
		}

		linkTargetBytes, err := ctx.Domain.Cache().GetValue(fmt.Sprintf(OutLinkTargetKeyPrefPattern+LinkKeySuff1Pattern, ctx.Self.ID, linkName))
		if err != nil {
			sosc.Integreate(common.SyncOpIdle(fmt.Sprintf("link from=%s with name=%s does not exist", ctx.Self.ID, linkName))).Reply()
			return
		}
		oldLinkBody, err := ctx.Domain.Cache().GetValueAsJSON(fmt.Sprintf(OutLinkBodyKeyPrefPatternNEW+LinkKeySuff1Pattern, ctx.Self.ID, linkName))
		if err != nil {
			sosc.Integreate(common.SyncOpFailed(fmt.Sprintf("link body from=%s with name=%s does not exist", ctx.Self.ID, linkName))).Reply()
			return
		}

		linkTargetStr := string(linkTargetBytes)
		linkTargetTokens := strings.Split(linkTargetStr, ".")
		if len(linkTargetTokens) != 2 || len(linkTargetTokens[0]) == 0 || len(linkTargetTokens[1]) == 0 {
			sosc.Integreate(common.SyncOpFailed(fmt.Sprintf("link from=%s with name=%s, has invalid target: %s", ctx.Self.ID, linkName, linkTargetStr))).Reply()
		}
		linkType := linkTargetTokens[0]
		toId := linkTargetTokens[1]

		// Remove all indices -----------------------------
		indexKeys := ctx.Domain.Cache().GetKeysByPattern(fmt.Sprintf(OutLinkIndexPrefPattern+LinkKeySuff2Pattern, ctx.Self.ID, linkName, ">"))
		for _, indexKey := range indexKeys {
			ctx.Domain.Cache().DeleteValue(indexKey, true, -1, "")
		}
		// ------------------------------------------------

		// Delete link body -----------------
		ctx.Domain.Cache().DeleteValue(fmt.Sprintf(OutLinkBodyKeyPrefPatternNEW+LinkKeySuff1Pattern, selfId, linkName), true, -1, "") // Store link body in KV
		// ----------------------------------
		// Delete link target ---------------
		ctx.Domain.Cache().DeleteValue(fmt.Sprintf(OutLinkTargetKeyPrefPattern+LinkKeySuff1Pattern, selfId, linkName), true, -1, "") // Store link body in KV
		// ----------------------------------

		addLinkOpToOpStack(opStack, ctx.Self.Typename, selfId, toId, linkType, oldLinkBody, nil)

		// Delete in link on descendant object --------------------
		nextCallPayload := easyjson.NewJSONObject()
		nextCallPayload.SetByPath("in_name", easyjson.NewJSON(linkType))

		targetId := toId
		if toId == ctx.Self.ID {
			targetId = targetId + "===self_link"
		}
		sosc.Integreate(common.SyncOpMsgFromSfReply(ctx.Request(sfPlugins.AutoSelect, ctx.Self.Typename, targetId, &nextCallPayload, nil)))
		if sosc.GetLastSyncOp().Status == common.SYNC_OP_STATUS_FAILED {
			sosc.ReplyWithData(resultWithOpStack(nil, opStack).GetPtr())
			return
		}
		// --------------------------------------------------------

		sosc.Integreate(common.SyncOpOk(resultWithOpStack(nil, opStack))).Reply()
	}
}

/*
Reads and returns link's body.

Request:

	payload: json - required
		// Initial request from caller:
		to: string - required // ID for descendant object. If not defined random UUID will be generated. If a descandant with the specified uuid does not exist - will be created with empty body.
		type: string - required // Type of link leading to descendant. If not defined random UUID will be used.

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

	var linkName string
	if s, ok := payload.GetByPath("name").AsString(); ok {
		linkName = s
	} else {
		sosc.Integreate(common.SyncOpFailed(fmt.Sprintf("name is not defined"))).Reply()
		return
	}

	linkTargetBytes, err := ctx.Domain.Cache().GetValue(fmt.Sprintf(OutLinkTargetKeyPrefPattern+LinkKeySuff1Pattern, ctx.Self.ID, linkName))
	if err != nil {
		sosc.Integreate(common.SyncOpIdle(fmt.Sprintf("link from=%s with name=%s does not exist", ctx.Self.ID, linkName))).Reply()
		return
	}
	linkBody, err := ctx.Domain.Cache().GetValueAsJSON(fmt.Sprintf(OutLinkBodyKeyPrefPatternNEW+LinkKeySuff1Pattern, ctx.Self.ID, linkName))
	if err != nil {
		sosc.Integreate(common.SyncOpFailed(fmt.Sprintf("link body from=%s with name=%s does not exist", ctx.Self.ID, linkName))).Reply()
		return
	}

	linkTargetStr := string(linkTargetBytes)
	linkTargetTokens := strings.Split(linkTargetStr, ".")
	if len(linkTargetTokens) != 2 || len(linkTargetTokens[0]) == 0 || len(linkTargetTokens[1]) == 0 {
		sosc.Integreate(common.SyncOpFailed(fmt.Sprintf("link from=%s with name=%s, has invalid target: %s", ctx.Self.ID, linkName, linkTargetStr))).Reply()
	}
	linkType := linkTargetTokens[0]
	toId := linkTargetTokens[1]

	addLinkOpToOpStack(opStack, ctx.Self.Typename, ctx.Self.ID, toId, linkType, nil, nil)

	sosc.Integreate(common.SyncOpOk(resultWithOpStack(easyjson.NewJSONObjectWithKeyValue("body", *linkBody).GetPtr(), opStack))).Reply()
}
