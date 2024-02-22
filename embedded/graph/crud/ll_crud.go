

// Foliage graph store crud package.
// Provides stateful functions of low-level crud operations for the graph store
package crud

import (
	"fmt"
	"strings"

	"github.com/foliagecp/easyjson"

	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
)

const (
	noLinkIdentifierMsg = "link identifier is not defined, or link does not exist"
)

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
	om := sfMediators.NewOpMediator(ctx)

	_, err := ctx.Domain.Cache().GetValue(ctx.Self.ID)
	if err == nil { // If vertex already exists
		om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("vertex with id=%s already exists", ctx.Self.ID))).Reply()
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

	om.AggregateOpMsg(sfMediators.OpMsgOk(resultWithOpStack(nil, opStack))).Reply()
}

/*
Updates a vertex in the graph with an id the function being called with. Merges or replaces the old vertice's body with the new one.

Request:

	payload: json - optional
		// Initial request from caller:
		body: json - optional // Body for object to be created with.
			<key>: <type> - optional // Any additional key and value to be stored in objects's body.
		replace: bool - optional // "false" - (default) body and tags will be merged, "true" - body and tags will be replaced

	options: json - optional
		return_op_stack: bool - optional

Reply:

	payload: json
		status: string
		result: any
		op_stack: json array - optional
*/
func LLAPIVertexUpdate(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	om := sfMediators.NewOpMediator(ctx)

	_, err := ctx.Domain.Cache().GetValue(ctx.Self.ID)
	if err != nil { // If vertex does not exist
		om.AggregateOpMsg(sfMediators.OpMsgIdle(fmt.Sprintf("vertex with id=%s does not exist", ctx.Self.ID))).Reply()
		return
	}

	payload := ctx.Payload
	opStack := getOpStackFromOptions(ctx.Options)

	oldBody := ctx.GetObjectContext()

	var replace bool = payload.GetByPath("replace").AsBoolDefault(false)

	var body easyjson.JSON
	if payload.GetByPath("body").IsObject() {
		body = payload.GetByPath("body")
	} else {
		body = easyjson.NewJSONObject()
	}

	if !replace { // merge
		newBody := oldBody.Clone().GetPtr()
		newBody.DeepMerge(body)
		body = *newBody
	}
	ctx.SetObjectContext(&body) // Update an object

	addVertexOpToOpStack(opStack, ctx.Self.Typename, ctx.Self.ID, oldBody, &body)

	om.AggregateOpMsg(sfMediators.OpMsgOk(resultWithOpStack(nil, opStack))).Reply()
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
	om := sfMediators.NewOpMediator(ctx)

	_, err := ctx.Domain.Cache().GetValue(ctx.Self.ID)
	if err != nil { // If vertex does not exist
		om.AggregateOpMsg(sfMediators.OpMsgIdle(fmt.Sprintf("vertex with id=%s does not exist", ctx.Self.ID))).Reply()
		return
	}

	opStack := getOpStackFromOptions(ctx.Options)

	// Delete all out links -------------------------------
	outLinkKeys := ctx.Domain.Cache().GetKeysByPattern(fmt.Sprintf(OutLinkBodyKeyPrefPattern+LinkKeySuff1Pattern, ctx.Self.ID, ">"))
	for _, outLinkKey := range outLinkKeys {
		inLinkKeyTokens := strings.Split(outLinkKey, ".")
		linkName := inLinkKeyTokens[len(inLinkKeyTokens)-1]

		deleteLinkPayload := easyjson.NewJSONObject()
		deleteLinkPayload.SetByPath("name", easyjson.NewJSON(linkName))
		om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.delete", ctx.Self.ID, &deleteLinkPayload, ctx.Options)))
		mergeOpStack(opStack, om.GetLastSyncOp().Data.GetByPath("op_stack").GetPtr())
		if om.GetLastSyncOp().Status == sfMediators.SYNC_OP_STATUS_FAILED {
			om.ReplyWithData(resultWithOpStack(nil, opStack).GetPtr())
			return
		}
	}
	// ----------------------------------------------------

	// Delete all in links --------------------------------
	inLinkKeys := ctx.Domain.Cache().GetKeysByPattern(fmt.Sprintf(InLinkKeyPrefPattern+LinkKeySuff1Pattern, ctx.Self.ID, ">"))
	for _, inLinkKey := range inLinkKeys {
		inLinkKeyTokens := strings.Split(inLinkKey, ".")
		fromObjectID := inLinkKeyTokens[len(inLinkKeyTokens)-2]
		linkName := inLinkKeyTokens[len(inLinkKeyTokens)-1]

		deleteLinkPayload := easyjson.NewJSONObject()
		deleteLinkPayload.SetByPath("name", easyjson.NewJSON(linkName))
		om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.delete", fromObjectID, &deleteLinkPayload, ctx.Options)))
		mergeOpStack(opStack, om.GetLastSyncOp().Data.GetByPath("op_stack").GetPtr())
		if om.GetLastSyncOp().Status == sfMediators.SYNC_OP_STATUS_FAILED {
			om.ReplyWithData(resultWithOpStack(nil, opStack).GetPtr())
			return
		}
	}
	// ----------------------------------------------------

	var oldBody *easyjson.JSON = nil
	if opStack != nil {
		oldBody = ctx.GetObjectContext()
	}
	ctx.Domain.Cache().DeleteValue(ctx.Self.ID, true, -1, "") // Delete object's body
	addVertexOpToOpStack(opStack, ctx.Self.Typename, ctx.Self.ID, oldBody, nil)

	om.AggregateOpMsg(sfMediators.OpMsgOk(resultWithOpStack(nil, opStack))).Reply()
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
	om := sfMediators.NewOpMediator(ctx)

	_, err := ctx.Domain.Cache().GetValue(ctx.Self.ID)
	if err != nil { // If vertex does not exist
		om.AggregateOpMsg(sfMediators.OpMsgIdle(fmt.Sprintf("vertex with id=%s does not exist", ctx.Self.ID))).Reply()
		return
	}

	opStack := getOpStackFromOptions(ctx.Options)

	body := ctx.GetObjectContext()
	addVertexOpToOpStack(opStack, ctx.Self.Typename, ctx.Self.ID, nil, nil)

	om.AggregateOpMsg(sfMediators.OpMsgOk(resultWithOpStack(easyjson.NewJSONObjectWithKeyValue("body", *body).GetPtr(), opStack))).Reply()
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
	om := sfMediators.NewOpMediator(ctx)

	selfId := strings.Split(ctx.Self.ID, "===")[0]
	_, err := ctx.Domain.Cache().GetValue(selfId)
	if err != nil { // If vertex does not exist
		om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("vertex with id=%s does not exist", selfId))).Reply()
		return
	}

	payload := ctx.Payload
	opStack := getOpStackFromOptions(ctx.Options)

	if payload.PathExists("in_name") {
		if inLinkName, ok := payload.GetByPath("in_name").AsString(); ok && len(inLinkName) > 0 {
			if linkFromObjectUUID := ctx.Caller.ID; len(linkFromObjectUUID) > 0 {
				ctx.Domain.Cache().SetValue(fmt.Sprintf(InLinkKeyPrefPattern+LinkKeySuff2Pattern, selfId, linkFromObjectUUID, inLinkName), nil, true, -1, "")
				om.AggregateOpMsg(sfMediators.OpMsgOk(easyjson.NewJSONNull())).Reply()
				return
			} else {
				om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("caller id is not defined, no source vertex id"))).Reply()
				return
			}
		} else {
			om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("in_name is not defined"))).Reply()
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
			om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("to is not defined"))).Reply()
			return
		}
		toId = ctx.Domain.CreateObjectIDWithThisDomainIfndef(toId)

		var linkName string
		if s, ok := payload.GetByPath("name").AsString(); ok {
			linkName = s
		} else {
			om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("name is not defined"))).Reply()
			return
		}

		var linkType string
		if s, ok := payload.GetByPath("type").AsString(); ok {
			linkType = s
		} else {
			om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("type is not defined"))).Reply()
			return
		}

		// Check if link with this name already exists --------------
		_, err := ctx.Domain.Cache().GetValue(fmt.Sprintf(OutLinkBodyKeyPrefPattern+LinkKeySuff1Pattern, selfId, linkName))
		if err == nil {
			om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("link from=%s with name=%s already exists", selfId, linkName))).Reply()
			return
		}
		// ----------------------------------------------------------
		// Check if link with this type "type" to "to" already exists
		_, err = ctx.Domain.Cache().GetValue(fmt.Sprintf(OutLinkTypeKeyPrefPattern+LinkKeySuff2Pattern, selfId, linkType, toId))
		if err == nil {
			om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("link from=%s with name=%s to=%s with type=%s already exists", selfId, linkName, toId, linkType))).Reply()
			return
		}
		// -----------------------------------------------------------

		// Create out link on this object -------------------------
		// Set link target ------------------
		ctx.Domain.Cache().SetValue(fmt.Sprintf(OutLinkTargetKeyPrefPattern+LinkKeySuff1Pattern, selfId, linkName), []byte(fmt.Sprintf("%s.%s", linkType, toId)), true, -1, "") // Store link body in KV
		// ----------------------------------
		// Set link body --------------------
		ctx.Domain.Cache().SetValue(fmt.Sprintf(OutLinkBodyKeyPrefPattern+LinkKeySuff1Pattern, selfId, linkName), linkBody.ToBytes(), true, -1, "") // Store link body in KV
		// ----------------------------------
		// Set link type --------------------
		ctx.Domain.Cache().SetValue(fmt.Sprintf(OutLinkTypeKeyPrefPattern+LinkKeySuff2Pattern, selfId, linkType, toId), []byte(linkName), true, -1, "") // Store link type
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

		addLinkOpToOpStack(opStack, ctx.Self.Typename, ctx.Self.ID, toId, linkName, linkType, nil, &linkBody)

		// Create in link on descendant object --------------------
		nextCallPayload := easyjson.NewJSONObject()
		nextCallPayload.SetByPath("in_name", easyjson.NewJSON(linkName))
		targetId := toId
		if toId == ctx.Self.ID {
			targetId = targetId + "===self_link"
		}
		om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, ctx.Self.Typename, targetId, &nextCallPayload, nil)))
		if om.GetLastSyncOp().Status == sfMediators.SYNC_OP_STATUS_FAILED {
			om.ReplyWithData(resultWithOpStack(nil, opStack).GetPtr())
			return
		}
		// --------------------------------------------------------

		om.AggregateOpMsg(sfMediators.OpMsgOk(resultWithOpStack(nil, opStack))).Reply()
	}
}

/*
Updates a link of type="type" from a vertex with id the funcion being called with to a vertex with id="to".

Request:

	payload: json - required
		name: string - required if "to" or "type" is not defined // Defines link's name which is unique among all object's output links.

		to: string - required if "name" is not defined // ID for descendant object.
		type: string - required if "name" is not defined // Type of link leading to descendant.

		tags: []string - optional // Defines link tags.
		replace: bool - optional // "false" - (default) body and tags will be merged, "true" - body and tags will be replaced
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
	om := sfMediators.NewOpMediator(ctx)

	payload := ctx.Payload

	opStack := getOpStackFromOptions(ctx.Options)

	var linkName string
	if s, ok := getLinkNameFromSpecifiedIdentifier(ctx); ok {
		linkName = s
	} else {
		om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf(noLinkIdentifierMsg))).Reply()
		return
	}

	linkTargetBytes, err := ctx.Domain.Cache().GetValue(fmt.Sprintf(OutLinkTargetKeyPrefPattern+LinkKeySuff1Pattern, ctx.Self.ID, linkName))
	if err != nil {
		om.AggregateOpMsg(sfMediators.OpMsgIdle(fmt.Sprintf("link from=%s with name=%s does not exist", ctx.Self.ID, linkName))).Reply()
		return
	}
	oldLinkBody, err := ctx.Domain.Cache().GetValueAsJSON(fmt.Sprintf(OutLinkBodyKeyPrefPattern+LinkKeySuff1Pattern, ctx.Self.ID, linkName))
	if err != nil {
		om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("link from=%s with name=%s", ctx.Self.ID, linkName))).Reply()
		return
	}

	linkTargetStr := string(linkTargetBytes)
	linkTargetTokens := strings.Split(linkTargetStr, ".")
	if len(linkTargetTokens) != 2 || len(linkTargetTokens[0]) == 0 || len(linkTargetTokens[1]) == 0 {
		om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("link from=%s with name=%s, has invalid target: %s", ctx.Self.ID, linkName, linkTargetStr))).Reply()
	}
	linkType := linkTargetTokens[0]
	toId := linkTargetTokens[1]

	var replace bool = payload.GetByPath("replace").AsBoolDefault(false)

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
	ctx.Domain.Cache().SetValue(fmt.Sprintf(OutLinkBodyKeyPrefPattern+LinkKeySuff1Pattern, ctx.Self.ID, linkName), linkBody.ToBytes(), true, -1, "") // Store link body in KV
	// ----------------------------------
	// Index link tags ------------------
	if payload.GetByPath("tags").IsNonEmptyArray() {
		if linkTags, ok := payload.GetByPath("tags").AsArrayString(); ok {
			for _, linkTag := range linkTags {
				ctx.Domain.Cache().SetValue(fmt.Sprintf(OutLinkIndexPrefPattern+LinkKeySuff3Pattern, ctx.Self.ID, linkName, "tag", linkTag), nil, true, -1, "")
			}
		}
	}
	// ----------------------------------
	// --------------------------------------------------------

	addLinkOpToOpStack(opStack, ctx.Self.Typename, ctx.Self.ID, toId, linkName, linkType, oldLinkBody, &linkBody)

	om.AggregateOpMsg(sfMediators.OpMsgOk(resultWithOpStack(nil, opStack))).Reply()
}

/*
Delete a link of type="type" from a vertex with id the funcion being called with to a vertex with id="to".

Request:

	payload: json - required
		// Initial request from caller:
		name: string - required // Defines link's name which is unique among all object's output links.

		to: string - required if "name" is not defined // ID for descendant object.
		type: string - required if "name" is not defined // Type of link leading to descendant.

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
	om := sfMediators.NewOpMediator(ctx)

	selfId := strings.Split(ctx.Self.ID, "===")[0]
	payload := ctx.Payload

	opStack := getOpStackFromOptions(ctx.Options)

	if payload.PathExists("in_name") {
		if inLinkName, ok := payload.GetByPath("in_name").AsString(); ok && len(inLinkName) > 0 {
			if linkFromObjectUUID := ctx.Caller.ID; len(linkFromObjectUUID) > 0 {
				ctx.Domain.Cache().DeleteValue(fmt.Sprintf(InLinkKeyPrefPattern+LinkKeySuff2Pattern, selfId, linkFromObjectUUID, inLinkName), true, -1, "")
				om.AggregateOpMsg(sfMediators.OpMsgOk(easyjson.NewJSONNull())).Reply()
				return
			} else {
				om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("caller id is not defined, no source vertex id"))).Reply()
				return
			}
		} else {
			om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("in_name is not defined"))).Reply()
			return
		}
	} else {
		var linkName string
		if s, ok := getLinkNameFromSpecifiedIdentifier(ctx); ok {
			linkName = s
		} else {
			om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf(noLinkIdentifierMsg))).Reply()
			return
		}

		linkTargetBytes, err := ctx.Domain.Cache().GetValue(fmt.Sprintf(OutLinkTargetKeyPrefPattern+LinkKeySuff1Pattern, ctx.Self.ID, linkName))
		if err != nil {
			om.AggregateOpMsg(sfMediators.OpMsgIdle(fmt.Sprintf("link from=%s with name=%s does not exist", ctx.Self.ID, linkName))).Reply()
			return
		}
		oldLinkBody, err := ctx.Domain.Cache().GetValueAsJSON(fmt.Sprintf(OutLinkBodyKeyPrefPattern+LinkKeySuff1Pattern, ctx.Self.ID, linkName))
		if err != nil {
			om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("link body from=%s with name=%s does not exist", ctx.Self.ID, linkName))).Reply()
			return
		}

		linkTargetStr := string(linkTargetBytes)
		linkTargetTokens := strings.Split(linkTargetStr, ".")
		if len(linkTargetTokens) != 2 || len(linkTargetTokens[0]) == 0 || len(linkTargetTokens[1]) == 0 {
			om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("link from=%s with name=%s, has invalid target: %s", ctx.Self.ID, linkName, linkTargetStr))).Reply()
		}
		linkType := linkTargetTokens[0]
		toId := linkTargetTokens[1]

		// Remove all indices -----------------------------
		indexKeys := ctx.Domain.Cache().GetKeysByPattern(fmt.Sprintf(OutLinkIndexPrefPattern+LinkKeySuff2Pattern, ctx.Self.ID, linkName, ">"))
		for _, indexKey := range indexKeys {
			ctx.Domain.Cache().DeleteValue(indexKey, true, -1, "")
		}
		// ------------------------------------------------

		// Set link type --------------------
		ctx.Domain.Cache().DeleteValue(fmt.Sprintf(OutLinkTypeKeyPrefPattern+LinkKeySuff2Pattern, selfId, linkType, toId), true, -1, "")
		// ----------------------------------
		// Delete link body -----------------
		ctx.Domain.Cache().DeleteValue(fmt.Sprintf(OutLinkBodyKeyPrefPattern+LinkKeySuff1Pattern, selfId, linkName), true, -1, "")
		// ----------------------------------
		// Delete link target ---------------
		ctx.Domain.Cache().DeleteValue(fmt.Sprintf(OutLinkTargetKeyPrefPattern+LinkKeySuff1Pattern, selfId, linkName), true, -1, "")
		// ----------------------------------

		addLinkOpToOpStack(opStack, ctx.Self.Typename, selfId, toId, linkName, linkType, oldLinkBody, nil)

		// Delete in link on descendant object --------------------
		nextCallPayload := easyjson.NewJSONObject()
		nextCallPayload.SetByPath("in_name", easyjson.NewJSON(linkName))

		targetId := toId
		if toId == ctx.Self.ID {
			targetId = targetId + "===self_link"
		}
		om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, ctx.Self.Typename, targetId, &nextCallPayload, nil)))
		if om.GetLastSyncOp().Status == sfMediators.SYNC_OP_STATUS_FAILED {
			om.ReplyWithData(resultWithOpStack(nil, opStack).GetPtr())
			return
		}
		// --------------------------------------------------------

		om.AggregateOpMsg(sfMediators.OpMsgOk(resultWithOpStack(nil, opStack))).Reply()
	}
}

/*
Reads and returns link's body.

Request:

	payload: json - required
		// Initial request from caller:
		name: string - required // Defines link's name which is unique among all object's output links.

		to: string - required if "name" is not defined // ID for descendant object.
		type: string - required if "name" is not defined // Type of link leading to descendant.

	options: json - optional
		return_op_stack: bool - optional

Reply:

	payload: json
		status: string
		result: json // Body for link.
		op_stack: json array - optional
*/
func LLAPILinkRead(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	om := sfMediators.NewOpMediator(ctx)

	opStack := getOpStackFromOptions(ctx.Options)

	var linkName string
	if s, ok := getLinkNameFromSpecifiedIdentifier(ctx); ok {
		linkName = s
	} else {
		om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf(noLinkIdentifierMsg))).Reply()
		return
	}

	linkTargetBytes, err := ctx.Domain.Cache().GetValue(fmt.Sprintf(OutLinkTargetKeyPrefPattern+LinkKeySuff1Pattern, ctx.Self.ID, linkName))
	if err != nil {
		om.AggregateOpMsg(sfMediators.OpMsgIdle(fmt.Sprintf("link from=%s with name=%s does not exist", ctx.Self.ID, linkName))).Reply()
		return
	}
	linkBody, err := ctx.Domain.Cache().GetValueAsJSON(fmt.Sprintf(OutLinkBodyKeyPrefPattern+LinkKeySuff1Pattern, ctx.Self.ID, linkName))
	if err != nil {
		om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("link body from=%s with name=%s does not exist", ctx.Self.ID, linkName))).Reply()
		return
	}

	linkTargetStr := string(linkTargetBytes)
	linkTargetTokens := strings.Split(linkTargetStr, ".")
	if len(linkTargetTokens) != 2 || len(linkTargetTokens[0]) == 0 || len(linkTargetTokens[1]) == 0 {
		om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("link from=%s with name=%s, has invalid target: %s", ctx.Self.ID, linkName, linkTargetStr))).Reply()
	}
	linkType := linkTargetTokens[0]
	toId := linkTargetTokens[1]

	addLinkOpToOpStack(opStack, ctx.Self.Typename, ctx.Self.ID, toId, linkName, linkType, nil, nil)

	om.AggregateOpMsg(sfMediators.OpMsgOk(resultWithOpStack(easyjson.NewJSONObjectWithKeyValue("body", *linkBody).GetPtr(), opStack))).Reply()
}
