// Copyright 2023 NJWS Inc.

// Foliage graph store crud package.
// Provides stateful functions of low-level crud operations for the graph store
package crud

import (
	"fmt"
	"strings"

	"github.com/foliagecp/easyjson"

	"github.com/foliagecp/sdk/embedded/graph/common"
	lg "github.com/foliagecp/sdk/statefun/logger"
	sfplugins "github.com/foliagecp/sdk/statefun/plugins"
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
		op.SetByPath("from_id", easyjson.NewJSON(fromVertexId))
		op.SetByPath("to_id", easyjson.NewJSON(toVertexId))
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

func addOpStackToResult(result *easyjson.JSON, opStack *easyjson.JSON) bool {
	if result != nil && result.IsObject() && opStack != nil && opStack.IsArray() {
		result.SetByPath("op_stack", *opStack)
		return true
	}
	return false
}

/*
Creates an object in the graph with an id the function being called with. Preliminarily deletes an existing one with the same id, if present.
If caller is not empty returns result to the caller else returns result to the nats topic.

Request:

	payload: json - required
		// Initial request from caller:
		query_id: string - optional // ID for this query. Transaction id for operations with the cache. Do not use the same for concurrent graph modify operations.
		body: json - required // Body for object to be created with.
			<key>: <type> - optional // Any additional key and value to be stored in objects's body.

	options: json - optional
		return_op_stack: bool - optional

Reply:

	payload: json
		status: string
		result: any
		op_stack: json array - optional
*/
func LLAPIVertexCreate(executor sfplugins.StatefunExecutor, contextProcessor *sfplugins.StatefunContextProcessor) {
	payload := contextProcessor.Payload

	result := easyjson.NewJSONObject().GetPtr()
	opStack := getOpStackFromOptions(contextProcessor.Options)

	queryID := common.GetQueryID(contextProcessor)
	//contextProcessor.GlobalCache.TransactionBegin(queryID)

	var objectBody easyjson.JSON
	if payload.GetByPath("body").IsObject() {
		objectBody = payload.GetByPath("body")
	} else {
		objectBody = easyjson.NewJSONObject()
	}

	_, err := contextProcessor.GlobalCache.GetValue(contextProcessor.Self.ID)
	if err == nil { // If vertex already exists
		// Delete existing object ---------------------------------------------
		deleteObjectPayload := easyjson.NewJSONObject()
		deleteObjectPayload.SetByPath("query_id", easyjson.NewJSON(queryID))
		res, err := contextProcessor.Request(sfplugins.GolangLocalRequest, "functions.graph.api.vertex.delete", contextProcessor.Self.ID, &deleteObjectPayload, contextProcessor.Options)
		system.MsgOnErrorReturn(err)
		if res != nil {
			mergeOpStack(opStack, res.GetByPath("op_stack").GetPtr())
		}
		// --------------------------------------------------------------------
	}

	contextProcessor.GlobalCache.SetValue(contextProcessor.Self.ID, objectBody.ToBytes(), true, -1, "")
	addVertexOpToOpStack(opStack, contextProcessor.Self.Typename, contextProcessor.Self.ID, nil, &objectBody)

	result.SetByPath("status", easyjson.NewJSON("ok"))
	result.SetByPath("result", easyjson.NewJSON(""))
	addOpStackToResult(result, opStack)

	common.ReplyQueryID(queryID, result, contextProcessor)

	//contextProcessor.GlobalCache.TransactionEnd(queryID)
}

/*
Updates an object in the graph with an id the function being called with. Merges the old object's body with the new one. Creates a new one if the object does not exist.
If caller is not empty returns result to the caller else returns result to the nats topic.

Request:

	payload: json - required
		// Initial request from caller:
		query_id: string - optional // ID for this query. Transaction id for operations with the cache. Do not use the same for concurrent graph modify operations.
		body: json - required // Body for object to be created with.
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
func LLAPIVertexUpdate(executor sfplugins.StatefunExecutor, contextProcessor *sfplugins.StatefunContextProcessor) {
	payload := contextProcessor.Payload

	errorString := ""
	result := easyjson.NewJSONObject().GetPtr()
	opStack := getOpStackFromOptions(contextProcessor.Options)

	queryID := common.GetQueryID(contextProcessor)

	var objectBody easyjson.JSON
	if payload.GetByPath("body").IsObject() {
		objectBody = payload.GetByPath("body")
	} else {
		errorString += fmt.Sprintf("ERROR LLAPIVertexUpdate %s: body:json is missing;", contextProcessor.Self.ID)
	}

	fixedOldBody := contextProcessor.GetObjectContext()
	newBody := fixedOldBody
	if len(errorString) == 0 {
		mode := payload.GetByPath("mode").AsStringDefault("merge")
		switch mode {
		case "replace":
			newBody = &objectBody
			contextProcessor.SetObjectContext(newBody) // Update an object
			result.SetByPath("status", easyjson.NewJSON("ok"))
		case "merge":
			fallthrough
		default:
			newBody = contextProcessor.GetObjectContext()
			newBody.DeepMerge(objectBody)
			contextProcessor.SetObjectContext(newBody) // Update an object
			result.SetByPath("status", easyjson.NewJSON("ok"))
		}
	} else {
		result.SetByPath("status", easyjson.NewJSON("failed"))
	}
	addVertexOpToOpStack(opStack, contextProcessor.Self.Typename, contextProcessor.Self.ID, fixedOldBody, newBody)

	result.SetByPath("result", easyjson.NewJSON(errorString))
	addOpStackToResult(result, opStack)

	common.ReplyQueryID(queryID, result, contextProcessor)
}

/*
Deletes an object with an id the function being called with from the graph and deletes all links related to it.
If caller is not empty returns result to the caller else returns result to the nats topic.

Request:

	payload: json - required
		// Initial request from caller:
		query_id: string - optional // ID for this query. Transaction id for operations with the cache. Do not use the same for concurrent graph modify operations.

	options: json - optional
		return_op_stack: bool - optional

Reply:

	payload: json
		status: string
		result: any
		op_stack: json array - optional
*/
func LLAPIVertexDelete(executor sfplugins.StatefunExecutor, contextProcessor *sfplugins.StatefunContextProcessor) {
	errorString := ""
	result := easyjson.NewJSONObject().GetPtr()
	opStack := getOpStackFromOptions(contextProcessor.Options)

	queryID := common.GetQueryID(contextProcessor)
	//contextProcessor.GlobalCache.TransactionBegin(queryID)

	// Delete all out links -------------------------------
	outLinkKeys := contextProcessor.GlobalCache.GetKeysByPattern(fmt.Sprintf(OutLinkBodyKeyPrefPattern+LinkKeySuff1Pattern, contextProcessor.Self.ID, ">"))
	for _, outLinkKey := range outLinkKeys {
		inLinkKeyTokens := strings.Split(outLinkKey, ".")
		toObjectID := inLinkKeyTokens[len(inLinkKeyTokens)-1]
		linkType := inLinkKeyTokens[len(inLinkKeyTokens)-2]

		deleteLinkPayload := easyjson.NewJSONObject()
		deleteLinkPayload.SetByPath("query_id", easyjson.NewJSON(queryID))
		deleteLinkPayload.SetByPath("descendant_uuid", easyjson.NewJSON(toObjectID))
		deleteLinkPayload.SetByPath("link_type", easyjson.NewJSON(linkType))
		res, err := contextProcessor.Request(sfplugins.GolangLocalRequest, "functions.graph.api.link.delete", contextProcessor.Self.ID, &deleteLinkPayload, contextProcessor.Options)
		system.MsgOnErrorReturn(err)
		if res != nil {
			mergeOpStack(opStack, res.GetByPath("op_stack").GetPtr())
		}
	}
	// ----------------------------------------------------

	// Delete all in links --------------------------------
	inLinkKeys := contextProcessor.GlobalCache.GetKeysByPattern(fmt.Sprintf(InLinkKeyPrefPattern+LinkKeySuff1Pattern, contextProcessor.Self.ID, ">"))
	for _, inLinkKey := range inLinkKeys {
		inLinkKeyTokens := strings.Split(inLinkKey, ".")
		fromObjectID := inLinkKeyTokens[len(inLinkKeyTokens)-2]
		linkType := inLinkKeyTokens[len(inLinkKeyTokens)-1]

		deleteLinkPayload := easyjson.NewJSONObject()
		deleteLinkPayload.SetByPath("query_id", easyjson.NewJSON(queryID))
		deleteLinkPayload.SetByPath("descendant_uuid", easyjson.NewJSON(contextProcessor.Self.ID))
		deleteLinkPayload.SetByPath("link_type", easyjson.NewJSON(linkType))
		res, err := contextProcessor.Request(sfplugins.GolangLocalRequest, "functions.graph.api.link.delete", fromObjectID, &deleteLinkPayload, contextProcessor.Options)
		system.MsgOnErrorReturn(err)
		if res != nil {
			mergeOpStack(opStack, res.GetByPath("op_stack").GetPtr())
		}
	}
	// ----------------------------------------------------

	// Delete link name generator -------------------------
	contextProcessor.GlobalCache.DeleteValue(fmt.Sprintf(OutLinkNameGenKeyPattern, contextProcessor.Self.ID), true, -1, "")
	// ----------------------------------------------------

	var oldBody *easyjson.JSON = nil
	if opStack != nil {
		oldBody = contextProcessor.GetObjectContext()
	}
	contextProcessor.GlobalCache.DeleteValue(contextProcessor.Self.ID, true, -1, "") // Delete object's body
	addVertexOpToOpStack(opStack, contextProcessor.Self.Typename, contextProcessor.Self.ID, oldBody, nil)

	result.SetByPath("status", easyjson.NewJSON("ok"))
	result.SetByPath("result", easyjson.NewJSON(errorString))
	addOpStackToResult(result, opStack)

	common.ReplyQueryID(queryID, result, contextProcessor)

	//contextProcessor.GlobalCache.TransactionEnd(queryID)
}

/*
Creates a link of type="link_type" from an object with id the funcion being called with to an object with id="descendant_uuid".
Preliminarily deletes an existing link with the same type leading to the same descendant if present.
If caller is not empty returns result to the caller else returns result to the nats topic.

Request:

	payload: json - required
		// Initial request from caller:
		query_id: string - optional // ID for this query. Transaction id for operations with the cache. Do not use the same for concurrent graph modify operations.
		descendant_uuid: string - optional // ID for descendant object. If not defined random UUID will be generated. If a descandant with the specified uuid does not exist - will be created with empty body.
		link_type: string - optional // Type of link leading to descendant. If not defined random UUID will be used.
		link_body: json - optional // Body for link leading to descendant.
			name: string - optional // Defines link's name which is unique among all object's output links. Will be generated automatically if not defined or if same named out link already exists.
			tags: []string - optional // Defines link tags.
			<key>: <type> - optional // Any additional key and value to be stored in link's body.

		// Self-requests to descendants (GolangCallSync): // ID can be composite: <object_id>===create_in_link - for non-blocking execution on the same object
			query_id: string - required // ID for this query.
			in_link_type: string - required // Type of input link to create

	options: json - optional
		return_op_stack: bool - optional

Reply:

	payload: json
		status: string
		result: any
		op_stack: json array - optional
*/
func LLAPILinkCreate(executor sfplugins.StatefunExecutor, contextProcessor *sfplugins.StatefunContextProcessor) {
	payload := contextProcessor.Payload

	queryID := common.GetQueryID(contextProcessor)
	//contextProcessor.GlobalCache.TransactionBegin(queryID)

	errorString := ""
	result := easyjson.NewJSONObject().GetPtr()
	opStack := getOpStackFromOptions(contextProcessor.Options)

	if payload.PathExists("in_link_type") {
		selfID := strings.Split(contextProcessor.Self.ID, "===")[0]
		// TODO: This vertex might not exist at all, what to do about that?
		if inLinkType, ok := payload.GetByPath("in_link_type").AsString(); ok && len(inLinkType) > 0 {
			if linkFromObjectUUID := contextProcessor.Caller.ID; len(linkFromObjectUUID) > 0 {
				contextProcessor.GlobalCache.SetValue(fmt.Sprintf(InLinkKeyPrefPattern+LinkKeySuff2Pattern, selfID, linkFromObjectUUID, inLinkType), nil, true, -1, "")
				result.SetByPath("status", easyjson.NewJSON("ok"))
			}
		} else {
			result.SetByPath("status", easyjson.NewJSON("failed"))
			errorString = fmt.Sprintf("LLAPILinkCreate %s: in_link_type:string must be a non empty string", selfID)
			lg.Logln(lg.ErrorLevel, errorString)
		}
		result.SetByPath("result", easyjson.NewJSON(errorString))
		common.ReplyQueryID(queryID, result, contextProcessor)
	} else {
		var linkBody easyjson.JSON
		if payload.GetByPath("link_body").IsObject() {
			linkBody = payload.GetByPath("link_body")
		} else {
			errorString += fmt.Sprintf("ERROR LLAPILinkCreate %s: link_body:json is missing;", contextProcessor.Self.ID)
		}
		if len(errorString) == 0 {
			var linkType string
			if s, ok := payload.GetByPath("link_type").AsString(); ok {
				linkType = s
			} else {
				linkType = system.GetUniqueStrID()
			}
			var descendantUUID string
			if s, ok := payload.GetByPath("descendant_uuid").AsString(); ok {
				descendantUUID = s
			} else {
				descendantUUID = system.GetUniqueStrID()
			}

			// Delete link if exists ----------------------------------
			_, err := contextProcessor.GlobalCache.GetValue(fmt.Sprintf(OutLinkBodyKeyPrefPattern+LinkKeySuff2Pattern, contextProcessor.Self.ID, linkType, descendantUUID))
			if err == nil {
				nextCallPayload := easyjson.NewJSONObject()
				nextCallPayload.SetByPath("query_id", easyjson.NewJSON(queryID))
				nextCallPayload.SetByPath("descendant_uuid", easyjson.NewJSON(descendantUUID))
				nextCallPayload.SetByPath("link_type", easyjson.NewJSON(linkType))
				res, err := contextProcessor.Request(sfplugins.GolangLocalRequest, "functions.graph.api.link.delete", contextProcessor.Self.ID, &nextCallPayload, contextProcessor.Options)
				system.MsgOnErrorReturn(err)
				if res != nil {
					mergeOpStack(opStack, res.GetByPath("op_stack").GetPtr())
				}
			}
			// --------------------------------------------------------

			// Create out link on this object -------------------------
			// Create unique link name ----------
			linkName, linkNameDefined := linkBody.GetByPath("name").AsString()
			if len(linkName) == 0 {
				linkNameDefined = false
			}
			sameNamedLinkExists := false
			if linkNameDefined {
				if _, err := contextProcessor.GlobalCache.GetValue(fmt.Sprintf(OutLinkLinkNamePrefPattern+LinkKeySuff1Pattern, contextProcessor.Self.ID, linkName)); err == nil {
					sameNamedLinkExists = true
				}
			}
			if !linkNameDefined || sameNamedLinkExists {
				var namegen int64 = 0
				if v, err := contextProcessor.GlobalCache.GetValue(fmt.Sprintf(OutLinkNameGenKeyPattern, contextProcessor.Self.ID)); err == nil {
					namegen = system.BytesToInt64(v)
				}
				linkName = fmt.Sprintf("name%d", namegen)
				namegen++
				contextProcessor.GlobalCache.SetValue(fmt.Sprintf(OutLinkNameGenKeyPattern, contextProcessor.Self.ID), system.Int64ToBytes(namegen), true, -1, "")
				linkBody.SetByPath("name", easyjson.NewJSON(linkName))
			}
			// ----------------------------------
			// Create link name -----------------
			contextProcessor.GlobalCache.SetValue(fmt.Sprintf(OutLinkLinkNamePrefPattern+LinkKeySuff1Pattern, contextProcessor.Self.ID, linkName), nil, true, -1, "")
			// ----------------------------------
			// Index link name ------------------
			contextProcessor.GlobalCache.SetValue(fmt.Sprintf(OutLinkIndexPrefPattern+LinkKeySuff4Pattern, contextProcessor.Self.ID, linkType, descendantUUID, "name", linkName), nil, true, -1, "")
			// ----------------------------------
			// Set link body --------------------
			contextProcessor.GlobalCache.SetValue(fmt.Sprintf(OutLinkBodyKeyPrefPattern+LinkKeySuff2Pattern, contextProcessor.Self.ID, linkType, descendantUUID), linkBody.ToBytes(), true, -1, "") // Store link body in KV
			// ----------------------------------
			// Store tags -----------------------
			if linkBody.GetByPath("tags").IsNonEmptyArray() {
				if linkTags, ok := linkBody.GetByPath("tags").AsArrayString(); ok {
					for _, linkTag := range linkTags {
						contextProcessor.GlobalCache.SetValue(fmt.Sprintf(OutLinkIndexPrefPattern+LinkKeySuff4Pattern, contextProcessor.Self.ID, linkType, descendantUUID, "tag", linkTag), nil, true, -1, "")
					}
				}
			}
			// ----------------------------------
			// --------------------------------------------------------

			// Create in link on descendant object --------------------
			nextCallPayload := easyjson.NewJSONObject()
			nextCallPayload.SetByPath("query_id", easyjson.NewJSON(queryID))
			nextCallPayload.SetByPath("in_link_type", easyjson.NewJSON(linkType))
			if descendantUUID == contextProcessor.Self.ID {
				system.MsgOnErrorReturn(contextProcessor.Request(sfplugins.GolangLocalRequest, contextProcessor.Self.Typename, descendantUUID+"===create_in_link", &nextCallPayload, nil))
			} else {
				system.MsgOnErrorReturn(contextProcessor.Request(sfplugins.GolangLocalRequest, contextProcessor.Self.Typename, descendantUUID, &nextCallPayload, nil))
			}
			// --------------------------------------------------------

			addLinkOpToOpStack(opStack, contextProcessor.Self.Typename, contextProcessor.Self.ID, descendantUUID, linkType, nil, &linkBody)

			result.SetByPath("status", easyjson.NewJSON("ok"))
			result.SetByPath("result", easyjson.NewJSON(errorString))
			addOpStackToResult(result, opStack)
		} else {
			result.SetByPath("status", easyjson.NewJSON("failed"))
			result.SetByPath("result", easyjson.NewJSON(errorString))
		}
		common.ReplyQueryID(queryID, result, contextProcessor)
	}
	//contextProcessor.GlobalCache.TransactionEnd(queryID)
}

/*
Updates a link of type="link_type" from an object with id the funcion being called with to an object with id="descendant_uuid".
Merges the old link's body with the new one. Creates a new one if the link does not exist.
If caller is not empty returns result to the caller else returns result to the nats topic.

Request:

	payload: json - required
		// Initial request from caller:
		query_id: string - optional // ID for this query. Transaction id for operations with the cache. Do not use the same for concurrent graph modify operations.
		descendant_uuid: string - required // ID for descendant object. If a descandant with the specified uuid does not exist - will be created with empty body.
		link_type: string - required // Type of link leading to descendant.
		link_body: json - required // Body for link leading to descendant.
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
func LLAPILinkUpdate(executor sfplugins.StatefunExecutor, contextProcessor *sfplugins.StatefunContextProcessor) {
	payload := contextProcessor.Payload

	queryID := common.GetQueryID(contextProcessor)
	//contextProcessor.GlobalCache.TransactionBegin(queryID)

	errorString := ""
	result := easyjson.NewJSONObject().GetPtr()
	opStack := getOpStackFromOptions(contextProcessor.Options)

	var linkBody easyjson.JSON
	if payload.GetByPath("link_body").IsObject() {
		linkBody = payload.GetByPath("link_body")
	} else {
		errorString += fmt.Sprintf("ERROR LLAPILinkUpdate %s: link_body:json is missing;", contextProcessor.Self.ID)
	}
	var linkType string
	if s, ok := payload.GetByPath("link_type").AsString(); ok {
		linkType = s
	} else {
		errorString += fmt.Sprintf("ERROR LLAPILinkUpdate %s: link_type:string is missing;", contextProcessor.Self.ID)
	}
	var descendantUUID string
	if s, ok := payload.GetByPath("descendant_uuid").AsString(); ok {
		descendantUUID = s
	} else {
		errorString += fmt.Sprintf("ERROR LLAPILinkUpdate %s: descendant_uuid:string is missing;", contextProcessor.Self.ID)
	}

	if len(errorString) == 0 {
		if fixedOldLinkBody, err := contextProcessor.GlobalCache.GetValueAsJSON(fmt.Sprintf(OutLinkBodyKeyPrefPattern+LinkKeySuff2Pattern, contextProcessor.Self.ID, linkType, descendantUUID)); err == nil {
			// Delete old indices -----------------------------------------
			// Link name ------------------------
			if linkName, ok := fixedOldLinkBody.GetByPath("name").AsString(); ok {
				contextProcessor.GlobalCache.DeleteValue(fmt.Sprintf(OutLinkLinkNamePrefPattern+LinkKeySuff1Pattern, contextProcessor.Self.ID, linkName), true, -1, "")
				contextProcessor.GlobalCache.DeleteValue(fmt.Sprintf(OutLinkIndexPrefPattern+LinkKeySuff4Pattern, contextProcessor.Self.ID, linkType, descendantUUID, "name", linkName), true, -1, "")
			}
			// ----------------------------------
			// Link tags ------------------------
			if fixedOldLinkBody.GetByPath("tags").IsNonEmptyArray() {
				if linkTags, ok := fixedOldLinkBody.GetByPath("tags").AsArrayString(); ok {
					for _, linkTag := range linkTags {
						contextProcessor.GlobalCache.DeleteValue(fmt.Sprintf(OutLinkIndexPrefPattern+LinkKeySuff4Pattern, contextProcessor.Self.ID, linkType, descendantUUID, "tag", linkTag), true, -1, "")
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
				if _, err := contextProcessor.GlobalCache.GetValue(fmt.Sprintf(OutLinkLinkNamePrefPattern+LinkKeySuff1Pattern, contextProcessor.Self.ID, linkName)); err == nil {
					sameNamedLinkExists = true
				}
			}
			if !linkNameDefined || sameNamedLinkExists {
				var namegen int64 = 0
				if v, err := contextProcessor.GlobalCache.GetValue(fmt.Sprintf(OutLinkNameGenKeyPattern, contextProcessor.Self.ID)); err == nil {
					namegen = system.BytesToInt64(v)
				}
				linkName = fmt.Sprintf("name%d", namegen)
				namegen++
				contextProcessor.GlobalCache.SetValue(fmt.Sprintf(OutLinkNameGenKeyPattern, contextProcessor.Self.ID), system.Int64ToBytes(namegen), true, -1, "")
				newBody.SetByPath("name", easyjson.NewJSON(linkName))
			}
			// ------------------------------------------------------------
			// Create link name -------------------------------------------
			contextProcessor.GlobalCache.SetValue(fmt.Sprintf(OutLinkLinkNamePrefPattern+LinkKeySuff1Pattern, contextProcessor.Self.ID, linkName), nil, true, -1, "")
			// ------------------------------------------------------------
			// Index link name --------------------------------------------
			contextProcessor.GlobalCache.SetValue(fmt.Sprintf(OutLinkIndexPrefPattern+LinkKeySuff4Pattern, contextProcessor.Self.ID, linkType, descendantUUID, "name", linkName), nil, true, -1, "")
			// ------------------------------------------------------------
			// Update link body -------------------------------------------
			contextProcessor.GlobalCache.SetValue(fmt.Sprintf(OutLinkBodyKeyPrefPattern+LinkKeySuff2Pattern, contextProcessor.Self.ID, linkType, descendantUUID), newBody.ToBytes(), true, -1, "") // Store link body in KV
			// ------------------------------------------------------------
			// Create new indices -----------------------------------------
			if newBody.GetByPath("tags").IsNonEmptyArray() {
				if linkTags, ok := newBody.GetByPath("tags").AsArrayString(); ok {
					for _, linkTag := range linkTags {
						contextProcessor.GlobalCache.SetValue(fmt.Sprintf(OutLinkIndexPrefPattern+LinkKeySuff4Pattern, contextProcessor.Self.ID, linkType, descendantUUID, "tag", linkTag), nil, true, -1, "")
					}
				}
			}
			// ------------------------------------------------------------
			addLinkOpToOpStack(opStack, contextProcessor.Self.Typename, contextProcessor.Self.ID, descendantUUID, linkType, fixedOldLinkBody, newBody)
		} else {
			// Create link if does not exist
			createLinkPayload := easyjson.NewJSONObject()
			createLinkPayload.SetByPath("query_id", easyjson.NewJSON(queryID))
			createLinkPayload.SetByPath("descendant_uuid", easyjson.NewJSON(descendantUUID))
			createLinkPayload.SetByPath("link_type", easyjson.NewJSON(linkType))
			createLinkPayload.SetByPath("link_body", linkBody)
			res, err := contextProcessor.Request(sfplugins.GolangLocalRequest, "functions.graph.api.link.create", contextProcessor.Self.ID, &createLinkPayload, contextProcessor.Options)
			system.MsgOnErrorReturn(err)
			if res != nil {
				mergeOpStack(opStack, res.GetByPath("op_stack").GetPtr())
			}
		}

		result.SetByPath("status", easyjson.NewJSON("ok"))
		result.SetByPath("result", easyjson.NewJSON(errorString))
	} else {
		result.SetByPath("status", easyjson.NewJSON("failed"))
		result.SetByPath("result", easyjson.NewJSON(errorString))
	}
	addOpStackToResult(result, opStack)
	common.ReplyQueryID(queryID, result, contextProcessor)

	//contextProcessor.GlobalCache.TransactionEnd(queryID)
}

/*
Delete a link of type="link_type" from an object with id the funcion being called with to an object with id="descendant_uuid".
If caller is not empty returns result to the caller else returns result to the nats topic.

Request:

	payload: json - required
		// Initial request from caller:
		query_id: string - optional // ID for this query. Transaction id for operations with the cache. Do not use the same for concurrent graph modify operations.
		descendant_uuid: string - required // ID for descendant object.
		link_type: string - required // Type of link leading to descendant.

		// Self-requests to descendants (GolangCallSync): // ID can be composite: <object_id>===delete_in_link - for non-blocking execution on the same object
		query_id: string - required // ID for this query.
		in_link_type: string - required // Type of input link to delete

	options: json - optional
		return_op_stack: bool - optional

Reply:

	payload: json
		status: string
		result: any
		op_stack: json array - optional
*/
func LLAPILinkDelete(executor sfplugins.StatefunExecutor, contextProcessor *sfplugins.StatefunContextProcessor) {
	payload := contextProcessor.Payload

	queryID := common.GetQueryID(contextProcessor)
	//contextProcessor.GlobalCache.TransactionBegin(queryID)

	errorString := ""
	result := easyjson.NewJSONObject().GetPtr()
	opStack := getOpStackFromOptions(contextProcessor.Options)

	if payload.PathExists("in_link_type") {
		selfID := strings.Split(contextProcessor.Self.ID, "===")[0]
		if inLinkType, ok := payload.GetByPath("in_link_type").AsString(); ok && len(inLinkType) > 0 {
			if linkFromObjectUUID := contextProcessor.Caller.ID; len(linkFromObjectUUID) > 0 {
				contextProcessor.GlobalCache.DeleteValue(fmt.Sprintf(InLinkKeyPrefPattern+LinkKeySuff2Pattern, selfID, linkFromObjectUUID, inLinkType), true, -1, "")
				result.SetByPath("status", easyjson.NewJSON("ok"))
			}
		} else {
			result.SetByPath("status", easyjson.NewJSON("failed"))
			errorString = fmt.Sprintf("LLAPILinkDelete %s: in_link_type:string must be a non empty string", selfID)
			lg.Logln(lg.ErrorLevel, errorString)
		}
		result.SetByPath("result", easyjson.NewJSON(errorString))
		common.ReplyQueryID(queryID, result, contextProcessor)
	} else {
		var linkType string
		if s, ok := payload.GetByPath("link_type").AsString(); ok {
			linkType = s
		} else {
			errorString += fmt.Sprintf("ERROR LLAPILinkDelete %s: link_type:string is missing;", contextProcessor.Self.ID)
		}
		var descendantUUID string
		if s, ok := payload.GetByPath("descendant_uuid").AsString(); ok {
			descendantUUID = s
		} else {
			errorString += fmt.Sprintf("ERROR LLAPILinkDelete %s: descendant_uuid:string is missing;", contextProcessor.Self.ID)
		}

		if len(errorString) == 0 {
			if _, err := contextProcessor.GlobalCache.GetValue(fmt.Sprintf(OutLinkBodyKeyPrefPattern+LinkKeySuff2Pattern, contextProcessor.Self.ID, linkType, descendantUUID)); err != nil {
				// Link does not exist - nothing to delete
				result.SetByPath("status", easyjson.NewJSON("ok"))
				result.SetByPath("result", easyjson.NewJSON("Link does not exist"))
			} else {
				lbk := fmt.Sprintf(OutLinkBodyKeyPrefPattern+LinkKeySuff2Pattern, contextProcessor.Self.ID, linkType, descendantUUID)
				linkBody, _ := contextProcessor.GlobalCache.GetValueAsJSON(lbk)
				contextProcessor.GlobalCache.DeleteValue(lbk, true, -1, "")

				if linkBody != nil {
					// Delete link name -------------------
					if linkName, ok := linkBody.GetByPath("name").AsString(); ok {
						contextProcessor.GlobalCache.DeleteValue(fmt.Sprintf(OutLinkLinkNamePrefPattern+LinkKeySuff1Pattern, contextProcessor.Self.ID, linkName), true, -1, "")
						contextProcessor.GlobalCache.DeleteValue(fmt.Sprintf(OutLinkIndexPrefPattern+LinkKeySuff4Pattern, contextProcessor.Self.ID, linkType, descendantUUID, "name", linkName), true, -1, "")
					}
					// -----------------------------------
					// Delete tags -----------------------
					if linkBody.GetByPath("tags").IsNonEmptyArray() {
						if linkTags, ok := linkBody.GetByPath("tags").AsArrayString(); ok {
							for _, linkTag := range linkTags {
								contextProcessor.GlobalCache.DeleteValue(fmt.Sprintf(OutLinkIndexPrefPattern+LinkKeySuff4Pattern, contextProcessor.Self.ID, linkType, descendantUUID, "tag", linkTag), true, -1, "")
							}
						}
					}
					// ------------------------------------
				}

				nextCallPayload := easyjson.NewJSONObject()
				nextCallPayload.SetByPath("query_id", easyjson.NewJSON(queryID))
				nextCallPayload.SetByPath("in_link_type", easyjson.NewJSON(linkType))
				if descendantUUID == contextProcessor.Self.ID {
					system.MsgOnErrorReturn(contextProcessor.Request(sfplugins.GolangLocalRequest, contextProcessor.Self.Typename, descendantUUID+"===delete_in_link", &nextCallPayload, nil))
				} else {
					system.MsgOnErrorReturn(contextProcessor.Request(sfplugins.GolangLocalRequest, contextProcessor.Self.Typename, descendantUUID, &nextCallPayload, nil))
				}

				addLinkOpToOpStack(opStack, contextProcessor.Self.Typename, contextProcessor.Self.ID, descendantUUID, linkType, linkBody, nil)

				result.SetByPath("status", easyjson.NewJSON("ok"))
				result.SetByPath("result", easyjson.NewJSON(errorString))
			}
		} else {
			result.SetByPath("status", easyjson.NewJSON("failed"))
			result.SetByPath("result", easyjson.NewJSON(errorString))
		}
		addOpStackToResult(result, opStack)
		common.ReplyQueryID(queryID, result, contextProcessor)
	}
	//contextProcessor.GlobalCache.TransactionEnd(queryID)
}
