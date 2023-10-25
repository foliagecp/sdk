

// Foliage graph store crud package.
// Provides stateful functions of low-level crud operations for the graph store
package crud

import (
	"fmt"
	"strings"

	"github.com/foliagecp/easyjson"

	"github.com/foliagecp/sdk/embedded/graph/common"
	"github.com/foliagecp/sdk/statefun"
	sfplugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
)

func RegisterAllFunctionTypes(runtime *statefun.Runtime) {
	// High-Level API Registration
	statefun.NewFunctionType(runtime, "functions.graph.api.object.create", CreateObject, *statefun.NewFunctionTypeConfig())
	statefun.NewFunctionType(runtime, "functions.graph.api.object.update", UpdateObject, *statefun.NewFunctionTypeConfig())
	statefun.NewFunctionType(runtime, "functions.graph.api.object.delete", DeleteObject, *statefun.NewFunctionTypeConfig())

	statefun.NewFunctionType(runtime, "functions.graph.api.type.create", CreateType, *statefun.NewFunctionTypeConfig())
	statefun.NewFunctionType(runtime, "functions.graph.api.type.update", UpdateType, *statefun.NewFunctionTypeConfig())
	statefun.NewFunctionType(runtime, "functions.graph.api.type.delete", DeleteType, *statefun.NewFunctionTypeConfig())

	statefun.NewFunctionType(runtime, "functions.graph.api.types.link.create", CreateTypesLink, *statefun.NewFunctionTypeConfig())
	statefun.NewFunctionType(runtime, "functions.graph.api.types.link.update", UpdateTypesLink, *statefun.NewFunctionTypeConfig())
	statefun.NewFunctionType(runtime, "functions.graph.api.types.link.delete", DeleteTypesLink, *statefun.NewFunctionTypeConfig())

	statefun.NewFunctionType(runtime, "functions.graph.api.objects.link.create", CreateObjectsLink, *statefun.NewFunctionTypeConfig())
	statefun.NewFunctionType(runtime, "functions.graph.api.objects.link.update", UpdateObjectsLink, *statefun.NewFunctionTypeConfig())
	statefun.NewFunctionType(runtime, "functions.graph.api.objects.link.delete", DeleteObejectsLink, *statefun.NewFunctionTypeConfig())
	// High-Level API End Registration

	// Low-Level API
	statefun.NewFunctionType(runtime, "functions.graph.ll.api.object.create", LLAPIObjectCreate, *statefun.NewFunctionTypeConfig())
	statefun.NewFunctionType(runtime, "functions.graph.ll.api.object.update", LLAPIObjectUpdate, *statefun.NewFunctionTypeConfig())
	statefun.NewFunctionType(runtime, "functions.graph.ll.api.object.delete", LLAPIObjectDelete, *statefun.NewFunctionTypeConfig())

	statefun.NewFunctionType(runtime, "functions.graph.ll.api.link.create", LLAPILinkCreate, *statefun.NewFunctionTypeConfig())
	statefun.NewFunctionType(runtime, "functions.graph.ll.api.link.update", LLAPILinkUpdate, *statefun.NewFunctionTypeConfig())
	statefun.NewFunctionType(runtime, "functions.graph.ll.api.link.delete", LLAPILinkDelete, *statefun.NewFunctionTypeConfig())
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

Reply:

	payload: json
		status: string
		result: any
*/
func LLAPIObjectCreate(executor sfplugins.StatefunExecutor, contextProcessor *sfplugins.StatefunContextProcessor) {
	payload := contextProcessor.Payload

	result := easyjson.NewJSONObject()

	queryID := common.GetQueryID(contextProcessor)
	contextProcessor.GlobalCache.TransactionBegin(queryID)

	var objectBody easyjson.JSON
	if payload.GetByPath("body").IsObject() {
		objectBody = payload.GetByPath("body")
	} else {
		objectBody = easyjson.NewJSONObject()
	}

	// Delete existing object ---------------------------------------------
	deleteObjectPayload := easyjson.NewJSONObject()
	deleteObjectPayload.SetByPath("query_id", easyjson.NewJSON(queryID))
	system.MsgOnErrorReturn(contextProcessor.Request(sfplugins.GolangLocalRequest, "functions.graph.ll.api.object.delete", contextProcessor.Self.ID, &deleteObjectPayload, nil))
	// --------------------------------------------------------------------

	contextProcessor.GlobalCache.SetValue(contextProcessor.Self.ID, objectBody.ToBytes(), true, -1, queryID)

	result.SetByPath("status", easyjson.NewJSON("ok"))
	result.SetByPath("result", easyjson.NewJSON(""))

	common.ReplyQueryID(queryID, &result, contextProcessor)

	contextProcessor.GlobalCache.TransactionEnd(queryID)
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

Reply:

	payload: json
		status: string
		result: any
*/
func LLAPIObjectUpdate(executor sfplugins.StatefunExecutor, contextProcessor *sfplugins.StatefunContextProcessor) {
	payload := contextProcessor.Payload

	errorString := ""
	result := easyjson.NewJSONObject()

	queryID := common.GetQueryID(contextProcessor)

	var objectBody easyjson.JSON
	if payload.GetByPath("body").IsObject() {
		objectBody = payload.GetByPath("body")
	} else {
		errorString += fmt.Sprintf("ERROR LLAPIObjectUpdate %s: body:json is missing;", contextProcessor.Self.ID)
	}

	if len(errorString) == 0 {
		oldObjectBody := contextProcessor.GetObjectContext()
		oldObjectBody.DeepMerge(objectBody)
		contextProcessor.SetObjectContext(oldObjectBody) // Update an object
		result.SetByPath("status", easyjson.NewJSON("ok"))
	} else {
		result.SetByPath("status", easyjson.NewJSON("failed"))
	}
	result.SetByPath("result", easyjson.NewJSON(errorString))

	common.ReplyQueryID(queryID, &result, contextProcessor)
}

/*
Deletes an object with an id the function being called with from the graph and deletes all links related to it.
If caller is not empty returns result to the caller else returns result to the nats topic.

Request:

	payload: json - required
		// Initial request from caller:
		query_id: string - optional // ID for this query. Transaction id for operations with the cache. Do not use the same for concurrent graph modify operations.

Reply:

	payload: json
		status: string
		result: any
*/
func LLAPIObjectDelete(executor sfplugins.StatefunExecutor, contextProcessor *sfplugins.StatefunContextProcessor) {
	errorString := ""
	result := easyjson.NewJSONObject()

	queryID := common.GetQueryID(contextProcessor)
	contextProcessor.GlobalCache.TransactionBegin(queryID)

	// Delete all out links -------------------------------
	outLinkKeys := contextProcessor.GlobalCache.GetKeysByPattern(contextProcessor.Self.ID + ".out.ltp_oid-bdy.>")
	for _, outLinkKey := range outLinkKeys {
		inLinkKeyTokens := strings.Split(outLinkKey, ".")
		toObjectID := inLinkKeyTokens[len(inLinkKeyTokens)-1]
		linkType := inLinkKeyTokens[len(inLinkKeyTokens)-2]

		deleteLinkPayload := easyjson.NewJSONObject()
		deleteLinkPayload.SetByPath("query_id", easyjson.NewJSON(queryID))
		deleteLinkPayload.SetByPath("descendant_uuid", easyjson.NewJSON(toObjectID))
		deleteLinkPayload.SetByPath("link_type", easyjson.NewJSON(linkType))
		system.MsgOnErrorReturn(contextProcessor.Request(sfplugins.GolangLocalRequest, "functions.graph.ll.api.link.delete", contextProcessor.Self.ID, &deleteLinkPayload, nil))
	}
	// ----------------------------------------------------

	// Delete all in links --------------------------------
	inLinkKeys := contextProcessor.GlobalCache.GetKeysByPattern(contextProcessor.Self.ID + ".in.oid_ltp-nil.>")
	for _, inLinkKey := range inLinkKeys {
		inLinkKeyTokens := strings.Split(inLinkKey, ".")
		fromObjectID := inLinkKeyTokens[len(inLinkKeyTokens)-2]
		linkType := inLinkKeyTokens[len(inLinkKeyTokens)-1]

		deleteLinkPayload := easyjson.NewJSONObject()
		deleteLinkPayload.SetByPath("query_id", easyjson.NewJSON(queryID))
		deleteLinkPayload.SetByPath("descendant_uuid", easyjson.NewJSON(contextProcessor.Self.ID))
		deleteLinkPayload.SetByPath("link_type", easyjson.NewJSON(linkType))
		system.MsgOnErrorReturn(contextProcessor.Request(sfplugins.GolangLocalRequest, "functions.graph.ll.api.link.delete", fromObjectID, &deleteLinkPayload, nil))
	}
	// ----------------------------------------------------
	contextProcessor.GlobalCache.DeleteValue(contextProcessor.Self.ID, true, -1, queryID) // Delete object's body

	result.SetByPath("status", easyjson.NewJSON("ok"))
	result.SetByPath("result", easyjson.NewJSON(errorString))

	common.ReplyQueryID(queryID, &result, contextProcessor)

	contextProcessor.GlobalCache.TransactionEnd(queryID)
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
			tags: []string - optional // Defines link tags.
			<key>: <type> - optional // Any additional key and value to be stored in link's body.

		// Self-requests to descendants (GolangCallSync): // ID can be composite: <object_id>===create_in_link - for non-blocking execution on the same object
			query_id: string - required // ID for this query.
			in_link_type: string - required // Type of input link to create

Reply:

	payload: json
		status: string
		result: any
*/
func LLAPILinkCreate(executor sfplugins.StatefunExecutor, contextProcessor *sfplugins.StatefunContextProcessor) {
	payload := contextProcessor.Payload

	queryID := common.GetQueryID(contextProcessor)
	contextProcessor.GlobalCache.TransactionBegin(queryID)

	errorString := ""
	result := easyjson.NewJSONObject()

	if payload.PathExists("in_link_type") {
		selfID := strings.Split(contextProcessor.Self.ID, "===")[0]
		if inLinkType, ok := payload.GetByPath("in_link_type").AsString(); ok && len(inLinkType) > 0 {
			if linkFromObjectUUID := contextProcessor.Caller.ID; len(linkFromObjectUUID) > 0 {
				contextProcessor.GlobalCache.SetValue(selfID+".in.oid_ltp-nil."+linkFromObjectUUID+"."+inLinkType, nil, true, -1, queryID)
				result.SetByPath("status", easyjson.NewJSON("ok"))
			}
		} else {
			result.SetByPath("status", easyjson.NewJSON("failed"))
			errorString = fmt.Sprintf("ERROR LLAPILinkCreate %s: in_link_type:string must be a non empty string", selfID)
			fmt.Println(errorString)
		}
		result.SetByPath("result", easyjson.NewJSON(errorString))
		contextProcessor.RequestReplyData = &result
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
			nextCallPayload := easyjson.NewJSONObject()
			nextCallPayload.SetByPath("query_id", easyjson.NewJSON(queryID))
			nextCallPayload.SetByPath("descendant_uuid", easyjson.NewJSON(descendantUUID))
			nextCallPayload.SetByPath("link_type", easyjson.NewJSON(linkType))
			system.MsgOnErrorReturn(contextProcessor.Request(sfplugins.GolangLocalRequest, "functions.graph.ll.api.link.delete", contextProcessor.Self.ID, &nextCallPayload, nil))
			// --------------------------------------------------------

			// Create out link on this object -------------------------
			contextProcessor.GlobalCache.SetValue(contextProcessor.Self.ID+".out.ltp_oid-bdy."+linkType+"."+descendantUUID, linkBody.ToBytes(), true, -1, queryID) // Store link body in KV
			if linkBody.GetByPath("tags").IsNonEmptyArray() {
				if linkTags, ok := linkBody.GetByPath("tags").AsArrayString(); ok {
					for _, linkTag := range linkTags {
						contextProcessor.GlobalCache.SetValue(contextProcessor.Self.ID+".out.tag_ltp_oid-nil."+linkTag+"."+linkType+"."+descendantUUID, nil, true, -1, queryID)
					}
				}
			}
			// --------------------------------------------------------

			// Create in link on descendant object --------------------
			nextCallPayload = easyjson.NewJSONObject()
			nextCallPayload.SetByPath("query_id", easyjson.NewJSON(queryID))
			nextCallPayload.SetByPath("in_link_type", easyjson.NewJSON(linkType))
			if descendantUUID == contextProcessor.Self.ID {
				system.MsgOnErrorReturn(contextProcessor.Request(sfplugins.GolangLocalRequest, contextProcessor.Self.Typename, descendantUUID+"===create_in_link", &nextCallPayload, nil))
			} else {
				system.MsgOnErrorReturn(contextProcessor.Request(sfplugins.GolangLocalRequest, contextProcessor.Self.Typename, descendantUUID, &nextCallPayload, nil))
			}
			// --------------------------------------------------------

			result.SetByPath("status", easyjson.NewJSON("ok"))
			result.SetByPath("result", easyjson.NewJSON(errorString))
		} else {
			result.SetByPath("status", easyjson.NewJSON("failed"))
			result.SetByPath("result", easyjson.NewJSON(errorString))
		}
		common.ReplyQueryID(queryID, &result, contextProcessor)
	}
	contextProcessor.GlobalCache.TransactionEnd(queryID)
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

Reply:

	payload: json
		status: string
		result: any
*/
func LLAPILinkUpdate(executor sfplugins.StatefunExecutor, contextProcessor *sfplugins.StatefunContextProcessor) {
	payload := contextProcessor.Payload

	queryID := common.GetQueryID(contextProcessor)
	contextProcessor.GlobalCache.TransactionBegin(queryID)

	errorString := ""
	result := easyjson.NewJSONObject()

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
		if oldLinkBody, err := contextProcessor.GlobalCache.GetValueAsJSON(contextProcessor.Self.ID + ".out.ltp_oid-bdy." + linkType + "." + descendantUUID); err == nil {
			// Delete old indices -----------------------------------------
			if oldLinkBody.GetByPath("tags").IsNonEmptyArray() {
				if linkTags, ok := oldLinkBody.GetByPath("tags").AsArrayString(); ok {
					for _, linkTag := range linkTags {
						contextProcessor.GlobalCache.DeleteValue(contextProcessor.Self.ID+".out.tag_ltp_oid-nil."+linkTag+"."+linkType+"."+descendantUUID, true, -1, queryID)
					}
				}
			}
			// ------------------------------------------------------------
			// Update link body -------------------------------------------
			oldLinkBody.DeepMerge(linkBody)
			contextProcessor.GlobalCache.SetValue(contextProcessor.Self.ID+".out.ltp_oid-bdy."+linkType+"."+descendantUUID, oldLinkBody.ToBytes(), true, -1, queryID) // Store link body in KV
			// ------------------------------------------------------------
			// Create new indices -----------------------------------------
			if oldLinkBody.GetByPath("tags").IsNonEmptyArray() {
				if linkTags, ok := oldLinkBody.GetByPath("tags").AsArrayString(); ok {
					for _, linkTag := range linkTags {
						contextProcessor.GlobalCache.SetValue(contextProcessor.Self.ID+".out.tag_ltp_oid-nil."+linkTag+"."+linkType+"."+descendantUUID, nil, true, -1, queryID)
					}
				}
			}
			// ------------------------------------------------------------
		} else {
			// Create link if does not exist
			createLinkPayload := easyjson.NewJSONObject()
			createLinkPayload.SetByPath("query_id", easyjson.NewJSON(queryID))
			createLinkPayload.SetByPath("descendant_uuid", easyjson.NewJSON(descendantUUID))
			createLinkPayload.SetByPath("link_type", easyjson.NewJSON(linkType))
			createLinkPayload.SetByPath("link_body", linkBody)
			system.MsgOnErrorReturn(contextProcessor.Request(sfplugins.GolangLocalRequest, "functions.graph.ll.api.link.create", contextProcessor.Self.ID, &createLinkPayload, nil))
		}

		result.SetByPath("status", easyjson.NewJSON("ok"))
		result.SetByPath("result", easyjson.NewJSON(errorString))
	} else {
		result.SetByPath("status", easyjson.NewJSON("failed"))
		result.SetByPath("result", easyjson.NewJSON(errorString))
	}
	common.ReplyQueryID(queryID, &result, contextProcessor)

	contextProcessor.GlobalCache.TransactionEnd(queryID)
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

Reply:

	payload: json
		status: string
		result: any
*/
func LLAPILinkDelete(executor sfplugins.StatefunExecutor, contextProcessor *sfplugins.StatefunContextProcessor) {
	payload := contextProcessor.Payload

	queryID := common.GetQueryID(contextProcessor)
	contextProcessor.GlobalCache.TransactionBegin(queryID)

	errorString := ""
	result := easyjson.NewJSONObject()

	if payload.PathExists("in_link_type") {
		selfID := strings.Split(contextProcessor.Self.ID, "===")[0]
		if inLinkType, ok := payload.GetByPath("in_link_type").AsString(); ok && len(inLinkType) > 0 {
			if linkFromObjectUUID := contextProcessor.Caller.ID; len(linkFromObjectUUID) > 0 {
				contextProcessor.GlobalCache.DeleteValue(selfID+".in.oid_ltp-nil."+linkFromObjectUUID+"."+inLinkType, true, -1, queryID)
				result.SetByPath("status", easyjson.NewJSON("ok"))
			}
		} else {
			result.SetByPath("status", easyjson.NewJSON("failed"))
			errorString = fmt.Sprintf("ERROR LLAPILinkDelete %s: in_link_type:string must be a non empty string", selfID)
			fmt.Println(errorString)
		}
		result.SetByPath("result", easyjson.NewJSON(errorString))
		contextProcessor.RequestReplyData = &result
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
			if _, err := contextProcessor.GlobalCache.GetValue(contextProcessor.Self.ID + ".out.ltp_oid-bdy." + linkType + "." + descendantUUID); err != nil {
				// Link does not exist - nothing to delete
				result.SetByPath("status", easyjson.NewJSON("ok"))
				result.SetByPath("result", easyjson.NewJSON("Link does not exist"))
			} else {
				lbk := contextProcessor.Self.ID + ".out.ltp_oid-bdy." + linkType + "." + descendantUUID
				linkBody, _ := contextProcessor.GlobalCache.GetValueAsJSON(lbk)
				contextProcessor.GlobalCache.DeleteValue(lbk, true, -1, queryID)

				if linkBody != nil && linkBody.GetByPath("tags").IsNonEmptyArray() {
					if linkTags, ok := linkBody.GetByPath("tags").AsArrayString(); ok {
						for _, linkTag := range linkTags {
							contextProcessor.GlobalCache.DeleteValue(contextProcessor.Self.ID+".out.tag_ltp_oid-nil."+linkTag+"."+linkType+"."+descendantUUID, true, -1, queryID)
						}
					}
				}

				nextCallPayload := easyjson.NewJSONObject()
				nextCallPayload.SetByPath("query_id", easyjson.NewJSON(queryID))
				nextCallPayload.SetByPath("in_link_type", easyjson.NewJSON(linkType))
				if descendantUUID == contextProcessor.Self.ID {
					system.MsgOnErrorReturn(contextProcessor.Request(sfplugins.GolangLocalRequest, contextProcessor.Self.Typename, descendantUUID+"===delete_in_link", &nextCallPayload, nil))
				} else {
					system.MsgOnErrorReturn(contextProcessor.Request(sfplugins.GolangLocalRequest, contextProcessor.Self.Typename, descendantUUID, &nextCallPayload, nil))
				}
				result.SetByPath("status", easyjson.NewJSON("ok"))
				result.SetByPath("result", easyjson.NewJSON(errorString))
			}
		} else {
			result.SetByPath("status", easyjson.NewJSON("failed"))
			result.SetByPath("result", easyjson.NewJSON(errorString))
		}
		common.ReplyQueryID(queryID, &result, contextProcessor)
	}
	contextProcessor.GlobalCache.TransactionEnd(queryID)
}
