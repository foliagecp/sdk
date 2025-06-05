// Foliage graph store crud package.
// Provides stateful functions of low-level crud operations for the graph store
package crud

import (
	"fmt"
	"regexp"
	"sort"
	"strings"

	"github.com/foliagecp/easyjson"

	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
)

const (
	noLinkIdentifierMsg = "link identifier is not defined, or link does not exist"
)

var (
	validLinkName                    = regexp.MustCompile(`\A[a-zA-Z0-9\/_$#@%+=-]+\z`)
	graphIdKeyMutex *system.KeyMutex = system.NewKeyMutex()
)

func getVertexBody(ctx *sfPlugins.StatefunContextProcessor, keyValueID string) *easyjson.JSON {
	if j, err := ctx.Domain.Cache().GetValueAsJSON(keyValueID); err == nil {
		return j
	}
	j := easyjson.NewJSONObject()
	return &j
}

func injectParentHoldsLocks(ctx *sfPlugins.StatefunContextProcessor, downstreamPayload *easyjson.JSON) *easyjson.JSON {
	var newDownstreamPayload easyjson.JSON
	if downstreamPayload != nil && downstreamPayload.IsNonEmptyObject() {
		newDownstreamPayload = downstreamPayload.Clone()
	} else {
		newDownstreamPayload = easyjson.NewJSONObject()
	}

	parentHoldLocks := easyjson.NewJSONObject()
	setParentHoldLocks := false

	if ctx.Payload.PathExists("__key_locks") {
		parentHoldLocks.DeepMerge(ctx.Payload.GetByPath("__key_locks"))
		setParentHoldLocks = true
	}
	if ctx.Payload.PathExists("__parent_holds_locks") {
		parentHoldLocks.DeepMerge(ctx.Payload.GetByPath("__parent_holds_locks"))
		setParentHoldLocks = true
	}
	if setParentHoldLocks {
		newDownstreamPayload.SetByPath("__parent_holds_locks", parentHoldLocks)
	}

	newDownstreamPayload.RemoveByPath("__key_locks")
	return &newDownstreamPayload
}

func getOriginalID(ID string) string {
	return strings.Split(ID, "===")[0]
}

// All child operations must be sequence free
func makeSequenceFreeParentBasedID(ctx *sfPlugins.StatefunContextProcessor, targetID string, arbitrarySuffix ...string) string {
	finalId := targetID

	added := false

	tokens := strings.Split(ctx.Self.ID, "===")
	if len(tokens) > 1 {
		added = true
		finalId += "===" + tokens[1]
	} else {
		if ctx.Payload.PathExists(fmt.Sprintf("__key_locks.%s", targetID)) || ctx.Payload.PathExists(fmt.Sprintf("__parent_holds_locks.%s", targetID)) {
			added = true
			finalId += "===" + system.GetHashStr(ctx.Self.Typename+ctx.Self.ID)
		}
	}

	if len(arbitrarySuffix) > 0 {
		if added {
			finalId += arbitrarySuffix[0]
		} else {
			finalId += "===" + arbitrarySuffix[0]
		}
	}

	return finalId
}

func operationKeysMutexLock(ctx *sfPlugins.StatefunContextProcessor, keys []string, writeOperation bool) {
	//fmt.Printf("---- Graph Key Locking >>>> %s keys:[%s] %s\n", ctx.Self.Typename, strings.Join(keys, " "), ctx.Self.ID)
	//fmt.Printf("---- caller %s:%s\n", ctx.Caller.Typename, ctx.Caller.ID)
	keys = system.UniqueStrings(keys)
	sort.Strings(keys)
	for _, k := range keys {
		if writeOperation {
			if !ctx.Payload.PathExists(fmt.Sprintf("__parent_holds_locks.%s.w", k)) {
				//fmt.Printf("-- locking w key: %s\n", k)
				graphIdKeyMutex.Lock(k)
				ctx.Payload.SetByPath(fmt.Sprintf("__key_locks.%s.w", k), easyjson.NewJSON(true))
			}
		} else {
			if !ctx.Payload.PathExists(fmt.Sprintf("__parent_holds_locks.%s", k)) {
				//fmt.Printf("-- locking r key: %s\n", k)
				graphIdKeyMutex.RLock(k)
				ctx.Payload.SetByPath(fmt.Sprintf("__key_locks.%s.r", k), easyjson.NewJSON(true))
			}
		}
	}
	//fmt.Printf("---- Graph Key Locked All\n")
}

func operationKeysMutexUnlock(ctx *sfPlugins.StatefunContextProcessor) {
	if ctx.Payload.PathExists("__key_locks") {
		//fmt.Printf("---- Graph Key Unlocking <<<< %s %s\n", ctx.Self.Typename, ctx.Self.ID)
		for _, k := range ctx.Payload.GetByPath("__key_locks").ObjectKeys() {
			for _, t := range ctx.Payload.GetByPath(fmt.Sprintf("__key_locks.%s", k)).ObjectKeys() {
				switch t {
				case "w":
					//fmt.Printf("-- unlocking w key: %s\n", k)
					graphIdKeyMutex.Unlock(k)
				case "r":
					//fmt.Printf("-- unlocking r key: %s\n", k)
					graphIdKeyMutex.RUnlock(k)
				}
			}
		}
		ctx.Payload.RemoveByPath("__key_locks")
		//fmt.Printf("---- Graph Key Unlocked\n")
	}
}

/*
Creates a vertex in the graph with an id the function being called with.

Request:

	payload: json - optional
		// Initial request from caller:
		body: json - optional // Body for vertex to be created with.
			<key>: <type> - optional // Any additional key and value to be stored in objects's body.

	options: json - optional
		op_stack: bool - optional

Reply:

	payload: json
		status: string
		details: string
		data: json
			op_stack: json array - optional
*/
func LLAPIVertexCreate(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	selfID := getOriginalID(ctx.Self.ID)
	operationKeysMutexLock(ctx, []string{selfID}, true)
	defer operationKeysMutexUnlock(ctx)

	om := sfMediators.NewOpMediator(ctx)

	_, err := ctx.Domain.Cache().GetValue(selfID)
	if err == nil { // If vertex already exists
		om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("vertex with id=%s already exists", selfID))).Reply()
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

	ctx.Domain.Cache().SetValue(selfID, objectBody.ToBytes(), true, -1, "")

	indexVertexBody(ctx, objectBody, -1, false)
	addVertexOpToOpStack(opStack, ctx.Self.Typename, selfID, nil, &objectBody)

	om.AggregateOpMsg(sfMediators.OpMsgOk(resultWithOpStack(nil, opStack))).Reply()
}

/*
Updates a vertex in the graph with an id the function being called with. Merges or replaces the old vertice's body with the new one.

Request:

	payload: json - optional
		body: json - optional // Body for vertex to be created with.
			<key>: <type> - optional // Any additional key and value to be stored in vertex's body.
		upsert: bool // "false" - (default), "true" - will create vertex if does not exist
		replace: bool - optional // "false" - (default) body and tags will be merged, "true" - body and tags will be replaced

	options: json - optional
		op_stack: bool - optional

Reply:

	payload: json
		status: string
		details: string
		data: json
			op_stack: json array - optional
*/
func LLAPIVertexUpdate(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	selfID := getOriginalID(ctx.Self.ID)
	operationKeysMutexLock(ctx, []string{selfID}, true)
	defer operationKeysMutexUnlock(ctx)

	om := sfMediators.NewOpMediator(ctx)

	payload := ctx.Payload
	upsert := payload.GetByPath("upsert").AsBoolDefault(false)

	_, err := ctx.Domain.Cache().GetValue(selfID)
	if err != nil { // If vertex does not exist
		if upsert {
			om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.create", makeSequenceFreeParentBasedID(ctx, selfID), injectParentHoldsLocks(ctx, ctx.Payload), ctx.Options)))
			om.Reply()
		} else {
			om.AggregateOpMsg(sfMediators.OpMsgIdle(fmt.Sprintf("vertex with id=%s does not exist", selfID))).Reply()
		}
		return
	}

	opStack := getOpStackFromOptions(ctx.Options)

	oldBody := getVertexBody(ctx, selfID)

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
	ctx.Domain.Cache().SetValue(selfID, body.ToBytes(), true, -1, "")
	indexVertexBody(ctx, body, -1, true)

	addVertexOpToOpStack(opStack, ctx.Self.Typename, selfID, oldBody, &body)

	om.AggregateOpMsg(sfMediators.OpMsgOk(resultWithOpStack(nil, opStack))).Reply()
}

/*
Deletes a vartex with an id the function being called with from the graph and deletes all links related to it.

Request:

	options: json - optional
		op_stack: bool - optional

Reply:

	payload: json
		status: string
		details: string
		data: json
			op_stack: json array - optional
*/
func LLAPIVertexDelete(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	selfID := getOriginalID(ctx.Self.ID)
	om := sfMediators.NewOpMediator(ctx)

	_, err := ctx.Domain.Cache().GetValue(selfID)
	if err != nil { // If vertex does not exist
		om.AggregateOpMsg(sfMediators.OpMsgIdle(fmt.Sprintf("vertex with id=%s does not exist", selfID))).Reply()
		return
	}

	opStack := getOpStackFromOptions(ctx.Options)

	operationKeysMutexLock(ctx, []string{selfID}, true)
	defer operationKeysMutexUnlock(ctx)

	// Delete all out links -------------------------------
	outLinkKeys := ctx.Domain.Cache().GetKeysByPattern(fmt.Sprintf(OutLinkBodyKeyPrefPattern+KeySuff1Pattern, selfID, ">"))
	for _, outLinkKey := range outLinkKeys {
		inLinkKeyTokens := strings.Split(outLinkKey, ".")
		linkName := inLinkKeyTokens[len(inLinkKeyTokens)-1]

		deleteLinkPayload := easyjson.NewJSONObject()
		deleteLinkPayload.SetByPath("name", easyjson.NewJSON(linkName))
		//fmt.Println("             Deleting OUT link:", selfID, linkName)
		om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.delete", makeSequenceFreeParentBasedID(ctx, selfID), injectParentHoldsLocks(ctx, &deleteLinkPayload), ctx.Options)))
		mergeOpStack(opStack, om.GetLastSyncOp().Data.GetByPath("op_stack").GetPtr())
		if om.GetLastSyncOp().Status == sfMediators.SYNC_OP_STATUS_FAILED {
			system.MsgOnErrorReturn(om.ReplyWithData(resultWithOpStack(nil, opStack).GetPtr()))
			return
		}
	}
	// ----------------------------------------------------

	// Delete all in links --------------------------------
	inLinkKeys := ctx.Domain.Cache().GetKeysByPattern(fmt.Sprintf(InLinkKeyPrefPattern+KeySuff1Pattern, selfID, ">"))
	for _, inLinkKey := range inLinkKeys {
		inLinkKeyTokens := strings.Split(inLinkKey, ".")
		fromObjectID := inLinkKeyTokens[len(inLinkKeyTokens)-2]
		linkName := inLinkKeyTokens[len(inLinkKeyTokens)-1]

		deleteLinkPayload := easyjson.NewJSONObject()
		deleteLinkPayload.SetByPath("name", easyjson.NewJSON(linkName))
		//fmt.Println("             Deleting IN link:", fromObjectID, linkName)
		om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.delete", makeSequenceFreeParentBasedID(ctx, fromObjectID), injectParentHoldsLocks(ctx, &deleteLinkPayload), ctx.Options)))
		mergeOpStack(opStack, om.GetLastSyncOp().Data.GetByPath("op_stack").GetPtr())
		if om.GetLastSyncOp().Status == sfMediators.SYNC_OP_STATUS_FAILED {
			system.MsgOnErrorReturn(om.ReplyWithData(resultWithOpStack(nil, opStack).GetPtr()))
			return
		}
	}
	// ----------------------------------------------------

	var oldBody *easyjson.JSON = nil
	if opStack != nil {
		oldBody = getVertexBody(ctx, selfID)
	}

	ctx.Domain.Cache().DeleteValue(selfID, true, -1, "") // Delete vertex's body
	indexRemoveVertexBody(ctx)
	addVertexOpToOpStack(opStack, ctx.Self.Typename, selfID, oldBody, nil)

	om.AggregateOpMsg(sfMediators.OpMsgOk(resultWithOpStack(nil, opStack))).Reply()
}

/*
Reads and returns vertice's body.

Request:

	payload: json - optional
		details: bool - optional // "false" - (default) only body will be returned, "true" - body and links info will be returned

	options: json - optional
		op_stack: bool - optional

Reply:

	payload: json
		status: string
		details: string
		data: json
			body: json // Vertice's body
			links: json - optional // Vertice's links
				out: json
					names: json string array
					types: json string array
					ids: json string array
				in: json string array
					{from: string, name: string}, // from vertex id; link name
					...
			op_stack: json array - optional
*/
func LLAPIVertexRead(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	selfID := getOriginalID(ctx.Self.ID)
	operationKeysMutexLock(ctx, []string{selfID}, false)
	defer operationKeysMutexUnlock(ctx)

	om := sfMediators.NewOpMediator(ctx)

	_, err := ctx.Domain.Cache().GetValue(selfID)
	if err != nil { // If vertex does not exist
		om.AggregateOpMsg(sfMediators.OpMsgIdle(fmt.Sprintf("vertex with id=%s does not exist", selfID))).Reply()
		return
	}

	opStack := getOpStackFromOptions(ctx.Options)

	j := getVertexBody(ctx, selfID)
	result := easyjson.NewJSONObjectWithKeyValue("body", *j)

	if ctx.Payload.GetByPath("details").AsBoolDefault(false) {
		outLinkNames := []string{}
		outLinkTypes := []string{}
		outLinkIds := []string{}
		outLinkKeys := ctx.Domain.Cache().GetKeysByPattern(fmt.Sprintf(OutLinkTargetKeyPrefPattern+KeySuff1Pattern, selfID, ">"))
		for _, outLinkKey := range outLinkKeys {
			linkKeyTokens := strings.Split(outLinkKey, ".")
			linkName := linkKeyTokens[len(linkKeyTokens)-1]
			outLinkNames = append(outLinkNames, linkName)

			linkTargetBytes, err := ctx.Domain.Cache().GetValue(fmt.Sprintf(OutLinkTargetKeyPrefPattern+KeySuff1Pattern, selfID, linkName))
			brokenTarget := true
			if err == nil {
				tokens := strings.Split(string(linkTargetBytes), ".")
				if len(tokens) == 2 {
					brokenTarget = false
					outLinkTypes = append(outLinkTypes, tokens[0])
					outLinkIds = append(outLinkIds, tokens[1])
				}
			}
			if brokenTarget {
				outLinkTypes = append(outLinkTypes, "")
				outLinkIds = append(outLinkIds, "")
			}
		}
		result.SetByPath("links.out.names", easyjson.JSONFromArray(outLinkNames))
		result.SetByPath("links.out.types", easyjson.JSONFromArray(outLinkTypes))
		result.SetByPath("links.out.ids", easyjson.JSONFromArray(outLinkIds))

		inLinkKeys := ctx.Domain.Cache().GetKeysByPattern(fmt.Sprintf(InLinkKeyPrefPattern+KeySuff1Pattern, selfID, ">"))
		inLinks := easyjson.NewJSONArray()
		for _, inLinkKey := range inLinkKeys {
			linkKeyTokens := strings.Split(inLinkKey, ".")
			linkName := linkKeyTokens[len(linkKeyTokens)-1]
			linkFromVId := linkKeyTokens[len(linkKeyTokens)-2]
			inLinkJson := easyjson.NewJSONObjectWithKeyValue("from", easyjson.NewJSON(linkFromVId))
			inLinkJson.SetByPath("name", easyjson.NewJSON(linkName))
			inLinks.AddToArray(inLinkJson)
		}
		result.SetByPath("links.in", inLinks)
	}

	addVertexOpToOpStack(opStack, ctx.Self.Typename, selfID, nil, nil)

	om.AggregateOpMsg(sfMediators.OpMsgOk(resultWithOpStack(result.GetPtr(), opStack))).Reply()
}

/*
Creates a link.

Request:

	payload: json - required
		// Initial request from caller:
		force: bool - optional // Creates even if already exists
		to: string - required // ID for descendant vertex.
		name: string - required // Defines link's name which is unique among all vertex's output links.
		type: string - required // Type of link leading to descendant.
		tags: []string - optional // Defines link tags.
		body: json - optional // Body for link leading to descendant.
			<key>: <type> - optional // Any additional key and value to be stored in link's body.

		// Self-requests to descendants (RequestReply): // ID can be composite: <object_id>===self_link - for non-blocking execution on the same vertex
			in_name: string - required // Creating input link's name

	options: json - optional
		op_stack: bool - optional

Reply:

	payload: json
		status: string
		details: string
		data: json
			op_stack: json array - optional
*/
func LLAPILinkCreate(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	selfID := getOriginalID(ctx.Self.ID)
	om := sfMediators.NewOpMediator(ctx)

	forceCreate := ctx.Payload.GetByPath("force").AsBoolDefault(false)

	_, err := ctx.Domain.Cache().GetValue(selfID)
	if err != nil { // If vertex does not exist
		om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("vertex with id=%s does not exist", selfID))).Reply()
		return
	}

	payload := ctx.Payload
	opStack := getOpStackFromOptions(ctx.Options)

	if payload.PathExists("in_name") {
		if inLinkName, ok := payload.GetByPath("in_name").AsString(); ok && len(inLinkName) > 0 {
			if linkFromObjectUUID := getOriginalID(ctx.Caller.ID); len(linkFromObjectUUID) > 0 {
				ctx.Domain.Cache().SetValue(fmt.Sprintf(InLinkKeyPrefPattern+KeySuff2Pattern, selfID, linkFromObjectUUID, inLinkName), nil, true, -1, "")
				//fmt.Println("create vertex in link: ", selfID, linkFromObjectUUID)
				om.AggregateOpMsg(sfMediators.OpMsgOk(easyjson.NewJSONNull())).Reply()
				return
			} else {
				om.AggregateOpMsg(sfMediators.OpMsgFailed("caller id is not defined, no source vertex id")).Reply()
				return
			}
		} else {
			om.AggregateOpMsg(sfMediators.OpMsgFailed("in_name is not defined")).Reply()
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
			om.AggregateOpMsg(sfMediators.OpMsgFailed("to is not defined")).Reply()
			return
		}
		toId = ctx.Domain.CreateObjectIDWithThisDomain(toId, false)

		var linkName string
		if s, ok := payload.GetByPath("name").AsString(); ok {
			linkName = s
			if !validLinkName.MatchString(linkName) {
				om.AggregateOpMsg(sfMediators.OpMsgFailed("invalid link name")).Reply()
				return
			}
		} else {
			om.AggregateOpMsg(sfMediators.OpMsgFailed("name is not defined")).Reply()
			return
		}

		var linkType string
		if s, ok := payload.GetByPath("type").AsString(); ok {
			linkType = s
		} else {
			om.AggregateOpMsg(sfMediators.OpMsgFailed("type is not defined")).Reply()
			return
		}

		operationKeysMutexLock(ctx, []string{selfID, toId}, true)
		defer operationKeysMutexUnlock(ctx)

		if !forceCreate {
			// Check if link with this name already exists --------------
			_, err := ctx.Domain.Cache().GetValue(fmt.Sprintf(OutLinkBodyKeyPrefPattern+KeySuff1Pattern, selfID, linkName))
			if err == nil {
				om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("link from=%s with name=%s already exists", selfID, linkName))).Reply()
				return
			}
			// ----------------------------------------------------------
			// Check if link with this type "type" to "to" already exists
			_, err = ctx.Domain.Cache().GetValue(fmt.Sprintf(OutLinkTypeKeyPrefPattern+KeySuff2Pattern, selfID, linkType, toId))
			if err == nil {
				om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("link from=%s with name=%s to=%s with type=%s already exists, two vertices can have a link with this type and direction only once", selfID, linkName, toId, linkType))).Reply()
				return
			}
			// -----------------------------------------------------------
		}

		// Create out link on this vertex -------------------------
		// Set link target ------------------
		ctx.Domain.Cache().SetValue(fmt.Sprintf(OutLinkTargetKeyPrefPattern+KeySuff1Pattern, selfID, linkName), []byte(fmt.Sprintf("%s.%s", linkType, toId)), true, -1, "") // Store link body in KV
		// ----------------------------------
		// Set link body --------------------
		ctx.Domain.Cache().SetValue(fmt.Sprintf(OutLinkBodyKeyPrefPattern+KeySuff1Pattern, selfID, linkName), linkBody.ToBytes(), true, -1, "") // Store link body in KV
		// ----------------------------------
		// Set link type --------------------
		ctx.Domain.Cache().SetValue(fmt.Sprintf(OutLinkTypeKeyPrefPattern+KeySuff2Pattern, selfID, linkType, toId), []byte(linkName), true, -1, "") // Store link type
		// ----------------------------------
		// Index link type ------------------
		ctx.Domain.Cache().SetValue(fmt.Sprintf(OutLinkIndexPrefPattern+KeySuff3Pattern, selfID, linkName, "type", linkType), nil, true, -1, "")
		// ----------------------------------
		// Index link tags ------------------
		if payload.GetByPath("tags").IsNonEmptyArray() {
			if linkTags, ok := payload.GetByPath("tags").AsArrayString(); ok {
				for _, linkTag := range linkTags {
					ctx.Domain.Cache().SetValue(fmt.Sprintf(OutLinkIndexPrefPattern+KeySuff3Pattern, selfID, linkName, "tag", linkTag), nil, true, -1, "")
				}
			}
		}
		//fmt.Println("create vertex out link: ", selfID, toId)
		// ----------------------------------
		indexVertexLinkBody(ctx, linkName, linkBody, -1, false)
		// --------------------------------------------------------

		addLinkOpToOpStack(opStack, ctx.Self.Typename, selfID, toId, linkName, linkType, nil, &linkBody)

		// Create in link on descendant vertex --------------------
		nextCallPayload := easyjson.NewJSONObject()
		nextCallPayload.SetByPath("in_name", easyjson.NewJSON(linkName))

		om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, ctx.Self.Typename, makeSequenceFreeParentBasedID(ctx, toId, "inlink"), injectParentHoldsLocks(ctx, &nextCallPayload), ctx.Options)))
		if om.GetLastSyncOp().Status == sfMediators.SYNC_OP_STATUS_FAILED {
			system.MsgOnErrorReturn(om.ReplyWithData(resultWithOpStack(nil, opStack).GetPtr()))
			return
		}
		// --------------------------------------------------------

		om.AggregateOpMsg(sfMediators.OpMsgOk(resultWithOpStack(nil, opStack))).Reply()
	}
}

/*
Updates a link.

Request:

	payload: json - required
		name: string - required if "to" or "type" is not defined. required if "upsert" is set to "true" // Defines link's name which is unique among all vertex's output links.

		to: string - required if "name" is not defined. required if "upsert" is set to "true" // ID for descendant vertex.
		type: string - required if "name" is not defined. required if "upsert" is set to "true" // Type of link leading to descendant.

		tags: []string - optional // Defines link tags.
		upsert: bool // "false" - (default), "true" - will create link if does not exist
		replace: bool - optional // "false" - (default) body and tags will be merged, "true" - body and tags will be replaced
		body: json - optional // Body for link leading to descendant.
			<key>: <type> - optional // Any additional key and value to be stored in link's body.

	options: json - optional
		op_stack: bool - optional

Reply:

	payload: json
		status: string
		details: string
		data: json
			op_stack: json array - optional
*/
func LLAPILinkUpdate(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	selfID := getOriginalID(ctx.Self.ID)
	om := sfMediators.NewOpMediator(ctx)

	payload := ctx.Payload

	opStack := getOpStackFromOptions(ctx.Options)

	var linkName string
	if s, ok := getLinkNameFromSpecifiedIdentifier(ctx); ok {
		linkName = s
	} else {
		om.AggregateOpMsg(sfMediators.OpMsgFailed(noLinkIdentifierMsg)).Reply()
		return
	}
	if !validLinkName.MatchString(linkName) {
		om.AggregateOpMsg(sfMediators.OpMsgFailed("invalid link name")).Reply()
		return
	}

	upsert := payload.GetByPath("upsert").AsBoolDefault(false)

	/*
		// Check if link with this name already exists --------------
			_, err := ctx.Domain.Cache().GetValue(fmt.Sprintf(OutLinkBodyKeyPrefPattern+KeySuff1Pattern, selfID, linkName))
			if err == nil {
				om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("link from=%s with name=%s already exists", selfID, linkName))).Reply()
				return
			}
			// ----------------------------------------------------------
	*/

	linkTargetBytes, err := ctx.Domain.Cache().GetValue(fmt.Sprintf(OutLinkTargetKeyPrefPattern+KeySuff1Pattern, selfID, linkName))
	if err != nil { // Link does not exist
		if upsert {
			p := payload.Clone()
			p.SetByPath("force", easyjson.NewJSON(true))
			om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.create", makeSequenceFreeParentBasedID(ctx, selfID), injectParentHoldsLocks(ctx, &p), ctx.Options)))
			om.Reply()
		} else {
			om.AggregateOpMsg(sfMediators.OpMsgIdle(fmt.Sprintf("link from=%s with name=%s does not exist", selfID, linkName))).Reply()
		}
		return
	}
	oldLinkBody, err := ctx.Domain.Cache().GetValueAsJSON(fmt.Sprintf(OutLinkBodyKeyPrefPattern+KeySuff1Pattern, selfID, linkName))
	if err != nil {
		om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("link from=%s with name=%s", selfID, linkName))).Reply()
		return
	}

	linkTargetStr := string(linkTargetBytes)
	linkTargetTokens := strings.Split(linkTargetStr, ".")
	if len(linkTargetTokens) != 2 || len(linkTargetTokens[0]) == 0 || len(linkTargetTokens[1]) == 0 {
		om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("link from=%s with name=%s, has invalid target: %s", selfID, linkName, linkTargetStr))).Reply()
	}
	linkType := linkTargetTokens[0]
	toId := linkTargetTokens[1]

	operationKeysMutexLock(ctx, []string{selfID, toId}, true)
	defer operationKeysMutexUnlock(ctx)

	var replace bool = payload.GetByPath("replace").AsBoolDefault(false)

	var linkBody easyjson.JSON
	if payload.GetByPath("body").IsObject() {
		linkBody = payload.GetByPath("body")
	} else {
		linkBody = easyjson.NewJSONObject()
	}

	if replace {
		// Remove all indices -----------------------------
		indexKeys := ctx.Domain.Cache().GetKeysByPattern(fmt.Sprintf(OutLinkIndexPrefPattern+KeySuff2Pattern, selfID, linkName, ">"))
		for _, indexKey := range indexKeys {
			ctx.Domain.Cache().DeleteValue(indexKey, true, -1, "")
		}
		// ------------------------------------------------
	} else { // merge
		newBody := oldLinkBody.Clone().GetPtr()
		newBody.DeepMerge(linkBody)
		linkBody = *newBody
	}

	// Create out link on this vertex -------------------------
	// Set link body --------------------
	ctx.Domain.Cache().SetValue(fmt.Sprintf(OutLinkBodyKeyPrefPattern+KeySuff1Pattern, selfID, linkName), linkBody.ToBytes(), true, -1, "") // Store link body in KV
	// ----------------------------------
	// Index link type ------------------
	ctx.Domain.Cache().SetValue(fmt.Sprintf(OutLinkIndexPrefPattern+KeySuff3Pattern, selfID, linkName, "type", linkType), nil, true, -1, "")
	// ----------------------------------
	// Index link tags ------------------
	if payload.GetByPath("tags").IsNonEmptyArray() {
		if linkTags, ok := payload.GetByPath("tags").AsArrayString(); ok {
			for _, linkTag := range linkTags {
				ctx.Domain.Cache().SetValue(fmt.Sprintf(OutLinkIndexPrefPattern+KeySuff3Pattern, selfID, linkName, "tag", linkTag), nil, true, -1, "")
			}
		}
	}
	// ----------------------------------
	indexVertexLinkBody(ctx, linkName, linkBody, -1, true)
	// --------------------------------------------------------

	addLinkOpToOpStack(opStack, ctx.Self.Typename, selfID, toId, linkName, linkType, oldLinkBody, &linkBody)

	om.AggregateOpMsg(sfMediators.OpMsgOk(resultWithOpStack(nil, opStack))).Reply()
}

/*
Delete a link.

Request:

	payload: json - required
		// Initial request from caller:
		name: string - required // Defines link's name which is unique among all vertex's output links.

		to: string - required if "name" is not defined // ID for descendant vertex.
		type: string - required if "name" is not defined // Type of link leading to descendant.

		// Self-requests to descendants (RequestReply): // ID can be composite: <object_id>===self_link - for non-blocking execution on the same vertex
		in_name: string - required // Deleting input link's name

	options: json - optional
		op_stack: bool - optional

Reply:

	payload: json
		status: string
		details: string
		data: json
			op_stack: json array - optional
*/
func LLAPILinkDelete(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	selfID := getOriginalID(ctx.Self.ID)
	om := sfMediators.NewOpMediator(ctx)

	payload := ctx.Payload

	opStack := getOpStackFromOptions(ctx.Options)

	if payload.PathExists("in_name") {
		if inLinkName, ok := payload.GetByPath("in_name").AsString(); ok && len(inLinkName) > 0 {
			if linkFromObjectUUID := getOriginalID(ctx.Caller.ID); len(linkFromObjectUUID) > 0 {
				//fmt.Println("delete vertex in link: ", selfID, linkFromObjectUUID)
				ctx.Domain.Cache().DeleteValue(fmt.Sprintf(InLinkKeyPrefPattern+KeySuff2Pattern, selfID, linkFromObjectUUID, inLinkName), true, -1, "")
				om.AggregateOpMsg(sfMediators.OpMsgOk(easyjson.NewJSONNull())).Reply()
				return
			} else {
				om.AggregateOpMsg(sfMediators.OpMsgFailed("caller id is not defined, no source vertex id")).Reply()
				return
			}
		} else {
			om.AggregateOpMsg(sfMediators.OpMsgFailed("in_name is not defined")).Reply()
			return
		}
	} else {
		var linkName string
		if s, ok := getLinkNameFromSpecifiedIdentifier(ctx); ok {
			linkName = s
		} else {
			om.AggregateOpMsg(sfMediators.OpMsgFailed(noLinkIdentifierMsg)).Reply()
			return
		}
		if !validLinkName.MatchString(linkName) {
			om.AggregateOpMsg(sfMediators.OpMsgFailed("invalid link name")).Reply()
			return
		}

		linkTargetBytes, err := ctx.Domain.Cache().GetValue(fmt.Sprintf(OutLinkTargetKeyPrefPattern+KeySuff1Pattern, selfID, linkName))
		if err != nil {
			om.AggregateOpMsg(sfMediators.OpMsgIdle(fmt.Sprintf("link from=%s with name=%s does not exist", selfID, linkName))).Reply()
			return
		}
		oldLinkBody, err := ctx.Domain.Cache().GetValueAsJSON(fmt.Sprintf(OutLinkBodyKeyPrefPattern+KeySuff1Pattern, selfID, linkName))
		if err != nil {
			om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("link body from=%s with name=%s does not exist", selfID, linkName))).Reply()
			return
		}

		linkTargetStr := string(linkTargetBytes)
		linkTargetTokens := strings.Split(linkTargetStr, ".")
		if len(linkTargetTokens) != 2 || len(linkTargetTokens[0]) == 0 || len(linkTargetTokens[1]) == 0 {
			om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("link from=%s with name=%s, has invalid target: %s", selfID, linkName, linkTargetStr))).Reply()
		}
		linkType := linkTargetTokens[0]
		toId := linkTargetTokens[1]

		operationKeysMutexLock(ctx, []string{selfID, toId}, true)
		defer operationKeysMutexUnlock(ctx)

		// Remove all indices -----------------------------
		indexKeys := ctx.Domain.Cache().GetKeysByPattern(fmt.Sprintf(OutLinkIndexPrefPattern+KeySuff2Pattern, selfID, linkName, ">"))
		for _, indexKey := range indexKeys {
			ctx.Domain.Cache().DeleteValue(indexKey, true, -1, "")
		}
		// ------------------------------------------------
		indexRemoveVertexLinkBody(ctx, linkName)

		// Set link type --------------------
		ctx.Domain.Cache().DeleteValue(fmt.Sprintf(OutLinkTypeKeyPrefPattern+KeySuff2Pattern, selfID, linkType, toId), true, -1, "")
		// ----------------------------------
		// Delete link body -----------------
		ctx.Domain.Cache().DeleteValue(fmt.Sprintf(OutLinkBodyKeyPrefPattern+KeySuff1Pattern, selfID, linkName), true, -1, "")
		// ----------------------------------
		// Delete link target ---------------
		ctx.Domain.Cache().DeleteValue(fmt.Sprintf(OutLinkTargetKeyPrefPattern+KeySuff1Pattern, selfID, linkName), true, -1, "")
		// ----------------------------------

		//fmt.Println("delete vertex out link: ", selfID, toId)

		addLinkOpToOpStack(opStack, ctx.Self.Typename, selfID, toId, linkName, linkType, oldLinkBody, nil)

		// Delete in link on descendant vertex --------------------
		nextCallPayload := easyjson.NewJSONObject()
		nextCallPayload.SetByPath("in_name", easyjson.NewJSON(linkName))

		om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, ctx.Self.Typename, makeSequenceFreeParentBasedID(ctx, toId, "inlink"), injectParentHoldsLocks(ctx, &nextCallPayload), ctx.Options)))
		if om.GetLastSyncOp().Status == sfMediators.SYNC_OP_STATUS_FAILED {
			system.MsgOnErrorReturn(om.ReplyWithData(resultWithOpStack(nil, opStack).GetPtr()))
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
		name: string - required // Defines link's name which is unique among all vertex's output links.

		to: string - required if "name" is not defined // ID for descendant vertex.
		type: string - required if "name" is not defined // Type of link leading to descendant.

		details: bool - optional // "false" - (default) only body will be returned, "true" - body and info will be returned

	options: json - optional
		op_stack: bool - optional

Reply:

	payload: json
		status: string
		details: string
		data: json
			body: json // link's body
			name: string - optional // link's name
			type: string - optional // link's type
			tags: string array - optional // link's tags
			from: string - optional // link goes out from vertex id
			to: string - optional // link goes into vertex id
			op_stack: json array - optional
*/
func LLAPILinkRead(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	selfID := getOriginalID(ctx.Self.ID)
	om := sfMediators.NewOpMediator(ctx)

	opStack := getOpStackFromOptions(ctx.Options)

	var linkName string
	if s, ok := getLinkNameFromSpecifiedIdentifier(ctx); ok {
		linkName = s
	} else {
		om.AggregateOpMsg(sfMediators.OpMsgFailed(noLinkIdentifierMsg)).Reply()
		return
	}
	if !validLinkName.MatchString(linkName) {
		om.AggregateOpMsg(sfMediators.OpMsgFailed("invalid link name")).Reply()
		return
	}

	linkTargetBytes, err := ctx.Domain.Cache().GetValue(fmt.Sprintf(OutLinkTargetKeyPrefPattern+KeySuff1Pattern, selfID, linkName))
	if err != nil {
		om.AggregateOpMsg(sfMediators.OpMsgIdle(fmt.Sprintf("link from=%s with name=%s does not exist", selfID, linkName))).Reply()
		return
	}
	linkBody, err := ctx.Domain.Cache().GetValueAsJSON(fmt.Sprintf(OutLinkBodyKeyPrefPattern+KeySuff1Pattern, selfID, linkName))
	if err != nil {
		om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("link body from=%s with name=%s does not exist", selfID, linkName))).Reply()
		return
	}

	result := easyjson.NewJSONObjectWithKeyValue("body", *linkBody)

	linkTargetStr := string(linkTargetBytes)
	linkTargetTokens := strings.Split(linkTargetStr, ".")
	if len(linkTargetTokens) != 2 || len(linkTargetTokens[0]) == 0 || len(linkTargetTokens[1]) == 0 {
		om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("link from=%s with name=%s, has invalid target: %s", selfID, linkName, linkTargetStr))).Reply()
	}
	linkType := linkTargetTokens[0]
	toId := linkTargetTokens[1]

	operationKeysMutexLock(ctx, []string{selfID, toId}, false)
	defer operationKeysMutexUnlock(ctx)

	if ctx.Payload.GetByPath("details").AsBoolDefault(false) {
		result.SetByPath("name", easyjson.NewJSON(linkName))
		result.SetByPath("type", easyjson.NewJSON(linkType))
		result.SetByPath("from", easyjson.NewJSON(selfID))
		result.SetByPath("to", easyjson.NewJSON(toId))

		tags := []string{}
		tagKeys := ctx.Domain.Cache().GetKeysByPattern(fmt.Sprintf(OutLinkIndexPrefPattern+KeySuff3Pattern, selfID, linkName, "tag", ">"))
		for _, tagKey := range tagKeys {
			tagKeyTokens := strings.Split(tagKey, ".")
			tags = append(tags, tagKeyTokens[len(tagKeyTokens)-1])
		}
		result.SetByPath("tags", easyjson.JSONFromArray(tags))
	}

	addLinkOpToOpStack(opStack, ctx.Self.Typename, selfID, toId, linkName, linkType, nil, nil)

	om.AggregateOpMsg(sfMediators.OpMsgOk(resultWithOpStack(result.GetPtr(), opStack))).Reply()
}
