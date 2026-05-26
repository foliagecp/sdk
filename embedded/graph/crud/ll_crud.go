// Foliage graph store crud package.
// Provides stateful functions of low-level crud operations for the graph store
package crud

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/foliagecp/easyjson"

	lg "github.com/foliagecp/sdk/statefun/logger"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
)

/*const (
	noLinkIdentifierMsg = "link identifier is not defined, or link does not exist"
)*/

var (
	validLinkName                    = regexp.MustCompile(`\A[a-zA-Z0-9\/_$#@%+=-]+\z`)
	graphIdKeyMutex *system.KeyMutex = system.NewKeyMutex()

	// graphKeyLockTimeout bounds how long operationKeysMutexLock blocks trying
	// to acquire a per-key graph lock. The lock is normally held only for the
	// duration of one operation (incl. its sub-requests, each capped by
	// requestTimeoutSec), so a wait longer than this indicates a genuinely
	// stuck/deadlocked holder. On timeout the operation proceeds WITHOUT that
	// key's lock (logged) rather than hanging the worker forever — recovery and
	// liveness over strict serialization in the pathological case. Tunable via
	// GRAPH_KEY_LOCK_TIMEOUT_SEC.
	graphKeyLockTimeout = time.Duration(system.GetEnvMustProceed[int]("GRAPH_KEY_LOCK_TIMEOUT_SEC", 300)) * time.Second
)

func getVertexBody(ctx *sfPlugins.StatefunContextProcessor, keyValueID string) *easyjson.JSON {
	if j, err := ctx.Domain.Cache().GetValueJSON(keyValueID); err == nil {
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

func operationKeysMutexLock(ctx *sfPlugins.StatefunContextProcessor, keys []string, writeOperation bool, opTime int64) {
	keys = system.UniqueStrings(keys)
	sort.Strings(keys)

	lockedWriteAny := false
	for _, k := range keys {
		if writeOperation {
			if !ctx.Payload.PathExists(fmt.Sprintf("__parent_holds_locks.%s.w", k)) {
				// Bounded acquire: never hang a worker forever on a stuck
				// holder. On timeout we record nothing (so Unlock won't touch a
				// lock we don't hold) and proceed — the holder is presumed
				// deadlocked/frozen, so it is not actively mutating this key.
				if graphIdKeyMutex.LockTimeout(k, graphKeyLockTimeout) {
					ctx.Payload.SetByPath(fmt.Sprintf("__key_locks.%s.w", k), easyjson.NewJSON(true))
					lockedWriteAny = true
				} else {
					lg.Logf(lg.WarnLevel, "operationKeysMutexLock: write-lock acquire for key=%s timed out after %s; proceeding without it", k, graphKeyLockTimeout)
				}
			}
		} else {
			if !ctx.Payload.PathExists(fmt.Sprintf("__parent_holds_locks.%s", k)) {
				if graphIdKeyMutex.RLockTimeout(k, graphKeyLockTimeout) {
					ctx.Payload.SetByPath(fmt.Sprintf("__key_locks.%s.r", k), easyjson.NewJSON(true))
				} else {
					lg.Logf(lg.WarnLevel, "operationKeysMutexLock: read-lock acquire for key=%s timed out after %s; proceeding without it", k, graphKeyLockTimeout)
				}
			}
		}
	}
	if lockedWriteAny {
		ctx.Payload.SetByPath("__key_lock_time", easyjson.NewJSON(opTime))
		ctx.Domain.Cache().MarkOperationActive(opTime)
	}
}

func operationKeysMutexUnlock(ctx *sfPlugins.StatefunContextProcessor) {
	unlockedWriteAny := false
	if ctx.Payload.PathExists("__key_locks") {
		for _, k := range ctx.Payload.GetByPath("__key_locks").ObjectKeys() {
			for _, t := range ctx.Payload.GetByPath(fmt.Sprintf("__key_locks.%s", k)).ObjectKeys() {
				switch t {
				case "w":
					graphIdKeyMutex.Unlock(k)
					unlockedWriteAny = true
				case "r":
					graphIdKeyMutex.RUnlock(k)
				}
			}
		}
		ctx.Payload.RemoveByPath("__key_locks")
	}
	if opTime, ok := ctx.Payload.GetByPath("__key_lock_time").AsInt64(); unlockedWriteAny && ok {
		ctx.Domain.Cache().MarkOperationDone(opTime)
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
	om := sfMediators.NewOpMediator(ctx)

	opTime := getOpTimeFromPayloadIfExist(ctx.Payload)
	operationKeysMutexLock(ctx, []string{selfID}, true, opTime)
	if ctx.Domain.Cache().ExistsJson(selfID) { // If vertex already exists
		operationKeysMutexUnlock(ctx)
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

	ctx.Domain.Cache().SetValueJSON(selfID, &objectBody, true, opTime)

	operationKeysMutexUnlock(ctx)

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

	opTime := getOpTimeFromPayloadIfExist(ctx.Payload)
	ctx.Payload.SetByPath("op_time", easyjson.NewJSON(opTime))

	om := sfMediators.NewOpMediator(ctx)

	payload := ctx.Payload
	upsert := payload.GetByPath("upsert").AsBoolDefault(false)

	operationKeysMutexLock(ctx, []string{selfID}, true, opTime)
	if !ctx.Domain.Cache().ExistsJson(selfID) { // If vertex does not exist
		operationKeysMutexUnlock(ctx)
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

	// No-op short-circuit: if the resulting body is structurally identical
	// to the current one, nothing has to be written to the KV. This covers
	// both replace=true (caller pushed the same body) and replace=false
	// (merge produced an equal tree — common when external SDK consumers
	// re-send the same state). Skipping the write avoids polluting the
	// WAL, suppresses dirty publish + trigger fan-out, and keeps connected
	// graphs quiet under idempotent upsert pressure.
	//
	// Equality uses easyjson.JSON.Equals → reflect.DeepEqual on the parsed
	// tree, so object-key ordering does not matter. Both sides came from
	// json.Unmarshal (float64 numbers), so type-coercion mismatches are not
	// expected on the steady-state update path.
	if oldBody != nil && body.Equals(*oldBody) {
		operationKeysMutexUnlock(ctx)
		om.AggregateOpMsg(sfMediators.OpMsgIdle(fmt.Sprintf("vertex with id=%s body unchanged", selfID))).Reply()
		return
	}

	ctx.Domain.Cache().SetValueJSON(selfID, &body, true, opTime)

	operationKeysMutexUnlock(ctx)

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

	opTime := getOpTimeFromPayloadIfExist(ctx.Payload)

	om := sfMediators.NewOpMediator(ctx)

	if !ctx.Domain.Cache().ExistsJson(selfID) { // If vertex does not exist
		om.AggregateOpMsg(sfMediators.OpMsgIdle(fmt.Sprintf("vertex with id=%s does not exist", selfID))).Reply()
		return
	}

	opStack := getOpStackFromOptions(ctx.Options)

	// Delete all out links -------------------------------
	outLinkKeys := ctx.Domain.Cache().GetKeysByPattern(fmt.Sprintf(OutLinkBodyKeyPrefPattern+KeySuff1Pattern, selfID, ">"))
	for _, outLinkKey := range outLinkKeys {
		inLinkKeyTokens := strings.Split(outLinkKey, ".")
		linkName := inLinkKeyTokens[len(inLinkKeyTokens)-1]

		deleteLinkPayload := easyjson.NewJSONObject()
		deleteLinkPayload.SetByPath("name", easyjson.NewJSON(linkName))
		deleteLinkPayload.SetByPath("op_time", easyjson.NewJSON(opTime))
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
		deleteLinkPayload.SetByPath("op_time", easyjson.NewJSON(opTime))
		//fmt.Println("             Deleting IN link:", fromObjectID, linkName)
		om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.delete", makeSequenceFreeParentBasedID(ctx, fromObjectID), injectParentHoldsLocks(ctx, &deleteLinkPayload), ctx.Options)))
		mergeOpStack(opStack, om.GetLastSyncOp().Data.GetByPath("op_stack").GetPtr())
		if om.GetLastSyncOp().Status == sfMediators.SYNC_OP_STATUS_FAILED {
			system.MsgOnErrorReturn(om.ReplyWithData(resultWithOpStack(nil, opStack).GetPtr()))
			return
		}
	}
	// ----------------------------------------------------

	operationKeysMutexLock(ctx, []string{selfID}, true, opTime)

	var oldBody *easyjson.JSON = nil
	if opStack != nil {
		oldBody = getVertexBody(ctx, selfID)
	}

	ctx.Domain.Cache().DeleteValue(selfID, true, opTime) // Delete vertex's body

	operationKeysMutexUnlock(ctx)

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
					{from: string, name: string, type: string}, // from vertex id; link name; link type
					...
			op_stack: json array - optional
*/
func LLAPIVertexRead(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	detailsV2 := ctx.Payload.GetByPath("details_v2").AsBoolDefault(false)
	details := detailsV2 || ctx.Payload.GetByPath("details").AsBoolDefault(false)

	selfID := getOriginalID(ctx.Self.ID)

	om := sfMediators.NewOpMediator(ctx)

	opTime := getOpTimeFromPayloadIfExist(ctx.Payload)
	if details {
		operationKeysMutexLock(ctx, []string{selfID}, false, opTime)
	}
	j, err := ctx.Domain.Cache().GetValueJSON(selfID)
	if err != nil { // If vertex does not exist
		if details {
			operationKeysMutexUnlock(ctx)
		}
		om.AggregateOpMsg(sfMediators.OpMsgIdle(fmt.Sprintf("vertex with id=%s does not exist", selfID))).Reply()
		return
	}

	opStack := getOpStackFromOptions(ctx.Options)

	result := easyjson.NewJSONObjectWithKeyValue("body", *j)

	if details {
		outLinkKeys := ctx.Domain.Cache().GetKeysByPattern(fmt.Sprintf(OutLinkTargetKeyPrefPattern+KeySuff1Pattern, selfID, ">"))

		if detailsV2 {
			// Structured format: links.out as array of {to, name, type} objects
			outLinks := easyjson.NewJSONArray()
			for _, outLinkKey := range outLinkKeys {
				linkKeyTokens := strings.Split(outLinkKey, ".")
				linkName := linkKeyTokens[len(linkKeyTokens)-1]

				toId := ""
				linkType := ""
				linkTargetBytes, err := ctx.Domain.Cache().GetValue(fmt.Sprintf(OutLinkTargetKeyPrefPattern+KeySuff1Pattern, selfID, linkName))
				if err == nil {
					tokens := strings.Split(string(linkTargetBytes), ".")
					if len(tokens) == 2 {
						linkType = tokens[0]
						toId = tokens[1]
					}
				}

				outLinkJson := easyjson.NewJSONObjectWithKeyValue("to", easyjson.NewJSON(toId))
				outLinkJson.SetByPath("name", easyjson.NewJSON(linkName))
				outLinkJson.SetByPath("type", easyjson.NewJSON(linkType))
				outLinks.AddToArray(outLinkJson)
			}
			result.SetByPath("links.out", outLinks)
		} else {
			// Legacy format: links.out as parallel arrays {names, types, ids}
			outLinkNames := []string{}
			outLinkTypes := []string{}
			outLinkIds := []string{}
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
					outLinkTypes = append(outLinkTypes)
					outLinkIds = append(outLinkIds)
				}
			}
			result.SetByPath("links.out.names", easyjson.NewJSON(outLinkNames))
			result.SetByPath("links.out.types", easyjson.NewJSON(outLinkTypes))
			result.SetByPath("links.out.ids", easyjson.NewJSON(outLinkIds))
		}

		inLinkKeys := ctx.Domain.Cache().GetKeysByPattern(fmt.Sprintf(InLinkKeyPrefPattern+KeySuff1Pattern, selfID, ">"))
		operationKeysMutexUnlock(ctx)

		inLinks := easyjson.NewJSONArray()
		for _, inLinkKey := range inLinkKeys {
			linkKeyTokens := strings.Split(inLinkKey, ".")
			linkName := linkKeyTokens[len(linkKeyTokens)-1]
			linkFromVId := linkKeyTokens[len(linkKeyTokens)-2]

			linkType := ""
			linkTypeBytes, err := ctx.Domain.Cache().GetValue(inLinkKey)
			if err == nil {
				linkType = ctx.Domain.GetObjectIDWithoutDomain(string(linkTypeBytes))
			}

			inLinkJson := easyjson.NewJSONObjectWithKeyValue("from", easyjson.NewJSON(linkFromVId))
			inLinkJson.SetByPath("name", easyjson.NewJSON(linkName))
			inLinkJson.SetByPath("type", easyjson.NewJSON(linkType))
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

	opTime := getOpTimeFromPayloadIfExist(ctx.Payload)

	om := sfMediators.NewOpMediator(ctx)

	forceCreate := ctx.Payload.GetByPath("force").AsBoolDefault(false)

	if !ctx.Domain.Cache().ExistsJson(selfID) { // If vertex does not exist
		om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("vertex with id=%s does not exist", selfID))).Reply()
		return
	}

	payload := ctx.Payload
	opStack := getOpStackFromOptions(ctx.Options)

	if payload.PathExists("in_name") {
		if inLinkName, ok := payload.GetByPath("in_name").AsString(); ok && len(inLinkName) > 0 {
			inLinkType := payload.GetByPath("in_type").AsStringDefault("")
			if linkFromObjectUUID := getOriginalID(ctx.Caller.ID); len(linkFromObjectUUID) > 0 {
				ctx.Domain.Cache().SetValue(fmt.Sprintf(InLinkKeyPrefPattern+KeySuff2Pattern, selfID, linkFromObjectUUID, inLinkName), []byte(inLinkType), true, opTime)
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
		linkName = ctx.Domain.GetObjectIDWithoutDomain(linkName)

		var linkType string
		if s, ok := payload.GetByPath("type").AsString(); ok {
			linkType = ctx.Domain.GetObjectIDWithoutDomain(s)
		} else {
			om.AggregateOpMsg(sfMediators.OpMsgFailed("type is not defined")).Reply()
			return
		}

		operationKeysMutexLock(ctx, []string{selfID, toId}, true, opTime)

		if !forceCreate {
			// Check if link with this name already exists --------------
			if ctx.Domain.Cache().ExistsJson(fmt.Sprintf(OutLinkBodyKeyPrefPattern+KeySuff1Pattern, selfID, linkName)) {
				operationKeysMutexUnlock(ctx)
				om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("link from=%s with name=%s already exists", selfID, linkName))).Reply()
				return
			}
			// ----------------------------------------------------------
			// Check if link with this type "type" to "to" already exists
			if ctx.Domain.Cache().Exists(fmt.Sprintf(OutLinkTypeKeyPrefPattern+KeySuff2Pattern, selfID, linkType, toId)) {
				operationKeysMutexUnlock(ctx)
				om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("link from=%s with name=%s to=%s with type=%s already exists, two vertices can have a link with this type and direction only once", selfID, linkName, toId, linkType))).Reply()
				return
			}
			// -----------------------------------------------------------
		}

		// Create in link on descendant vertex --------------------
		if ctx.Domain.GetDomainFromObjectID(toId) == ctx.Domain.Name() {
			ctx.Domain.Cache().SetValue(fmt.Sprintf(InLinkKeyPrefPattern+KeySuff2Pattern, toId, selfID, linkName), []byte(linkType), true, opTime)
		} else {
			nextCallPayload := easyjson.NewJSONObject()
			nextCallPayload.SetByPath("in_name", easyjson.NewJSON(linkName))
			nextCallPayload.SetByPath("in_type", easyjson.NewJSON(linkType))
			nextCallPayload.SetByPath("op_time", easyjson.NewJSON(opTime))

			om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, ctx.Self.Typename, makeSequenceFreeParentBasedID(ctx, toId, "inlink"), injectParentHoldsLocks(ctx, &nextCallPayload), ctx.Options)))
			if om.GetLastSyncOp().Status == sfMediators.SYNC_OP_STATUS_FAILED {
				operationKeysMutexUnlock(ctx)
				system.MsgOnErrorReturn(om.ReplyWithData(resultWithOpStack(nil, opStack).GetPtr()))
				return
			}
		}
		// --------------------------------------------------------

		// Create out link on this vertex -------------------------
		// Set link target ------------------
		ctx.Domain.Cache().SetValue(fmt.Sprintf(OutLinkTargetKeyPrefPattern+KeySuff1Pattern, selfID, linkName), []byte(fmt.Sprintf("%s.%s", linkType, toId)), true, opTime) // Store link body in KV
		// ----------------------------------
		// Set link body --------------------
		ctx.Domain.Cache().SetValueJSON(fmt.Sprintf(OutLinkBodyKeyPrefPattern+KeySuff1Pattern, selfID, linkName), &linkBody, true, opTime) // Store link body in KV
		// ----------------------------------
		// Set link type --------------------
		ctx.Domain.Cache().SetValue(fmt.Sprintf(OutLinkTypeKeyPrefPattern+KeySuff2Pattern, selfID, linkType, toId), []byte(linkName), true, opTime) // Store link type
		// ----------------------------------
		// Index link type ------------------
		ctx.Domain.Cache().SetValue(fmt.Sprintf(OutLinkIndexPrefPattern+KeySuff3Pattern, selfID, linkName, "type", linkType), nil, true, opTime)
		// ----------------------------------
		// Index link tags ------------------
		if payload.GetByPath("tags").IsNonEmptyArray() {
			if linkTags, ok := payload.GetByPath("tags").AsArrayString(); ok {
				for _, linkTag := range linkTags {
					ctx.Domain.Cache().SetValue(fmt.Sprintf(OutLinkIndexPrefPattern+KeySuff3Pattern, selfID, linkName, "tag", linkTag), nil, true, opTime)
				}
			}
		}
		//fmt.Println("create vertex out link: ", selfID, toId)
		// ----------------------------------

		addLinkOpToOpStack(opStack, ctx.Self.Typename, selfID, toId, linkName, linkType, nil, &linkBody)

		operationKeysMutexUnlock(ctx)
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

	opTime := getOpTimeFromPayloadIfExist(ctx.Payload)

	om := sfMediators.NewOpMediator(ctx)

	payload := ctx.Payload
	upsert := payload.GetByPath("upsert").AsBoolDefault(false)

	opStack := getOpStackFromOptions(ctx.Options)

	//operationKeysMutexLock(ctx, []string{selfID}, true)
	linkType, linkName, toId, linkExists := getFullLinkInfoFromSpecifiedIdentifier(ctx)
	if !linkExists {
		if upsert {
			p := payload.Clone()
			p.SetByPath("force", easyjson.NewJSON(true))
			p.SetByPath("op_time", easyjson.NewJSON(opTime))
			om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.create", makeSequenceFreeParentBasedID(ctx, selfID), injectParentHoldsLocks(ctx, &p), ctx.Options)))
			//operationKeysMutexUnlock(ctx)
			om.Reply()
		} else {
			//operationKeysMutexUnlock(ctx)
			om.AggregateOpMsg(sfMediators.OpMsgIdle(fmt.Sprintf("link from=%s with name=%s does not exist", selfID, linkName))).Reply()
		}
		return
	}
	if !validLinkName.MatchString(linkName) {
		//operationKeysMutexUnlock(ctx)
		om.AggregateOpMsg(sfMediators.OpMsgFailed("invalid link name")).Reply()
		return
	}
	//operationKeysMutexUnlock(ctx)

	operationKeysMutexLock(ctx, []string{selfID, toId}, true, opTime)

	oldLinkBody, err := ctx.Domain.Cache().GetValueJSON(fmt.Sprintf(OutLinkBodyKeyPrefPattern+KeySuff1Pattern, selfID, linkName))
	if err != nil {
		operationKeysMutexUnlock(ctx)
		om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("link from=%s with name=%s", selfID, linkName))).Reply()
		return
	}

	var replace bool = payload.GetByPath("replace").AsBoolDefault(false)

	var linkBody easyjson.JSON
	if payload.GetByPath("body").IsObject() {
		linkBody = payload.GetByPath("body")
	} else {
		linkBody = easyjson.NewJSONObject()
	}

	// Compute the FINAL link body (what would end up stored after the op)
	// without yet touching the KV — needed for the no-op check below.
	if !replace { // merge
		newBody := oldLinkBody.Clone().GetPtr()
		newBody.DeepMerge(linkBody)
		linkBody = *newBody
	}

	// No-op short-circuit — must run BEFORE the destructive index sweep
	// below (once DeleteValue lands the state has already moved).
	//
	// Two separate fast paths, kept apart on purpose so merge-mode does
	// not pay for the existing-tag index scan in the common case:
	//
	//   • merge mode (replace=false): merge can only ADD tags, never
	//     remove. So if the caller did not supply tags AND the body did
	//     not change, the operation is provably a no-op without reading
	//     the existing tag set at all. If the caller did supply tags we
	//     conservatively go through the full re-issue (some of those
	//     tags may be new — checking would cost a GetKeysByPattern that
	//     we want to skip on the hot incremental-upsert path).
	//
	//   • replace mode (replace=true): we MUST read the existing tag
	//     set, because the index sweep below would wipe whichever tags
	//     are not in payload.tags. Compare the requested tag set
	//     against the existing one; if they match AND body is equal,
	//     short-circuit.
	//
	// Equality on body uses easyjson.JSON.Equals (reflect.DeepEqual on
	// the parsed tree, key-order independent).
	if oldLinkBody != nil && linkBody.Equals(*oldLinkBody) {
		if !replace {
			// Merge mode: no-op iff caller did not ask to touch tags.
			if !payload.GetByPath("tags").IsNonEmptyArray() {
				operationKeysMutexUnlock(ctx)
				om.AggregateOpMsg(sfMediators.OpMsgIdle(fmt.Sprintf("link from=%s with name=%s body unchanged", selfID, linkName))).Reply()
				return
			}
		} else {
			// Replace mode: compare the requested tag set against the
			// existing one. Only here do we pay for the index scan.
			existingTagKeys := ctx.Domain.Cache().GetKeysByPattern(
				fmt.Sprintf(OutLinkIndexPrefPattern+KeySuff3Pattern, selfID, linkName, "tag", ">"))
			existingTags := make(map[string]struct{}, len(existingTagKeys))
			for _, k := range existingTagKeys {
				toks := strings.Split(k, ".")
				existingTags[toks[len(toks)-1]] = struct{}{}
			}
			requestedTagsArr, _ := payload.GetByPath("tags").AsArrayString()
			requestedTags := make(map[string]struct{}, len(requestedTagsArr))
			for _, t := range requestedTagsArr {
				requestedTags[t] = struct{}{}
			}
			tagsEqual := len(existingTags) == len(requestedTags)
			if tagsEqual {
				for t := range existingTags {
					if _, ok := requestedTags[t]; !ok {
						tagsEqual = false
						break
					}
				}
			}
			if tagsEqual {
				operationKeysMutexUnlock(ctx)
				om.AggregateOpMsg(sfMediators.OpMsgIdle(fmt.Sprintf("link from=%s with name=%s body and tags unchanged", selfID, linkName))).Reply()
				return
			}
		}
	}

	if replace {
		// Remove all indices -----------------------------
		// Mirrors the previous behaviour: replace mode wipes BOTH the
		// type index and all tag indices, then re-issues them below.
		indexKeys := ctx.Domain.Cache().GetKeysByPattern(fmt.Sprintf(OutLinkIndexPrefPattern+KeySuff2Pattern, selfID, linkName, ">"))
		for _, indexKey := range indexKeys {
			ctx.Domain.Cache().DeleteValue(indexKey, true, opTime)
		}
		// ------------------------------------------------
	}

	// Create out link on this vertex -------------------------
	// Set link body --------------------
	ctx.Domain.Cache().SetValueJSON(fmt.Sprintf(OutLinkBodyKeyPrefPattern+KeySuff1Pattern, selfID, linkName), &linkBody, true, opTime) // Store link body in KV
	// ----------------------------------
	// Index link type ------------------
	ctx.Domain.Cache().SetValue(fmt.Sprintf(OutLinkIndexPrefPattern+KeySuff3Pattern, selfID, linkName, "type", linkType), nil, true, opTime)
	// ----------------------------------
	// Index link tags ------------------
	if payload.GetByPath("tags").IsNonEmptyArray() {
		if linkTags, ok := payload.GetByPath("tags").AsArrayString(); ok {
			for _, linkTag := range linkTags {
				ctx.Domain.Cache().SetValue(fmt.Sprintf(OutLinkIndexPrefPattern+KeySuff3Pattern, selfID, linkName, "tag", linkTag), nil, true, opTime)
			}
		}
	}
	// ----------------------------------

	operationKeysMutexUnlock(ctx)

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

	opTime := getOpTimeFromPayloadIfExist(ctx.Payload)

	om := sfMediators.NewOpMediator(ctx)

	payload := ctx.Payload

	opStack := getOpStackFromOptions(ctx.Options)

	if payload.PathExists("in_name") {
		if inLinkName, ok := payload.GetByPath("in_name").AsString(); ok && len(inLinkName) > 0 {
			if linkFromObjectUUID := getOriginalID(ctx.Caller.ID); len(linkFromObjectUUID) > 0 {
				//fmt.Println("delete vertex in link: ", selfID, linkFromObjectUUID)
				ctx.Domain.Cache().DeleteValue(fmt.Sprintf(InLinkKeyPrefPattern+KeySuff2Pattern, selfID, linkFromObjectUUID, inLinkName), true, opTime)
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
		//operationKeysMutexLock(ctx, []string{selfID}, true)
		linkType, linkName, toId, linkExists := getFullLinkInfoFromSpecifiedIdentifier(ctx)
		if !linkExists {
			//operationKeysMutexUnlock(ctx)
			// linkName may have been wiped by the lookup helper when no match
			// was found; preserve the original identifier from payload for a
			// useful idle message.
			requestedName := ctx.Payload.GetByPath("name").AsStringDefault("")
			requestedTo := ctx.Payload.GetByPath("to").AsStringDefault("")
			requestedType := ctx.Payload.GetByPath("type").AsStringDefault("")
			descriptor := ""
			if requestedName != "" {
				descriptor = fmt.Sprintf("name=%s", requestedName)
			} else if requestedTo != "" && requestedType != "" {
				descriptor = fmt.Sprintf("type=%s to=%s", requestedType, requestedTo)
			} else if requestedTo != "" {
				descriptor = fmt.Sprintf("to=%s", requestedTo)
			} else {
				descriptor = "(no identifier provided)"
			}
			om.AggregateOpMsg(sfMediators.OpMsgIdle(fmt.Sprintf("link from=%s with %s does not exist", ctx.Self.ID, descriptor))).Reply()
			return
		}
		if !validLinkName.MatchString(linkName) {
			//operationKeysMutexUnlock(ctx)
			om.AggregateOpMsg(sfMediators.OpMsgFailed("invalid link name")).Reply()
			return
		}
		//operationKeysMutexUnlock(ctx)

		operationKeysMutexLock(ctx, []string{selfID, toId}, true, opTime)

		oldLinkBody, err := ctx.Domain.Cache().GetValueJSON(fmt.Sprintf(OutLinkBodyKeyPrefPattern+KeySuff1Pattern, selfID, linkName))
		if err != nil {
			operationKeysMutexUnlock(ctx)
			om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("link body from=%s with name=%s does not exist", selfID, linkName))).Reply()
			return
		}

		// Remove all indices -----------------------------
		indexKeys := ctx.Domain.Cache().GetKeysByPattern(fmt.Sprintf(OutLinkIndexPrefPattern+KeySuff2Pattern, selfID, linkName, ">"))
		for _, indexKey := range indexKeys {
			ctx.Domain.Cache().DeleteValue(indexKey, true, opTime)
		}
		// ------------------------------------------------

		// Set link type --------------------
		ctx.Domain.Cache().DeleteValue(fmt.Sprintf(OutLinkTypeKeyPrefPattern+KeySuff2Pattern, selfID, linkType, toId), true, opTime)
		// ----------------------------------
		// Delete link body -----------------
		ctx.Domain.Cache().DeleteValue(fmt.Sprintf(OutLinkBodyKeyPrefPattern+KeySuff1Pattern, selfID, linkName), true, opTime)
		// ----------------------------------
		// Delete link target ---------------
		ctx.Domain.Cache().DeleteValue(fmt.Sprintf(OutLinkTargetKeyPrefPattern+KeySuff1Pattern, selfID, linkName), true, opTime)
		// ----------------------------------

		// If this was the __type link of a CMDB object, invalidate the cached
		// (objectID -> typeID) mapping. Without this, findObjectType would
		// keep returning the stale type and HL CMDB operations would falsely
		// believe the invariant still holds.
		if linkType == TO_TYPELINK {
			cacheDeleteObjectType(selfID)
		}

		//fmt.Println("delete vertex out link: ", selfID, toId)

		addLinkOpToOpStack(opStack, ctx.Self.Typename, selfID, toId, linkName, linkType, oldLinkBody, nil)

		// Delete in link on descendant vertex --------------------
		if ctx.Domain.GetDomainFromObjectID(toId) == ctx.Domain.Name() {
			ctx.Domain.Cache().DeleteValue(fmt.Sprintf(InLinkKeyPrefPattern+KeySuff2Pattern, toId, selfID, linkName), true, opTime)
		} else {
			nextCallPayload := easyjson.NewJSONObject()
			nextCallPayload.SetByPath("in_name", easyjson.NewJSON(linkName))
			nextCallPayload.SetByPath("op_time", easyjson.NewJSON(opTime))

			om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, ctx.Self.Typename, makeSequenceFreeParentBasedID(ctx, toId, "inlink"), injectParentHoldsLocks(ctx, &nextCallPayload), ctx.Options)))
			if om.GetLastSyncOp().Status == sfMediators.SYNC_OP_STATUS_FAILED {
				operationKeysMutexUnlock(ctx)
				system.MsgOnErrorReturn(om.ReplyWithData(resultWithOpStack(nil, opStack).GetPtr()))
				return
			}
		}
		// --------------------------------------------------------

		operationKeysMutexUnlock(ctx)
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

	linkType, linkName, toId, linkExists := getFullLinkInfoFromSpecifiedIdentifier(ctx)
	if !linkExists {
		om.AggregateOpMsg(sfMediators.OpMsgIdle(fmt.Sprintf("link from=%s with name=%s does not exist", ctx.Self.ID, linkName))).Reply()
		return
	}
	if !validLinkName.MatchString(linkName) {
		om.AggregateOpMsg(sfMediators.OpMsgFailed("invalid link name")).Reply()
		return
	}

	details := ctx.Payload.GetByPath("details").AsBoolDefault(false)

	opTime := getOpTimeFromPayloadIfExist(ctx.Payload)
	if details {
		operationKeysMutexLock(ctx, []string{selfID, toId}, false, opTime)
	}

	linkBody, err := ctx.Domain.Cache().GetValueJSON(fmt.Sprintf(OutLinkBodyKeyPrefPattern+KeySuff1Pattern, selfID, linkName))
	if err != nil {
		if details {
			operationKeysMutexUnlock(ctx)
		}
		om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("link body from=%s with name=%s does not exist", selfID, linkName))).Reply()
		return
	}

	result := easyjson.NewJSONObjectWithKeyValue("body", *linkBody)

	if details {
		tags := []string{}
		tagKeys := ctx.Domain.Cache().GetKeysByPattern(fmt.Sprintf(OutLinkIndexPrefPattern+KeySuff3Pattern, selfID, linkName, "tag", ">"))
		operationKeysMutexUnlock(ctx)

		for _, tagKey := range tagKeys {
			tagKeyTokens := strings.Split(tagKey, ".")
			tags = append(tags, tagKeyTokens[len(tagKeyTokens)-1])
		}

		result.SetByPath("name", easyjson.NewJSON(linkName))
		result.SetByPath("type", easyjson.NewJSON(linkType))
		result.SetByPath("from", easyjson.NewJSON(selfID))
		result.SetByPath("to", easyjson.NewJSON(toId))

		result.SetByPath("tags", easyjson.NewJSON(tags))
	}

	addLinkOpToOpStack(opStack, ctx.Self.Typename, selfID, toId, linkName, linkType, nil, nil)

	om.AggregateOpMsg(sfMediators.OpMsgOk(resultWithOpStack(result.GetPtr(), opStack))).Reply()
}
