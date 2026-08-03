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
	validLinkName = regexp.MustCompile(`\A[a-zA-Z0-9\/_$#@%+=-]+\z`)
	// validVertexID: same class as link names — in particular NO dots. "." is
	// the cache key separator: a dotted vertex id nests into foreign key
	// families in the KV tree and breaks every path-based piece of bookkeeping
	// built on ids. Enforced on CREATE paths only (vertex.create and the `to`
	// of link.create); reading/deleting pre-existing dotted ids stays possible
	// so legacy data can be cleaned up.
	validVertexID                    = regexp.MustCompile(`\A[a-zA-Z0-9\/_$#@%+=-]+\z`)
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

// SetGraphKeyLockTimeoutForTest overrides graphKeyLockTimeout. graphKeyLockTimeout
// is resolved once at package init, so a test cannot lower it via the environment;
// this lets a deadlock-reproduction test bound how long a genuinely stuck operation
// blocks before proceeding without the lock, keeping the test (and the subsequent
// runtime shutdown) fast instead of waiting the production default. Call it before
// issuing operations (e.g. from a test init); not for non-test use.
func SetGraphKeyLockTimeoutForTest(d time.Duration) { graphKeyLockTimeout = d }

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

// edgeLockKey is the graph-mutex key identifying a single out-link (owner, name).
// Write-locking the EDGE (instead of the whole owner vertex) lets operations on
// DIFFERENT out-links of the same vertex run concurrently; operations on the SAME
// edge still serialize on it. The owner and target vertices are taken as READ
// guards alongside (see the mixed-lock call sites), so a concurrent vertex delete
// is still excluded.
//
// A hash gives a key that is unique per edge and never equals a vertex id.
// (Historically the hash also had to keep the key dot-free for the payload
// lock bookkeeping; the bookkeeping now hashes recorded keys itself — see
// lockRecSeg — so that constraint is no longer load-bearing here.)
func edgeLockKey(ownerID, linkName string) string {
	return "edge_" + system.GetHashStr(ownerID+"\x00"+linkName)
}

// lockRecSeg returns the payload path segment under which a held graph-key
// lock is recorded inside __key_locks / __parent_holds_locks. The segment is a
// HASH of the key, never the key itself: SetByPath treats "." as a path
// separator, so recording a raw key containing dots (an unvalidated legacy id,
// a KV-style key) would nest it into the payload where the unlock walk could
// never find it — leaking a permanently held lock AND skipping the operation-
// completion mark, which wedges the WAL publisher for good. The original key
// is stored alongside under "k" so unlock never needs to reverse the hash.
func lockRecSeg(key string) string { return system.GetHashStr(key) }

func recordHeldLock(ctx *sfPlugins.StatefunContextProcessor, key, mode string) {
	seg := lockRecSeg(key)
	ctx.Payload.SetByPath(fmt.Sprintf("__key_locks.%s.m", seg), easyjson.NewJSON(mode))
	ctx.Payload.SetByPath(fmt.Sprintf("__key_locks.%s.k", seg), easyjson.NewJSON(key))
}

func parentHoldsWriteLock(ctx *sfPlugins.StatefunContextProcessor, key string) bool {
	return ctx.Payload.GetByPath(fmt.Sprintf("__parent_holds_locks.%s.m", lockRecSeg(key))).AsStringDefault("") == "w"
}

func parentHoldsAnyLock(ctx *sfPlugins.StatefunContextProcessor, key string) bool {
	return ctx.Payload.PathExists(fmt.Sprintf("__parent_holds_locks.%s", lockRecSeg(key)))
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
		seg := lockRecSeg(targetID)
		if ctx.Payload.PathExists(fmt.Sprintf("__key_locks.%s", seg)) || ctx.Payload.PathExists(fmt.Sprintf("__parent_holds_locks.%s", seg)) {
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
			if !parentHoldsWriteLock(ctx, k) {
				// Bounded acquire: never hang a worker forever on a stuck
				// holder. On timeout we record nothing (so Unlock won't touch a
				// lock we don't hold) and proceed — the holder is presumed
				// deadlocked/frozen, so it is not actively mutating this key.
				if graphIdKeyMutex.LockTimeout(k, graphKeyLockTimeout) {
					recordHeldLock(ctx, k, "w")
					lockedWriteAny = true
				} else {
					lg.Logf(lg.WarnLevel, "operationKeysMutexLock: write-lock acquire for key=%s timed out after %s; proceeding without it", k, graphKeyLockTimeout)
				}
			}
		} else {
			if !parentHoldsAnyLock(ctx, k) {
				if graphIdKeyMutex.RLockTimeout(k, graphKeyLockTimeout) {
					recordHeldLock(ctx, k, "r")
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

// operationKeysMutexLockMixed locks several graph keys in a SINGLE sorted pass,
// each in its own mode: writeKeys exclusively, readKeys shared. The single
// sorted order keeps it deadlock-compatible with operationKeysMutexLock (which
// also sorts).
//
// A shared (read) lock here means "I only append my OWN distinct child under
// this vertex (e.g. objects.out.to.<myObj>), which the cache child container
// already serializes per-child (node mutex / shard locks) — so I do not
// conflict with other appenders, only with operations that need the vertex's
// whole child set exclusively, which take a write lock". This lets CreateObject
// stop serializing every creation on the single shared `objects` root and the
// type vertex. Unlock with operationKeysMutexUnlock as usual (it honours the
// recorded .w/.r mode per key).
func operationKeysMutexLockMixed(ctx *sfPlugins.StatefunContextProcessor, writeKeys []string, readKeys []string, opTime int64) {
	write := map[string]bool{}
	for _, k := range writeKeys {
		write[k] = true
	}
	for _, k := range readKeys {
		if _, ok := write[k]; !ok {
			write[k] = false
		}
	}
	all := make([]string, 0, len(write))
	for k := range write {
		all = append(all, k)
	}
	sort.Strings(all)

	lockedWriteAny := false
	for _, k := range all {
		if write[k] {
			if !parentHoldsWriteLock(ctx, k) {
				if graphIdKeyMutex.LockTimeout(k, graphKeyLockTimeout) {
					recordHeldLock(ctx, k, "w")
					lockedWriteAny = true
				} else {
					lg.Logf(lg.WarnLevel, "operationKeysMutexLockMixed: write-lock acquire for key=%s timed out after %s; proceeding without it", k, graphKeyLockTimeout)
				}
			}
		} else {
			if !parentHoldsAnyLock(ctx, k) {
				if graphIdKeyMutex.RLockTimeout(k, graphKeyLockTimeout) {
					recordHeldLock(ctx, k, "r")
				} else {
					lg.Logf(lg.WarnLevel, "operationKeysMutexLockMixed: read-lock acquire for key=%s timed out after %s; proceeding without it", k, graphKeyLockTimeout)
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
	if ctx.Payload.PathExists("__key_locks") {
		held := ctx.Payload.GetByPath("__key_locks")
		for _, seg := range held.ObjectKeys() {
			key := held.GetByPath(fmt.Sprintf("%s.k", seg)).AsStringDefault("")
			if key == "" {
				continue // unrecognized record — nothing safe to release
			}
			switch held.GetByPath(fmt.Sprintf("%s.m", seg)).AsStringDefault("") {
			case "w":
				graphIdKeyMutex.Unlock(key)
			case "r":
				graphIdKeyMutex.RUnlock(key)
			}
		}
		ctx.Payload.RemoveByPath("__key_locks")
	}
	// Completion marking is deliberately DECOUPLED from the lock records:
	// __key_lock_time is a flat field that always parses, and it is set exactly
	// when MarkOperationActive was called — so even a bookkeeping bug that
	// loses a lock record can orphan at most that one lock, never the
	// activeOps entry (an orphaned entry wedges the WAL publisher forever).
	// The field is consumed so repeated lock/unlock pairs within one handler
	// stay symmetric.
	if opTime, ok := ctx.Payload.GetByPath("__key_lock_time").AsInt64(); ok {
		ctx.Domain.Cache().MarkOperationDone(opTime)
		ctx.Payload.RemoveByPath("__key_lock_time")
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
// writeVertexKV performs the cache write + op_stack entry for creating a new
// vertex — the exact effect of LLAPIVertexCreate's success path. The caller must
// already hold the write lock on vertexID and have confirmed it does not yet
// exist. Factored out so the HL CreateObject pipeline (which already holds the
// locks) can write the object vertex inline instead of paying a worker-pool
// round-trip via ctx.Request.
func writeVertexKV(ctx *sfPlugins.StatefunContextProcessor, vertexID string, body *easyjson.JSON, opTime int64, opStack *easyjson.JSON) {
	vb := easyjson.NewJSONObject()
	if body != nil && body.IsObject() {
		vb = *body
	}
	ctx.Domain.Cache().SetValueJSON(vertexID, &vb, true, opTime)
	addVertexOpToOpStack(opStack, "functions.graph.api.vertex.create", vertexID, nil, &vb)
}

// writeOutLinkKV performs the cache writes + op_stack entry for creating an
// out-link from `from` to `toId` — the exact effect of LLAPILinkCreate's write
// section. Arguments are already normalized (from/toId domain-qualified,
// linkName/linkType domain-stripped). The in-link is written inline when the
// descendant lives in this domain, otherwise forwarded with one link.create
// request exactly as LLAPILinkCreate does. The caller must already hold the
// write locks on `from` and `toId`. Returns a FAILED OpMsg only if a
// cross-domain in-link forward fails (op_stack left untouched in that case,
// matching LLAPILinkCreate); otherwise OK.
func writeOutLinkKV(ctx *sfPlugins.StatefunContextProcessor, from, toId, linkName, linkType string, linkBody *easyjson.JSON, tags []string, opTime int64, opStack *easyjson.JSON) sfMediators.OpMsg {
	body := easyjson.NewJSONObject()
	if linkBody != nil && linkBody.IsObject() {
		body = *linkBody
	}

	// Create in link on descendant vertex.
	if ctx.Domain.GetDomainFromObjectID(toId) == ctx.Domain.Name() {
		ctx.Domain.Cache().SetValue(fmt.Sprintf(InLinkKeyPrefPattern+KeySuff2Pattern, toId, from, linkName), []byte(linkType), true, opTime)
	} else {
		nextCallPayload := easyjson.NewJSONObject()
		nextCallPayload.SetByPath("in_name", easyjson.NewJSON(linkName))
		nextCallPayload.SetByPath("in_type", easyjson.NewJSON(linkType))
		nextCallPayload.SetByPath("op_time", easyjson.NewJSON(opTime))
		m := sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.create", makeSequenceFreeParentBasedID(ctx, toId, "inlink"), injectParentHoldsLocks(ctx, &nextCallPayload), ctx.Options))
		if m.Status == sfMediators.SYNC_OP_STATUS_FAILED {
			return m
		}
	}

	ctx.Domain.Cache().SetValue(fmt.Sprintf(OutLinkTargetKeyPrefPattern+KeySuff1Pattern, from, linkName), []byte(fmt.Sprintf("%s.%s", linkType, toId)), true, opTime)
	ctx.Domain.Cache().SetValueJSON(fmt.Sprintf(OutLinkBodyKeyPrefPattern+KeySuff1Pattern, from, linkName), &body, true, opTime)
	ctx.Domain.Cache().SetValue(fmt.Sprintf(OutLinkTypeKeyPrefPattern+KeySuff2Pattern, from, linkType, toId), []byte(linkName), true, opTime)
	ctx.Domain.Cache().SetValue(fmt.Sprintf(OutLinkIndexPrefPattern+KeySuff3Pattern, from, linkName, "type", linkType), nil, true, opTime)
	for _, linkTag := range tags {
		ctx.Domain.Cache().SetValue(fmt.Sprintf(OutLinkIndexPrefPattern+KeySuff3Pattern, from, linkName, "tag", linkTag), nil, true, opTime)
	}

	addLinkOpToOpStack(opStack, "functions.graph.api.link.create", from, toId, linkName, linkType, nil, &body)
	return sfMediators.OpMsgOk(easyjson.NewJSONNull())
}

func LLAPIVertexCreate(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	selfID := getOriginalID(ctx.Self.ID)
	om := sfMediators.NewOpMediator(ctx)

	if !validVertexID.MatchString(selfID) {
		om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("invalid vertex id=%s: allowed characters are a-zA-Z0-9/_$#@%%+=- (\".\" is the cache key separator)", selfID))).Reply()
		return
	}

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

	writeVertexKV(ctx, selfID, &objectBody, opTime, opStack)

	operationKeysMutexUnlock(ctx)

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
// resolveOutLinkByName resolves the (linkType, toId) of the out-link named
// linkName owned by ownerID, reading the OutLinkTarget key the same way
// getFullLinkInfoFromSpecifiedIdentifier does. ownerID must be local. When
// the out.to key is missing or corrupt, falls back to the ltype scan so a
// partially written link stays deletable.
func resolveOutLinkByName(ctx *sfPlugins.StatefunContextProcessor, ownerID, linkName string) (linkType, toId string, ok bool) {
	b, err := ctx.Domain.Cache().GetValue(fmt.Sprintf(OutLinkTargetKeyPrefPattern+KeySuff1Pattern, ownerID, linkName))
	if err != nil {
		return resolveOutLinkByLtypeScan(ctx, ownerID, linkName)
	}
	tokens := strings.Split(string(b), ".")
	if len(tokens) < 2 {
		return resolveOutLinkByLtypeScan(ctx, ownerID, linkName)
	}
	return ctx.Domain.GetObjectIDWithoutDomain(tokens[0]), tokens[1], true
}

// resolveOutLinkByLtypeScan recovers the (linkType, toId) of the out-link
// named linkName when its out.to key is MISSING (interrupted write, partial
// replication). The ltype family encodes both the type and the target in the
// KEY itself (`<owner>.ltype.<type>.<toId>` -> value: link name), so a scan
// of the owner's ltype subtree — local, bounded by the vertex's out-degree —
// rebuilds what out.to lost. Without this fallback such a link was
// permanently undeletable: every delete path resolved the target via out.to,
// gave up with IDLE and left the remaining key families behind forever.
func resolveOutLinkByLtypeScan(ctx *sfPlugins.StatefunContextProcessor, ownerID, linkName string) (linkType, toId string, ok bool) {
	prefix := fmt.Sprintf(OutLinkTypeKeyPrefPattern, ownerID)
	for _, key := range ctx.Domain.Cache().GetKeysByPattern(prefix + ">") {
		nameBytes, err := ctx.Domain.Cache().GetValue(key)
		if err != nil || string(nameBytes) != linkName {
			continue
		}
		tokens := strings.SplitN(strings.TrimPrefix(key, prefix), ".", 2)
		if len(tokens) < 2 {
			continue
		}
		return ctx.Domain.GetObjectIDWithoutDomain(tokens[0]), tokens[1], true
	}
	return "", "", false
}

// deleteOutLinkFromSideKeys removes the from-side keys of the link
// ownerID -> toId (indices, out-link type/body/target), invalidates the __type
// cache when it was a TO_TYPELINK, and records the op on opStack. ownerID must
// be local and its {ownerID, toId} write lock already held. The caller deletes
// the descendant's in-link key separately (in-process for a local descendant,
// routed for a remote one). Extracted from LLAPILinkDelete so the vertex-delete
// cascade deletes a link identically, without a per-link statefun round-trip.
// opName is recorded in the op-stack (the link-delete function, so the op-stack
// entry matches the routed path byte-for-byte).
func deleteOutLinkFromSideKeys(ctx *sfPlugins.StatefunContextProcessor, opName, ownerID, linkType, linkName, toId string, oldLinkBody *easyjson.JSON, opStack *easyjson.JSON, opTime int64) {
	indexKeys := ctx.Domain.Cache().GetKeysByPattern(fmt.Sprintf(OutLinkIndexPrefPattern+KeySuff2Pattern, ownerID, linkName, ">"))
	for _, indexKey := range indexKeys {
		ctx.Domain.Cache().DeleteValue(indexKey, true, opTime)
	}
	ctx.Domain.Cache().DeleteValue(fmt.Sprintf(OutLinkTypeKeyPrefPattern+KeySuff2Pattern, ownerID, linkType, toId), true, opTime)
	ctx.Domain.Cache().DeleteValue(fmt.Sprintf(OutLinkBodyKeyPrefPattern+KeySuff1Pattern, ownerID, linkName), true, opTime)
	ctx.Domain.Cache().DeleteValue(fmt.Sprintf(OutLinkTargetKeyPrefPattern+KeySuff1Pattern, ownerID, linkName), true, opTime)
	if linkType == TO_TYPELINK {
		cacheDeleteObjectType(ownerID)
	}
	addLinkOpToOpStack(opStack, opName, ownerID, toId, linkName, linkType, oldLinkBody, nil)
}

func LLAPIVertexDelete(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	selfID := getOriginalID(ctx.Self.ID)

	opTime := getOpTimeFromPayloadIfExist(ctx.Payload)

	om := sfMediators.NewOpMediator(ctx)

	if !ctx.Domain.Cache().ExistsJson(selfID) { // If vertex does not exist
		// Even an idempotent no-op delete must drop the objectTypeCache
		// entry: a partially deleted vertex (body already gone) otherwise
		// keeps its entry in the process-global cache forever.
		cacheDeleteObjectType(selfID)
		om.AggregateOpMsg(sfMediators.OpMsgIdle(fmt.Sprintf("vertex with id=%s does not exist", selfID))).Reply()
		return
	}

	opStack := getOpStackFromOptions(ctx.Options)

	// Delete all out links -------------------------------
	// When both endpoints are local (the common case) the whole link is deleted
	// in-process under one {selfID, toId} write lock — selfID-side keys plus the
	// descendant's in-link key — with no per-link statefun round-trip. A link
	// whose descendant lives in another domain keeps the routed link.delete (its
	// in-link key is physically remote).
	const linkDeleteTypename = "functions.graph.api.link.delete"
	outLinkKeys := ctx.Domain.Cache().GetKeysByPattern(fmt.Sprintf(OutLinkBodyKeyPrefPattern+KeySuff1Pattern, selfID, ">"))
	for _, outLinkKey := range outLinkKeys {
		outLinkKeyTokens := strings.Split(outLinkKey, ".")
		linkName := outLinkKeyTokens[len(outLinkKeyTokens)-1]

		if linkType, toId, ok := resolveOutLinkByName(ctx, selfID, linkName); ok && ctx.Domain.GetDomainFromObjectID(toId) == ctx.Domain.Name() {
			operationKeysMutexLock(ctx, []string{selfID, toId}, true, opTime)
			var oldLinkBody *easyjson.JSON
			if opStack != nil {
				oldLinkBody, _ = ctx.Domain.Cache().GetValueJSON(fmt.Sprintf(OutLinkBodyKeyPrefPattern+KeySuff1Pattern, selfID, linkName))
			}
			deleteOutLinkFromSideKeys(ctx, linkDeleteTypename, selfID, linkType, linkName, toId, oldLinkBody, opStack, opTime)
			ctx.Domain.Cache().DeleteValue(fmt.Sprintf(InLinkKeyPrefPattern+KeySuff2Pattern, toId, selfID, linkName), true, opTime)
			operationKeysMutexUnlock(ctx)
			continue
		}

		// Cross-domain descendant (or unresolved): keep the routed link.delete.
		deleteLinkPayload := easyjson.NewJSONObject()
		deleteLinkPayload.SetByPath("name", easyjson.NewJSON(linkName))
		deleteLinkPayload.SetByPath("op_time", easyjson.NewJSON(opTime))
		om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, linkDeleteTypename, makeSequenceFreeParentBasedID(ctx, selfID), injectParentHoldsLocks(ctx, &deleteLinkPayload), ctx.Options)))
		mergeOpStack(opStack, om.GetLastSyncOp().Data.GetByPath("op_stack").GetPtr())
		if om.GetLastSyncOp().Status == sfMediators.SYNC_OP_STATUS_FAILED {
			cacheDeleteObjectType(selfID) // aborted mid-delete: drop the possibly-stale cache entry
			system.MsgOnErrorReturn(om.ReplyWithData(resultWithOpStack(nil, opStack).GetPtr()))
			return
		}
	}
	// ----------------------------------------------------

	// Delete all in links --------------------------------
	// from-vertex local: delete its out-side keys and our in-link key in-process
	// under one {fromObjectID, selfID} lock. from-vertex in another domain: keep
	// the routed link.delete (its out-side keys are physically remote).
	inLinkKeys := ctx.Domain.Cache().GetKeysByPattern(fmt.Sprintf(InLinkKeyPrefPattern+KeySuff1Pattern, selfID, ">"))
	for _, inLinkKey := range inLinkKeys {
		inLinkKeyTokens := strings.Split(inLinkKey, ".")
		fromObjectID := inLinkKeyTokens[len(inLinkKeyTokens)-2]
		linkName := inLinkKeyTokens[len(inLinkKeyTokens)-1]

		if ctx.Domain.GetDomainFromObjectID(fromObjectID) == ctx.Domain.Name() {
			operationKeysMutexLock(ctx, []string{fromObjectID, selfID}, true, opTime)
			if linkType, toId, ok := resolveOutLinkByName(ctx, fromObjectID, linkName); ok {
				var oldLinkBody *easyjson.JSON
				if opStack != nil {
					oldLinkBody, _ = ctx.Domain.Cache().GetValueJSON(fmt.Sprintf(OutLinkBodyKeyPrefPattern+KeySuff1Pattern, fromObjectID, linkName))
				}
				deleteOutLinkFromSideKeys(ctx, linkDeleteTypename, fromObjectID, linkType, linkName, toId, oldLinkBody, opStack, opTime)
			}
			ctx.Domain.Cache().DeleteValue(fmt.Sprintf(InLinkKeyPrefPattern+KeySuff2Pattern, selfID, fromObjectID, linkName), true, opTime)
			operationKeysMutexUnlock(ctx)
			continue
		}

		deleteLinkPayload := easyjson.NewJSONObject()
		deleteLinkPayload.SetByPath("name", easyjson.NewJSON(linkName))
		deleteLinkPayload.SetByPath("op_time", easyjson.NewJSON(opTime))
		om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, linkDeleteTypename, makeSequenceFreeParentBasedID(ctx, fromObjectID), injectParentHoldsLocks(ctx, &deleteLinkPayload), ctx.Options)))
		mergeOpStack(opStack, om.GetLastSyncOp().Data.GetByPath("op_stack").GetPtr())
		if om.GetLastSyncOp().Status == sfMediators.SYNC_OP_STATUS_FAILED {
			cacheDeleteObjectType(selfID) // aborted mid-delete: drop the possibly-stale cache entry
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

	// The vertex's per-id FUNCTION CONTEXTS (`<typename>.<id>` cache keys) die
	// with the vertex. Without this, a context written without an expiration
	// mark outlived its object forever: the context GC reclaims only marked
	// contexts and nothing ever references a deleted id again. One cheap
	// mostly-miss cache lookup per registered function type — plus one
	// single-level scan for SALTED variants: an invocation parallelized via
	// the sequence-free suffix (`<id>===<hash>`) stores its context under the
	// full salted id, a sibling key of the exact one, which the exact delete
	// would miss.
	if ctx.ListRegisteredFunctionTypes != nil {
		saltedPrefix := selfID + "==="
		for _, tn := range ctx.ListRegisteredFunctionTypes() {
			ctx.Domain.Cache().DeleteValue(tn+"."+selfID, true, opTime)
			for _, key := range ctx.Domain.Cache().GetKeysByPattern(tn + ".*") {
				if strings.HasPrefix(key[len(tn)+1:], saltedPrefix) {
					ctx.Domain.Cache().DeleteValue(key, true, opTime)
				}
			}
		}
	}

	// Belt-and-braces: the __type-link branch of deleteOutLinkFromSideKeys
	// already purged the objectTypeCache entry for a well-formed object; this
	// covers vertices deleted without a __type link so the invariant is
	// simple — after vertex.delete the entry for this id is always gone.
	cacheDeleteObjectType(selfID)

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
		if !validVertexID.MatchString(toId) {
			om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("invalid link target id=%s: allowed characters are a-zA-Z0-9/_$#@%%+=- (\".\" is the cache key separator)", toId))).Reply()
			return
		}

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

		operationKeysMutexLockMixed(ctx, []string{edgeLockKey(selfID, linkName)}, []string{selfID, toId}, opTime)

		if forceCreate {
			// force is an atomic REPLACE of an existing link with this name:
			// the old link's keys are dropped first. Historically force only
			// overwrote the name-keyed keys, so retargeting stranded the old
			// target's ltype and in keys forever — no delete path ever finds
			// them afterwards. The old target is not additionally locked (a
			// second, unsorted acquisition would reopen the lock-order
			// deadlock class); the only writes to it are idempotent deletes,
			// so racing a concurrent delete is benign.
			if oldType, oldTo, ok := resolveOutLinkByName(ctx, selfID, linkName); ok && (oldType != linkType || oldTo != toId) {
				var oldLinkBody *easyjson.JSON
				if opStack != nil {
					oldLinkBody, _ = ctx.Domain.Cache().GetValueJSON(fmt.Sprintf(OutLinkBodyKeyPrefPattern+KeySuff1Pattern, selfID, linkName))
				}
				deleteOutLinkFromSideKeys(ctx, "functions.graph.api.link.delete", selfID, oldType, linkName, oldTo, oldLinkBody, opStack, opTime)
				if ctx.Domain.GetDomainFromObjectID(oldTo) == ctx.Domain.Name() {
					ctx.Domain.Cache().DeleteValue(fmt.Sprintf(InLinkKeyPrefPattern+KeySuff2Pattern, oldTo, selfID, linkName), true, opTime)
				} else {
					nextCallPayload := easyjson.NewJSONObject()
					nextCallPayload.SetByPath("in_name", easyjson.NewJSON(linkName))
					nextCallPayload.SetByPath("op_time", easyjson.NewJSON(opTime))
					om.AggregateOpMsg(sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.delete", makeSequenceFreeParentBasedID(ctx, oldTo, "inlink"), injectParentHoldsLocks(ctx, &nextCallPayload), ctx.Options)))
					if om.GetLastSyncOp().Status == sfMediators.SYNC_OP_STATUS_FAILED {
						operationKeysMutexUnlock(ctx)
						system.MsgOnErrorReturn(om.ReplyWithData(resultWithOpStack(nil, opStack).GetPtr()))
						return
					}
				}
			}
		} else {
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

		var linkTags []string
		if payload.GetByPath("tags").IsNonEmptyArray() {
			linkTags, _ = payload.GetByPath("tags").AsArrayString()
		}

		m := writeOutLinkKV(ctx, selfID, toId, linkName, linkType, &linkBody, linkTags, opTime, opStack)
		operationKeysMutexUnlock(ctx)
		if m.Status == sfMediators.SYNC_OP_STATUS_FAILED {
			om.AggregateOpMsg(m)
			system.MsgOnErrorReturn(om.ReplyWithData(resultWithOpStack(nil, opStack).GetPtr()))
			return
		}
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
			// Upsert-create is a genuine create of a not-yet-existing link, so
			// it MUST honour the link invariants — let link.create run its
			// constraint checks instead of forcing past them. getFullLinkInfo
			// above only confirmed the link is absent along the dimension the
			// caller addressed it by (its name, OR its type+to), but a create
			// can still violate the OTHER dimension: a different name may
			// already own this (type -> to) edge (at most one link of a type per
			// direction), or this name may already be taken by another edge (at
			// most one out-link per name). Forcing bypassed both checks and let
			// upsert silently create a duplicate-type edge / steal a name,
			// corrupting the ltype and out.to indices.
			p.SetByPath("force", easyjson.NewJSON(false))
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

	operationKeysMutexLockMixed(ctx, []string{edgeLockKey(selfID, linkName)}, []string{selfID, toId}, opTime)

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

		operationKeysMutexLockMixed(ctx, []string{edgeLockKey(selfID, linkName)}, []string{selfID, toId}, opTime)

		oldLinkBody, err := ctx.Domain.Cache().GetValueJSON(fmt.Sprintf(OutLinkBodyKeyPrefPattern+KeySuff1Pattern, selfID, linkName))
		if err != nil {
			operationKeysMutexUnlock(ctx)
			om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("link body from=%s with name=%s does not exist", selfID, linkName))).Reply()
			return
		}

		deleteOutLinkFromSideKeys(ctx, ctx.Self.Typename, selfID, linkType, linkName, toId, oldLinkBody, opStack, opTime)

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
		// Read locks only the link owner (selfID): the link's body and tags are stored
		// under selfID keys; the target (toId) vertex is never read here, only echoed
		// into the reply. Locking toId is unnecessary AND caused a lock-order deadlock
		// with the high-level delete cascade — DeleteObject locks object→type (object
		// first, then type in its nested vertex.delete), the reverse of this read's
		// sorted [type, object] order. Read-lock the EDGE (not the owner vertex)
		// so a concurrent update/delete of THIS link is excluded (no dirty read)
		// while operations on the owner's OTHER links run in parallel.
		operationKeysMutexLock(ctx, []string{edgeLockKey(selfID, linkName)}, false, opTime)
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
