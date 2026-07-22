package crud

// Trash can — "nothing dies instantly", implemented IN THE GRAPH itself.
//
// The SDK's primary storage is the in-memory cache; no feature may bind to a
// particular persistence backend (NATS KV is merely the current restore/export
// target). The trash can is therefore a built-in TYPE (BUILT_IN_TRASH_CAN):
// deleting an object does not erase it — the object keeps its body, loses all
// its links (the usual cascade) and is re-linked from its original type to the
// trash-can type. The original type and the deletion moment are recorded on
// the trash-can→object edge, so a later restore under a DIFFERENT type can be
// flagged. Everything rides the ordinary cache→WAL→export pipeline — zero
// extra round-trips, zero extra storages.
//
// Retention is by OBJECT COUNT: when parking an object pushes the bin over
// OBJECT_TRASH_CAN_MAX_OBJECTS, the OLDEST parked object is physically deleted
// (and logged). Counting/eviction are ordinary graph reads off the trash-can
// type's link indices.
//
// A TRUE re-creation of a parked id (plain create and upsert both) restores
// it: the PROTECTED body fields (PROTECTED_BODY_FIELDS, default "usr") are
// grafted from the parked body into the fresh one — stale inventory fields are
// never resurrected — the trash links are removed and the normal type links
// are built. Deleting an already-parked object is the physical deletion.

import (
	"fmt"
	"strings"

	"github.com/foliagecp/easyjson"
	lg "github.com/foliagecp/sdk/statefun/logger"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
)

var (
	// protectedBodyFields is the ordered list of protected top-level body keys
	// (env PROTECTED_BODY_FIELDS, comma-separated, default "usr"). The trash-can
	// restore grafts exactly these; the same list drives the write-path
	// protection of body fields.
	protectedBodyFields = parseProtectedBodyFields(system.GetEnvMustProceed[string]("PROTECTED_BODY_FIELDS", "usr"))

	// trashCanMaxObjects caps how many objects the trash can holds
	// (env OBJECT_TRASH_CAN_MAX_OBJECTS). Exceeding the cap evicts the oldest.
	trashCanMaxObjects = system.GetEnvMustProceed[int]("OBJECT_TRASH_CAN_MAX_OBJECTS", 10000)
)

func parseProtectedBodyFields(raw string) []string {
	var out []string
	for _, tok := range strings.Split(raw, ",") {
		if f := strings.TrimSpace(tok); len(f) > 0 {
			out = append(out, f)
		}
	}
	return out
}

// SetProtectedBodyFieldsForTest overrides the protected fields list; resolved
// once from the environment at package init, so tests cannot use os.Setenv.
func SetProtectedBodyFieldsForTest(fields []string) { protectedBodyFields = fields }

// SetTrashCanMaxObjectsForTest overrides the trash can capacity for tests.
func SetTrashCanMaxObjectsForTest(n int) { trashCanMaxObjects = n }

func trashCanTypeID(ctx *sfPlugins.StatefunContextProcessor) string {
	return ctx.Domain.CreateObjectIDWithHubDomain(BUILT_IN_TRASH_CAN, false)
}

func isTrashCanType(ctx *sfPlugins.StatefunContextProcessor, typeID string) bool {
	return typeID != "" && ctx.Domain.GetObjectIDWithoutDomain(typeID) == BUILT_IN_TRASH_CAN
}

// jsonNonEmpty reports whether v is worth grafting: a non-empty object, a
// non-empty array, or any other existing non-null value. Empty containers are
// skipped so an empty protected space never materializes in a body (snapshot
// diff semantics: "absent = empty").
func jsonNonEmpty(v easyjson.JSON) bool {
	if v.IsObject() {
		return v.IsNonEmptyObject()
	}
	if v.IsArray() {
		return v.IsNonEmptyArray()
	}
	return !v.IsNull()
}

// moveObjectToTrashCan re-homes a just-unlinked object under the trash-can
// type: trash_can→object (OBJECT_TYPELINK, edge body {original_type,
// deleted_at}) plus the object's own __type link to the trash can, so
// findObjectType resolves parked objects to the trash-can type. Called by
// DeleteObject under the object's write lock (trash-can type read-guarded),
// AFTER the links-only cascade wiped every previous link.
func moveObjectToTrashCan(ctx *sfPlugins.StatefunContextProcessor, selfID, originalType string, opTime int64) sfMediators.OpMsg {
	trashType := trashCanTypeID(ctx)
	linkName := ctx.Domain.GetObjectIDWithoutDomain(selfID)

	edgeBody := easyjson.NewJSONObject()
	edgeBody.SetByPath("original_type", easyjson.NewJSON(ctx.Domain.GetObjectIDWithoutDomain(originalType)))
	edgeBody.SetByPath("deleted_at", easyjson.NewJSON(opTime))
	if m := writeOutLinkKV(ctx, trashType, selfID, linkName, OBJECT_TYPELINK, &edgeBody, nil, opTime, nil); m.Status == sfMediators.SYNC_OP_STATUS_FAILED {
		return m
	}

	typeLinkBody := easyjson.NewJSONObject()
	if m := writeOutLinkKV(ctx, selfID, trashType, "type", TO_TYPELINK, &typeLinkBody, nil, opTime, nil); m.Status == sfMediators.SYNC_OP_STATUS_FAILED {
		return m
	}

	cacheSetObjectType(selfID, trashType)
	return sfMediators.OpMsgOk(easyjson.NewJSONNull())
}

// trashCanEdgeInfo reads the trash_can→object edge body of a parked object.
func trashCanEdgeInfo(ctx *sfPlugins.StatefunContextProcessor, selfID string) (originalType string, deletedAt int64) {
	trashType := trashCanTypeID(ctx)
	linkName := ctx.Domain.GetObjectIDWithoutDomain(selfID)
	if b, err := ctx.Domain.Cache().GetValueJSON(fmt.Sprintf(OutLinkBodyKeyPrefPattern+KeySuff1Pattern, trashType, linkName)); err == nil {
		return b.GetByPath("original_type").AsStringDefault(""), int64(b.GetByPath("deleted_at").AsNumericDefault(0))
	}
	return "", 0
}

// restoreObjectFromTrashCan is the returning-object path of true creation: the
// vertex EXISTS but is parked under the trash-can type. It grafts the
// protected body fields of the parked body into the incoming one (a field the
// caller passed wins — "passed = owned"), removes the two trash links and
// leaves the regular create pipeline to write the fresh body and the normal
// CMDB links. A restore under a type different from the recorded original one
// is flagged with a WARNING — an object's identity carries its type, so this
// usually means the id got reused or the model changed underneath the user.
//
// Caller (createObjectInline) holds the object's write lock; the trash links
// are removed under additional per-edge write locks accumulated into the same
// lock set (released by the caller's operationKeysMutexUnlock).
func restoreObjectFromTrashCan(ctx *sfPlugins.StatefunContextProcessor, selfID, requestedType string, incomingBody *easyjson.JSON, opTime int64) {
	trashType := trashCanTypeID(ctx)
	linkName := ctx.Domain.GetObjectIDWithoutDomain(selfID)

	originalType, _ := trashCanEdgeInfo(ctx, selfID)
	if requestedShort := ctx.Domain.GetObjectIDWithoutDomain(requestedType); originalType != "" && requestedShort != originalType {
		lg.Logf(lg.WarnLevel,
			"trash can restore: object %s was deleted as type %q but is being restored as type %q — verify this is the same object",
			selfID, originalType, requestedShort)
	}

	// Graft the protected slice of the parked body; stale inventory fields
	// stay dead — inventory brings fresh truth itself.
	parkedBody := getVertexBody(ctx, selfID)
	for _, field := range protectedBodyFields {
		if incomingBody.PathExists(field) {
			continue
		}
		if v := parkedBody.GetByPath(field); jsonNonEmpty(v) {
			incomingBody.SetByPath(field, v)
		}
	}

	// Remove the two trash links (both endpoint sides), under their edge locks.
	// NOTE: the trash-can type itself is NOT locked here — both restore-reachable
	// callers (CreateObject and the UpdateObject upsert diversion) already hold
	// its shared guard, and re-locking a key recorded in the same lock set would
	// leak a reader on unlock (the set stores mode, not a counter).
	operationKeysMutexLockMixed(ctx,
		[]string{edgeLockKey(trashType, linkName), edgeLockKey(selfID, "type")},
		nil, opTime)

	edgeBody, _ := ctx.Domain.Cache().GetValueJSON(fmt.Sprintf(OutLinkBodyKeyPrefPattern+KeySuff1Pattern, trashType, linkName))
	deleteOutLinkFromSideKeys(ctx, "functions.graph.api.link.delete", trashType, OBJECT_TYPELINK, linkName, selfID, edgeBody, nil, opTime)
	ctx.Domain.Cache().DeleteValue(fmt.Sprintf(InLinkKeyPrefPattern+KeySuff2Pattern, selfID, trashType, linkName), true, opTime)

	typeLinkBody, _ := ctx.Domain.Cache().GetValueJSON(fmt.Sprintf(OutLinkBodyKeyPrefPattern+KeySuff1Pattern, selfID, "type"))
	deleteOutLinkFromSideKeys(ctx, "functions.graph.api.link.delete", selfID, TO_TYPELINK, "type", trashType, typeLinkBody, nil, opTime)
	ctx.Domain.Cache().DeleteValue(fmt.Sprintf(InLinkKeyPrefPattern+KeySuff2Pattern, trashType, selfID, "type"), true, opTime)

	cacheDeleteObjectType(selfID) // the create pipeline re-caches the real type
}

// enforceTrashCanLimit evicts the oldest parked objects while the bin exceeds
// trashCanMaxObjects. Called AFTER the parking operation released its locks;
// best-effort — an eviction failure only defers cleanup to the next parking.
// Counting and oldest-selection are ordinary reads of the trash-can type's
// link indices in the cache.
func enforceTrashCanLimit(ctx *sfPlugins.StatefunContextProcessor) {
	trashType := trashCanTypeID(ctx)
	for i := 0; i < 8; i++ { // bounded: evict at most a few per parking
		linkNames := ctx.Domain.Cache().GetKeysByPattern(fmt.Sprintf(OutLinkBodyKeyPrefPattern+KeySuff1Pattern, trashType, ">"))
		if len(linkNames) <= trashCanMaxObjects {
			return
		}

		oldestID, oldestName, oldestAt := "", "", int64(0)
		for _, key := range linkNames {
			toks := strings.Split(key, ".")
			name := toks[len(toks)-1]
			b, err := ctx.Domain.Cache().GetValueJSON(key)
			if err != nil {
				continue
			}
			deletedAt := int64(b.GetByPath("deleted_at").AsNumericDefault(0))
			if oldestName == "" || deletedAt < oldestAt {
				if _, toId, ok := resolveOutLinkByName(ctx, trashType, name); ok {
					oldestID, oldestName, oldestAt = toId, name, deletedAt
				}
			}
		}
		if oldestName == "" {
			return
		}

		lg.Logf(lg.WarnLevel,
			"trash can is over capacity (%d > %d): permanently deleting the oldest parked object %s (deleted_at=%d)",
			len(linkNames), trashCanMaxObjects, oldestID, oldestAt)

		payload := easyjson.NewJSONObjectWithKeyValue("op_time", easyjson.NewJSON(system.GetCurrentTimeNs()))
		if _, err := ctx.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.object.delete", makeSequenceFreeParentBasedID(ctx, oldestID, "trashevict"), &payload, nil); err != nil {
			lg.Logf(lg.WarnLevel, "trash can eviction of %s failed: %v", oldestID, err)
			return
		}
	}
}
