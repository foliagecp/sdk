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
// Retention has TWO dimensions, both configurable, both enforced by physically
// deleting what falls out (and logging it):
//
//	age   — OBJECT_TRASH_CAN_MAX_AGE_SEC (default 7 days): the product-level
//	        "configurable retention period" a parked object is guaranteed;
//	set 0 to disable either dimension.
//	count — OBJECT_TRASH_CAN_MAX_OBJECTS (default 10000): bounds growth under
//	        heavy churn, evicting the oldest parked objects first;
//
// Enforcement runs both after each parking (prompt count enforcement) and from
// a periodic per-domain sweep (OBJECT_TRASH_CAN_SWEEP_INTERVAL_SEC, active
// instance only) — without the sweep the age dimension would depend on delete
// traffic. Counting/eviction are ordinary graph reads off the trash-can type's
// link indices.
//
// A TRUE re-creation of a parked id (plain create and upsert both) restores
// it: the PROTECTED body fields (as the graph declares them, see
// protected_fields.go) are grafted from the parked body into the fresh one —
// stale inventory fields are never resurrected — the trash links are removed
// and the normal type links are built. Deleting an already-parked object is the
// physical deletion.

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/statefun"
	lg "github.com/foliagecp/sdk/statefun/logger"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
)

// Retention tunables. Stored atomically: the periodic sweep goroutine reads
// them concurrently with the test setters below (and any future runtime
// reconfiguration), so plain package vars would be a data race.
var (
	// trashCanMaxAgeNs is the retention PERIOD of a parked object
	// (env OBJECT_TRASH_CAN_MAX_AGE_SEC, default 7 days; 0 disables the
	// dimension). This is the product-level "configurable retention" — user
	// knowledge outlives an object's disappearance for at least this long.
	trashCanMaxAgeNs atomic.Int64
	// trashCanMaxObjectsV caps how many objects the trash can holds
	// (env OBJECT_TRASH_CAN_MAX_OBJECTS, 0 disables the dimension). It bounds
	// growth under heavy churn, where the age dimension alone would keep
	// everything; exceeding the cap evicts the oldest.
	trashCanMaxObjectsV atomic.Int64
	// trashCanSweepIntervalNs is how often the periodic retention sweep runs
	// (env OBJECT_TRASH_CAN_SWEEP_INTERVAL_SEC, default 5 min).
	trashCanSweepIntervalNs atomic.Int64
	// trashCanEvictBatchSizeV caps how many objects one retention run evicts
	// (env OBJECT_TRASH_CAN_EVICT_BATCH_SIZE, default 64; <= 0 means unbounded),
	// so a lowered cap or a long outage cannot turn a single run into a storm;
	// the remainder is logged and picked up by the next sweep.
	trashCanEvictBatchSizeV atomic.Int64
)

func init() {
	trashCanMaxAgeNs.Store(int64(time.Duration(system.GetEnvMustProceed[int]("OBJECT_TRASH_CAN_MAX_AGE_SEC", 7*24*3600)) * time.Second))
	trashCanMaxObjectsV.Store(int64(system.GetEnvMustProceed[int]("OBJECT_TRASH_CAN_MAX_OBJECTS", 10000)))
	trashCanSweepIntervalNs.Store(int64(time.Duration(system.GetEnvMustProceed[int]("OBJECT_TRASH_CAN_SWEEP_INTERVAL_SEC", 300)) * time.Second))
	trashCanEvictBatchSizeV.Store(int64(system.GetEnvMustProceed[int]("OBJECT_TRASH_CAN_EVICT_BATCH_SIZE", 64)))
}

func trashCanMaxAge() time.Duration        { return time.Duration(trashCanMaxAgeNs.Load()) }
func trashCanMaxObjects() int              { return int(trashCanMaxObjectsV.Load()) }
func trashCanSweepInterval() time.Duration { return time.Duration(trashCanSweepIntervalNs.Load()) }
func trashCanEvictBatchSize() int          { return int(trashCanEvictBatchSizeV.Load()) }

// SetTrashCanMaxObjectsForTest overrides the trash can capacity for tests.
func SetTrashCanMaxObjectsForTest(n int) { trashCanMaxObjectsV.Store(int64(n)) }

// SetTrashCanMaxAgeForTest overrides the trash can retention period for tests.
func SetTrashCanMaxAgeForTest(d time.Duration) { trashCanMaxAgeNs.Store(int64(d)) }

// SetTrashCanSweepIntervalForTest overrides the periodic sweep interval; call it
// BEFORE the runtime starts (the ticker is created once, at sweep startup).
func SetTrashCanSweepIntervalForTest(d time.Duration) { trashCanSweepIntervalNs.Store(int64(d)) }

// SetTrashCanEvictBatchSizeForTest overrides the per-run eviction cap for tests
// (<= 0 means unbounded).
func SetTrashCanEvictBatchSizeForTest(n int) { trashCanEvictBatchSizeV.Store(int64(n)) }

func trashCanTypeID(ctx *sfPlugins.StatefunContextProcessor) string {
	return ctx.Domain.CreateObjectIDWithHubDomain(BUILT_IN_TRASH_CAN, false)
}

func isTrashCanType(ctx *sfPlugins.StatefunContextProcessor, typeID string) bool {
	return typeID != "" && ctx.Domain.GetObjectIDWithoutDomain(typeID) == BUILT_IN_TRASH_CAN
}

// moveObjectToTrashCan re-homes a just-unlinked object under the trash-can
// type: trash-can→object (OBJECT_TYPELINK, edge body {original_type,
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

// trashCanEdgeInfo reads the trash-can→object edge body of a parked object.
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
	for _, field := range ctx.Domain.ProtectedBodyFields() {
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

// trashCanEntry is one parked object as recorded on the trash-can type's edges.
type trashCanEntry struct {
	objectID  string
	deletedAt int64
}

// listTrashCanEntries lists the parked objects of THIS domain, oldest first, in
// a single cache scan (the trash-can edges of a domain's objects live in that
// domain's own cache — same as any type→object link). No round-trips.
func listTrashCanEntries(dm sfPlugins.Domain) []trashCanEntry {
	trashType := dm.CreateObjectIDWithHubDomain(BUILT_IN_TRASH_CAN, false)
	keys := dm.Cache().GetKeysByPattern(fmt.Sprintf(OutLinkBodyKeyPrefPattern+KeySuff1Pattern, trashType, ">"))

	entries := make([]trashCanEntry, 0, len(keys))
	for _, key := range keys {
		toks := strings.Split(key, ".")
		name := toks[len(toks)-1]
		_, objectID, ok := resolveOutLinkByNameInDomain(dm, trashType, name)
		if !ok {
			continue
		}
		deletedAt := int64(0)
		if b, err := dm.Cache().GetValueJSON(key); err == nil {
			deletedAt = int64(b.GetByPath("deleted_at").AsNumericDefault(0))
		}
		entries = append(entries, trashCanEntry{objectID: objectID, deletedAt: deletedAt})
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].deletedAt < entries[j].deletedAt })
	return entries
}

// enforceTrashCanRetention applies BOTH retention dimensions to this domain's
// bin and physically deletes what falls out:
//
//	age   — parked longer than OBJECT_TRASH_CAN_MAX_AGE_SEC (the product-level
//	        "configurable retention period"); 0 disables the dimension;
//	count — while still over OBJECT_TRASH_CAN_MAX_OBJECTS, the oldest remaining
//	        ones (protects the model from unbounded growth under churn, where the
//	        age dimension alone would keep everything); 0 disables it.
//
// Called after a parking released its locks (prompt count enforcement) and from
// the periodic sweep (so the age dimension holds even with no delete traffic).
// Best-effort: an eviction failure only defers cleanup to the next run. Work is
// capped per invocation at trashCanEvictBatchSize, and a truncated batch is
// logged — never silently dropped.
func enforceTrashCanRetention(dm sfPlugins.Domain, request sfPlugins.SFRequestFunc, evictID func(string) string, now int64) {
	entries := listTrashCanEntries(dm)

	type victim struct {
		entry  trashCanEntry
		reason string
	}
	var victims []victim
	evicted := map[string]struct{}{}

	maxAge, maxObjects := trashCanMaxAge(), trashCanMaxObjects()
	if maxAge > 0 {
		cutoff := now - maxAge.Nanoseconds()
		for _, e := range entries {
			if e.deletedAt > 0 && e.deletedAt < cutoff {
				victims = append(victims, victim{e, "retention age exceeded"})
				evicted[e.objectID] = struct{}{}
			}
		}
	}
	if maxObjects > 0 {
		over := len(entries) - len(evicted) - maxObjects
		for _, e := range entries {
			if over <= 0 {
				break
			}
			if _, done := evicted[e.objectID]; done {
				continue
			}
			victims = append(victims, victim{e, "object count over capacity"})
			evicted[e.objectID] = struct{}{}
			over--
		}
	}
	if len(victims) == 0 {
		return
	}

	deferred := 0
	if batch := trashCanEvictBatchSize(); batch > 0 && len(victims) > batch {
		deferred = len(victims) - batch
		victims = victims[:batch]
	}

	for _, v := range victims {
		lg.Logf(lg.WarnLevel,
			"trash can: permanently deleting parked object %s (%s; parked=%d, max_objects=%d, max_age=%s)",
			v.entry.objectID, v.reason, len(entries), maxObjects, maxAge)

		// Low-level: the object API cannot touch what is in the bin (a parked
		// object does not exist for it), and there is nothing left for it to do
		// anyway — the links are gone and the delete event was dispatched when
		// the object was parked. What remains is the vertex.
		payload := easyjson.NewJSONObjectWithKeyValue("op_time", easyjson.NewJSON(system.GetCurrentTimeNs()))
		if _, err := request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.delete", evictID(v.entry.objectID), &payload, nil); err != nil {
			lg.Logf(lg.WarnLevel, "trash can eviction of %s failed: %v; retrying on the next sweep", v.entry.objectID, err)
			return
		}
	}
	if deferred > 0 {
		lg.Logf(lg.WarnLevel, "trash can: %d more objects still due for eviction, deferred to the next sweep", deferred)
	}
}

// enforceTrashCanRetentionFromStatefun runs the retention from inside a CRUD
// handler (the parking path). Eviction targets a DIFFERENT object than the one
// being handled, so its id is made sequence-free the usual way to keep the
// nested delete off this handler's per-id worker.
func enforceTrashCanRetentionFromStatefun(ctx *sfPlugins.StatefunContextProcessor) {
	enforceTrashCanRetention(ctx.Domain, ctx.Request,
		func(objectID string) string { return makeSequenceFreeParentBasedID(ctx, objectID, "trashevict") },
		system.GetCurrentTimeNs())
}

// trashCanRetentionSweep starts the periodic retention sweep of this domain's
// bin. Registered as a (non-async) after-start hook: it only spawns the ticker
// goroutine and returns, so it never holds the after-start wait group. The
// goroutine exits when the runtime's phase-one context is cancelled (shutdown),
// and only acts on the ACTIVE instance so HA peers do not evict in parallel.
//
// Without this sweep the age dimension would depend on delete traffic:
// enforcement from the parking path alone means a bin that stops receiving
// deletions keeps its content past the retention period indefinitely.
func trashCanRetentionSweep(ctx context.Context, runtime *statefun.Runtime) error {
	if trashCanMaxAge() <= 0 && trashCanMaxObjects() <= 0 {
		return nil // both dimensions disabled — nothing to sweep
	}
	go func() {
		system.GlobalPrometrics.GetRoutinesCounter().Started("crud_trash_can_sweep")
		defer system.GlobalPrometrics.GetRoutinesCounter().Stopped("crud_trash_can_sweep")

		ticker := time.NewTicker(trashCanSweepInterval())
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if ctx.Err() != nil || !runtime.IsActiveInstance() {
					continue
				}
				enforceTrashCanRetention(runtime.Domain, runtime.Request,
					func(objectID string) string { return objectID },
					system.GetCurrentTimeNs())
			}
		}
	}()
	return nil
}
