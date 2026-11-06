package crud

import (
	"fmt"
	"strings"

	"github.com/foliagecp/easyjson"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
)

// Only link families belong to the cascade. An isolated vertex body is valid
// low-level data and must not be treated as garbage merely for lacking links.
func vertexLinkResidue(ctx *sfPlugins.StatefunContextProcessor, id string) []string {
	var keys []string
	for _, key := range ctx.Domain.Cache().GetKeysByPattern(id + ".>") {
		if strings.HasPrefix(key, id+".out.") || strings.HasPrefix(key, id+".ltype.") || strings.HasPrefix(key, id+".in.") {
			keys = append(keys, key)
		}
	}
	return keys
}

func deletionKeysAreStale(ctx *sfPlugins.StatefunContextProcessor, keys []string, opTime int64) bool {
	for _, key := range keys {
		if ctx.Domain.Cache().GetValueUpdateTime(key) > opTime {
			return true
		}
	}
	return false
}

func deleteRecoveryKey(ctx *sfPlugins.StatefunContextProcessor, key string, opTime int64) bool {
	ctx.Domain.Cache().DeleteValue(key, true, opTime)
	return !ctx.Domain.Cache().Exists(key)
}

func deletionEdgeKeys(ctx *sfPlugins.StatefunContextProcessor, from, to, name, kind string) []string {
	keys := ctx.Domain.Cache().GetKeysByPattern(fmt.Sprintf(OutLinkIndexPrefPattern+KeySuff2Pattern, from, name, ">"))
	keys = append(keys,
		fmt.Sprintf(OutLinkBodyKeyPrefPattern+KeySuff1Pattern, from, name),
		fmt.Sprintf(OutLinkTargetKeyPrefPattern+KeySuff1Pattern, from, name),
		fmt.Sprintf(OutLinkTypeKeyPrefPattern+KeySuff2Pattern, from, kind, to))
	if ctx.Domain.GetDomainFromObjectID(to) == ctx.Domain.Name() {
		keys = append(keys, fmt.Sprintf(InLinkKeyPrefPattern+KeySuff2Pattern, to, from, name))
	}
	return keys
}

func deletionKeysRemain(ctx *sfPlugins.StatefunContextProcessor, keys []string) bool {
	for _, key := range keys {
		if ctx.Domain.Cache().Exists(key) {
			return true
		}
	}
	return false
}

// An incoming anchor may outlive a retargeted edge with the same name.
// Remove only the old endpoint's evidence in that case; never follow the name
// blindly and delete the live replacement edge.
func deleteIncomingLocalResidue(ctx *sfPlugins.StatefunContextProcessor, target, owner, name string, opTime int64, opStack *easyjson.JSON) error {
	operationKeysMutexLock(ctx, []string{owner, target}, true, opTime)
	defer operationKeysMutexUnlock(ctx)
	c := ctx.Domain.Cache()
	inKey := fmt.Sprintf(InLinkKeyPrefPattern+KeySuff2Pattern, target, owner, name)
	oldKind, _ := c.GetValue(inKey)
	kind, to, found := resolveOutLinkByName(ctx, owner, name)
	keys := []string{inKey}
	if found && to == target {
		keys = append(keys, deletionEdgeKeys(ctx, owner, target, name, kind)...)
	} else if !found {
		keys = append(keys, c.GetKeysByPattern(fmt.Sprintf(OutLinkIndexPrefPattern+KeySuff2Pattern, owner, name, ">"))...)
		keys = append(keys, fmt.Sprintf(OutLinkBodyKeyPrefPattern+KeySuff1Pattern, owner, name), fmt.Sprintf(OutLinkTargetKeyPrefPattern+KeySuff1Pattern, owner, name))
	}
	if len(oldKind) > 0 {
		oldPair := fmt.Sprintf(OutLinkTypeKeyPrefPattern+KeySuff2Pattern, owner, string(oldKind), target)
		if value, err := c.GetValue(oldPair); err == nil && string(value) == name {
			keys = append(keys, oldPair)
		}
	}
	if deletionKeysAreStale(ctx, keys, opTime) {
		return fmt.Errorf("stale incoming-link deletion: %s", name)
	}
	// The body goes with the keys below; capture it first so link-delete
	// triggers of a cascaded deletion still carry old_body.
	var oldLinkBody *easyjson.JSON
	if found && to == target && opStack != nil {
		oldLinkBody, _ = c.GetValueJSON(fmt.Sprintf(OutLinkBodyKeyPrefPattern+KeySuff1Pattern, owner, name))
	}
	// Keep the incoming anchor until all other removals have succeeded.
	for _, key := range keys[1:] {
		if !deleteRecoveryKey(ctx, key, opTime) {
			return fmt.Errorf("incoming-link cleanup was refused: %s", name)
		}
	}
	if !deleteRecoveryKey(ctx, inKey, opTime) {
		return fmt.Errorf("incoming-link anchor cleanup was refused: %s", name)
	}
	if found && to == target {
		if kind == TO_TYPELINK {
			cacheDeleteObjectType(owner)
		}
		addLinkOpToOpStack(opStack, "functions.graph.api.link.delete", owner, target, name, kind, oldLinkBody, nil)
	}
	return nil
}

// The ordinary body-key walk cannot reach a bare target, ltype or index entry.
// Enumerate only this vertex's remaining keys and remove every addressable
// endpoint, including obsolete ltype entries which share a link name.
func recoverVertexDeleteResidue(ctx *sfPlugins.StatefunContextProcessor, id string, opTime int64, opStack *easyjson.JSON) error {
	type edge struct{ kind, to, name string }
	edges := map[edge]bool{}
	names := map[string]bool{}
	for _, key := range vertexLinkResidue(ctx, id) {
		switch {
		case strings.HasPrefix(key, id+".ltype."):
			parts := strings.SplitN(strings.TrimPrefix(key, id+".ltype."), ".", 2)
			name, err := ctx.Domain.Cache().GetValue(key)
			if len(parts) != 2 || err != nil {
				return fmt.Errorf("cannot resolve residual link %s", key)
			}
			edges[edge{parts[0], parts[1], string(name)}] = true
			names[string(name)] = true
		case strings.HasPrefix(key, id+".out.to."):
			name := strings.TrimPrefix(key, id+".out.to.")
			value, err := ctx.Domain.Cache().GetValue(key)
			parts := strings.SplitN(string(value), ".", 2)
			if err != nil || len(parts) != 2 {
				return fmt.Errorf("cannot resolve residual target %s", key)
			}
			edges[edge{parts[0], parts[1], name}] = true
			names[name] = true
		case strings.HasPrefix(key, id+".out.body."):
			names[strings.TrimPrefix(key, id+".out.body.")] = true
		case strings.HasPrefix(key, id+".out.index."):
			name := strings.SplitN(strings.TrimPrefix(key, id+".out.index."), ".", 2)[0]
			names[name] = true
		}
	}
	for e := range edges {
		if ctx.Domain.GetDomainFromObjectID(e.to) != ctx.Domain.Name() {
			payload := easyjson.NewJSONObjectWithKeyValue("in_name", easyjson.NewJSON(e.name))
			payload.SetByPath("op_time", easyjson.NewJSON(opTime))
			m := sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.delete", makeSequenceFreeParentBasedID(ctx, e.to, "residue"), injectParentHoldsLocks(ctx, &payload), ctx.Options))
			if m.Status != sfMediators.SYNC_OP_STATUS_OK && m.Status != sfMediators.SYNC_OP_STATUS_IDLE {
				return fmt.Errorf("residual remote link cleanup failed: %s", m.Details)
			}
		}
		keys := []string{id}
		if ctx.Domain.GetDomainFromObjectID(e.to) == ctx.Domain.Name() {
			keys = append(keys, e.to)
		}
		operationKeysMutexLock(ctx, keys, true, opTime)
		edgeKeys := deletionEdgeKeys(ctx, id, e.to, e.name, e.kind)
		if deletionKeysAreStale(ctx, edgeKeys, opTime) {
			operationKeysMutexUnlock(ctx)
			return fmt.Errorf("stale deletion of residual link %s", e.name)
		}
		body, _ := ctx.Domain.Cache().GetValueJSON(fmt.Sprintf(OutLinkBodyKeyPrefPattern+KeySuff1Pattern, id, e.name))
		deleteOutLinkFromSideKeys(ctx, "functions.graph.api.link.delete", id, e.kind, e.name, e.to, body, opStack, opTime)
		if ctx.Domain.GetDomainFromObjectID(e.to) == ctx.Domain.Name() {
			deleteRecoveryKey(ctx, fmt.Sprintf(InLinkKeyPrefPattern+KeySuff2Pattern, e.to, id, e.name), opTime)
		}
		left := deletionKeysRemain(ctx, edgeKeys)
		operationKeysMutexUnlock(ctx)
		if left {
			return fmt.Errorf("residual link %s was not fully removed", e.name)
		}
	}
	for name := range names {
		operationKeysMutexLock(ctx, []string{id}, true, opTime)
		keys := ctx.Domain.Cache().GetKeysByPattern(fmt.Sprintf(OutLinkIndexPrefPattern+KeySuff2Pattern, id, name, ">"))
		keys = append(keys, fmt.Sprintf(OutLinkBodyKeyPrefPattern+KeySuff1Pattern, id, name), fmt.Sprintf(OutLinkTargetKeyPrefPattern+KeySuff1Pattern, id, name))
		if deletionKeysAreStale(ctx, keys, opTime) {
			operationKeysMutexUnlock(ctx)
			return fmt.Errorf("stale deletion of residual link keys %s", name)
		}
		deleteOutLinkKeysByName(ctx, id, name, opTime)
		left := deletionKeysRemain(ctx, keys)
		operationKeysMutexUnlock(ctx)
		if left {
			return fmt.Errorf("residual link keys %s were not removed", name)
		}
	}
	return nil
}

// A local object's body and reverse type link decide whether it belongs in a
// type listing. Recheck damage under the object lock so an in-flight create,
// move or restore can finish before read repair removes any reference.
// Missing local cache entries say nothing about remote objects.
func repairTypeReadMember(ctx *sfPlugins.StatefunContextProcessor, typeID, objectID, name string, opTime int64) (bool, error) {
	if ctx.Domain.GetDomainFromObjectID(objectID) != ctx.Domain.Name() {
		return true, nil
	}
	valid := func() bool {
		kind, target, ok := lookupOutLinkByName(ctx.Domain, objectID, "type")
		return ctx.Domain.Cache().ExistsJson(objectID) && ctx.Domain.Cache().ExistsJson(typeID) && ok && kind == TO_TYPELINK && target == typeID
	}
	if valid() {
		return repairExistingObjectStructure(ctx, objectID, typeID, opTime)
	}
	operationKeysMutexLock(ctx, []string{objectID}, true, opTime)
	defer operationKeysMutexUnlock(ctx)
	if valid() {
		return true, nil
	}
	c := ctx.Domain.Cache()
	// Revalidation under the object lock proves this membership obsolete.
	// Stamp the repair now rather than replaying the read's earlier time.
	opTime = system.GetCurrentTimeNs()
	c.MarkOperationActive(opTime)
	defer c.MarkOperationDone(opTime)
	var keys []string
	if kind, target, ok := lookupOutLinkByName(ctx.Domain, typeID, name); ok && kind == OBJECT_TYPELINK && target == objectID {
		keys = c.GetKeysByPattern(fmt.Sprintf(OutLinkIndexPrefPattern+KeySuff2Pattern, typeID, name, ">"))
		keys = append(keys, fmt.Sprintf(OutLinkBodyKeyPrefPattern+KeySuff1Pattern, typeID, name), fmt.Sprintf(OutLinkTargetKeyPrefPattern+KeySuff1Pattern, typeID, name))
	}
	membership := fmt.Sprintf(OutLinkTypeKeyPrefPattern+KeySuff2Pattern, typeID, OBJECT_TYPELINK, objectID)
	if value, err := c.GetValue(membership); err == nil && string(value) == name {
		keys = append(keys, membership)
	}
	mirror := fmt.Sprintf(InLinkKeyPrefPattern+KeySuff2Pattern, objectID, typeID, name)
	if value, err := c.GetValue(mirror); err == nil && string(value) == OBJECT_TYPELINK {
		keys = append(keys, mirror)
	}
	// A parked object may still carry an obsolete reverse ltype entry from
	// the old type. Never remove the common 'type' link now pointing at the bin.
	_, actualType, _ := lookupOutLinkByName(ctx.Domain, objectID, "type")
	if actualType != typeID || !c.ExistsJson(objectID) {
		reverse := fmt.Sprintf(OutLinkTypeKeyPrefPattern+KeySuff2Pattern, objectID, TO_TYPELINK, typeID)
		if value, err := c.GetValue(reverse); err == nil && string(value) == "type" {
			keys = append(keys, reverse)
		}
		reverseMirror := fmt.Sprintf(InLinkKeyPrefPattern+KeySuff2Pattern, typeID, objectID, "type")
		if value, err := c.GetValue(reverseMirror); err == nil && string(value) == TO_TYPELINK {
			keys = append(keys, reverseMirror)
		}
	}
	if deletionKeysAreStale(ctx, keys, opTime) {
		return false, fmt.Errorf("type membership repair is older than stored values")
	}
	for _, key := range keys {
		if !deleteRecoveryKey(ctx, key, opTime) {
			return false, fmt.Errorf("type membership repair was refused")
		}
	}
	// The graph's current type pointer won over this stale membership. Any
	// process-local type lookup derived from the old graph must be discarded
	// too, including for a surviving body which is already in the trash can.
	cacheDeleteObjectType(objectID)
	return false, nil
}
