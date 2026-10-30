package crud

import (
	"fmt"

	"github.com/foliagecp/easyjson"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
)

// A surviving body and the object's own type pointer are the authority for
// structural repair. Memberships on other vertices never recreate a body or
// choose a type for an untyped object. The ordinary low-level isolated vertex
// is therefore left alone.
func repairExistingObjectStructure(ctx *sfPlugins.StatefunContextProcessor, id, typeID string, opTime int64) (bool, error) {
	c := ctx.Domain.Cache()
	if ctx.Domain.GetDomainFromObjectID(id) != ctx.Domain.Name() || ctx.Domain.GetDomainFromObjectID(typeID) != ctx.Domain.Name() {
		return true, nil
	}
	type edge struct{ from, to, name, kind string }
	name := ctx.Domain.GetObjectIDWithoutDomain(id)
	edges := []edge{{id, typeID, "type", TO_TYPELINK}, {typeID, id, name, OBJECT_TYPELINK}}
	objectsID := ctx.Domain.CreateObjectIDWithHubDomain(BUILT_IN_OBJECTS, false)
	if !isTrashCanType(ctx, typeID) && ctx.Domain.GetDomainFromObjectID(objectsID) == ctx.Domain.Name() {
		edges = append(edges, edge{objectsID, id, name, OBJECT_TYPELINK})
	}
	linkBodyExists := func(key string) bool {
		if c.ExistsJson(key) {
			return true
		}
		body, err := c.GetValueJSON(key)
		return err == nil && body.IsObject()
	}
	// Most reads need no repair or locks beyond their normal read path.
	complete := func() bool {
		for _, e := range edges {
			kind, to, ok := lookupOutLinkByName(ctx.Domain, e.from, e.name)
			pair, _ := c.GetValue(fmt.Sprintf(OutLinkTypeKeyPrefPattern+KeySuff2Pattern, e.from, e.kind, e.to))
			mirror, _ := c.GetValue(fmt.Sprintf(InLinkKeyPrefPattern+KeySuff2Pattern, e.to, e.from, e.name))
			if !ok || kind != e.kind || to != e.to || string(pair) != e.name || string(mirror) != e.kind ||
				!linkBodyExists(fmt.Sprintf(OutLinkBodyKeyPrefPattern+KeySuff1Pattern, e.from, e.name)) ||
				!c.Exists(fmt.Sprintf(OutLinkIndexPrefPattern+KeySuff3Pattern, e.from, e.name, "type", e.kind)) {
				return false
			}
		}
		return true
	}
	if complete() {
		return true, nil
	}
	operationKeysMutexLock(ctx, []string{id}, true, opTime)
	defer operationKeysMutexUnlock(ctx)
	operationKeysMutexLock(ctx, []string{typeID, objectsID}, false, opTime)
	kind, actualType, ok := lookupOutLinkByName(ctx.Domain, id, "type")
	if !c.ExistsJson(id) || !c.ExistsJson(typeID) || !ok || kind != TO_TYPELINK || actualType != typeID {
		return false, nil
	}
	if complete() {
		return true, nil // the operation we waited for finished its links
	}
	// Repair is a new mutation derived from the state just checked under
	// lock, not a replay of the earlier read's snapshot. Otherwise an
	// intervening restore makes a harmless read fail as a stale writer.
	opTime = system.GetCurrentTimeNs()
	c.MarkOperationActive(opTime)
	defer c.MarkOperationDone(opTime)
	for _, e := range edges {
		if !c.ExistsJson(e.from) || !c.ExistsJson(e.to) {
			return false, fmt.Errorf("structural repair requires existing endpoint vertices")
		}
		if kind, target, ok := lookupOutLinkByName(ctx.Domain, e.from, e.name); ok && (kind != e.kind || target != e.to) {
			return false, fmt.Errorf("structural repair found a conflicting link target")
		}
		bodyKey := fmt.Sprintf(OutLinkBodyKeyPrefPattern+KeySuff1Pattern, e.from, e.name)
		if !linkBodyExists(bodyKey) && (c.Exists(bodyKey) || e.from == trashCanTypeID(ctx)) {
			// A missing trash edge has lost its deletion time and original
			// type. Inventing either would change restoration/retention.
			return false, fmt.Errorf("structural link metadata is unavailable; repair cannot infer its contents")
		}
		if deletionKeysAreStale(ctx, deletionEdgeKeys(ctx, e.from, e.to, e.name, e.kind), opTime) {
			return false, fmt.Errorf("structural repair is older than stored values")
		}
	}
	for _, e := range edges {
		values := map[string][]byte{
			fmt.Sprintf(OutLinkTargetKeyPrefPattern+KeySuff1Pattern, e.from, e.name):     []byte(e.kind + "." + e.to),
			fmt.Sprintf(OutLinkTypeKeyPrefPattern+KeySuff2Pattern, e.from, e.kind, e.to): []byte(e.name),
			fmt.Sprintf(InLinkKeyPrefPattern+KeySuff2Pattern, e.to, e.from, e.name):      []byte(e.kind),
		}
		for key, value := range values {
			if old, err := c.GetValue(key); err != nil || string(old) != string(value) {
				if !c.SetValue(key, value, true, opTime) {
					return false, fmt.Errorf("structural repair write was refused")
				}
			}
		}
		index := fmt.Sprintf(OutLinkIndexPrefPattern+KeySuff3Pattern, e.from, e.name, "type", e.kind)
		if !c.Exists(index) && !c.SetValue(index, nil, true, opTime) {
			return false, fmt.Errorf("structural index repair was refused")
		}
		bodyKey := fmt.Sprintf(OutLinkBodyKeyPrefPattern+KeySuff1Pattern, e.from, e.name)
		if !linkBodyExists(bodyKey) {
			// Link metadata can be absent; the vertex body is never touched.
			body := easyjson.NewJSONObject()
			if !c.SetValueJSON(bodyKey, &body, true, opTime) {
				return false, fmt.Errorf("structural link repair was refused")
			}
		}
	}
	return true, nil
}
