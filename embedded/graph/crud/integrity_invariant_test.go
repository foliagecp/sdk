package crud_test

// The FULL structural invariant of a CMDB object, and the mirror invariant of
// any vertex — the two things nothing checked before.
//
// A healthy object is three edges, and every edge is four cache keys:
//
//	objects --<name>--> obj    out.to | out.body | ltype | in (on obj)
//	obj     --type-->   type   out.to | out.body | ltype | in (on type)
//	type    --<name>--> obj    out.to | out.body | ltype | in (on obj)
//
// Twelve keys. The stand incident (a live objects->obj out-side with the
// mirror in-key gone from the runtime cache) is exactly one of them missing,
// and no test in the suite could have caught it: assertObjectConsistent only
// ever looked at the owner's own out-side.
//
// assertMirrorSymmetry generalises the same idea to any vertex: an out-link
// without its in-key (or an in-key without its out-link) is a broken edge no
// matter which pair of vertices it connects.

import (
	"fmt"
	"strings"
	"testing"

	"github.com/foliagecp/sdk/embedded/graph/crud"
	"github.com/foliagecp/sdk/statefun"
	"github.com/foliagecp/sdk/statefun/cache"
	"github.com/stretchr/testify/assert"
)

// integrityKey is one expected cache key of the object skeleton.
type integrityKey struct {
	key  string // full cache key
	want string // exact expected value; "" means "must exist, value irrelevant"
	json bool   // value is JSON (checked with ExistsJson, never compared)
	desc string
}

// cmdbObjectKeys returns the twelve keys that MUST exist for objID of typeID.
// objIDShort/typeIDShort are the ids as the user knows them (no domain).
func cmdbObjectKeys(dm *statefun.Domain, objIDShort, typeIDShort string) []integrityKey {
	objID := dm.CreateObjectIDWithThisDomain(objIDShort, false)
	typeID := dm.CreateObjectIDWithHubDomain(typeIDShort, false)
	objectsID := dm.CreateObjectIDWithHubDomain(crud.BUILT_IN_OBJECTS, false)
	name := dm.GetObjectIDWithoutDomain(objID) // link name = object id without domain

	k := func(pattern string, args ...any) string { return fmt.Sprintf(pattern, args...) }

	return []integrityKey{
		// edge A: objects --name--> obj
		{k(crud.OutLinkTargetKeyPrefPattern+crud.KeySuff1Pattern, objectsID, name),
			crud.OBJECT_TYPELINK + "." + objID, false, "objects->obj out.to"},
		{k(crud.OutLinkBodyKeyPrefPattern+crud.KeySuff1Pattern, objectsID, name),
			"", true, "objects->obj out.body"},
		{k(crud.OutLinkTypeKeyPrefPattern+crud.KeySuff2Pattern, objectsID, crud.OBJECT_TYPELINK, objID),
			name, false, "objects->obj ltype"},
		{k(crud.InLinkKeyPrefPattern+crud.KeySuff2Pattern, objID, objectsID, name),
			crud.OBJECT_TYPELINK, false, "objects->obj MIRROR in-key on obj"},

		// edge B: obj --type--> type
		{k(crud.OutLinkTargetKeyPrefPattern+crud.KeySuff1Pattern, objID, "type"),
			crud.TO_TYPELINK + "." + typeID, false, "obj->type out.to"},
		{k(crud.OutLinkBodyKeyPrefPattern+crud.KeySuff1Pattern, objID, "type"),
			"", true, "obj->type out.body"},
		{k(crud.OutLinkTypeKeyPrefPattern+crud.KeySuff2Pattern, objID, crud.TO_TYPELINK, typeID),
			"type", false, "obj->type ltype"},
		{k(crud.InLinkKeyPrefPattern+crud.KeySuff2Pattern, typeID, objID, "type"),
			crud.TO_TYPELINK, false, "obj->type MIRROR in-key on type"},

		// edge C: type --name--> obj
		{k(crud.OutLinkTargetKeyPrefPattern+crud.KeySuff1Pattern, typeID, name),
			crud.OBJECT_TYPELINK + "." + objID, false, "type->obj out.to"},
		{k(crud.OutLinkBodyKeyPrefPattern+crud.KeySuff1Pattern, typeID, name),
			"", true, "type->obj out.body"},
		{k(crud.OutLinkTypeKeyPrefPattern+crud.KeySuff2Pattern, typeID, crud.OBJECT_TYPELINK, objID),
			name, false, "type->obj ltype"},
		{k(crud.InLinkKeyPrefPattern+crud.KeySuff2Pattern, objID, typeID, name),
			crud.OBJECT_TYPELINK, false, "type->obj MIRROR in-key on obj"},
	}
}

// missingIntegrityKeys returns the human-readable descriptions of every
// skeleton key that is absent or holds the wrong value.
func missingIntegrityKeys(c *cache.Store, dm *statefun.Domain, objIDShort, typeIDShort string) []string {
	var broken []string
	for _, ik := range cmdbObjectKeys(dm, objIDShort, typeIDShort) {
		if ik.json {
			if !c.ExistsJson(ik.key) {
				broken = append(broken, ik.desc+" ["+ik.key+"] MISSING")
			}
			continue
		}
		v, err := c.GetValue(ik.key)
		if err != nil {
			broken = append(broken, ik.desc+" ["+ik.key+"] MISSING")
			continue
		}
		if ik.want != "" && string(v) != ik.want {
			broken = append(broken, fmt.Sprintf("%s [%s] = %q, want %q", ik.desc, ik.key, string(v), ik.want))
		}
	}
	return broken
}

// assertObjectIntegrity fails with the exact list of broken skeleton keys.
func assertObjectIntegrity(t *testing.T, c *cache.Store, dm *statefun.Domain, objIDShort, typeIDShort, context string) {
	t.Helper()
	if broken := missingIntegrityKeys(c, dm, objIDShort, typeIDShort); len(broken) > 0 {
		assert.Failf(t, "object skeleton is broken",
			"%s\nobject=%q type=%q\n  - %s", context, objIDShort, typeIDShort, strings.Join(broken, "\n  - "))
	}
}

// mirrorBreaks returns every edge of vertexID whose two halves disagree:
// an out-link without the in-key on its target, or an in-key without the
// out-link on its source. Works for any vertex, CMDB object or not.
func mirrorBreaks(c *cache.Store, vertexID string) []string {
	var breaks []string

	// out-links of vertexID -> mirror in-key must exist on the target
	for _, key := range c.GetKeysByPattern(fmt.Sprintf(crud.OutLinkTargetKeyPrefPattern+"%s", vertexID, ">")) {
		tokens := strings.Split(key, ".")
		linkName := tokens[len(tokens)-1]
		raw, err := c.GetValue(key)
		if err != nil {
			continue
		}
		parts := strings.SplitN(string(raw), ".", 2)
		if len(parts) != 2 {
			breaks = append(breaks, fmt.Sprintf("out.to %q holds malformed target %q", linkName, string(raw)))
			continue
		}
		linkType, toID := parts[0], parts[1]
		inKey := fmt.Sprintf(crud.InLinkKeyPrefPattern+crud.KeySuff2Pattern, toID, vertexID, linkName)
		if _, err := c.GetValue(inKey); err != nil {
			breaks = append(breaks, fmt.Sprintf("out-link %q (%s -> %s) has NO mirror in-key [%s]", linkName, linkType, toID, inKey))
		}
	}

	// in-keys of vertexID -> the source must still own the out-link
	for _, key := range c.GetKeysByPattern(fmt.Sprintf(crud.InLinkKeyPrefPattern+"%s", vertexID, ">")) {
		tokens := strings.Split(key, ".")
		if len(tokens) < 2 {
			continue
		}
		linkName := tokens[len(tokens)-1]
		fromID := tokens[len(tokens)-2]
		outKey := fmt.Sprintf(crud.OutLinkTargetKeyPrefPattern+crud.KeySuff1Pattern, fromID, linkName)
		if _, err := c.GetValue(outKey); err != nil {
			breaks = append(breaks, fmt.Sprintf("in-key from %q name %q has NO out-link on the source [%s]", fromID, linkName, outKey))
		}
	}
	return breaks
}

// assertMirrorSymmetry fails with every broken half-edge of the vertex.
func assertMirrorSymmetry(t *testing.T, c *cache.Store, vertexID, context string) {
	t.Helper()
	if breaks := mirrorBreaks(c, vertexID); len(breaks) > 0 {
		assert.Failf(t, "vertex has half-edges (in/out mirror broken)",
			"%s\nvertex=%q\n  - %s", context, vertexID, strings.Join(breaks, "\n  - "))
	}
}
