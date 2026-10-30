package crud_test

// An object whose body is gone while its links stayed.
//
// The type of an object resolves from its __type link alone, so such an
// object still reads as typed, and the delete used to park it: the links-only
// cascade answered IDLE ("vertex does not exist") without touching a single
// link, the trash links were hung on the missing vertex, and the reply was OK.
// Every later delete then saw a parked object and answered IDLE too — the old
// links stayed for good, and the bin held a vertex that was not there.
//
// A delete of such an object must leave no key of it anywhere.

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/embedded/graph/crud"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
)

// residueOf lists every key of the given vertices' subtrees (and the vertices'
// own bodies) that still mentions objectID.
func (s *TrashCanTestSuite) residueOf(objectID string, vertices ...string) []string {
	c := s.Runtime().Domain.Cache()
	var left []string
	if c.ExistsJson(objectID) {
		left = append(left, objectID)
	}
	for _, v := range vertices {
		for _, k := range c.GetKeysByPattern(v + ".>") {
			if mentionsObject(k, objectID) {
				left = append(left, k)
			}
		}
	}
	return left
}

// mentionsObject reports whether a cache key refers to objectID: by the full
// id, or by its short form as a whole token — a type and `objects` name their
// link to an object after the object without its domain
// (`<type>.out.to.<short id>`).
func mentionsObject(key, objectID string) bool {
	if strings.Contains(key, objectID) {
		return true
	}
	short := objectID[strings.LastIndex(objectID, "/")+1:]
	for _, tok := range strings.Split(key, ".") {
		if tok == short {
			return true
		}
	}
	return false
}

func (s *TrashCanTestSuite) dropBody(id string) {
	s.Runtime().Domain.Cache().DeleteValue(id, true, system.GetCurrentTimeNs())
	s.Require().False(s.Runtime().Domain.Cache().ExistsJson(id))
}

// A live object without a body is erased, not parked.
func (s *TrashCanTestSuite) Test_Delete_BodylessObjectLeavesNoResidue() {
	s.boot()
	s.NoError(s.cmdb.TypeCreate("tc_bl"))
	s.NoError(s.cmdb.ObjectCreate("bl1", "tc_bl", easyjson.NewJSONObject()))

	objID := s.SetThisDomainPreffix("bl1")
	typeID := s.SetThisDomainPreffix("tc_bl")
	objectsID := s.SetThisDomainPreffix(crud.BUILT_IN_OBJECTS)
	trashID := s.SetThisDomainPreffix(crud.BUILT_IN_TRASH_CAN)
	s.dropBody(objID)
	s.Require().NotEmpty(s.residueOf(objID, objID, typeID, objectsID), "the links must outlive the body for this test")

	s.Equal("ok", s.objectOp("object.delete", "bl1", easyjson.NewJSONObject()))
	s.Empty(s.residueOf(objID, objID, typeID, objectsID, trashID))
	s.False(s.typeHoldsObject(crud.BUILT_IN_TRASH_CAN, "bl1"), "a bodyless object must not be parked")

	s.Equal("idle", s.objectOp("object.delete", "bl1", easyjson.NewJSONObject()))
}

// The state the old delete left behind, as found in a production snapshot: no
// body, the original type and objects still linking to the object, the trash
// links added on top, and the object's "type" link re-pointed to the trash can
// while its ltype entry for the original type (and that entry's mirror in-key
// on the type) stayed. A delete must clean all of it — including that stale
// ltype entry, which neither the out.body nor the in-key walk reaches.
func (s *TrashCanTestSuite) Test_Delete_BodylessParkedObjectIsCleanedUp() {
	s.boot()
	s.NoError(s.cmdb.TypeCreate("tc_bp"))
	s.NoError(s.cmdb.ObjectCreate("bp1", "tc_bp", easyjson.NewJSONObject()))

	c := s.Runtime().Domain.Cache()
	objID := s.SetThisDomainPreffix("bp1")
	typeID := s.SetThisDomainPreffix("tc_bp")
	objectsID := s.SetThisDomainPreffix(crud.BUILT_IN_OBJECTS)
	trashID := s.SetThisDomainPreffix(crud.BUILT_IN_TRASH_CAN)
	s.dropBody(objID)

	// The trash links exactly as moveObjectToTrashCan writes them.
	now := system.GetCurrentTimeNs()
	edge := easyjson.NewJSONObjectWithKeyValue("original_type", easyjson.NewJSON("tc_bp"))
	edge.SetByPath("deleted_at", easyjson.NewJSON(now))
	empty := easyjson.NewJSONObject()
	c.SetValue(fmt.Sprintf(crud.InLinkKeyPrefPattern+crud.KeySuff2Pattern, objID, trashID, "bp1"), []byte(crud.OBJECT_TYPELINK), true, now)
	c.SetValue(fmt.Sprintf(crud.OutLinkTargetKeyPrefPattern+crud.KeySuff1Pattern, trashID, "bp1"), []byte(crud.OBJECT_TYPELINK+"."+objID), true, now)
	c.SetValueJSON(fmt.Sprintf(crud.OutLinkBodyKeyPrefPattern+crud.KeySuff1Pattern, trashID, "bp1"), &edge, true, now)
	c.SetValue(fmt.Sprintf(crud.OutLinkTypeKeyPrefPattern+crud.KeySuff2Pattern, trashID, crud.OBJECT_TYPELINK, objID), []byte("bp1"), true, now)
	c.SetValue(fmt.Sprintf(crud.OutLinkIndexPrefPattern+crud.KeySuff3Pattern, trashID, "bp1", "type", crud.OBJECT_TYPELINK), nil, true, now)
	c.SetValue(fmt.Sprintf(crud.InLinkKeyPrefPattern+crud.KeySuff2Pattern, trashID, objID, "type"), []byte(crud.TO_TYPELINK), true, now)
	c.SetValue(fmt.Sprintf(crud.OutLinkTargetKeyPrefPattern+crud.KeySuff1Pattern, objID, "type"), []byte(crud.TO_TYPELINK+"."+trashID), true, now)
	c.SetValueJSON(fmt.Sprintf(crud.OutLinkBodyKeyPrefPattern+crud.KeySuff1Pattern, objID, "type"), &empty, true, now)
	c.SetValue(fmt.Sprintf(crud.OutLinkTypeKeyPrefPattern+crud.KeySuff2Pattern, objID, crud.TO_TYPELINK, trashID), []byte("type"), true, now)
	c.SetValue(fmt.Sprintf(crud.OutLinkIndexPrefPattern+crud.KeySuff3Pattern, objID, "type", "type", crud.TO_TYPELINK), nil, true, now)

	staleLtype := fmt.Sprintf(crud.OutLinkTypeKeyPrefPattern+crud.KeySuff2Pattern, objID, crud.TO_TYPELINK, typeID)
	staleMirror := fmt.Sprintf(crud.InLinkKeyPrefPattern+crud.KeySuff2Pattern, typeID, objID, "type")
	s.Require().True(c.Exists(staleLtype), "the stale ltype entry of the original type must be there")
	s.Require().True(c.Exists(staleMirror), "and its mirror in-key on the type")
	s.Require().True(s.typeHoldsObject("tc_bp", "bp1"), "the original type still links to the object")

	s.Equal("ok", s.objectOp("object.delete", "bp1", easyjson.NewJSONObject()))
	s.Empty(s.residueOf(objID, objID, typeID, objectsID, trashID))

	s.Equal("idle", s.objectOp("object.delete", "bp1", easyjson.NewJSONObject()))
}

// The low-level delete of a bodyless vertex runs the cascade too: it answers
// IDLE only when nothing of the vertex is left.
func (s *TrashCanTestSuite) Test_VertexDelete_BodylessVertexCascades() {
	s.boot()
	s.NoError(s.cmdb.TypeCreate("tc_bv"))
	s.NoError(s.cmdb.ObjectCreate("bv1", "tc_bv", easyjson.NewJSONObject()))

	objID := s.SetThisDomainPreffix("bv1")
	typeID := s.SetThisDomainPreffix("tc_bv")
	objectsID := s.SetThisDomainPreffix(crud.BUILT_IN_OBJECTS)
	s.dropBody(objID)
	// A link body with neither a target pointer nor an ltype entry: nothing
	// resolves it, so only the sweep can remove it.
	ghost := easyjson.NewJSONObject()
	s.Runtime().Domain.Cache().SetValueJSON(fmt.Sprintf(crud.OutLinkBodyKeyPrefPattern+crud.KeySuff1Pattern, objID, "ghost"), &ghost, true, system.GetCurrentTimeNs())

	res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.delete", "bv1", easyjson.NewJSONObject().GetPtr(), nil)
	s.Require().NoError(err)
	s.Equal("ok", res.GetByPath("status").AsStringDefault(""))
	s.Empty(s.residueOf(objID, objID, typeID, objectsID))

	res, err = s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.delete", "bv1", easyjson.NewJSONObject().GetPtr(), nil)
	s.Require().NoError(err)
	s.Equal("idle", res.GetByPath("status").AsStringDefault(""))
}

// The production snapshot itself, replayed: every live record of the bucket
// that mentions the object (a JSON array of {subject, data, headers}, as read
// from the stream), seeded into the cache under this test's domain, then
// deleted. Skipped unless BODYLESS_SNAPSHOT_RECORDS names that file — the
// snapshot is customer data and does not live in the repository.
func (s *TrashCanTestSuite) Test_Delete_BodylessSnapshotReplay() {
	path := os.Getenv("BODYLESS_SNAPSHOT_RECORDS")
	if path == "" {
		s.T().Skip("BODYLESS_SNAPSHOT_RECORDS is not set")
	}
	const (
		srcDomain   = "pak1/"
		storePrefix = "$KV.pak1_graph_cache_cache_bucket.store."
		objectName  = "c511e2c83f5254f6b3e00417fff4d6ab"
	)
	raw, err := os.ReadFile(path)
	s.Require().NoError(err)
	var records []struct {
		Subject string              `json:"subject"`
		Data    *string             `json:"data"`
		Headers map[string][]string `json:"headers"`
		Error   string              `json:"error"`
	}
	s.Require().NoError(json.Unmarshal(raw, &records))

	s.boot()
	c := s.Runtime().Domain.Cache()
	dstDomain := strings.TrimSuffix(s.SetThisDomainPreffix(objectName), objectName)
	objID := dstDomain + objectName

	now := system.GetCurrentTimeNs()
	owners := map[string]struct{}{}
	seeded := 0
	for _, r := range records {
		if r.Error != "" || r.Data == nil || !strings.HasPrefix(r.Subject, storePrefix) {
			continue
		}
		if op := r.Headers["KV-Operation"]; len(op) > 0 && op[0] != "PUT" {
			continue // a delete marker is not data
		}
		key := strings.ReplaceAll(strings.TrimPrefix(r.Subject, storePrefix), srcDomain, dstDomain)
		if !strings.Contains(key, objectName) {
			continue
		}
		value := strings.ReplaceAll(*r.Data, srcDomain, dstDomain)
		if strings.Contains(key, ".out.body.") {
			j, ok := easyjson.JSONFromString(value)
			s.Require().True(ok, "link body of %s is not JSON", key)
			c.SetValueJSON(key, &j, true, now)
		} else if value == "" {
			c.SetValue(key, nil, true, now)
		} else {
			c.SetValue(key, []byte(value), true, now)
		}
		owners[strings.SplitN(key, ".", 2)[0]] = struct{}{}
		seeded++
	}
	s.Require().Greater(seeded, 50, "the snapshot did not seed what it should")
	s.Require().False(c.ExistsJson(objID), "the snapshot object has no body")

	vertices := make([]string, 0, len(owners))
	for v := range owners {
		vertices = append(vertices, v)
	}
	s.Require().NotEmpty(s.residueOf(objID, vertices...))

	s.Equal("ok", s.objectOp("object.delete", objectName, easyjson.NewJSONObject()))
	s.Empty(s.residueOf(objID, vertices...))
	s.Equal("idle", s.objectOp("object.delete", objectName, easyjson.NewJSONObject()))
}
