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
	"path/filepath"
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

// Replay a generated damaged snapshot rather than depending on a private
// archive or an environment-variable skip. The record shape matches a KV
// export, including delete markers and failed records which are not data.
func (s *TrashCanTestSuite) Test_Delete_BodylessSnapshotReplay() {
	s.boot()
	const objectName = "replay-object"
	s.Require().NoError(s.cmdb.TypeCreate("snapshot-type"))
	s.Require().NoError(s.cmdb.ObjectCreate(objectName, "snapshot-type", easyjson.NewJSONObject()))
	c := s.Runtime().Domain.Cache()
	objID := s.SetThisDomainPreffix(objectName)
	owners := []string{objID, s.SetThisDomainPreffix("snapshot-type"), s.SetThisDomainPreffix(crud.BUILT_IN_OBJECTS), s.SetThisDomainPreffix(crud.BUILT_IN_TRASH_CAN)}
	for i := 0; i < 10; i++ {
		peer := fmt.Sprintf("snapshot-peer-%d", i)
		_, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.create", peer, easyjson.NewJSONObject().GetPtr(), nil)
		s.Require().NoError(err)
		payload := easyjson.NewJSONObjectWithKeyValue("to", easyjson.NewJSON(objID))
		payload.SetByPath("name", easyjson.NewJSON(objectName))
		payload.SetByPath("type", easyjson.NewJSON("relation"))
		_, err = s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.create", peer, &payload, nil)
		s.Require().NoError(err)
		owners = append(owners, s.SetThisDomainPreffix(peer))
	}
	s.dropBody(objID)
	s.seedTypeMember(s.SetThisDomainPreffix(crud.BUILT_IN_TRASH_CAN), objID, objectName)
	// Preserve the old reverse pair while pointing the common type key at
	// the bin, reproducing overlapping remnants without adding a body.
	c.SetValue(fmt.Sprintf(crud.OutLinkTargetKeyPrefPattern+crud.KeySuff1Pattern, objID, "type"), []byte(crud.TO_TYPELINK+"."+s.SetThisDomainPreffix(crud.BUILT_IN_TRASH_CAN)), true, system.GetCurrentTimeNs())
	type record struct {
		Subject string              `json:"subject"`
		Data    *string             `json:"data"`
		Headers map[string][]string `json:"headers"`
		Error   string              `json:"error"`
	}
	var records []record
	prefix := strings.TrimSuffix(objID, objectName)
	for _, owner := range owners {
		for _, key := range c.GetKeysByPattern(owner + ".>") {
			if !mentionsObject(key, objID) {
				continue
			}
			value, err := c.GetValue(key)
			s.Require().NoError(err)
			text := strings.ReplaceAll(string(value), prefix, "example/")
			records = append(records, record{Subject: "$KV.example.store." + strings.ReplaceAll(key, prefix, "example/"), Data: &text, Headers: map[string][]string{"KV-Operation": {"PUT"}}})
			c.DeleteValue(key, true, system.GetCurrentTimeNs())
		}
	}
	empty := ""
	records = append(records, record{Subject: "$KV.example.store.example/" + objectName, Data: &empty, Headers: map[string][]string{"KV-Operation": {"DEL"}}})
	records = append(records, record{Subject: "$KV.example.store.example/" + objectName, Data: &empty, Error: "injected unreadable record"})
	raw, err := json.Marshal(records)
	s.Require().NoError(err)
	path := filepath.Join(s.T().TempDir(), "synthetic-records.json")
	s.Require().NoError(os.WriteFile(path, raw, 0600))
	raw, err = os.ReadFile(path)
	s.Require().NoError(err)
	var replay []record
	s.Require().NoError(json.Unmarshal(raw, &replay))
	seeded := 0
	for _, r := range replay {
		if r.Error != "" || r.Data == nil || !strings.HasPrefix(r.Subject, "$KV.example.store.") {
			continue
		}
		if op := r.Headers["KV-Operation"]; len(op) > 0 && op[0] != "PUT" {
			continue
		}
		key := strings.ReplaceAll(strings.TrimPrefix(r.Subject, "$KV.example.store."), "example/", prefix)
		value := strings.ReplaceAll(*r.Data, "example/", prefix)
		if strings.Contains(key, ".out.body.") {
			body, ok := easyjson.JSONFromString(value)
			s.Require().True(ok)
			c.SetValueJSON(key, &body, true, system.GetCurrentTimeNs())
		} else {
			c.SetValue(key, []byte(value), true, system.GetCurrentTimeNs())
		}
		seeded++
	}
	s.Require().Greater(seeded, 50)
	s.Require().False(c.ExistsJson(objID))
	s.Require().False(c.Exists(objID), "delete markers and error records must not become vertex data")
	s.Equal("ok", s.objectOp("object.delete", objectName, easyjson.NewJSONObject()))
	s.Empty(s.residueOf(objID, owners...))
	s.False(c.ExistsJson(objID))
	s.Equal("idle", s.objectOp("object.delete", objectName, easyjson.NewJSONObject()))
}
