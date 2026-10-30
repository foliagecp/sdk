package crud_test

import (
	"fmt"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/embedded/graph/crud"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
)

func (s *TrashCanTestSuite) seedTypeMember(typeID, objectID, name string) {
	c := s.Runtime().Domain.Cache()
	now := system.GetCurrentTimeNs()
	body := easyjson.NewJSONObject()
	c.SetValue(fmt.Sprintf(crud.OutLinkTargetKeyPrefPattern+crud.KeySuff1Pattern, typeID, name), []byte(crud.OBJECT_TYPELINK+"."+objectID), true, now)
	c.SetValueJSON(fmt.Sprintf(crud.OutLinkBodyKeyPrefPattern+crud.KeySuff1Pattern, typeID, name), &body, true, now)
	c.SetValue(fmt.Sprintf(crud.OutLinkTypeKeyPrefPattern+crud.KeySuff2Pattern, typeID, crud.OBJECT_TYPELINK, objectID), []byte(name), true, now)
	c.SetValue(fmt.Sprintf(crud.OutLinkIndexPrefPattern+crud.KeySuff3Pattern, typeID, name, "type", crud.OBJECT_TYPELINK), nil, true, now)
	if s.Runtime().Domain.GetDomainFromObjectID(objectID) == s.Runtime().Domain.Name() {
		c.SetValue(fmt.Sprintf(crud.InLinkKeyPrefPattern+crud.KeySuff2Pattern, objectID, typeID, name), []byte(crud.OBJECT_TYPELINK), true, now)
	}
}

func (s *TrashCanTestSuite) Test_TypeRead_RemovesBodylessMember() {
	s.boot()
	s.Require().NoError(s.cmdb.TypeCreate("read-recovery"))
	s.Require().NoError(s.cmdb.ObjectCreate("missing-body", "read-recovery", easyjson.NewJSONObject()))
	s.Require().NoError(s.cmdb.ObjectCreate("healthy-member", "read-recovery", easyjson.NewJSONObject()))
	missing := s.SetThisDomainPreffix("missing-body")
	s.dropBody(missing)
	data, err := s.cmdb.TypeRead("read-recovery")
	s.Require().NoError(err)
	members, ok := data.GetByPath("object_ids").AsArrayString()
	s.Require().True(ok)
	s.NotContains(members, missing)
	s.Contains(members, s.SetThisDomainPreffix("healthy-member"))
	s.False(s.typeHoldsObject("read-recovery", "missing-body"), "the stale membership must be repaired, not just hidden")
}

func (s *TrashCanTestSuite) Test_TypeRead_PreservesParkedBodyAndRemovesOldMembership() {
	s.boot()
	s.Require().NoError(s.cmdb.TypeCreate("previous-type"))
	s.Require().NoError(s.cmdb.ObjectCreate("parked-member", "previous-type", easyjson.NewJSONObjectWithKeyValue("protected", easyjson.NewJSON("keep"))))
	s.Require().Equal("ok", s.objectOp("object.delete", "parked-member", easyjson.NewJSONObject()))
	id := s.SetThisDomainPreffix("parked-member")
	s.seedTypeMember(s.SetThisDomainPreffix("previous-type"), id, "parked-member")
	data, err := s.cmdb.TypeRead("previous-type")
	s.Require().NoError(err)
	members, ok := data.GetByPath("object_ids").AsArrayString()
	s.Require().True(ok)
	s.NotContains(members, id)
	s.False(s.typeHoldsObject("previous-type", "parked-member"))
	s.True(s.typeHoldsObject(crud.BUILT_IN_TRASH_CAN, "parked-member"))
	s.Equal("keep", s.parkedBody("parked-member").GetByPath("protected").AsStringDefault(""))
}

func (s *TrashCanTestSuite) Test_TypeRead_DoesNotPruneUnverifiedRemoteMember() {
	s.boot()
	s.Require().NoError(s.cmdb.TypeCreate("remote-members"))
	id := "remote/member"
	s.seedTypeMember(s.SetThisDomainPreffix("remote-members"), id, "remote-member")
	data, err := s.cmdb.TypeRead("remote-members")
	s.Require().NoError(err)
	members, ok := data.GetByPath("object_ids").AsArrayString()
	s.Require().True(ok)
	s.Contains(members, id, "local cache absence does not prove a remote object is missing")
}

func (s *TrashCanTestSuite) Test_TypeRead_DoesNotDeleteIsolatedLowLevelVertex() {
	s.boot()
	s.Require().NoError(s.cmdb.TypeCreate("unrelated-type"))
	body := easyjson.NewJSONObjectWithKeyValue("v", easyjson.NewJSON("valid"))
	payload := easyjson.NewJSONObjectWithKeyValue("body", body)
	_, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.create", "isolated-vertex", &payload, nil)
	s.Require().NoError(err)
	_, err = s.cmdb.TypeRead("unrelated-type")
	s.Require().NoError(err)
	s.True(s.Runtime().Domain.Cache().ExistsJson(s.SetThisDomainPreffix("isolated-vertex")))
}

func (s *TrashCanTestSuite) Test_TypeRead_InvalidatesStaleDerivedTypeCache() {
	s.boot()
	s.Require().NoError(s.cmdb.TypeCreate("cached-type"))
	s.Require().NoError(s.cmdb.ObjectCreate("cached-object", "cached-type", easyjson.NewJSONObject()))
	c := s.Runtime().Domain.Cache()
	id, bin := s.SetThisDomainPreffix("cached-object"), s.SetThisDomainPreffix(crud.BUILT_IN_TRASH_CAN)
	// Model repaired persisted links while the process-local type cache still
	// remembers the old type. That derived cache must not override the graph.
	s.seedTypeMember(bin, id, "cached-object")
	now := system.GetCurrentTimeNs()
	c.SetValue(fmt.Sprintf(crud.OutLinkTargetKeyPrefPattern+crud.KeySuff1Pattern, id, "type"), []byte(crud.TO_TYPELINK+"."+bin), true, now)
	c.SetValue(fmt.Sprintf(crud.OutLinkTypeKeyPrefPattern+crud.KeySuff2Pattern, id, crud.TO_TYPELINK, bin), []byte("type"), true, now)
	c.SetValue(fmt.Sprintf(crud.InLinkKeyPrefPattern+crud.KeySuff2Pattern, bin, id, "type"), []byte(crud.TO_TYPELINK), true, now)
	_, err := s.cmdb.TypeRead("cached-type")
	s.Require().NoError(err)
	s.False(s.typeHoldsObject("cached-type", "cached-object"))
	s.Equal("idle", s.objectOp("object.delete", "cached-object", easyjson.NewJSONObject()), "derived type cache must not re-park an already parked object")
	s.True(c.ExistsJson(id))
}
