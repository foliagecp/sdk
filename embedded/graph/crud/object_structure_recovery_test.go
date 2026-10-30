package crud_test

import (
	"fmt"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/embedded/graph/crud"
	"github.com/foliagecp/sdk/statefun/system"
)

func (s *TrashCanTestSuite) Test_TypeRead_RebuildsMembershipAroundExistingBody() {
	s.boot()
	s.Require().NoError(s.cmdb.TypeCreate("structure-type"))
	body := easyjson.NewJSONObjectWithKeyValue("protected", easyjson.NewJSON("original"))
	s.Require().NoError(s.cmdb.ObjectCreate("structure-object", "structure-type", body))
	c := s.Runtime().Domain.Cache()
	id, typeID := s.SetThisDomainPreffix("structure-object"), s.SetThisDomainPreffix("structure-type")
	bodyTime := c.GetValueUpdateTime(id)
	// Keep only the object's authoritative type pointer and its mirror on
	// the type. The whole type->object edge needs to be reconstructed.
	for _, key := range c.GetKeysByPattern(typeID + ".out.>") {
		c.DeleteValue(key, true, system.GetCurrentTimeNs())
	}
	c.DeleteValue(fmt.Sprintf(crud.OutLinkTypeKeyPrefPattern+crud.KeySuff2Pattern, typeID, crud.OBJECT_TYPELINK, id), true, system.GetCurrentTimeNs())
	c.DeleteValue(fmt.Sprintf(crud.InLinkKeyPrefPattern+crud.KeySuff2Pattern, id, typeID, "structure-object"), true, system.GetCurrentTimeNs())
	data, err := s.cmdb.TypeRead("structure-type")
	s.Require().NoError(err)
	members, ok := data.GetByPath("object_ids").AsArrayString()
	s.Require().True(ok)
	s.Equal([]string{id}, members)
	s.True(s.typeHoldsObject("structure-type", "structure-object"))
	s.Equal(bodyTime, c.GetValueUpdateTime(id), "repair must not rewrite the vertex body")
	stored, err := c.GetValueJSON(id)
	s.Require().NoError(err)
	s.Equal(body.ToString(), stored.ToString())
	_, err = s.cmdb.ObjectRead("structure-object")
	s.NoError(err)
}

func (s *TrashCanTestSuite) Test_ObjectRead_RebuildsMissingStructuralMirror() {
	s.boot()
	s.Require().NoError(s.cmdb.TypeCreate("mirror-type"))
	s.Require().NoError(s.cmdb.ObjectCreate("mirror-object", "mirror-type", easyjson.NewJSONObjectWithKeyValue("value", easyjson.NewJSON(7))))
	c := s.Runtime().Domain.Cache()
	id, typeID := s.SetThisDomainPreffix("mirror-object"), s.SetThisDomainPreffix("mirror-type")
	bodyTime := c.GetValueUpdateTime(id)
	mirror := fmt.Sprintf(crud.InLinkKeyPrefPattern+crud.KeySuff2Pattern, id, typeID, "mirror-object")
	c.DeleteValue(mirror, true, system.GetCurrentTimeNs())
	data, err := s.cmdb.ObjectRead("mirror-object")
	s.Require().NoError(err)
	s.Equal(int64(7), data.GetByPath("body.value").AsInt64Default(0))
	s.True(c.Exists(mirror))
	s.Equal(bodyTime, c.GetValueUpdateTime(id))
}

func (s *TrashCanTestSuite) Test_TypeRead_DoesNotInventTypeForUntypedBody() {
	s.boot()
	s.Require().NoError(s.cmdb.TypeCreate("unproven-type"))
	s.Require().NoError(s.cmdb.ObjectCreate("untyped-object", "unproven-type", easyjson.NewJSONObjectWithKeyValue("value", easyjson.NewJSON("keep"))))
	c := s.Runtime().Domain.Cache()
	id := s.SetThisDomainPreffix("untyped-object")
	bodyTime := c.GetValueUpdateTime(id)
	pointer := fmt.Sprintf(crud.OutLinkTargetKeyPrefPattern+crud.KeySuff1Pattern, id, "type")
	c.DeleteValue(pointer, true, system.GetCurrentTimeNs())
	data, err := s.cmdb.TypeRead("unproven-type")
	s.Require().NoError(err)
	members, _ := data.GetByPath("object_ids").AsArrayString()
	s.NotContains(members, id, "an old membership is not enough to choose the object's type")
	s.False(c.Exists(pointer))
	s.True(c.ExistsJson(id), "an untyped body is not proof that the vertex must be erased")
	s.Equal(bodyTime, c.GetValueUpdateTime(id))
}

func (s *TrashCanTestSuite) Test_TypeRead_RepeatedRepairNeverRecreatesMissingBody() {
	s.boot()
	s.Require().NoError(s.cmdb.TypeCreate("absent-type"))
	s.Require().NoError(s.cmdb.ObjectCreate("absent-object", "absent-type", easyjson.NewJSONObject()))
	id := s.SetThisDomainPreffix("absent-object")
	s.dropBody(id)
	for i := 0; i < 3; i++ {
		data, err := s.cmdb.TypeRead("absent-type")
		s.Require().NoError(err)
		members, _ := data.GetByPath("object_ids").AsArrayString()
		s.NotContains(members, id)
		s.False(s.Runtime().Domain.Cache().ExistsJson(id))
		s.False(s.typeHoldsObject(crud.BUILT_IN_TRASH_CAN, "absent-object"))
	}
	s.reloadFromKV()
	s.False(s.Runtime().Domain.Cache().ExistsJson(id), "repair must not have published a replacement body to WAL")
}

func (s *TrashCanTestSuite) Test_TypeRead_DoesNotInventLostTrashMetadata() {
	s.boot()
	s.Require().NoError(s.cmdb.TypeCreate("metadata-type"))
	s.Require().NoError(s.cmdb.ObjectCreate("metadata-object", "metadata-type", easyjson.NewJSONObjectWithKeyValue("value", easyjson.NewJSON("keep"))))
	s.Require().Equal("ok", s.objectOp("object.delete", "metadata-object", easyjson.NewJSONObject()))
	c := s.Runtime().Domain.Cache()
	id := s.SetThisDomainPreffix("metadata-object")
	bodyTime := c.GetValueUpdateTime(id)
	key := fmt.Sprintf(crud.OutLinkBodyKeyPrefPattern+crud.KeySuff1Pattern, s.SetThisDomainPreffix(crud.BUILT_IN_TRASH_CAN), "metadata-object")
	c.DeleteValue(key, true, system.GetCurrentTimeNs())
	_, err := s.cmdb.TypeRead(crud.BUILT_IN_TRASH_CAN)
	s.Error(err, "missing deletion metadata cannot be reconstructed from a vertex body")
	s.False(c.Exists(key), "repair must not invent deletion time or original type")
	s.True(c.ExistsJson(id))
	s.Equal(bodyTime, c.GetValueUpdateTime(id))
	s.False(s.typeHoldsObject("metadata-type", "metadata-object"))
}
