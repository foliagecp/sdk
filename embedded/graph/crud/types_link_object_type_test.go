package crud_test

// The objects-link type a types link declares, after the declaration changes.
//
// A types link says with which type the objects of one type link to the
// objects of another. UpdateTypesLink can change it (upsert + object_type),
// and from then on every object link between the two types must carry the new
// type. It did not: the process kept the type in a cache of its own, the
// update read the old type into that cache on its way to the write and never
// replaced it, and object links went on being created and looked up under the
// type the schema no longer declared, until the process restarted.

import (
	"fmt"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/embedded/graph/crud"
)

// linkPair creates one object of each type under the given ids and links them.
func (s *CMDBClientContractTestSuite) linkPair(from, fromType, to, toType string) {
	s.NoError(s.dbc.CMDB.ObjectCreate(from, fromType))
	s.NoError(s.dbc.CMDB.ObjectCreate(to, toType))
	s.NoError(s.dbc.CMDB.ObjectsLinkUpdate(from, to, nil, easyjson.NewJSONObject(), false, to))
}

func (s *CMDBClientContractTestSuite) declaredObjectType(from, to string) string {
	link, err := s.dbc.CMDB.TypesLinkRead(from, to)
	s.NoError(err)
	return link.GetByPath("body.type").AsStringDefault("")
}

// cachedObjectType is what the process holds for the types link, valid or not.
func (s *CMDBClientContractTestSuite) cachedObjectType(from, to string) string {
	return crud.CachedObjectsLinkTypeForTest(s.SetThisDomainPreffix(from), s.SetThisDomainPreffix(to))
}

func (s *CMDBClientContractTestSuite) Test_TypesLinkUpdate_ChangedObjectTypeIsUsedByTheNextLink() {
	s.bootstrap()
	s.NoError(s.dbc.CMDB.TypeCreate("OltA"))
	s.NoError(s.dbc.CMDB.TypeCreate("OltB"))

	s.NoError(s.dbc.CMDB.TypesLinkUpdate("OltA", "OltB", nil, easyjson.NewJSONObject(), false, "olt-t1"))
	s.linkPair("olt-a1", "OltA", "olt-b1", "OltB")
	s.True(s.hasOutLinkOfType("olt-a1", "olt-t1"), "sanity: the first link carries the first declared type")

	s.NoError(s.dbc.CMDB.TypesLinkUpdate("OltA", "OltB", nil, easyjson.NewJSONObject(), false, "olt-t2"))
	s.Equal("olt-t2", s.declaredObjectType("OltA", "OltB"))

	s.linkPair("olt-a2", "OltA", "olt-b2", "OltB")
	s.True(s.hasOutLinkOfType("olt-a2", "olt-t2"), "a link created after the change carries the new type")
	s.False(s.hasOutLinkOfType("olt-a2", "olt-t1"), "and not the one the schema no longer declares")
	_, err := s.dbc.CMDB.ObjectsLinkRead("olt-a2", "olt-b2")
	s.NoError(err, "the link is found under the type it was created with")
}

// The first thing the process does with the types link is change it: nothing
// it read on the way to the write may outlive the write.
func (s *CMDBClientContractTestSuite) Test_TypesLinkUpdate_ChangeAsTheFirstOperationOfTheProcess() {
	s.bootstrap()
	s.NoError(s.dbc.CMDB.TypeCreate("OlcA"))
	s.NoError(s.dbc.CMDB.TypeCreate("OlcB"))
	s.NoError(s.dbc.CMDB.TypesLinkCreate("OlcA", "OlcB", "olc-t1", nil))
	crud.ResetPackageCachesForTest() // a process that has not seen this types link yet

	s.NoError(s.dbc.CMDB.TypesLinkUpdate("OlcA", "OlcB", nil, easyjson.NewJSONObject(), false, "olc-t2"))
	s.NotEqual("olc-t1", s.cachedObjectType("OlcA", "OlcB"), "the update must not leave the type it replaced behind")

	s.linkPair("olc-a", "OlcA", "olc-b", "OlcB")
	s.True(s.hasOutLinkOfType("olc-a", "olc-t2"))
	s.False(s.hasOutLinkOfType("olc-a", "olc-t1"))
}

// An update that names no object type keeps the declared one, and a types link
// deleted and declared again is used with its new type: what worked before.
func (s *CMDBClientContractTestSuite) Test_TypesLink_ObjectTypeSurvivesOtherUpdatesAndFollowsRecreate() {
	s.bootstrap()
	s.NoError(s.dbc.CMDB.TypeCreate("OlrA"))
	s.NoError(s.dbc.CMDB.TypeCreate("OlrB"))
	s.NoError(s.dbc.CMDB.TypesLinkCreate("OlrA", "OlrB", "olr-t1", nil))

	s.NoError(s.dbc.CMDB.TypesLinkUpdate("OlrA", "OlrB", nil, easyjson.NewJSONObjectWithKeyValue("weight", easyjson.NewJSON(1)), false))
	s.Equal("olr-t1", s.declaredObjectType("OlrA", "OlrB"), "an update without object_type keeps the type")
	s.linkPair("olr-a1", "OlrA", "olr-b1", "OlrB")
	s.True(s.hasOutLinkOfType("olr-a1", "olr-t1"))

	s.NoError(s.dbc.CMDB.ObjectsLinkDelete("olr-a1", "olr-b1"))
	s.NoError(s.dbc.CMDB.TypesLinkDelete("OlrA", "OlrB"))
	s.NoError(s.dbc.CMDB.TypesLinkCreate("OlrA", "OlrB", "olr-t3", nil))
	s.linkPair("olr-a2", "OlrA", "olr-b2", "OlrB")
	s.True(s.hasOutLinkOfType("olr-a2", "olr-t3"))
	s.False(s.hasOutLinkOfType("olr-a2", "olr-t1"))
}

// The declared type is held by the process between operations, and the held
// value is checked against the store each time: a types link body rewritten
// behind the handlers, as an import or a restore does it, is what the next
// object link goes by.
func (s *CMDBClientContractTestSuite) Test_TypesLink_ObjectTypeFollowsABodyChangedBehindTheHandlers() {
	s.bootstrap()
	s.NoError(s.dbc.CMDB.TypeCreate("OlhA"))
	s.NoError(s.dbc.CMDB.TypeCreate("OlhB"))
	s.NoError(s.dbc.CMDB.TypesLinkCreate("OlhA", "OlhB", "olh-t1", nil))

	s.linkPair("olh-a1", "OlhA", "olh-b1", "OlhB")
	s.True(s.hasOutLinkOfType("olh-a1", "olh-t1"))
	s.Equal("olh-t1", s.cachedObjectType("OlhA", "OlhB"), "the declared type is kept for the next operation")

	bodyKey := fmt.Sprintf(crud.OutLinkBodyKeyPrefPattern+crud.KeySuff1Pattern, s.SetThisDomainPreffix("OlhA"), "OlhB")
	rewritten := easyjson.NewJSONObjectWithKeyValue("type", easyjson.NewJSON("olh-t2"))
	s.True(s.Runtime().Domain.Cache().SetValueJSON(bodyKey, &rewritten, true, -1))

	s.linkPair("olh-a2", "OlhA", "olh-b2", "OlhB")
	s.True(s.hasOutLinkOfType("olh-a2", "olh-t2"), "the kept type is not used once the store holds another body")
	s.False(s.hasOutLinkOfType("olh-a2", "olh-t1"))
	s.Equal("olh-t2", s.cachedObjectType("OlhA", "OlhB"))
}
