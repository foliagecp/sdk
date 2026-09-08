package crud_test

// Where the type used for a repair is allowed to come from.
//
// An operation can carry a type — object.create does, and so does an upsert —
// and a damaged object can be missing the one it was created under. The two
// must not be confused: what the graph still says about an object outranks
// what a caller says about it. An object created under one type and then
// updated, by mistake, under another must come out of that update as what it
// was, not as what the mistake named.
//
// The caller is believed in exactly one case: the graph still says the vertex
// is an object — the objects vertex links to it — and no longer says of what.
// Restoring it under the named type keeps everything else the object holds,
// which erasing it would not.

import (
	"fmt"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/embedded/graph/crud"
)

// stripTypeTraces removes everything that names the object's type, leaving its
// membership of the objects vertex — and therefore its objecthood — intact.
func (s *CMDBClientContractTestSuite) stripTypeTraces(objShort, typeShort string) {
	objID, typeID, _, name := s.ids(objShort, typeShort)
	s.dropOutSide(objID, "type", crud.TO_TYPELINK, typeID)                                        // B.out
	s.dropKey(fmt.Sprintf(crud.InLinkKeyPrefPattern+crud.KeySuff2Pattern, typeID, objID, "type")) // B.in
	s.dropOutSide(typeID, name, crud.OBJECT_TYPELINK, objID)                                      // C.out
	s.dropKey(fmt.Sprintf(crud.InLinkKeyPrefPattern+crud.KeySuff2Pattern, objID, typeID, name))   // C.in
	crud.ResetPackageCachesForTest()
}

// An upsert naming the wrong type must not retype the object. The graph still
// knows what it is, and that answer wins.
func (s *CMDBClientContractTestSuite) Test_Integrity_CallerTypeNeverOverridesTheGraph() {
	s.bootstrap()
	const right, wrong = "TsRight", "TsWrong"
	s.Require().NoError(s.dbc.CMDB.TypeCreate(right))
	s.Require().NoError(s.dbc.CMDB.TypeCreate(wrong))
	s.Require().NoError(s.dbc.CMDB.ObjectUpdate("ts-keep", easyjson.NewJSONObject(), false, right))

	// Break the object without touching what names its type.
	objID, _, objectsID, name := s.ids("ts-keep", right)
	s.dropKey(fmt.Sprintf(crud.InLinkKeyPrefPattern+crud.KeySuff2Pattern, objID, objectsID, name))
	s.Require().NotEmpty(missingIntegrityKeys(s.Runtime().Domain.Cache(), s.Runtime().Domain, "ts-keep", right),
		"precondition: the object must be damaged")

	// Somebody upserts it under the wrong type.
	s.Require().NoError(s.dbc.CMDB.ObjectUpdate("ts-keep", easyjson.NewJSONObjectWithKeyValue("v", easyjson.NewJSON(1)), false, wrong))

	read, err := s.dbc.CMDB.ObjectReadV2("ts-keep")
	s.Require().NoError(err, "the object must read after the upsert")
	s.Require().Equalf(s.Runtime().Domain.CreateObjectIDWithHubDomain(right, false),
		read.GetByPath("type").AsStringDefault(""),
		"the upsert retyped the object to the type it named")

	assertObjectIntegrity(s.T(), s.Runtime().Domain.Cache(), s.Runtime().Domain, "ts-keep", right,
		"the object must be whole again, and whole as what it was")

	wrongID := s.Runtime().Domain.CreateObjectIDWithHubDomain(wrong, false)
	s.Falsef(s.Runtime().Domain.Cache().Exists(
		fmt.Sprintf(crud.OutLinkTypeKeyPrefPattern+crud.KeySuff2Pattern, wrongID, crud.OBJECT_TYPELINK, objID)),
		"the type named by the upsert took ownership of an object that is not its")
}

// And the case where the caller IS the only one who knows: the graph still
// says this is an object, nothing says of what. The object is restored, not
// erased, and everything else it holds survives.
func (s *CMDBClientContractTestSuite) Test_Integrity_CallerTypeRestoresWhatTheGraphForgot() {
	s.bootstrap()
	const objType = "TsRestore"
	s.Require().NoError(s.dbc.CMDB.TypeCreate(objType))
	s.Require().NoError(s.dbc.CMDB.TypesLinkCreate(objType, objType, "ts-peer", nil))
	s.Require().NoError(s.dbc.CMDB.ObjectUpdate("ts-a", easyjson.NewJSONObject(), false, objType))
	s.Require().NoError(s.dbc.CMDB.ObjectUpdate("ts-b", easyjson.NewJSONObject(), false, objType))
	s.Require().NoError(s.dbc.CMDB.ObjectsLinkCreate("ts-a", "ts-b", "peer", nil))

	s.stripTypeTraces("ts-a", objType)

	s.Require().NoError(s.dbc.CMDB.ObjectUpdate("ts-a", easyjson.NewJSONObjectWithKeyValue("v", easyjson.NewJSON(2)), false, objType))

	assertObjectIntegrity(s.T(), s.Runtime().Domain.Cache(), s.Runtime().Domain, "ts-a", objType,
		"the object must be restored under the type the operation named")

	link, err := s.dbc.CMDB.ObjectsLinkRead("ts-a", "ts-b")
	s.Require().NoErrorf(err, "restoring the object destroyed the links it held")
	s.NotNil(link)
}
