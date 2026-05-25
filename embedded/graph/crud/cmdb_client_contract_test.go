package crud_test

// Contract tests for the SDK's public client API (db.DBSyncClient).
//
// These exercise the SDK's public client surface (db.DBSyncClient) the way
// downstream consumers drive it, so that any SDK change is regression-checked
// against the real usage contract:
//
//   1. schema bootstrap (Type/TypesLink) + object/link upsert;
//   2. steady-state idempotency (no-op upsert) and ObjectsLinkUpdateWithDetails
//      exposing op_stack.0.old_body (consumers build read-modify-write diffs on
//      exactly that path);
//   3. ObjectReadV2 shape — links.out as an array of {to,name,type}, the form
//      consumers iterate when walking an object's edges;
//   4. JPGQLCtraQuery enumeration ".*[l:type('__object')]" — the form consumers
//      use to list a type's objects.
//
// External test package (crud_test): it registers both crud and jpgql function
// types, which is why it cannot live in `package crud` (jpgql imports crud).
//
// Follow-ups (separate pass): SuperType cross-domain via the client; Signal
// delivery. Kept out here to keep this suite fully synchronous and non-flaky.

import (
	"fmt"
	"testing"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/clients/go/db"
	"github.com/foliagecp/sdk/embedded/graph/crud"
	"github.com/foliagecp/sdk/embedded/graph/jpgql"
	"github.com/foliagecp/sdk/statefun/test"
	"github.com/stretchr/testify/suite"
)

type CMDBClientContractTestSuite struct {
	test.StatefunTestSuite
	dbc db.DBSyncClient
}

func TestCMDBClientContractTestSuite(t *testing.T) {
	suite.Run(t, new(CMDBClientContractTestSuite))
}

func (s *CMDBClientContractTestSuite) bootstrap() {
	crud.RegisterAllFunctionTypes(s.Runtime())
	jpgql.RegisterAllFunctionTypes(s.Runtime())
	s.NoError(s.StartRuntime())
	s.waitForVertex(crud.BUILT_IN_TYPES, 15*time.Second)
	s.waitForVertex(crud.BUILT_IN_OBJECTS, 15*time.Second)

	dbc, err := db.NewDBSyncClientFromRequestFunction(s.Runtime().Request)
	s.NoError(err)
	s.dbc = dbc
}

func (s *CMDBClientContractTestSuite) waitForVertex(id string, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := s.CacheValue(id); err == nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	s.T().Fatalf("vertex %q did not appear within %s", id, timeout)
}

// hasOutLinkOfType reports whether `from` has any out-link of the given type,
// read straight from the ltype index in the cache.
func (s *CMDBClientContractTestSuite) hasOutLinkOfType(from, linkType string) bool {
	domID := s.SetThisDomainPreffix(from)
	pattern := fmt.Sprintf(crud.OutLinkTypeKeyPrefPattern+"%s.>", domID, linkType)
	return len(s.Runtime().Domain.Cache().GetKeysByPattern(pattern)) > 0
}

// 1. Schema bootstrap + object/link upsert through the client.
func (s *CMDBClientContractTestSuite) Test_Contract_SchemaAndUpsertCrud() {
	s.bootstrap()

	s.NoError(s.dbc.CMDB.TypeCreate("CcTypeA"))
	s.NoError(s.dbc.CMDB.TypeCreate("CcTypeB"))
	s.NoError(s.dbc.CMDB.TypesLinkCreate("CcTypeA", "CcTypeB", "cc-rel", nil))

	// Upsert objects with origin_type (replace=false + originType4Upsert).
	s.NoError(s.dbc.CMDB.ObjectUpdate("cc-a", easyjson.NewJSONObject(), false, "CcTypeA"))
	s.NoError(s.dbc.CMDB.ObjectUpdate("cc-b", easyjson.NewJSONObject(), false, "CcTypeB"))

	// Invariant: each created object carries its __type link.
	s.True(s.hasOutLinkOfType("cc-a", crud.TO_TYPELINK), "cc-a must have a __type link")
	s.True(s.hasOutLinkOfType("cc-b", crud.TO_TYPELINK), "cc-b must have a __type link")

	// Upsert object→object link (name4Upsert); type is resolved from the
	// TypesLink schema. Verify via ObjectsLinkRead (robust to the stored-type
	// string and also exercises the read contract).
	s.NoError(s.dbc.CMDB.ObjectsLinkUpdate("cc-a", "cc-b", nil, easyjson.NewJSONObject(), false, "cc-edge"))

	linkData, err := s.dbc.CMDB.ObjectsLinkRead("cc-a", "cc-b")
	s.NoError(err)
	s.True(linkData.PathExists("body"), "ObjectsLinkRead must return the created edge body; got: %s", linkData.ToString())
}

//  2. Steady-state idempotency (no-op upsert) + ObjectsLinkUpdateWithDetails
//     exposing op_stack.0.old_body.
func (s *CMDBClientContractTestSuite) Test_Contract_Idempotency_AndLinkDetails() {
	s.bootstrap()

	s.NoError(s.dbc.CMDB.TypeCreate("CcTypeC"))
	body := easyjson.NewJSONObjectWithKeyValue("v", easyjson.NewJSON(1))
	s.NoError(s.dbc.CMDB.ObjectUpdate("cc-c", body, false, "CcTypeC"))
	// Re-applying the identical body must be a no-op and must not error.
	s.NoError(s.dbc.CMDB.ObjectUpdate("cc-c", body, false, "CcTypeC"))

	s.NoError(s.dbc.CMDB.TypeCreate("CcTypeD"))
	s.NoError(s.dbc.CMDB.TypesLinkCreate("CcTypeC", "CcTypeD", "cd-rel", nil))
	s.NoError(s.dbc.CMDB.ObjectUpdate("cc-d", easyjson.NewJSONObject(), false, "CcTypeD"))

	// Create the edge, then update it with a changed body and inspect details.
	_, err := s.dbc.CMDB.ObjectsLinkUpdateWithDetails("cc-c", "cc-d", nil,
		easyjson.NewJSONObjectWithKeyValue("w", easyjson.NewJSON(1)), false, "cd-edge")
	s.NoError(err)

	details, err := s.dbc.CMDB.ObjectsLinkUpdateWithDetails("cc-c", "cc-d", nil,
		easyjson.NewJSONObjectWithKeyValue("w", easyjson.NewJSON(2)), true, "cd-edge")
	s.NoError(err)
	s.Truef(details.PathExists("op_stack.0.old_body"),
		"ObjectsLinkUpdateWithDetails must expose op_stack.0.old_body; got: %s", details.ToString())
}

// 3. ObjectReadV2 shape: body + links.out as an array of {to,name,type}.
func (s *CMDBClientContractTestSuite) Test_Contract_ObjectReadV2_Shape() {
	s.bootstrap()

	s.NoError(s.dbc.CMDB.TypeCreate("CcTypeE"))
	s.NoError(s.dbc.CMDB.TypeCreate("CcTypeF"))
	s.NoError(s.dbc.CMDB.TypesLinkCreate("CcTypeE", "CcTypeF", "ef-rel", nil))
	s.NoError(s.dbc.CMDB.ObjectUpdate("cc-e", easyjson.NewJSONObjectWithKeyValue("name", easyjson.NewJSON("e1")), false, "CcTypeE"))
	s.NoError(s.dbc.CMDB.ObjectUpdate("cc-f", easyjson.NewJSONObject(), false, "CcTypeF"))
	s.NoError(s.dbc.CMDB.ObjectsLinkUpdate("cc-e", "cc-f", nil, easyjson.NewJSONObject(), false, "ef-edge"))

	data, err := s.dbc.CMDB.ObjectReadV2("cc-e")
	s.NoError(err)
	s.Truef(data.PathExists("body"), "ReadV2 must include body; got: %s", data.ToString())
	s.Truef(data.GetByPath("links.out").IsArray(), "ReadV2 must expose links.out as an array; got: %s", data.ToString())

	out := data.GetByPath("links.out")
	foundEdge := false
	for i := 0; i < out.ArraySize(); i++ {
		l := out.ArrayElement(i)
		if l.GetByPath("type").AsStringDefault("") == "ef-rel" {
			s.NotEmpty(l.GetByPath("to").AsStringDefault(""), "links.out entry must carry 'to'")
			s.NotEmpty(l.GetByPath("name").AsStringDefault(""), "links.out entry must carry 'name'")
			foundEdge = true
		}
	}
	s.Truef(foundEdge, "ReadV2 links.out must contain the ef-rel edge with to/name/type; got: %s", data.ToString())
}

//  4. JPGQLCtraQuery enumeration: list all objects of a type via its __object
//     out-links.
func (s *CMDBClientContractTestSuite) Test_Contract_JPGQLEnumerateObjectsOfType() {
	s.bootstrap()

	s.NoError(s.dbc.CMDB.TypeCreate("CcTypeG"))
	for _, id := range []string{"g1", "g2", "g3"} {
		s.NoError(s.dbc.CMDB.ObjectUpdate(id, easyjson.NewJSONObject(), false, "CcTypeG"))
	}

	ids, err := s.dbc.Query.JPGQLCtraQuery("CcTypeG", ".*[l:type('__object')]")
	s.NoError(err)
	s.Lenf(ids, 3, "JPGQL enumerate must return all 3 objects of CcTypeG; got: %v", ids)
}
