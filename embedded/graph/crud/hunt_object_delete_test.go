package crud_test

// Bug hunt: object delete completeness and recreate freshness — the orphan /
// phantom class (the highest-risk area by incident history).
//
// Adversarial post-conditions: after ObjectDelete the object must vanish from
// the graph entirely — gone from its type's __object enumeration (no phantom),
// vertex body removed, no out-link keys dangling. A create→delete→recreate
// cycle must leave no stale state from the previous incarnation.
//
// Methods on CMDBClientContractTestSuite to reuse its bootstrap.

import (
	"fmt"
	"strings"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/embedded/graph/crud"
)

// After delete, the object must disappear from its type's __object enumeration
// (a leftover type→object link would make it a phantom in queries).
func (s *CMDBClientContractTestSuite) Test_Hunt_ObjectDelete_RemovedFromTypeEnumeration() {
	s.bootstrap()
	s.NoError(s.dbc.CMDB.TypeCreate("DoType"))
	s.NoError(s.dbc.CMDB.ObjectUpdate("do-1", easyjson.NewJSONObject(), false, "DoType"))
	s.NoError(s.dbc.CMDB.ObjectUpdate("do-2", easyjson.NewJSONObject(), false, "DoType"))

	ids, err := s.dbc.Query.JPGQLCtraQuery("DoType", ".*[l:type('__object')]")
	s.NoError(err)
	s.Lenf(ids, 2, "both objects must be enumerated before delete; got %v", ids)

	s.NoError(s.dbc.CMDB.ObjectDelete("do-1"))

	ids2, err := s.dbc.Query.JPGQLCtraQuery("DoType", ".*[l:type('__object')]")
	s.NoError(err)
	s.Lenf(ids2, 1, "deleted object must be gone from enumeration (no phantom); got %v", ids2)
	for _, id := range ids2 {
		s.Falsef(strings.HasSuffix(id, "/do-1") || id == "do-1", "do-1 still enumerated after delete: %v", ids2)
	}

	// Trash-can contract: after the first delete the object is PARKED — body
	// kept, only the trash-can __type link remains (no links into the model).
	_, cacheErr := s.CacheValue("do-1")
	s.NoErrorf(cacheErr, "parked object vertex body must still be in cache")
	aID := s.SetThisDomainPreffix("do-1")
	outLinks := s.Runtime().Domain.Cache().GetKeysByPattern(fmt.Sprintf(crud.OutLinkTargetKeyPrefPattern+">", aID))
	s.Lenf(outLinks, 1, "parked object must keep exactly its trash-can __type out-link; got %v", outLinks)

	// A parked object no longer exists for the object API: deleting it again is
	// a no-op, and erasing it is the low-level vertex delete.
	s.NoError(s.dbc.CMDB.ObjectDelete("do-1"))
	_, cacheErr = s.CacheValue("do-1")
	s.NoErrorf(cacheErr, "a second object.delete must not erase the parked object")

	s.NoError(s.dbc.Graph.VertexDelete("do-1"))
	_, cacheErr = s.CacheValue("do-1")
	s.Errorf(cacheErr, "physically deleted object vertex body must be gone from cache")
	s.Emptyf(s.Runtime().Domain.Cache().GetKeysByPattern(fmt.Sprintf(crud.OutLinkTargetKeyPrefPattern+">", aID)),
		"physically deleted object must have no dangling out-links")
}

// A create→delete→recreate cycle must not leak state from the first incarnation.
func (s *CMDBClientContractTestSuite) Test_Hunt_ObjectRecreate_NoStaleState() {
	s.bootstrap()
	s.NoError(s.dbc.CMDB.TypeCreate("DrTypeA"))
	s.NoError(s.dbc.CMDB.TypeCreate("DrTypeB"))
	s.NoError(s.dbc.CMDB.TypesLinkCreate("DrTypeA", "DrTypeB", "dr-rel", nil))
	s.NoError(s.dbc.CMDB.ObjectUpdate("dr-b", easyjson.NewJSONObject(), false, "DrTypeB"))

	// First incarnation: body {a:1} + an out-link.
	s.NoError(s.dbc.CMDB.ObjectUpdate("dr-a", easyjson.NewJSONObjectWithKeyValue("a", easyjson.NewJSON(1)), false, "DrTypeA"))
	s.NoError(s.dbc.CMDB.ObjectsLinkUpdate("dr-a", "dr-b", nil, easyjson.NewJSONObject(), false, "dr-edge"))
	s.True(s.hasOutLinkOfType("dr-a", "dr-rel"), "edge must exist in first incarnation")

	s.NoError(s.dbc.CMDB.ObjectDelete("dr-a"))

	// Second incarnation: body {b:2}, no link.
	s.NoError(s.dbc.CMDB.ObjectUpdate("dr-a", easyjson.NewJSONObjectWithKeyValue("b", easyjson.NewJSON(2)), false, "DrTypeA"))

	data, err := s.dbc.CMDB.ObjectRead("dr-a")
	s.NoError(err)
	s.Equalf(int64(2), int64(data.GetByPath("body.b").AsNumericDefault(0)), "recreated body must carry new field; got %s", data.ToString())
	s.Falsef(data.PathExists("body.a"), "stale field from first incarnation leaked into recreate; got %s", data.ToString())
	s.Equalf(1, s.outLinkCountOfType("dr-a", crud.TO_TYPELINK), "recreate must have exactly one __type link")
	s.Falsef(s.hasOutLinkOfType("dr-a", "dr-rel"), "stale edge from first incarnation must not survive recreate")
}
