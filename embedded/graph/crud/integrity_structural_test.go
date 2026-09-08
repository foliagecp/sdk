package crud_test

// The repair may only ever touch objects.
//
// Everything else in the graph is structure: the roots (root, objects, types),
// the declared types, and any vertex somebody wrote through the low-level API.
// None of them has an object's skeleton, and none of them may be given one —
// or taken away for not having one.
//
// A type is the dangerous case, because from the outside it can look like an
// object that lost its membership: a link between two types is an __type link
// too, so a type that participates in a types-link appears to "have a type".
// Reading that as an object's type link would hang the type off the objects
// vertex and invent a membership that never existed.

import (
	"fmt"
	"sort"
	"strings"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/embedded/graph/crud"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
)

// vertexFootprint is every key the cache holds for a vertex, sorted — the
// vertex itself, its links, their bodies and indexes, and the in-keys written
// on it.
func (s *CMDBClientContractTestSuite) vertexFootprint(vertexID string) []string {
	c := s.Runtime().Domain.Cache()
	keys := append([]string{}, c.GetKeysByPattern(vertexID+".>")...)
	if c.ExistsJson(vertexID) {
		keys = append(keys, vertexID+" (body)")
	}
	sort.Strings(keys)
	return keys
}

func (s *CMDBClientContractTestSuite) Test_Integrity_RepairLeavesNonObjectsAlone() {
	s.bootstrap()
	dm := s.Runtime().Domain

	// A type that takes part in a types-link — an __type out-link of its own,
	// which is exactly the shape an object's type link has.
	const typeA, typeB = "StType", "StPartner"
	s.Require().NoError(s.dbc.CMDB.TypeCreate(typeA))
	s.Require().NoError(s.dbc.CMDB.TypeCreate(typeB))
	s.Require().NoError(s.dbc.CMDB.TypesLinkCreate(typeA, typeB, "st-rel", nil))

	// A vertex written through the low-level API, member of nothing.
	bare := easyjson.NewJSONObjectWithKeyValue("kind", easyjson.NewJSON("bare"))
	_, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.create",
		"st-bare", &bare, nil)
	s.Require().NoError(err)

	subjects := map[string]string{
		"root":            dm.CreateObjectIDWithHubDomain(crud.BUILT_IN_ROOT, false),
		"objects":         dm.CreateObjectIDWithHubDomain(crud.BUILT_IN_OBJECTS, false),
		"types":           dm.CreateObjectIDWithHubDomain(crud.BUILT_IN_TYPES, false),
		"a declared type": dm.CreateObjectIDWithHubDomain(typeA, false),
		"a bare vertex":   dm.CreateObjectIDWithThisDomain("st-bare", false),
	}

	before := map[string][]string{}
	for what, id := range subjects {
		before[what] = s.vertexFootprint(id)
		s.Require().NotEmptyf(before[what], "precondition: %s must hold something", what)
	}

	// Every CMDB object operation, aimed at each of them. What they answer is
	// their own business — none of them may CHANGE anything.
	body := easyjson.NewJSONObjectWithKeyValue("v", easyjson.NewJSON(1))
	for what, id := range subjects {
		short := dm.GetObjectIDWithoutDomain(id)
		_, _ = s.dbc.CMDB.ObjectRead(short)
		_, _ = s.dbc.CMDB.ObjectReadV2(short)
		_ = s.dbc.CMDB.ObjectUpdate(short, body, false)
		_, _ = s.dbc.CMDB.ObjectsLinkRead(short, short)

		after := s.vertexFootprint(id)
		s.Require().Equalf(before[what], after,
			"%s (%s) was changed by CMDB object operations:\n  before:\n    %s\n  after:\n    %s",
			what, id, strings.Join(before[what], "\n    "), strings.Join(after, "\n    "))
	}

	// And the objects vertex must not have gained a membership link to any of
	// them — the specific damage an over-eager repair would do.
	objectsID := subjects["objects"]
	for what, id := range subjects {
		if what == "objects" {
			continue
		}
		key := fmt.Sprintf(crud.OutLinkTypeKeyPrefPattern+crud.KeySuff2Pattern, objectsID, crud.OBJECT_TYPELINK, id)
		s.Falsef(s.Runtime().Domain.Cache().Exists(key),
			"%s was made a member of the objects topology by the repair [%s]", what, key)
	}
}
