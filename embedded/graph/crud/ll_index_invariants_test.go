package crud_test

// LL link storage & query invariants.
//
// These pin the contract behind the link "type" index so a future storage
// optimization (e.g. dropping the redundant out.index.<name>.type.<type> key in
// favour of reading the type from out.to) is regression-checked: the stable
// structural keys must stay, and l:type traversal must keep returning the edge.
//
// Method on CMDBClientContractTestSuite to reuse its bootstrap (crud + jpgql).

import (
	"fmt"
	"strings"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/embedded/graph/crud"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
)

func (s *CMDBClientContractTestSuite) Test_LL_LinkStorageAndQueryInvariants() {
	s.bootstrap()

	for _, id := range []string{"lia", "lib"} {
		res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.create", id, easyjson.NewJSONObject().GetPtr(), nil)
		s.NoError(err)
		s.Equal("ok", res.GetByPath("status").AsStringDefault(""))
	}

	lp := easyjson.NewJSONObject()
	lp.SetByPath("to", easyjson.NewJSON("lib"))
	lp.SetByPath("name", easyjson.NewJSON("li-edge"))
	lp.SetByPath("type", easyjson.NewJSON("rel"))
	lp.SetByPath("tags", easyjson.NewJSON([]string{"t1", "t2"}))
	lp.SetByPath("body.x", easyjson.NewJSON(1))
	res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.create", "lia", &lp, nil)
	s.NoError(err)
	s.Equal("ok", res.GetByPath("status").AsStringDefault(""))

	c := s.Runtime().Domain.Cache()
	aID := s.SetThisDomainPreffix("lia")

	// Stable structural keys (these must survive a type-index refactor):
	//   out.to encodes "<type>.<toId>"; out.body exists; ltype index exists.
	toVal, err := c.GetValue(fmt.Sprintf(crud.OutLinkTargetKeyPrefPattern+"%s", aID, "li-edge"))
	s.NoError(err)
	s.Truef(strings.HasPrefix(string(toVal), "rel."),
		"out.to must encode the link type prefix; got %q", string(toVal))
	s.NotEmptyf(c.GetKeysByPattern(fmt.Sprintf(crud.OutLinkBodyKeyPrefPattern+"%s", aID, "li-edge")),
		"out.body key must exist for the created link")
	s.Truef(s.hasOutLinkOfType("lia", "rel"), "ltype index for type 'rel' must exist")

	// Query behavior that must hold regardless of how the type index is stored:
	// l:type('rel') traversal returns the linked target.
	byType, err := s.dbc.Query.JPGQLCtraQuery("lia", ".*[l:type('rel')]")
	s.NoError(err)
	s.Lenf(byType, 1, "l:type('rel') must return the linked target; got %v", byType)
}
