package crud_test

// Updating or deleting an object link must not depend on how many links its
// FROM-vertex has.
//
// Both paths resolved the edge by searching (from → to): a walk over every
// out-link of the from-vertex, on every call. On a hub vertex — a rack holding
// a thousand hosts, a type holding its objects — that is thousands of visits
// per operation, and it was measured as the largest single share of the
// runtime's CPU under an inventory rebuild.
//
// The name is enough: the create path names an edge after its target unless the
// caller says otherwise, so the ordinary call knows the name before it asks.
// The search stays as the fallback for edges named otherwise.
//
// Counted, not timed: the number of keys the search walks is exact, while a
// timing threshold on a loaded machine proves nothing.

import (
	"fmt"
	"testing"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/clients/go/db"
	"github.com/foliagecp/sdk/embedded/graph/crud"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/test"
	"github.com/stretchr/testify/suite"
)

type LinkResolveTestSuite struct {
	test.StatefunTestSuite
	cmdb db.CMDBSyncClient
}

func TestLinkResolveTestSuite(t *testing.T) { suite.Run(t, new(LinkResolveTestSuite)) }

func (s *LinkResolveTestSuite) boot() {
	crud.RegisterAllFunctionTypes(s.Runtime())
	s.NoError(s.StartRuntime())
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := s.CacheValue(crud.BUILT_IN_TYPES); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	dbc, err := db.NewDBSyncClientFromRequestFunction(s.Runtime().Request)
	s.NoError(err)
	s.cmdb = dbc.CMDB
}

// hub builds one from-vertex with `fanout` outgoing links, so a search over its
// out-links has something to walk.
func (s *LinkResolveTestSuite) hub(tag string, fanout int) (from string, targets []string) {
	s.NoError(s.cmdb.TypeCreate(tag + "_t"))
	s.NoError(s.cmdb.TypesLinkCreate(tag+"_t", tag+"_t", tag+"_rel", nil))
	from = tag + "_from"
	s.NoError(s.cmdb.ObjectCreate(from, tag+"_t", easyjson.NewJSONObject()))
	for i := 0; i < fanout; i++ {
		to := fmt.Sprintf("%s_to%d", tag, i)
		s.NoError(s.cmdb.ObjectCreate(to, tag+"_t", easyjson.NewJSONObject()))
		s.NoError(s.cmdb.ObjectsLinkCreate(from, to, to, nil, easyjson.NewJSONObject()))
		targets = append(targets, to)
	}
	return from, targets
}

// The guarantee: an ordinary update and an ordinary delete resolve the edge by
// name and walk nothing.
func (s *LinkResolveTestSuite) Test_UpdateAndDeleteDoNotScanOutLinks() {
	s.boot()
	from, targets := s.hub("lrs", 20)

	scannedBefore := crud.LinkResolveScannedKeysForTest()

	body := easyjson.NewJSONObjectWithKeyValue("weight", easyjson.NewJSON(1))
	for _, to := range targets {
		s.NoError(s.cmdb.ObjectsLinkUpdate(from, to, nil, body, false))
	}
	for _, to := range targets {
		s.NoError(s.cmdb.ObjectsLinkDelete(from, to))
	}

	s.Equalf(scannedBefore, crud.LinkResolveScannedKeysForTest(),
		"%d updates and %d deletes walked the from-vertex's out-links — the ordinary path must resolve by name",
		len(targets), len(targets))
}

// An edge named by the caller (not after its target) is still resolved — by the
// name the caller passes back.
func (s *LinkResolveTestSuite) Test_CustomNamedLinkResolvesByItsName() {
	s.boot()
	s.NoError(s.cmdb.TypeCreate("lrc_t"))
	s.NoError(s.cmdb.TypesLinkCreate("lrc_t", "lrc_t", "lrc_rel", nil))
	s.NoError(s.cmdb.ObjectCreate("lrc_a", "lrc_t", easyjson.NewJSONObject()))
	s.NoError(s.cmdb.ObjectCreate("lrc_b", "lrc_t", easyjson.NewJSONObject()))
	s.NoError(s.cmdb.ObjectsLinkCreate("lrc_a", "lrc_b", "custom", nil, easyjson.NewJSONObject()))

	scannedBefore := crud.LinkResolveScannedKeysForTest()

	p := easyjson.NewJSONObjectWithKeyValue("to", easyjson.NewJSON("lrc_b"))
	p.SetByPath("name", easyjson.NewJSON("custom"))
	p.SetByPath("body", easyjson.NewJSONObjectWithKeyValue("weight", easyjson.NewJSON(2)))
	res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.objects.link.update", "lrc_a", &p, nil)
	s.Require().NoError(err)
	s.Equal("ok", res.GetByPath("status").AsStringDefault(""), res.ToString())

	s.Equal(scannedBefore, crud.LinkResolveScannedKeysForTest(),
		"a caller-supplied name must resolve without a search either")
}

// A name that belongs to an edge pointing SOMEWHERE ELSE must not be taken at
// face value: the operation is addressed to (from → to), and the name is only a
// shortcut to it.
func (s *LinkResolveTestSuite) Test_NameOfAnotherTargetIsNotUsed() {
	s.boot()
	s.NoError(s.cmdb.TypeCreate("lrn_t"))
	s.NoError(s.cmdb.TypesLinkCreate("lrn_t", "lrn_t", "lrn_rel", nil))
	for _, id := range []string{"lrn_a", "lrn_b", "lrn_c"} {
		s.NoError(s.cmdb.ObjectCreate(id, "lrn_t", easyjson.NewJSONObject()))
	}
	// a → b under the name "shared"; a → c does not exist at all.
	s.NoError(s.cmdb.ObjectsLinkCreate("lrn_a", "lrn_b", "shared", nil,
		easyjson.NewJSONObjectWithKeyValue("weight", easyjson.NewJSON(1))))

	// Delete a → c naming the edge that points to b.
	p := easyjson.NewJSONObjectWithKeyValue("to", easyjson.NewJSON("lrn_c"))
	p.SetByPath("name", easyjson.NewJSON("shared"))
	res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.objects.link.delete", "lrn_a", &p, nil)
	s.Require().NoError(err)
	s.Equal("idle", res.GetByPath("status").AsStringDefault(""),
		"deleting a link that does not exist must be a no-op, whatever name was passed")

	read, err := s.cmdb.ObjectsLinkRead("lrn_a", "lrn_b")
	s.Require().NoError(err, "the edge to another target must survive untouched")
	s.Equal(int64(1), int64(read.GetByPath("body.weight").AsNumericDefault(0)))
}

// F4: a claim edge belongs to the supertype API, and the base API must not
// touch it — even though both name an edge after its target, so the base call
// meets the claim edge under exactly the name it would look for.
func (s *LinkResolveTestSuite) Test_BaseApiDoesNotTouchAClaimEdge() {
	s.boot()

	// Two parent types with a schema link between them, and children of each.
	for _, t := range []string{"lrf_SuperFrom", "lrf_SuperTo", "lrf_ChildFrom", "lrf_ChildTo"} {
		s.NoError(s.cmdb.TypeCreate(t))
	}
	s.setSubtype("lrf_SuperFrom", "lrf_ChildFrom")
	s.setSubtype("lrf_SuperTo", "lrf_ChildTo")
	s.NoError(s.cmdb.TypesLinkCreate("lrf_SuperFrom", "lrf_SuperTo", "lrf_rel", nil))

	s.NoError(s.cmdb.ObjectCreate("lrf_a", "lrf_ChildFrom", easyjson.NewJSONObject()))
	s.NoError(s.cmdb.ObjectCreate("lrf_b", "lrf_ChildTo", easyjson.NewJSONObject()))

	// A claim edge named after its target — the name the base API looks for.
	s.NoError(s.cmdb.ObjectsLinkSuperTypeCreate("lrf_a", "lrf_b", "lrf_SuperFrom", "lrf_SuperTo", "lrf_b", nil,
		easyjson.NewJSONObjectWithKeyValue("weight", easyjson.NewJSON(1))))
	compound := "lrf_SuperFrom#lrf_SuperTo#lrf_rel"
	s.Require().True(s.claimEdgeExists("lrf_a", compound, "lrf_b"), "sanity: the claim edge is there")

	// The base API has no edge of its own between this pair: it must say so and
	// leave the claim edge alone, not adopt it.
	s.Error(s.cmdb.ObjectsLinkUpdate("lrf_a", "lrf_b", nil,
		easyjson.NewJSONObjectWithKeyValue("weight", easyjson.NewJSON(99)), true),
		"a base update must not silently take over a claim edge")
	s.NoError(s.cmdb.ObjectsLinkDelete("lrf_a", "lrf_b"))

	s.True(s.claimEdgeExists("lrf_a", compound, "lrf_b"), "the claim edge must survive the base API")
	s.Equal(int64(1), int64(s.claimEdgeBody("lrf_a", "lrf_b").GetByPath("weight").AsNumericDefault(0)),
		"and keep its body untouched")
}

func (s *LinkResolveTestSuite) setSubtype(parent, child string) {
	p := easyjson.NewJSONObjectWithKeyValue("sub_type", easyjson.NewJSON(child))
	res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.type.subtype.set", parent, &p, nil)
	s.Require().NoError(err)
	s.Require().Equal("ok", res.GetByPath("status").AsStringDefault(""), res.ToString())
}

// claimEdgeBody reads the edge body straight from the store: the base read API
// deliberately does not see a claim edge, which is the point of the test.
func (s *LinkResolveTestSuite) claimEdgeBody(from, name string) easyjson.JSON {
	key := fmt.Sprintf(crud.OutLinkBodyKeyPrefPattern+crud.KeySuff1Pattern, s.SetThisDomainPreffix(from), name)
	b, err := s.Runtime().Domain.Cache().GetValueJSON(key)
	s.Require().NoError(err)
	return *b
}

func (s *LinkResolveTestSuite) claimEdgeExists(from, linkType, to string) bool {
	key := fmt.Sprintf(crud.OutLinkTypeKeyPrefPattern+crud.KeySuff2Pattern,
		s.SetThisDomainPreffix(from), linkType, s.SetThisDomainPreffix(to))
	return s.Runtime().Domain.Cache().Exists(key)
}
