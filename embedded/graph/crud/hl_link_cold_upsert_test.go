package crud_test

// The COLD path: writing an object link that does not exist yet.
//
// An inventory feeder writes a hub's links once, on the first pass, and every
// one of them is new. The cost of that pass must not depend on how many links
// the hub already has: a vertex with two thousand out-links takes the same
// number of steps to gain one more as a vertex with two.
//
// It did not. Resolving an edge by name reads the owner's out.to key, and when
// that key is missing the resolver assumed a DAMAGED link — one that lost its
// target pointer — and walked the owner's whole ltype family to rebuild it,
// reading every key. For a link that does not exist yet the key is missing by
// definition, so every cold write paid a full walk, several times over: once
// per name the high-level resolver tried, once more inside the low-level
// update. On a hub with ~1 900 out-links that is millions of key reads per
// hub, and it was measured as 54–58 % of the runtime's CPU during the first
// tick of an inventory load, with the tick itself six times longer than the
// second one.
//
// Two counters pin the guarantee. The scan counter says the ltype family was
// never walked; the store's enumerated-keys counter says nothing else walked
// the vertex either, whatever it might be called. Counted, not timed.

import (
	"fmt"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/embedded/graph/crud"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
)

// coldTargets creates `n` objects of the hub's type without linking them: the
// targets of the links the test is about to write, made outside the window it
// measures.
func (s *LinkResolveTestSuite) coldTargets(tag string, n int) []string {
	targets := make([]string, 0, n)
	for i := 0; i < n; i++ {
		to := fmt.Sprintf("%s_new%d", tag, i)
		s.NoError(s.cmdb.ObjectCreate(to, tag+"_t", easyjson.NewJSONObject()))
		targets = append(targets, to)
	}
	return targets
}

// linkWeight reads the body of the base edge from → to.
func (s *LinkResolveTestSuite) linkWeight(from, to string) int64 {
	read, err := s.cmdb.ObjectsLinkRead(from, to)
	s.Require().NoError(err)
	return int64(read.GetByPath("body.weight").AsNumericDefault(-1))
}

// The guarantee, on the path the feeder uses: an upsert of a link that does
// not exist walks nothing on the from-vertex.
func (s *LinkResolveTestSuite) Test_ColdUpsertDoesNotScanOutLinks() {
	s.boot()
	// Past the size of one records bucket, so the hub holds a split directory —
	// the shape a real hub always has.
	from, _ := s.hub("lcu", 40)
	targets := s.coldTargets("lcu", 8)

	scannedBefore := crud.LinkResolveScannedKeysForTest()
	body := easyjson.NewJSONObjectWithKeyValue("weight", easyjson.NewJSON(1))
	for _, to := range targets {
		s.Require().NoError(s.cmdb.ObjectsLinkUpdate(from, to, nil, body, false, to))
	}

	s.Equalf(scannedBefore, crud.LinkResolveScannedKeysForTest(),
		"%d cold upserts walked the from-vertex's out-links — a link that does not exist yet must be found absent by direct key",
		len(targets))
	for _, to := range targets {
		s.Equal(int64(1), s.linkWeight(from, to), "the cold upsert must have created the link")
	}
}

// The same guarantee stated the way it will be broken next time: whatever the
// operation does, it does not do more of it on a bigger vertex. Two hubs, one
// eight times the other, the same eight cold upserts on each — the store hands
// out the same number of keys for both.
func (s *LinkResolveTestSuite) Test_ColdUpsertCostDoesNotGrowWithOutDegree() {
	s.boot()
	store := s.Runtime().Domain.Cache()
	body := easyjson.NewJSONObjectWithKeyValue("weight", easyjson.NewJSON(1))

	measure := func(tag string, fanout int) int64 {
		from, _ := s.hub(tag, fanout)
		targets := s.coldTargets(tag, 8)
		before := store.EnumeratedKeys()
		for _, to := range targets {
			s.Require().NoError(s.cmdb.ObjectsLinkUpdate(from, to, nil, body, false, to))
		}
		return store.EnumeratedKeys() - before
	}

	small := measure("lds", 16)
	big := measure("ldb", 128)
	s.T().Logf("keys enumerated by eight cold upserts: %d on a hub of 16 links, %d on a hub of 128", small, big)
	s.LessOrEqualf(big, small,
		"eight cold upserts enumerated %d keys on a hub of 128 links and %d on a hub of 16: the cost grows with the out-degree",
		big, small)
}

// A cold create walks nothing either. It never did — the create path checks its
// two invariants by direct key — and it is pinned here so the two ways of
// writing a new link stay equally cheap.
func (s *LinkResolveTestSuite) Test_ColdCreateDoesNotScanOutLinks() {
	s.boot()
	from, _ := s.hub("lcc", 40)
	targets := s.coldTargets("lcc", 8)
	store := s.Runtime().Domain.Cache()

	scannedBefore := crud.LinkResolveScannedKeysForTest()
	enumeratedBefore := store.EnumeratedKeys()
	for _, to := range targets {
		s.Require().NoError(s.cmdb.ObjectsLinkCreate(from, to, to, nil, easyjson.NewJSONObject()))
	}
	s.Equal(scannedBefore, crud.LinkResolveScannedKeysForTest(), "a cold create walked the from-vertex's out-links")
	s.Equalf(enumeratedBefore, store.EnumeratedKeys(), "a cold create enumerated keys on the from-vertex")
}

// The supertype flavour reaches the low-level update directly, with the name,
// type and target all known — the second entry into the walk the profile
// showed. It must be as cheap as the base one.
func (s *LinkResolveTestSuite) Test_ColdSuperTypeUpsertDoesNotScanOutLinks() {
	s.boot()
	for _, t := range []string{"lsu_SuperFrom", "lsu_SuperTo", "lsu_ChildFrom", "lsu_ChildTo"} {
		s.NoError(s.cmdb.TypeCreate(t))
	}
	s.setSubtype("lsu_SuperFrom", "lsu_ChildFrom")
	s.setSubtype("lsu_SuperTo", "lsu_ChildTo")
	s.NoError(s.cmdb.TypesLinkCreate("lsu_SuperFrom", "lsu_SuperTo", "lsu_rel", nil))
	// The from-object is a hub: forty base links of its own type, so a walk
	// over its out-links would have something to count.
	s.NoError(s.cmdb.TypesLinkCreate("lsu_ChildFrom", "lsu_ChildFrom", "lsu_self", nil))
	s.NoError(s.cmdb.ObjectCreate("lsu_a", "lsu_ChildFrom", easyjson.NewJSONObject()))
	for i := 0; i < 40; i++ {
		peer := fmt.Sprintf("lsu_peer%d", i)
		s.NoError(s.cmdb.ObjectCreate(peer, "lsu_ChildFrom", easyjson.NewJSONObject()))
		s.NoError(s.cmdb.ObjectsLinkCreate("lsu_a", peer, peer, nil, easyjson.NewJSONObject()))
	}
	targets := make([]string, 0, 8)
	for i := 0; i < 8; i++ {
		to := fmt.Sprintf("lsu_b%d", i)
		s.NoError(s.cmdb.ObjectCreate(to, "lsu_ChildTo", easyjson.NewJSONObject()))
		targets = append(targets, to)
	}

	scannedBefore := crud.LinkResolveScannedKeysForTest()
	body := easyjson.NewJSONObjectWithKeyValue("weight", easyjson.NewJSON(1))
	for _, to := range targets {
		s.Require().NoError(s.cmdb.ObjectsLinkSuperTypeUpdate("lsu_a", to, "lsu_SuperFrom", "lsu_SuperTo", to, nil, body, false))
	}
	s.Equalf(scannedBefore, crud.LinkResolveScannedKeysForTest(),
		"%d cold supertype upserts walked the from-vertex's out-links", len(targets))
	for _, to := range targets {
		s.True(s.claimEdgeExists("lsu_a", "lsu_SuperFrom#lsu_SuperTo#lsu_rel", to), "the cold supertype upsert must have created the edge")
	}
}

// What the walk was for, kept: a link that LOST its target pointer is still
// found and updated. The high-level path finds it through the ltype key it can
// address directly — the reference type and the target are both known — so
// the repair costs one read, not a walk.
func (s *LinkResolveTestSuite) Test_LinkThatLostItsTargetKeyIsStillUpdated() {
	s.boot()
	s.NoError(s.cmdb.TypeCreate("llt_t"))
	s.NoError(s.cmdb.TypesLinkCreate("llt_t", "llt_t", "llt_rel", nil))
	s.NoError(s.cmdb.ObjectCreate("llt_a", "llt_t", easyjson.NewJSONObject()))
	s.NoError(s.cmdb.ObjectCreate("llt_b", "llt_t", easyjson.NewJSONObject()))
	s.NoError(s.cmdb.ObjectsLinkCreate("llt_a", "llt_b", "custom", nil,
		easyjson.NewJSONObjectWithKeyValue("weight", easyjson.NewJSON(1))))

	s.dropKey(fmt.Sprintf(crud.OutLinkTargetKeyPrefPattern+crud.KeySuff1Pattern, s.SetThisDomainPreffix("llt_a"), "custom"))

	scannedBefore := crud.LinkResolveScannedKeysForTest()
	// No name passed: the caller knows the pair, not the name the edge was
	// created under.
	s.Require().NoError(s.cmdb.ObjectsLinkUpdate("llt_a", "llt_b", nil,
		easyjson.NewJSONObjectWithKeyValue("weight", easyjson.NewJSON(2)), false))
	s.Equal(scannedBefore, crud.LinkResolveScannedKeysForTest(), "the damaged link must be found by its ltype key, not by a walk")
	s.Equal(int64(2), s.linkWeight("llt_a", "llt_b"), "the update must have reached the damaged link")
}

// The low-level update addressed by NAME alone has no type and no target to
// go by. It still recovers a link that lost its target pointer — by the walk —
// but only when the link left its body behind: a name with neither key is not
// a damaged link, it is an absent one, and absent is what every new link
// looks like.
func (s *LinkResolveTestSuite) Test_LowLevelUpdateByNameRecoversOnlyALinkWithABody() {
	s.boot()
	s.NoError(s.cmdb.TypeCreate("lln_t"))
	s.NoError(s.cmdb.TypesLinkCreate("lln_t", "lln_t", "lln_rel", nil))
	s.NoError(s.cmdb.ObjectCreate("lln_a", "lln_t", easyjson.NewJSONObject()))
	s.NoError(s.cmdb.ObjectCreate("lln_b", "lln_t", easyjson.NewJSONObject()))
	s.NoError(s.cmdb.ObjectsLinkCreate("lln_a", "lln_b", "custom", nil,
		easyjson.NewJSONObjectWithKeyValue("weight", easyjson.NewJSON(1))))
	from := s.SetThisDomainPreffix("lln_a")

	update := func(weight int) string {
		p := easyjson.NewJSONObjectWithKeyValue("name", easyjson.NewJSON("custom"))
		p.SetByPath("body", easyjson.NewJSONObjectWithKeyValue("weight", easyjson.NewJSON(weight)))
		res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.update", "lln_a", &p, nil)
		s.Require().NoError(err)
		return res.GetByPath("status").AsStringDefault("")
	}

	// Target pointer gone, body kept: recovered, by the walk.
	s.dropKey(fmt.Sprintf(crud.OutLinkTargetKeyPrefPattern+crud.KeySuff1Pattern, from, "custom"))
	scannedBefore := crud.LinkResolveScannedKeysForTest()
	s.Equal("ok", update(2), "a link that kept its body must still be updated by name")
	s.Greater(crud.LinkResolveScannedKeysForTest(), scannedBefore, "and that recovery is the walk — it must be counted")
	s.Equal(int64(2), s.linkWeight("lln_a", "lln_b"))

	// Body gone too: nothing to update, and nothing to walk for.
	s.dropKey(fmt.Sprintf(crud.OutLinkBodyKeyPrefPattern+crud.KeySuff1Pattern, from, "custom"))
	scannedBefore = crud.LinkResolveScannedKeysForTest()
	s.Equal("idle", update(3), "a name with neither target nor body is an absent link")
	s.Equal(scannedBefore, crud.LinkResolveScannedKeysForTest(), "and an absent link is not walked for")
}

// The delete by name follows the same rule, for the same reason: it needs the
// body next — it records the old one in the op-stack — and used to fail on a
// bare remnant right after walking for it. A link that lost its target
// pointer but kept its body is still found by the walk and removed whole; a
// name with neither is absent, and absent costs nothing.
func (s *LinkResolveTestSuite) Test_LowLevelDeleteByNameRecoversOnlyALinkWithABody() {
	s.boot()
	s.NoError(s.cmdb.TypeCreate("lld_t"))
	s.NoError(s.cmdb.TypesLinkCreate("lld_t", "lld_t", "lld_rel", nil))
	s.NoError(s.cmdb.ObjectCreate("lld_a", "lld_t", easyjson.NewJSONObject()))
	for _, to := range []string{"lld_b", "lld_c"} {
		s.NoError(s.cmdb.ObjectCreate(to, "lld_t", easyjson.NewJSONObject()))
		s.NoError(s.cmdb.ObjectsLinkCreate("lld_a", to, "to_"+to, nil, easyjson.NewJSONObject()))
	}
	from := s.SetThisDomainPreffix("lld_a")
	store := s.Runtime().Domain.Cache()

	del := func(name string) string {
		p := easyjson.NewJSONObjectWithKeyValue("name", easyjson.NewJSON(name))
		res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.delete", "lld_a", &p, nil)
		s.Require().NoError(err)
		return res.GetByPath("status").AsStringDefault("")
	}
	ltypeKey := func(to string) string {
		return fmt.Sprintf(crud.OutLinkTypeKeyPrefPattern+crud.KeySuff2Pattern, from, "lld_rel", s.SetThisDomainPreffix(to))
	}

	// Target pointer gone, body kept: found by the walk, removed whole.
	s.dropKey(fmt.Sprintf(crud.OutLinkTargetKeyPrefPattern+crud.KeySuff1Pattern, from, "to_lld_b"))
	scannedBefore := crud.LinkResolveScannedKeysForTest()
	s.Equal("ok", del("to_lld_b"), "a link that kept its body must still be deleted by name")
	s.Greater(crud.LinkResolveScannedKeysForTest(), scannedBefore, "and that recovery is the walk — it must be counted")
	s.False(store.Exists(ltypeKey("lld_b")), "the ltype entry must go with the link")
	s.False(store.Exists(fmt.Sprintf(crud.InLinkKeyPrefPattern+crud.KeySuff2Pattern, s.SetThisDomainPreffix("lld_b"), from, "to_lld_b")),
		"and so must the mirror in-key")

	// Neither key left: an absent link, no walk.
	s.dropKey(fmt.Sprintf(crud.OutLinkTargetKeyPrefPattern+crud.KeySuff1Pattern, from, "to_lld_c"))
	s.dropKey(fmt.Sprintf(crud.OutLinkBodyKeyPrefPattern+crud.KeySuff1Pattern, from, "to_lld_c"))
	scannedBefore = crud.LinkResolveScannedKeysForTest()
	s.Equal("idle", del("to_lld_c"), "a name with neither target nor body is an absent link")
	s.Equal(scannedBefore, crud.LinkResolveScannedKeysForTest(), "and an absent link is not walked for")
}

// A link that lost BOTH its target pointer and its body is a bare remnant: an
// ltype entry and a mirror in-key, nothing readable. It is not addressable by
// name — it has no name key — but it is by its pair, which is all that is
// left of it, and that is enough: the delete removes what remains, and the
// pair can be linked again. It used to refuse ("link body ... does not
// exist") and leave the entry to block every create of the pair for good.
func (s *LinkResolveTestSuite) Test_BareRemnantIsDeletedByItsPair() {
	s.boot()
	s.NoError(s.cmdb.TypeCreate("llr_t"))
	s.NoError(s.cmdb.TypesLinkCreate("llr_t", "llr_t", "llr_rel", nil))
	for _, id := range []string{"llr_a", "llr_b", "llr_c"} {
		s.NoError(s.cmdb.ObjectCreate(id, "llr_t", easyjson.NewJSONObject()))
	}
	from := s.SetThisDomainPreffix("llr_a")
	store := s.Runtime().Domain.Cache()
	ltypeKey := func(to string) string {
		return fmt.Sprintf(crud.OutLinkTypeKeyPrefPattern+crud.KeySuff2Pattern, from, "llr_rel", s.SetThisDomainPreffix(to))
	}
	inKey := func(to, name string) string {
		return fmt.Sprintf(crud.InLinkKeyPrefPattern+crud.KeySuff2Pattern, s.SetThisDomainPreffix(to), from, name)
	}
	strip := func(to, name string) {
		s.NoError(s.cmdb.ObjectsLinkCreate("llr_a", to, name, nil, easyjson.NewJSONObjectWithKeyValue("weight", easyjson.NewJSON(1))))
		s.dropKey(fmt.Sprintf(crud.OutLinkTargetKeyPrefPattern+crud.KeySuff1Pattern, from, name))
		s.dropKey(fmt.Sprintf(crud.OutLinkBodyKeyPrefPattern+crud.KeySuff1Pattern, from, name))
		s.Require().True(store.Exists(ltypeKey(to)), "sanity: the ltype entry is what is left")
	}

	// Through the high-level API, by the pair: no name, no walk.
	strip("llr_b", "custom_b")
	scannedBefore := crud.LinkResolveScannedKeysForTest()
	s.Require().NoError(s.cmdb.ObjectsLinkDelete("llr_a", "llr_b"), "a bare remnant must be deletable by its pair")
	s.Equal(scannedBefore, crud.LinkResolveScannedKeysForTest(), "and found by its ltype key, not by a walk")
	s.False(store.Exists(ltypeKey("llr_b")), "the ltype entry must be gone")
	s.False(store.Exists(inKey("llr_b", "custom_b")), "and the mirror in-key with it")
	s.NoError(s.cmdb.ObjectsLinkCreate("llr_a", "llr_b", "custom_b", nil, easyjson.NewJSONObjectWithKeyValue("weight", easyjson.NewJSON(2))),
		"the pair must be linkable again once the remnant is gone")
	s.Equal(int64(2), s.linkWeight("llr_a", "llr_b"))

	// Through the low-level API, by type and target — the remnant's own key.
	strip("llr_c", "custom_c")
	p := easyjson.NewJSONObjectWithKeyValue("to", easyjson.NewJSON("llr_c"))
	p.SetByPath("type", easyjson.NewJSON("llr_rel"))
	res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.delete", "llr_a", &p, nil)
	s.Require().NoError(err)
	s.Equal("ok", res.GetByPath("status").AsStringDefault(""), res.ToString())
	s.False(store.Exists(ltypeKey("llr_c")), "the ltype entry must be gone")
	s.False(store.Exists(inKey("llr_c", "custom_c")), "and the mirror in-key with it")
}

// dropKey removes one key from the operating representation only — the damage
// the resolvers exist to survive.
func (s *LinkResolveTestSuite) dropKey(key string) {
	s.T().Helper()
	s.Require().Truef(s.Runtime().Domain.Cache().DeleteValue(key, false, system.GetCurrentTimeNs()),
		"sanity: %s was there to drop", key)
}
