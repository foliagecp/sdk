package crud_test

// The asymmetry that produced the production defect.
//
// In the vertex.delete cascade the loop over INCOMING keys removes the
// owner's out-side only when the out-link resolves, while the mirror in-key is
// removed UNCONDITIONALLY (ll_crud.go, "Delete all in links"):
//
//	if linkType, toId, ok := resolveOutLinkByName(...); ok {
//	    deleteOutLinkFromSideKeys(...)   // owner's out-side — only on success
//	}
//	cache.DeleteValue(in-key, ...)       // target's in-side — always
//
// So a source whose out-link cannot be resolved keeps its half of the edge
// while the target loses its own. From then on the edge is half-written, and
// the time guard freezes it: restoring the in-key needs a write NEWER than the
// deletion that removed it.
//
// A delete must leave no half behind — either both sides go, or neither.

import (
	"fmt"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/embedded/graph/crud"
	"github.com/foliagecp/sdk/statefun/cache"
)

// edgeResidue returns every key of the edge (from -> *, named linkName) that is
// still present: the owner keeps four key families, and any one of them left
// behind after a delete is a dangling half.
func edgeResidue(c *cache.Store, from, linkName string) []string {
	var left []string
	if _, err := c.GetValue(fmt.Sprintf(crud.OutLinkTargetKeyPrefPattern+crud.KeySuff1Pattern, from, linkName)); err == nil {
		left = append(left, "out.to")
	}
	if c.ExistsJson(fmt.Sprintf(crud.OutLinkBodyKeyPrefPattern+crud.KeySuff1Pattern, from, linkName)) {
		left = append(left, "out.body")
	}
	if n := len(c.GetKeysByPattern(fmt.Sprintf(crud.OutLinkIndexPrefPattern+crud.KeySuff2Pattern, from, linkName, ">"))); n > 0 {
		left = append(left, fmt.Sprintf("out.index x%d", n))
	}
	// The ltype family is keyed by type+target, not by link name, so the entry
	// of THIS edge is the one whose value is the link name — the vertex's other
	// edges (its __type link, for one) must not be counted as residue.
	for _, k := range c.GetKeysByPattern(fmt.Sprintf(crud.OutLinkTypeKeyPrefPattern+"%s", from, ">")) {
		if v, err := c.GetValue(k); err == nil && string(v) == linkName {
			left = append(left, "ltype")
		}
	}
	return left
}

// Deleting a vertex whose incoming edge cannot be resolved on the source side
// must still leave nothing dangling: today the in-key goes and the source keeps
// its half.
func (s *CMDBClientContractTestSuite) Test_HalfDelete_UnresolvableOutLinkLeavesNoResidue() {
	s.bootstrap()
	const tA, tB = "HdA", "HdB"
	s.NoError(s.dbc.CMDB.TypeCreate(tA))
	s.NoError(s.dbc.CMDB.TypeCreate(tB))
	s.NoError(s.dbc.CMDB.TypesLinkCreate(tA, tB, "hd-rel", nil))
	s.NoError(s.dbc.CMDB.ObjectUpdate("hd-a", easyjson.NewJSONObject(), false, tA))
	s.NoError(s.dbc.CMDB.ObjectUpdate("hd-b", easyjson.NewJSONObject(), false, tB))
	s.NoError(s.dbc.CMDB.ObjectsLinkUpdate("hd-a", "hd-b", nil, easyjson.NewJSONObject(), false, "hd-edge"))

	c, dm := s.Runtime().Domain.Cache(), s.Runtime().Domain
	aID := dm.CreateObjectIDWithThisDomain("hd-a", false)
	bID := dm.CreateObjectIDWithThisDomain("hd-b", false)
	const linkName = "hd-edge"

	// Make the out-link unresolvable while its other keys stay: drop out.to and
	// the ltype entry, keep out.body and the index. This is what an interrupted
	// write leaves behind, and it is what makes resolveOutLinkByName give up.
	future := int64(1) << 62
	c.DeleteValue(fmt.Sprintf(crud.OutLinkTargetKeyPrefPattern+crud.KeySuff1Pattern, aID, linkName), true, future)
	for _, k := range c.GetKeysByPattern(fmt.Sprintf(crud.OutLinkTypeKeyPrefPattern+"%s", aID, ">")) {
		c.DeleteValue(k, true, future)
	}
	s.Require().True(c.Exists(fmt.Sprintf(crud.InLinkKeyPrefPattern+crud.KeySuff2Pattern, bID, aID, linkName)),
		"precondition: the mirror in-key must still be there")

	// Delete the target vertex: its cascade walks the incoming keys.
	s.NoError(s.dbc.Graph.VertexDelete("hd-b"))

	inKey := fmt.Sprintf(crud.InLinkKeyPrefPattern+crud.KeySuff2Pattern, bID, aID, linkName)
	inGone := !c.Exists(inKey)
	residue := edgeResidue(c, aID, linkName)

	s.Falsef(inGone && len(residue) > 0,
		"the delete removed the target's in-key but left the source's half: %v — this is the production asymmetry", residue)
	s.Emptyf(residue, "a delete must leave no half of the edge behind; left: %v", residue)
}

// The healthy path, pinned so the fix cannot regress it: when the out-link does
// resolve, both halves disappear together.
func (s *CMDBClientContractTestSuite) Test_HalfDelete_ResolvableEdgeGoesWhole() {
	s.bootstrap()
	const tA, tB = "HrA", "HrB"
	s.NoError(s.dbc.CMDB.TypeCreate(tA))
	s.NoError(s.dbc.CMDB.TypeCreate(tB))
	s.NoError(s.dbc.CMDB.TypesLinkCreate(tA, tB, "hr-rel", nil))
	s.NoError(s.dbc.CMDB.ObjectUpdate("hr-a", easyjson.NewJSONObject(), false, tA))
	s.NoError(s.dbc.CMDB.ObjectUpdate("hr-b", easyjson.NewJSONObject(), false, tB))
	s.NoError(s.dbc.CMDB.ObjectsLinkUpdate("hr-a", "hr-b", nil, easyjson.NewJSONObject(), false, "hr-edge"))

	c, dm := s.Runtime().Domain.Cache(), s.Runtime().Domain
	aID := dm.CreateObjectIDWithThisDomain("hr-a", false)
	bID := dm.CreateObjectIDWithThisDomain("hr-b", false)

	s.NoError(s.dbc.Graph.VertexDelete("hr-b"))

	s.Falsef(c.Exists(fmt.Sprintf(crud.InLinkKeyPrefPattern+crud.KeySuff2Pattern, bID, aID, "hr-edge")),
		"the in-key must be gone")
	s.Emptyf(edgeResidue(c, aID, "hr-edge"), "the source must keep no half of the deleted edge")
}
