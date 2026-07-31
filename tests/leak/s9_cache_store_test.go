//go:build leak

package leak

import (
	"fmt"
	"testing"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/clients/go/db"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
	"github.com/stretchr/testify/suite"
)

// S9 — the cache store itself: tombstone-sweep effectiveness under direct
// deep-key churn (PASS), plus two probes that manufacture the partial link
// states the delete API cannot clean up:
//   - force-retarget residue — EXPECTED FAIL today (finding L4);
//   - orphaned out.to link — EXPECTED FAIL today (finding L5).

type S9Suite struct{ leakSuite }

func TestS9CacheStore(t *testing.T) { suite.Run(t, new(S9Suite)) }

// Test_CacheStoreSweep churns deep raw keys straight against the store (plus
// a small graph mix) and requires the post-order sweep to collapse every
// tombstone cascade back to the baseline tree.
func (s *S9Suite) Test_CacheStoreSweep() {
	s.bootCRUD()
	k := scaled(200)

	cycle := func(c int) error {
		cs := s.cacheStore()
		for i := 0; i < k; i++ {
			key := fmt.Sprintf("s9data.%d.%d.leaf", c, i)
			body := leakBody(64)
			if !cs.SetValueJSON(key, &body, true, system.GetCurrentTimeNs()) {
				return fmt.Errorf("SetValueJSON %s failed", key)
			}
		}
		for i := 0; i < k; i++ {
			cs.DeleteValue(fmt.Sprintf("s9data.%d.%d.leaf", c, i), true, -1)
		}
		for i := 0; i < 10; i++ {
			id := fmt.Sprintf("s9v-%d-%d", c, i)
			if err := s.dbc.Graph.VertexCreate(id, leakBody(50)); err != nil {
				return err
			}
			if err := s.dbc.Graph.VertexDelete(id); err != nil {
				return err
			}
		}
		return nil
	}

	rep := s.newRunner("s9_cache_sweep", cycle, s.collectCore).Run(s.T())
	rep.AssertClean(s.T())
	s.assertCoreStable(rep)
}

// forceLinkCreate issues functions.graph.api.link.create with force=true —
// the flag the Go client does not expose.
func (s *S9Suite) forceLinkCreate(from, to, name, linkType string) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("op_time", easyjson.NewJSON(system.GetCurrentTimeNs()))
	payload.SetByPath("to", easyjson.NewJSON(to))
	payload.SetByPath("name", easyjson.NewJSON(name))
	payload.SetByPath("type", easyjson.NewJSON(linkType))
	payload.SetByPath("body", easyjson.NewJSONObject())
	payload.SetByPath("force", easyjson.NewJSON(true))
	reply, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.create", from, &payload, nil)
	return db.OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(reply, err))
}

// Test_ForceLinkRetargetLeavesResidue — regression guard for finding L4.
//
// link.create force=true over an existing link with a NEW target historically
// overwrote only the name-keyed key families, stranding the old target's
// `ltype` and `in` keys forever. force is now an atomic replace — the old
// link's keys are dropped first — so after deleting the link and all three
// vertices the cache tree must return to baseline.
func (s *S9Suite) Test_ForceLinkRetargetLeavesResidue() {
	s.bootCRUD()
	k := scaled(10)

	cycle := func(c int) error {
		for i := 0; i < k; i++ {
			a := fmt.Sprintf("s9f-a-%d-%d", c, i)
			b := fmt.Sprintf("s9f-b-%d-%d", c, i)
			c2 := fmt.Sprintf("s9f-c-%d-%d", c, i)
			for _, id := range []string{a, b, c2} {
				if err := s.dbc.Graph.VertexCreate(id, leakBody(30)); err != nil {
					return err
				}
			}
			if err := s.dbc.Graph.VerticesLinkCreate(a, b, "e", "rel", nil); err != nil {
				return err
			}
			// Retarget the SAME name+type to another vertex, bypassing the
			// uniqueness checks.
			if err := s.forceLinkCreate(a, c2, "e", "rel"); err != nil {
				return err
			}
			// Full teardown through the public API must leave nothing behind.
			if err := s.dbc.Graph.VerticesLinkDelete(a, "e"); err != nil {
				return err
			}
			for _, id := range []string{a, b, c2} {
				if err := s.dbc.Graph.VertexDelete(id); err != nil {
					return err
				}
			}
		}
		return nil
	}

	rep := s.newRunner("s9_force_retarget", cycle, s.collectCore).Run(s.T())
	rep.AssertClean(s.T())
	s.assertCoreStable(rep)
}

// Test_OrphanOutToUndeletable — regression guard for finding L5.
//
// A link whose `<from>.out.to.<name>` key is missing (interrupted write,
// partial replication) was historically undeletable: every delete path
// resolved the target through that key, gave up with IDLE and left
// `out.body`/`out.index`/`ltype`/`in` behind forever. The delete paths now
// recover the target from the ltype family (which encodes type and target in
// the key itself), so the orphaned link deletes cleanly and the cache tree
// returns to baseline.
func (s *S9Suite) Test_OrphanOutToUndeletable() {
	s.bootCRUD()
	k := scaled(10)

	cycle := func(c int) error {
		for i := 0; i < k; i++ {
			a := fmt.Sprintf("s9o-a-%d-%d", c, i)
			b := fmt.Sprintf("s9o-b-%d-%d", c, i)
			if err := s.dbc.Graph.VertexCreate(a, leakBody(30)); err != nil {
				return err
			}
			if err := s.dbc.Graph.VertexCreate(b, leakBody(30)); err != nil {
				return err
			}
			if err := s.dbc.Graph.VerticesLinkCreate(a, b, "e", "rel", []string{"tag"}); err != nil {
				return err
			}
			// Manufacture the orphan: the out.to key vanishes.
			s.cacheStore().DeleteValue(s.domainID(a)+".out.to.e", true, -1)
			// The orphaned link must still delete cleanly via the ltype
			// fallback, and the vertices after it.
			if err := s.dbc.Graph.VerticesLinkDelete(a, "e"); err != nil {
				return fmt.Errorf("link.delete of orphaned link: %w", err)
			}
			if err := s.dbc.Graph.VertexDelete(a); err != nil {
				return fmt.Errorf("vertex.delete %s: %w", a, err)
			}
			if err := s.dbc.Graph.VertexDelete(b); err != nil {
				return fmt.Errorf("vertex.delete %s: %w", b, err)
			}
		}
		return nil
	}

	rep := s.newRunner("s9_orphan_outto", cycle, s.collectCore).Run(s.T())
	rep.AssertClean(s.T())
	s.assertCoreStable(rep)
}
