package crud_test

// Bug hunt H8: property-based random CRUD sequence.
//
// A deterministic (seeded) random walk of create/update/delete/link operations
// over a small pool of object ids, asserting graph consistency after EVERY step.
// This is the catch-all for emergent state leaks (orphans, phantoms, dangling
// indices) that targeted tests miss. A failure prints the seed/step for replay.
//
// Method on CMDBClientContractTestSuite to reuse its bootstrap.

import (
	"fmt"
	"math/rand"

	"github.com/foliagecp/easyjson"
)

func (s *CMDBClientContractTestSuite) Test_Hunt_PropertyRandomSequence() {
	s.bootstrap()
	s.NoError(s.dbc.CMDB.TypeCreate("PrType"))
	s.NoError(s.dbc.CMDB.TypesLinkCreate("PrType", "PrType", "pr-rel", nil))

	const idPool = 6
	const steps = 150
	idName := func(i int) string { return fmt.Sprintf("pr-%d", i) }

	rng := rand.New(rand.NewSource(1)) // deterministic for replay
	alive := map[string]struct{}{}

	checkAll := func(step int) {
		c := s.Runtime().Domain.Cache()
		for id := range alive {
			assertObjectConsistent(s.T(), c, s.SetThisDomainPreffix(id))
		}
		if s.T().Failed() {
			s.T().Fatalf("graph inconsistent at step %d (seed=1)", step)
		}
	}

	for step := 0; step < steps; step++ {
		id := idName(rng.Intn(idPool))
		switch rng.Intn(4) {
		case 0, 1: // upsert (create-or-update)
			b := easyjson.NewJSONObjectWithKeyValue("v", easyjson.NewJSON(step))
			s.NoErrorf(s.dbc.CMDB.ObjectUpdate(id, b, false, "PrType"), "step %d upsert %s", step, id)
			alive[id] = struct{}{}
		case 2: // delete (if alive)
			if _, ok := alive[id]; ok {
				s.NoErrorf(s.dbc.CMDB.ObjectDelete(id), "step %d delete %s", step, id)
				delete(alive, id)
			}
		case 3: // link two distinct alive objects
			other := idName(rng.Intn(idPool))
			_, a := alive[id]
			_, b := alive[other]
			if a && b && id != other {
				// link op may legitimately no-op/fail in some states; the
				// invariant after it is what matters, so don't assert the result.
				_ = s.dbc.CMDB.ObjectsLinkUpdate(id, other, nil, easyjson.NewJSONObject(), false, "ln-"+other)
			}
		}
		checkAll(step)
	}
}
