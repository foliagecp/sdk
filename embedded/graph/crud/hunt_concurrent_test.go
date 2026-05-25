package crud_test

// Bug hunt: concurrent CRUD on the same key — run with -race.
//
// Adversarial concurrency: many goroutines mutate the same object / the same
// source vertex's links at once. The runtime serializes per-id, but the cache
// tree, op_stack and index writes are shared structures — a race or a lost/
// duplicated invariant link would be a real defect. -race catches data races;
// the post-conditions catch corruption (e.g. a duplicated __type link).
//
// Methods on CMDBClientContractTestSuite to reuse its bootstrap.

import (
	"fmt"
	"sync"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/embedded/graph/crud"
)

// outLinkCountOfType counts a vertex's out-links of a given type via the ltype index.
func (s *CMDBClientContractTestSuite) outLinkCountOfType(from, linkType string) int {
	domID := s.SetThisDomainPreffix(from)
	pattern := fmt.Sprintf(crud.OutLinkTypeKeyPrefPattern+"%s.>", domID, linkType)
	return len(s.Runtime().Domain.Cache().GetKeysByPattern(pattern))
}

// Concurrent replace-updates of the SAME object must not race and must leave
// exactly one __type link (not zero, not duplicated) and a readable body.
func (s *CMDBClientContractTestSuite) Test_Hunt_ConcurrentObjectUpdate_SameObject() {
	s.bootstrap()
	s.NoError(s.dbc.CMDB.TypeCreate("CoType"))
	s.NoError(s.dbc.CMDB.ObjectUpdate("co-1", easyjson.NewJSONObject(), false, "CoType"))

	const n = 16
	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			b := easyjson.NewJSONObjectWithKeyValue("v", easyjson.NewJSON(i))
			errs[i] = s.dbc.CMDB.ObjectUpdate("co-1", b, true, "CoType")
		}(i)
	}
	wg.Wait()

	for i := range errs {
		s.NoErrorf(errs[i], "concurrent update %d failed", i)
	}
	s.Equalf(1, s.outLinkCountOfType("co-1", crud.TO_TYPELINK),
		"object must have exactly one __type link after concurrent updates")
	data, err := s.dbc.CMDB.ObjectRead("co-1")
	s.NoError(err)
	s.True(data.PathExists("body"), "object body must be readable after concurrent updates")
}

// Concurrent link upserts from the SAME source vertex (distinct link names,
// shared out.index/ltype subtree) must not race and all must land.
func (s *CMDBClientContractTestSuite) Test_Hunt_ConcurrentLinkCreate_SameSource() {
	s.bootstrap()
	s.NoError(s.dbc.CMDB.TypeCreate("CsA"))
	s.NoError(s.dbc.CMDB.TypeCreate("CsB"))
	s.NoError(s.dbc.CMDB.TypesLinkCreate("CsA", "CsB", "crel", nil))
	s.NoError(s.dbc.CMDB.ObjectUpdate("cs-src", easyjson.NewJSONObject(), false, "CsA"))

	const n = 16
	for i := 0; i < n; i++ {
		s.NoError(s.dbc.CMDB.ObjectUpdate(fmt.Sprintf("cs-tgt-%d", i), easyjson.NewJSONObject(), false, "CsB"))
	}

	errs := make([]error, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			tgt := fmt.Sprintf("cs-tgt-%d", i)
			errs[i] = s.dbc.CMDB.ObjectsLinkUpdate("cs-src", tgt, nil, easyjson.NewJSONObject(), false, "edge-"+tgt)
		}(i)
	}
	wg.Wait()

	for i := range errs {
		s.NoErrorf(errs[i], "concurrent link create %d failed", i)
	}
	s.Equalf(n, s.outLinkCountOfType("cs-src", "crel"),
		"all %d concurrent links must land (ltype index count)", n)
}
