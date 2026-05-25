package crud_test

// Bug hunt: JPGQL multi-hop correctness (H7m) and concurrent query+write (H11).
//
//   - multi-hop: a two-step traversal a -ab-> b -bc-> c must return c.
//   - concurrent: JPGQL reads (GetKeysByPattern-heavy) running against live link
//     writes on the same source under -race must not race and the final result
//     must include every committed edge.
//
// Methods on CMDBClientContractTestSuite to reuse its bootstrap.

import (
	"fmt"
	"sync"

	"github.com/foliagecp/easyjson"
)

// Two-hop traversal returns the far target.
func (s *CMDBClientContractTestSuite) Test_Hunt_JPGQL_MultiHop() {
	s.bootstrap()
	s.NoError(s.dbc.CMDB.TypeCreate("MhA"))
	s.NoError(s.dbc.CMDB.TypeCreate("MhB"))
	s.NoError(s.dbc.CMDB.TypeCreate("MhC"))
	s.NoError(s.dbc.CMDB.TypesLinkCreate("MhA", "MhB", "ab", nil))
	s.NoError(s.dbc.CMDB.TypesLinkCreate("MhB", "MhC", "bc", nil))

	s.NoError(s.dbc.CMDB.ObjectUpdate("mh-a", easyjson.NewJSONObject(), false, "MhA"))
	s.NoError(s.dbc.CMDB.ObjectUpdate("mh-b", easyjson.NewJSONObject(), false, "MhB"))
	s.NoError(s.dbc.CMDB.ObjectUpdate("mh-c", easyjson.NewJSONObject(), false, "MhC"))
	s.NoError(s.dbc.CMDB.ObjectsLinkUpdate("mh-a", "mh-b", nil, easyjson.NewJSONObject(), false, "a2b"))
	s.NoError(s.dbc.CMDB.ObjectsLinkUpdate("mh-b", "mh-c", nil, easyjson.NewJSONObject(), false, "b2c"))

	res, err := s.dbc.Query.JPGQLCtraQuery("mh-a", ".*[l:type('ab')].*[l:type('bc')]")
	s.NoError(err)
	s.Lenf(res, 1, "two-hop traversal must return exactly the far target; got %v", res)
	if len(res) == 1 {
		s.Truef(endsWith(res[0], "mh-c"), "two-hop target must be mh-c; got %v", res)
	}
}

func endsWith(s, suffix string) bool {
	return len(s) >= len(suffix) && s[len(s)-len(suffix):] == suffix
}

// Concurrent JPGQL reads while link writes land on the same source — race surface
// for GetKeysByPattern vs index writes. After the dust settles every edge is found.
func (s *CMDBClientContractTestSuite) Test_Hunt_ConcurrentQueryAndLinkWrite() {
	s.bootstrap()
	s.NoError(s.dbc.CMDB.TypeCreate("CqA"))
	s.NoError(s.dbc.CMDB.TypeCreate("CqB"))
	s.NoError(s.dbc.CMDB.TypesLinkCreate("CqA", "CqB", "cq", nil))
	s.NoError(s.dbc.CMDB.ObjectUpdate("cq-src", easyjson.NewJSONObject(), false, "CqA"))

	const n = 12
	for i := 0; i < n; i++ {
		s.NoError(s.dbc.CMDB.ObjectUpdate(fmt.Sprintf("cq-t-%d", i), easyjson.NewJSONObject(), false, "CqB"))
	}

	var wg sync.WaitGroup
	// writers: link src -> each target
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			t := fmt.Sprintf("cq-t-%d", i)
			_ = s.dbc.CMDB.ObjectsLinkUpdate("cq-src", t, nil, easyjson.NewJSONObject(), false, "e-"+t)
		}(i)
	}
	// readers: query concurrently with the writes
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for k := 0; k < 25; k++ {
				_, _ = s.dbc.Query.JPGQLCtraQuery("cq-src", ".*[l:type('cq')]")
			}
		}()
	}
	wg.Wait()

	res, err := s.dbc.Query.JPGQLCtraQuery("cq-src", ".*[l:type('cq')]")
	s.NoError(err)
	s.Lenf(res, n, "all %d concurrently-written edges must be found; got %d", n, len(res))
}
