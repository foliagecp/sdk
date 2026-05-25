package crud_test

// Bug hunt (found by the crud-soak system test): under concurrent create/delete
// of objects of the SAME type, the type vertex's `__object` enumeration index
// can be left with a dangling entry — JPGQL `[l:type('__object')]` then returns
// an id whose vertex no longer exists and is unreadable.
//
// All objects below are created and then deleted, so at quiesce the type's
// __object out-links must be EMPTY. Any survivor is a dangling index entry.
// Run with -race: the hot type vertex is mutated by many goroutines at once, so
// a data race on the shared cache subtree (or a lost index delete) is a real
// defect.

import (
	"fmt"
	"sync"

	"github.com/foliagecp/sdk/embedded/graph/crud"
)

// danglingObjectIndex returns the __object out-links of a type whose target
// object can no longer be read — i.e. dangling enumeration index entries.
func (s *CMDBClientContractTestSuite) danglingObjectIndex(typeName string) []string {
	domType := s.SetThisDomainPreffix(typeName)
	// `<type>.ltype.__object.<toId>` = linkName(=toId for CMDB objects)
	pattern := fmt.Sprintf(crud.OutLinkTypeKeyPrefPattern+"%s.>", domType, crud.OBJECT_TYPELINK)
	keys := s.Runtime().Domain.Cache().GetKeysByPattern(pattern)
	// key form: <domType>.ltype.__object.<toId>; recover the trailing object id.
	prefix := fmt.Sprintf(crud.OutLinkTypeKeyPrefPattern+"%s.", domType, crud.OBJECT_TYPELINK)
	var dangling []string
	for _, k := range keys {
		if len(k) <= len(prefix) {
			continue
		}
		id := k[len(prefix):]
		if _, err := s.dbc.CMDB.ObjectRead(id); err != nil {
			dangling = append(dangling, id)
		}
	}
	return dangling
}

// Concurrent create+delete churn on one hot type. Every object is deleted, so
// the type must end with zero __object index entries; a survivor that cannot be
// read is the dangling-index bug.
func (s *CMDBClientContractTestSuite) Test_Hunt_ConcurrentCreateDelete_TypeIndexNoDangling() {
	s.bootstrap()
	const typeName = "CtiType"
	s.NoError(s.dbc.CMDB.TypeCreate(typeName))

	const (
		workers = 16
		perW    = 25
	)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perW; i++ {
				id := fmt.Sprintf("cti-w%d-%d", w, i)
				if err := s.dbc.CMDB.ObjectCreate(id, typeName); err != nil {
					continue
				}
				_ = s.dbc.CMDB.ObjectDelete(id)
			}
		}(w)
	}
	wg.Wait()

	dangling := s.danglingObjectIndex(typeName)
	s.Emptyf(dangling, "type %q has %d dangling __object index entries after concurrent create/delete: %v",
		typeName, len(dangling), dangling)
}
