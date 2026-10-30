package crud_test

import (
	"fmt"
	"sync"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/embedded/graph/crud"
)

// Exercise real object deletes/restores alongside type reads. Each worker
// owns one ID; read repair must never create a second body or put a restored
// object back into the trash can from a stale membership.
func (s *TrashCanTestSuite) Test_TrashCan_ConcurrentRestoreAndTypeReadKeepSingleMembership() {
	s.boot()
	s.Require().NoError(s.cmdb.TypeCreate("recovery-load"))
	const workers, rounds = 6, 8
	for i := 0; i < workers; i++ {
		s.Require().NoError(s.cmdb.ObjectCreate(fmt.Sprintf("recovery-object-%d", i), "recovery-load", easyjson.NewJSONObject()))
	}
	failures := make(chan error, workers+1)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			id := fmt.Sprintf("recovery-object-%d", worker)
			for generation := 1; generation <= rounds; generation++ {
				if err := s.cmdb.ObjectDelete(id); err != nil {
					failures <- fmt.Errorf("delete %s: %w", id, err)
					return
				}
				body := easyjson.NewJSONObjectWithKeyValue("generation", easyjson.NewJSON(generation))
				if err := s.cmdb.ObjectCreate(id, "recovery-load", body); err != nil {
					failures <- fmt.Errorf("restore %s: %w", id, err)
					return
				}
			}
		}(i)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < rounds*2; i++ {
			for _, name := range []string{"recovery-load", crud.BUILT_IN_TRASH_CAN} {
				data, err := s.cmdb.TypeRead(name)
				if err != nil {
					failures <- fmt.Errorf("type read %s: %w", name, err)
					return
				}
				ids, _ := data.GetByPath("object_ids").AsArrayString()
				seen := map[string]bool{}
				for _, id := range ids {
					if seen[id] {
						failures <- fmt.Errorf("duplicate object %s in %s", id, name)
						return
					}
					seen[id] = true
				}
			}
		}
	}()
	wg.Wait()
	close(failures)
	for err := range failures {
		s.NoError(err)
	}
	data, err := s.cmdb.TypeRead("recovery-load")
	s.Require().NoError(err)
	ids, _ := data.GetByPath("object_ids").AsArrayString()
	s.Len(ids, workers)
	bin, err := s.cmdb.TypeRead(crud.BUILT_IN_TRASH_CAN)
	s.Require().NoError(err)
	parked, _ := bin.GetByPath("object_ids").AsArrayString()
	s.Empty(parked)
	for i := 0; i < workers; i++ {
		id := fmt.Sprintf("recovery-object-%d", i)
		body, err := s.cmdb.ObjectRead(id)
		s.Require().NoError(err)
		s.Equal(int64(rounds), body.GetByPath("body.generation").AsInt64Default(0))
		s.True(s.typeHoldsObject("recovery-load", id))
		s.False(s.typeHoldsObject(crud.BUILT_IN_TRASH_CAN, id))
	}
}
