package crud_test

import (
	"fmt"
	"sync"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/embedded/graph/crud"
	"github.com/foliagecp/sdk/statefun/system"
)

// recoveryLoadCall is one delete or restore as its worker saw it: the clock
// right before the call, which is at or before the stamp the client put on the
// operation, and the clock right after the reply.
type recoveryLoadCall struct {
	op            string
	before, after int64
}

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
	calls := make([][]recoveryLoadCall, workers) // each worker appends to its own
	defer func() {
		if s.T().Failed() {
			s.reportRecoveryLoad(calls, rounds*2)
		}
	}()
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			id := fmt.Sprintf("recovery-object-%d", worker)
			call := func(op string, do func() error) error {
				before := system.GetCurrentTimeNs()
				err := do()
				calls[worker] = append(calls[worker], recoveryLoadCall{op, before, system.GetCurrentTimeNs()})
				return err
			}
			for generation := 1; generation <= rounds; generation++ {
				if err := call("delete", func() error { return s.cmdb.ObjectDelete(id) }); err != nil {
					failures <- fmt.Errorf("delete %s: %w", id, err)
					return
				}
				body := easyjson.NewJSONObjectWithKeyValue("generation", easyjson.NewJSON(generation))
				if err := call("restore", func() error { return s.cmdb.ObjectCreate(id, "recovery-load", body) }); err != nil {
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

// reportRecoveryLoad says, for every object the test did not leave whole, what
// the graph holds for it and when its last operations were sent. An operation
// is applied key by key under its own time stamp and a key keeps the newest
// write, so a restore that changed nothing while answering ok shows here as
// keys stamped later than that restore was sent.
func (s *TrashCanTestSuite) reportRecoveryLoad(calls [][]recoveryLoadCall, wantCalls int) {
	c := s.Runtime().Domain.Cache()
	wantType := crud.TO_TYPELINK + "." + s.SetThisDomainPreffix("recovery-load")
	for worker, done := range calls {
		short := fmt.Sprintf("recovery-object-%d", worker)
		id := s.SetThisDomainPreffix(short)
		typeKey := fmt.Sprintf(crud.OutLinkTargetKeyPrefPattern+crud.KeySuff1Pattern, id, "type")
		typeLink, _ := c.GetValue(typeKey)
		for i := 1; i < len(done); i++ {
			if done[i].before < done[i-1].after {
				s.T().Logf("%s: the clock went back %dns between the reply to %s #%d and sending %s #%d",
					short, done[i-1].after-done[i].before, done[i-1].op, i, done[i].op, i+1)
			}
		}
		if string(typeLink) == wantType && len(done) == wantCalls && s.typeHoldsObject("recovery-load", short) {
			continue
		}
		binEdge := fmt.Sprintf(crud.OutLinkBodyKeyPrefPattern+crud.KeySuff1Pattern, s.SetThisDomainPreffix(crud.BUILT_IN_TRASH_CAN), short)
		s.T().Logf("%s: %d of %d calls done; type link %q written at %d; body written at %d; edge from the bin written at %d",
			short, len(done), wantCalls, typeLink, c.GetValueUpdateTime(typeKey), c.GetValueUpdateTime(id), c.GetValueUpdateTime(binEdge))
		for i := max(0, len(done)-3); i < len(done); i++ {
			s.T().Logf("%s: call #%d %s sent at or after %d, answered at %d", short, i+1, done[i].op, done[i].before, done[i].after)
		}
	}
}
