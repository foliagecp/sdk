package crud_test

// Mass parallel link upserts into ONE vertex must not lose a link's content.
//
// From the field: ~100k links written through CMDB upserts from a pool of
// goroutines, and afterwards a fraction of a percent of them answered
// "link from=… with name=…" on every later update, for as long as the runtime
// lived. The link was still findable through its type/target index, but its
// out.body was gone, so link.update failed at the read that precedes the write
// — and a restart cleared it, because the loss was never in KV, only in the
// operating representation.
//
// The mechanism is a records one: a vertex's links live in a bucket directory
// that is rebuilt as it grows and shrinks, and a rebuild that read its buckets
// without holding them dropped whatever write was going into one of them. It
// costs nothing to reproduce once the rebuild is made to run alongside the
// writes, which is what this does; on the code that had the defect it loses
// dozens of link bodies in seconds.
//
// The assertions are the report's own: for every link written, the owner must
// hold out.to and out.body, a second upsert must not still be failing, and
// what the WAL carried to KV must hold them too.

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/clients/go/db"
	"github.com/foliagecp/sdk/embedded/graph/crud"
	"github.com/foliagecp/sdk/statefun/test"
	"github.com/stretchr/testify/suite"
)

type LinkBodyLossTestSuite struct {
	test.StatefunTestSuite
	dbc db.DBSyncClient
}

func TestLinkBodyLossTestSuite(t *testing.T) {
	suite.Run(t, new(LinkBodyLossTestSuite))
}

func (s *LinkBodyLossTestSuite) bootstrap() {
	crud.RegisterAllFunctionTypes(s.Runtime())
	s.NoError(s.StartRuntime())
	deadline := time.Now().Add(15 * time.Second)
	for _, id := range []string{crud.BUILT_IN_TYPES, crud.BUILT_IN_OBJECTS} {
		for {
			if _, err := s.CacheValue(id); err == nil {
				break
			}
			if time.Now().After(deadline) {
				s.T().Fatalf("vertex %q did not appear in time", id)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	dbc, err := db.NewDBSyncClientFromRequestFunction(s.Runtime().Request)
	s.NoError(err)
	s.dbc = dbc
}

func (s *LinkBodyLossTestSuite) Test_MassParallelLinkUpsert_KeepsEveryLinkBody() {
	s.bootstrap()
	cmdb := s.dbc.CMDB
	const srcType, dstType = "LbSrc", "LbDst"
	const n = 5000

	s.Require().NoError(cmdb.TypeCreate(srcType))
	s.Require().NoError(cmdb.TypeCreate(dstType))
	s.Require().NoError(cmdb.TypesLinkCreate(srcType, dstType, "lb-rel", nil))
	s.Require().NoError(cmdb.ObjectCreate("lb-src", srcType))

	// The targets first, so the measured pass writes only links.
	for i := 0; i < n; i++ {
		s.Require().NoError(cmdb.ObjectCreate(fmt.Sprintf("lb-dst-%04d", i), dstType))
	}

	// The runtime sweeps and rebuilds records on its own timer; the window
	// this is about opens when a rebuild of the owner's directory meets a
	// write going into it, so the sweep is driven here rather than waited for.
	upsertAll := func() {
		stop := make(chan struct{})
		swept := make(chan struct{})
		go func() {
			defer close(swept)
			for {
				select {
				case <-stop:
					return
				default:
					s.Runtime().Domain.Cache().RunMaintenanceForTest()
				}
			}
		}()
		defer func() { close(stop); <-swept }()

		var wg sync.WaitGroup
		work := make(chan int, n)
		for w := 0; w < runtime.NumCPU(); w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := range work {
					body := easyjson.NewJSONObjectWithKeyValue("i", easyjson.NewJSON(i))
					_ = cmdb.ObjectsLinkUpdate("lb-src", fmt.Sprintf("lb-dst-%04d", i), nil, body, false,
						fmt.Sprintf("lb-e-%04d", i))
				}
			}()
		}
		for i := 0; i < n; i++ {
			work <- i
		}
		close(work)
		wg.Wait()
	}

	upsertAll()

	dm := s.Runtime().Domain
	c := dm.Cache()
	srcID := dm.CreateObjectIDWithThisDomain("lb-src", false)

	missing := func(what string) (noTo, noBody, noLtype []string) {
		for i := 0; i < n; i++ {
			name := fmt.Sprintf("lb-e-%04d", i)
			to := dm.CreateObjectIDWithThisDomain(fmt.Sprintf("lb-dst-%04d", i), false)
			if !c.Exists(fmt.Sprintf(crud.OutLinkTargetKeyPrefPattern+crud.KeySuff1Pattern, srcID, name)) {
				noTo = append(noTo, name)
			}
			if _, err := c.GetValueJSON(fmt.Sprintf(crud.OutLinkBodyKeyPrefPattern+crud.KeySuff1Pattern, srcID, name)); err != nil {
				noBody = append(noBody, name)
			}
			if !c.Exists(fmt.Sprintf(crud.OutLinkTypeKeyPrefPattern+crud.KeySuff2Pattern, srcID, "lb-rel", to)) {
				noLtype = append(noLtype, name)
			}
		}
		s.T().Logf("%s: out.to missing=%d, out.body missing=%d, ltype missing=%d (of %d)",
			what, len(noTo), len(noBody), len(noLtype), n)
		return
	}

	_, bodyGone, _ := missing("in memory after the first pass")

	// The report's symptom: a second upsert of the same links fails on exactly
	// those that lost their body.
	upsertAll()
	_, bodyGone2, _ := missing("in memory after the second pass")

	// And what the WAL carried: drain, reload from KV alone, look again.
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	s.Require().NoError(dm.WaitForKVCaughtUp(ctx, 120*time.Second))
	s.Require().NoError(c.RehydrateFromKV(ctx))
	_, bodyGoneKV, _ := missing("after reloading from KV")

	s.Emptyf(bodyGone, "links lost their body in memory: %v", firstFew(bodyGone))
	s.Emptyf(bodyGone2, "links still without a body after a second upsert: %v", firstFew(bodyGone2))
	s.Emptyf(bodyGoneKV, "links whose body never reached KV: %v", firstFew(bodyGoneKV))
}

func firstFew(v []string) []string {
	if len(v) > 8 {
		return v[:8]
	}
	return v
}
