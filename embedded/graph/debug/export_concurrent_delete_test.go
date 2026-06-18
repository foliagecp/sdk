package debug

// Regression tests for a lock-order deadlock between the GraphML export and a
// concurrent delete.
//
// The export's BFS reads each link with functions.graph.api.link.read. A link read
// must only lock the link OWNER (the "from" vertex) — its body and tags live there.
// It used to also lock the target ("to") vertex, which it never touches. That extra
// lock deadlocked with the delete path: deleting an object locks the object first
// and then, inside its cascade, the source vertices of the object's inbound links
// (its type and parents) — i.e. object→type, while the link read locks the keys in
// sorted order, type→object. Opposite order ⇒ the export holds the type and waits
// for the object while the delete holds the object and waits for the type; the cycle
// only broke at the per-key lock timeout.
//
// Both tests drive real CMDB delete entry points:
//   - object deletion: CMDB.ObjectDelete (functions.cmdb.api.object.delete)
//   - type deletion:   CMDB.TypeDelete   (functions.cmdb.api.type.delete, cascades
//                      to object.delete for every object of the type)
//
// Without the link-read fix the export deadlocks and does not return before the
// deadline; with it the export returns.

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/clients/go/db"
	"github.com/foliagecp/sdk/embedded/graph/crud"
	"github.com/foliagecp/sdk/statefun"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/test"
	"github.com/stretchr/testify/suite"
)

const exportDeleteDeadline = 2 * time.Second

// On UNFIXED code these tests reproduce a deadlock that only self-heals at the
// per-key lock timeout. Bound that timeout so the deadlock — and the runtime
// shutdown after the test fails, and any follow-on test — clears in seconds instead
// of the 5-minute production default. exportDeleteDeadline stays below it so the
// export is still observed to hang (the test fails) before the lock times out.
func init() { crud.SetGraphKeyLockTimeoutForTest(3 * time.Second) }

type ExportConcurrentDeleteSuite struct {
	test.StatefunTestSuite
	cmdb db.CMDBSyncClient
}

func TestExportConcurrentDeleteSuite(t *testing.T) {
	suite.Run(t, new(ExportConcurrentDeleteSuite))
}

func (s *ExportConcurrentDeleteSuite) boot() {
	crud.RegisterAllFunctionTypes(s.Runtime())
	debugCfg := *statefun.NewFunctionTypeConfig().
		SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).
		SetMsgAckWaitMs(MAX_ACK_WAIT_MS)
	s.RegisterFunction("functions.graph.api.object.debug.print.graph", LLAPIPrintGraph, debugCfg)
	s.NoError(s.StartRuntime())

	// Wait for the CMDB meta-schema system vertices (root -> types, objects) to be
	// in place before issuing type/object operations.
	s.waitForVertex(crud.BUILT_IN_TYPES)
	s.waitForVertex(crud.BUILT_IN_OBJECTS)

	dbc, err := db.NewDBSyncClientFromRequestFunction(s.Runtime().Request)
	s.NoError(err)
	s.cmdb = dbc.CMDB
}

func (s *ExportConcurrentDeleteSuite) waitForVertex(id string) {
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := s.CacheValue(id); err == nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	s.T().Fatalf("vertex %q did not appear in time", id)
}

// exportInline runs the export from rootID and reports whether it returned before
// the deadline. completed==false means it was stuck (the deadlock).
func (s *ExportConcurrentDeleteSuite) exportInline(rootID string) (r *easyjson.JSON, completed bool) {
	done := make(chan *easyjson.JSON, 1)
	go func() {
		p := easyjson.NewJSONObject()
		p.SetByPath("format", easyjson.NewJSON("dot"))
		p.SetByPath("delivery", easyjson.NewJSON("inline"))
		res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.object.debug.print.graph", rootID, &p, nil)
		if err != nil {
			done <- nil
			return
		}
		done <- res
	}()
	select {
	case r = <-done:
		return r, true
	case <-time.After(exportDeleteDeadline):
		return nil, false
	}
}

func waitOrTimeout(wg *sync.WaitGroup, d time.Duration) {
	c := make(chan struct{})
	go func() { wg.Wait(); close(c) }()
	select {
	case <-c:
	case <-time.After(d):
	}
}

// ---- Scenario 1: export during concurrent OBJECT deletion -------------------

func (s *ExportConcurrentDeleteSuite) Test_ExportCompletesDuringConcurrentObjectDeletion() {
	s.boot()

	const (
		ownerType = "owner"
		itemType  = "item"
		// root must sort BEFORE the item ids: the export's link.read(root, item)
		// then locks [root, item] while a concurrent object.delete(item) locks
		// [item, root] (item is the delete's self, root an inbound-link source) —
		// the opposite order that deadlocks without the link-read fix.
		root    = "container"
		nItems  = 64
		writers = 16
	)

	s.NoError(s.cmdb.TypeCreate(ownerType))
	s.NoError(s.cmdb.TypeCreate(itemType))
	s.NoError(s.cmdb.TypesLinkCreate(ownerType, itemType, "owns", []string{"rel"}))
	s.NoError(s.cmdb.ObjectCreate(root, ownerType))
	for i := 0; i < nItems; i++ {
		s.NoError(s.cmdb.ObjectCreate(fmt.Sprintf("item%03d", i), itemType))
	}
	for i := 0; i < nItems; i++ {
		s.NoError(s.cmdb.ObjectsLinkCreate(root, fmt.Sprintf("item%03d", i), fmt.Sprintf("l%03d", i), []string{"rel"}))
	}

	// Continuous real object.delete (+ recreate) of the owner's items. Each
	// object.delete cascades the owner->item link removal — locking item then owner —
	// while the export's link.read(owner, item) locks owner then item.
	stop := make(chan struct{})
	var wg sync.WaitGroup
	var deletes int64
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				for i := w; i < nItems; i += writers {
					id := fmt.Sprintf("item%03d", i)
					_ = s.cmdb.ObjectDelete(id)
					atomic.AddInt64(&deletes, 1)
					_ = s.cmdb.ObjectCreate(id, itemType)
					_ = s.cmdb.ObjectsLinkCreate(root, id, fmt.Sprintf("l%03d", i), []string{"rel"})
				}
			}
		}(w)
	}

	time.Sleep(200 * time.Millisecond)
	r, completed := s.exportInline(root)
	close(stop)
	waitOrTimeout(&wg, 8*time.Second)

	if !completed {
		s.FailNow("EXPORT HUNG", "export did not complete within %s under concurrent object.delete (lock-order deadlock)", exportDeleteDeadline)
	}
	s.Require().NotNil(r, "export request errored")
	s.Equal("ok", r.GetByPath("status").AsStringDefault(""), "export must succeed under concurrent object.delete")
	s.Greater(len(r.GetByPath("data.file").AsStringDefault("")), 0, "export output must be non-empty")
	s.T().Logf("OBJECT deletion: export completed after %d real object.delete calls", atomic.LoadInt64(&deletes))
}

// ---- Scenario 2: export during a concurrent TYPE deletion cascade -----------

func (s *ExportConcurrentDeleteSuite) Test_ExportCompletesDuringConcurrentTypeDeletion() {
	s.boot()

	const (
		ownerType = "owner"
		itemType  = "item"
		// root sorts BEFORE the item ids (see the object-deletion test): the cascade's
		// per-item object.delete locks [item, root] while the export's link.read(root,
		// item) locks [root, item] — the opposite order that deadlocks without the fix.
		// (The type itself can't drive the deadlock here: type.delete holds its write
		// lock across the cascade, so a link.read of the type just waits, not deadlocks.)
		root   = "container"
		nItems = 150
	)

	// owner1 (owner) references many items. type.delete on itemType cascades to
	// object.delete for each item. owner1 itself is not deleted, so the export
	// always has a root to return.
	s.NoError(s.cmdb.TypeCreate(ownerType))
	s.NoError(s.cmdb.TypeCreate(itemType))
	s.NoError(s.cmdb.TypesLinkCreate(ownerType, itemType, "owns", []string{"rel"}))
	s.NoError(s.cmdb.ObjectCreate(root, ownerType))
	for i := 0; i < nItems; i++ {
		s.NoError(s.cmdb.ObjectCreate(fmt.Sprintf("item%04d", i), itemType))
	}
	for i := 0; i < nItems; i++ {
		s.NoError(s.cmdb.ObjectsLinkCreate(root, fmt.Sprintf("item%04d", i), fmt.Sprintf("l%04d", i), []string{"rel"}))
	}

	// Real type.delete running concurrently with the export. The cascade's per-object
	// deletes contend with the export's link reads of those same objects.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = s.cmdb.TypeDelete(itemType)
	}()

	r, completed := s.exportInline(root)
	waitOrTimeout(&wg, 8*time.Second)

	if !completed {
		s.FailNow("EXPORT HUNG", "export did not complete within %s during a concurrent type.delete cascade (lock-order deadlock)", exportDeleteDeadline)
	}
	// Content is intentionally not asserted: the items are being cascade-deleted
	// concurrently, so a partial result is expected and fine. The regression check is
	// that the export RETURNS at all — i.e. does not deadlock.
	s.Require().NotNil(r, "export request errored during a concurrent type.delete cascade")
	s.T().Logf("TYPE deletion: export returned (no deadlock) during a concurrent type.delete cascade")
}
