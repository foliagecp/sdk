package crud

// The WAL write barrier rests on one symmetry: a handler takes its first write
// lock, that pass marks an operation active, and the single unlock at the end
// of the handler marks it done. The publisher refuses to write any transaction
// not older than the oldest active operation, so a mark that is never released
// wedges it for good — the graph keeps serving reads from memory while nothing
// reaches the KV any more.
//
// Two shapes break the symmetry, and both were reachable through the trash-can
// restore before they were fixed:
//
//   - SEVERAL lock passes inside one invocation (the restore adds per-edge
//     locks to the object lock the caller already holds) against one unlock;
//   - a NESTED call inheriting the bookkeeping and releasing the parent's mark
//     while the parent is still writing.
//
// These tests exercise the mechanics directly, with no CRUD operation involved,
// so a future write path that takes locks in more than one pass is covered by
// construction.

import (
	"testing"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/statefun"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/test"
	"github.com/stretchr/testify/suite"
)

type LockMarkSymmetryTestSuite struct {
	test.StatefunTestSuite
}

func TestLockMarkSymmetryTestSuite(t *testing.T) {
	suite.Run(t, new(LockMarkSymmetryTestSuite))
}

func (s *LockMarkSymmetryTestSuite) register(name string, h statefun.FunctionLogicHandler) {
	statefun.NewFunctionType(s.Runtime(), name, h,
		*statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect))
}

func (s *LockMarkSymmetryTestSuite) call(typename, id string) {
	payload := easyjson.NewJSONObject()
	res, err := s.Request(sfPlugins.AutoRequestSelect, typename, id, &payload, nil)
	s.Require().NoError(err)
	s.Require().Equal("ok", res.GetByPath("status").AsStringDefault(""), res.ToString())
}

// activeOps reports the in-flight operations holding the barrier right now.
func (s *LockMarkSymmetryTestSuite) activeOps() int {
	return s.Runtime().Domain.Cache().StatsForTest().ActiveOps
}

// requireBarrierReleased waits out the asynchronous tail of a call (triggers and
// signals dispatched on the way out) and then insists the barrier is free.
func (s *LockMarkSymmetryTestSuite) requireBarrierReleased() {
	s.T().Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if s.activeOps() == 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	s.Failf("WAL barrier still held", "%d in-flight operation(s) after the handler answered", s.activeOps())
}

// Several write-lock passes in one invocation, one unlock: the barrier must end
// up free. Before the fix each pass marked an operation active while the single
// unlock released one, leaving the rest pinned forever.
func (s *LockMarkSymmetryTestSuite) Test_SeveralLockPasses_OneUnlock_ReleaseTheBarrier() {
	s.register("test.lock.passes", func(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
		opTime := getOpTimeFromPayloadIfExist(ctx.Payload)
		operationKeysMutexLock(ctx, []string{"lms-a"}, true, opTime)
		operationKeysMutexLock(ctx, []string{"lms-b"}, true, opTime)
		operationKeysMutexLockMixed(ctx, []string{"lms-c"}, []string{"lms-d"}, opTime)
		operationKeysMutexUnlock(ctx)
		sfMediators.NewOpMediator(ctx).AggregateOpMsg(sfMediators.OpMsgOk(easyjson.NewJSONNull())).Reply()
	})
	s.NoError(s.StartRuntime())

	s.call("test.lock.passes", "lms1")

	s.requireBarrierReleased()
}

// A nested call must not consume the caller's mark: the barrier has to stay up
// for as long as the PARENT holds its lock, and come down only when the parent
// unlocks. Otherwise a transaction can be published while the parent is still
// writing.
func (s *LockMarkSymmetryTestSuite) Test_NestedCall_DoesNotReleaseTheParentsMark() {
	s.register("test.lock.child", func(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
		opTime := getOpTimeFromPayloadIfExist(ctx.Payload)
		operationKeysMutexLock(ctx, []string{"lmn-child"}, true, opTime)
		operationKeysMutexUnlock(ctx)
		sfMediators.NewOpMediator(ctx).AggregateOpMsg(sfMediators.OpMsgOk(easyjson.NewJSONNull())).Reply()
	})

	heldAfterChild := make(chan int, 1)
	s.register("test.lock.parent", func(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
		opTime := getOpTimeFromPayloadIfExist(ctx.Payload)
		operationKeysMutexLock(ctx, []string{"lmn-parent"}, true, opTime)

		_, _ = ctx.Request(sfPlugins.AutoRequestSelect, "test.lock.child",
			makeSequenceFreeParentBasedID(ctx, "lmn-c1"), injectParentHoldsLocks(ctx, ctx.Payload), nil)

		// The parent is still writing: its mark must be the one still standing.
		heldAfterChild <- ctx.Domain.Cache().StatsForTest().ActiveOps

		operationKeysMutexUnlock(ctx)
		sfMediators.NewOpMediator(ctx).AggregateOpMsg(sfMediators.OpMsgOk(easyjson.NewJSONNull())).Reply()
	})
	s.NoError(s.StartRuntime())

	s.call("test.lock.parent", "lmn1")

	select {
	case held := <-heldAfterChild:
		s.GreaterOrEqual(held, 1, "the child's unlock must not release the parent's mark")
	case <-time.After(5 * time.Second):
		s.Fail("the parent handler never reported")
	}
	s.requireBarrierReleased()
}
