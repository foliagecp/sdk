package batch_test

import (
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/clients/go/db"
	"github.com/foliagecp/sdk/embedded/graph/batch"
	"github.com/foliagecp/sdk/embedded/graph/crud"
	"github.com/foliagecp/sdk/statefun"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfp "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
	"github.com/foliagecp/sdk/statefun/test"
	"github.com/stretchr/testify/suite"
)

func TestMain(m *testing.M) {
	if system.GlobalPrometrics == nil {
		system.GlobalPrometrics = system.NewPrometrics("", "127.0.0.1:0")
	}
	os.Exit(m.Run())
}

type BatchSuite struct {
	test.StatefunTestSuite
	dbc db.DBSyncClient
}

func TestBatchSuite(t *testing.T) { suite.Run(t, new(BatchSuite)) }

func (s *BatchSuite) boot() {
	crud.RegisterAllFunctionTypes(s.Runtime())
	batch.RegisterAllFunctionTypes(s.Runtime())
	s.NoError(s.StartRuntime())
	s.waitForVertex(crud.BUILT_IN_TYPES)
	s.waitForVertex(crud.BUILT_IN_OBJECTS)
	dbc, err := db.NewDBSyncClientFromRequestFunction(s.Runtime().Request)
	s.NoError(err)
	s.dbc = dbc
}

func (s *BatchSuite) waitForVertex(id string) {
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := s.CacheValue(id); err == nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	s.T().Fatalf("vertex %q did not appear in time", id)
}

// upsertPayload mirrors what CMDB.ObjectUpdate sends for a create-via-upsert.
func upsertPayload(originType string, body easyjson.JSON) easyjson.JSON {
	p := easyjson.NewJSONObject()
	p.SetByPath("op_time", easyjson.NewJSON(system.GetCurrentTimeNs()))
	p.SetByPath("upsert", easyjson.NewJSON(true))
	p.SetByPath("origin_type", easyjson.NewJSON(originType))
	p.SetByPath("replace", easyjson.NewJSON(true))
	p.SetByPath("body", body)
	return p
}

// One batch, one round-trip: create an object, then read it back (sequential proves
// the create lands before the read), then read a non-existent id (per-op status).
func (s *BatchSuite) Test_SequentialBatch_WriteThenReadAndMissing() {
	s.boot()
	s.NoError(s.dbc.CMDB.TypeCreate("t1"))

	results, err := s.dbc.BatchCreate("test_batch").
		Operation("functions.cmdb.api.object.update", "bobj", upsertPayload("t1", easyjson.NewJSONObjectWithKeyValue("k", easyjson.NewJSON("v")))).
		Operation("functions.cmdb.api.object.read", "bobj", easyjson.NewJSONObject()).
		Operation("functions.cmdb.api.object.read", "nope", easyjson.NewJSONObject()).
		Commit()

	s.NoError(err)
	s.Require().Len(results, 3)

	s.True(results[0].OK(), "create op status: %s", results[0].Status)
	s.Equal(0, results[0].Index)

	s.True(results[1].OK(), "read op status: %s", results[1].Status)
	s.Equal("v", results[1].Data.GetByPath("body.k").AsStringDefault(""), "batched read must see the batched create")

	s.False(results[2].OK(), "read of a non-existent id must not be ok")
	s.Equal("idle", results[2].Status, "non-existent read status")
	s.Equal(2, results[2].Index)
}

// stop_on_error halts a sequential batch after the first failure.
func (s *BatchSuite) Test_StopOnError() {
	s.boot()

	results, err := s.dbc.BatchCreate().
		Operation("functions.cmdb.api.object.update", "x", easyjson.NewJSONObject()). // no origin_type/type -> fails
		Operation("functions.cmdb.api.object.read", "x", easyjson.NewJSONObject()).   // must be skipped
		StopOnError().
		Commit()

	s.NoError(err)
	s.Require().Len(results, 1, "stop_on_error must halt after the first failed op")
	s.False(results[0].OK())
}

// Parallel runs independent operations concurrently; results stay in input order.
func (s *BatchSuite) Test_ParallelBatch() {
	s.boot()
	s.NoError(s.dbc.CMDB.TypeCreate("t1"))
	s.NoError(s.dbc.CMDB.ObjectCreate("p1", "t1"))
	s.NoError(s.dbc.CMDB.ObjectCreate("p2", "t1"))

	results, err := s.dbc.BatchCreate().
		Operation("functions.cmdb.api.object.read", "p1", easyjson.NewJSONObject()).
		Operation("functions.cmdb.api.object.read", "p2", easyjson.NewJSONObject()).
		Parallel().
		Commit()

	s.NoError(err)
	s.Require().Len(results, 2)
	s.True(results[0].OK() && results[1].OK(), "both parallel reads must succeed")
	s.Equal("p1", results[0].ID)
	s.Equal("p2", results[1].ID)
}

// Parallel().StopOnError(): stopOnError must be a no-op across sub-batch splits when
// Parallel is set — a failure in chunk N must not prevent chunk N+1 from being sent.
func (s *BatchSuite) Test_Parallel_StopOnError_DoesNotAbortSubBatches() {
	s.boot()
	s.NoError(s.dbc.CMDB.TypeCreate("t1"))
	s.NoError(s.dbc.CMDB.ObjectCreate("exists", "t1"))

	// SubBatchSize(1) → each op is its own chunk.
	// chunk 1: read "missing" → idle (not ok)
	// chunk 2: read "exists"  → ok — must still execute despite chunk 1 failing
	results, err := s.dbc.BatchCreate().
		Operation("functions.cmdb.api.object.read", "missing", easyjson.NewJSONObject()).
		Operation("functions.cmdb.api.object.read", "exists", easyjson.NewJSONObject()).
		Parallel().
		StopOnError().
		SubBatchSize(1).
		Commit()

	s.NoError(err)
	s.Require().Len(results, 2, "Parallel() must suppress StopOnError: both chunks must execute")
	s.False(results[0].OK(), "read of missing id must not be ok")
	s.True(results[1].OK(), "read of existing id in chunk 2 must succeed despite prior failure")
}

// A per-batch SubBatchSize overrides the package default and actually splits the batch
// into that many ops per request — proven by counting executor invocations — while
// every op still runs and results stay in global input order.
func (s *BatchSuite) Test_SubBatchSize_PerBatchOverride() {
	s.boot()
	db.SetBatchSubBatchSizeForTest(1000) // package default would NOT split
	defer db.SetBatchSubBatchSizeForTest(100)

	// Count how many times the batch executor is actually invoked.
	var batchCalls int32
	counting := func(p sfp.RequestProvider, typename, id string, payload, options *easyjson.JSON, timeout ...time.Duration) (*easyjson.JSON, error) {
		if typename == "functions.graph.api.batch" {
			atomic.AddInt32(&batchCalls, 1)
		}
		return s.Runtime().Request(p, typename, id, payload, options, timeout...)
	}
	dbc, err := db.NewDBSyncClientFromRequestFunction(counting)
	s.NoError(err)

	s.NoError(dbc.CMDB.TypeCreate("t1"))
	ids := []string{"a1", "a2", "a3", "a4", "a5"}
	for _, id := range ids {
		s.NoError(dbc.CMDB.ObjectCreate(id, "t1"))
	}

	b := dbc.BatchCreate().SubBatchSize(2) // override -> [a1,a2] [a3,a4] [a5] = 3 requests
	for _, id := range ids {
		b.Operation("functions.cmdb.api.object.read", id, easyjson.NewJSONObject())
	}
	results, err := b.Commit()
	s.NoError(err)

	s.Equal(int32(3), atomic.LoadInt32(&batchCalls),
		"5 ops at SubBatchSize(2) must be 3 sub-batch requests (per-batch override beats the package default of 1000)")
	s.Require().Len(results, len(ids), "every op across all sub-batches must return")
	for i, id := range ids {
		s.Equal(i, results[i].Index, "global index preserved across the split")
		s.Equal(id, results[i].ID)
		s.True(results[i].OK(), "op %d (%s): %s", i, id, results[i].Status)
	}
}

// Batch.Timeout bounds the sub-batch request: a batch whose op blocks past the
// per-call timeout must fail fast AT that timeout, not ride the (60s) runtime
// default. Regression guard for the timeout being forwarded from Batch.Timeout()
// through commitChunk into the request.
//
// Exercised over the NATS request path, because that is (a) the real deployment
// topology of the DB client — an external process talking to the runtime, where
// the production request func (clients/go/db/common.go getRequestFunc) always
// nc.Request()s with the per-call timeout — and (b) the ONLY path a request
// timeout actually bounds: a co-located GolangLocalRequest runs the target
// handler synchronously (function_type.go workerTaskExecutor) before the timeout
// timer is even armed, so it cannot cut off a blocked handler. The request func
// below forces NatsCoreGlobalRequest exactly as getRequestFunc ignores the
// provider and always goes over NATS.
func (s *BatchSuite) Test_Batch_Timeout_BoundsSubBatchRequest() {
	// Target op that withholds its reply well past the 300ms per-call timeout but
	// under the 60s runtime default: a fast return can then only mean the per-call
	// timeout was applied. time.After(3s) lets the op finish on its own on the
	// "timeout not forwarded" regression path (so that case fails fast, ~3s, not at
	// 60s); release unblocks it promptly on teardown of the happy path.
	release := make(chan struct{})
	defer close(release)
	slow := func(_ sfp.StatefunExecutor, ctx *sfp.StatefunContextProcessor) {
		select {
		case <-release:
		case <-time.After(3 * time.Second):
		}
		sfMediators.NewOpMediator(ctx).AggregateOpMsg(sfMediators.OpMsgOk(easyjson.NewJSONObject())).Reply()
	}
	s.RegisterFunction("test.batch.slow", slow,
		*statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfp.AutoRequestSelect))
	batch.RegisterAllFunctionTypes(s.Runtime())
	s.NoError(s.StartRuntime())

	// Client over the NATS request path (mirrors the external DB client), forwarding
	// the per-call timeout — the getRequestFunc contract.
	natsReq := func(_ sfp.RequestProvider, typename, id string, payload, options *easyjson.JSON, timeout ...time.Duration) (*easyjson.JSON, error) {
		return s.Runtime().Request(sfp.NatsCoreGlobalRequest, typename, id, payload, options, timeout...)
	}
	dbc, err := db.NewDBSyncClientFromRequestFunction(natsReq)
	s.NoError(err)

	const perCall = 300 * time.Millisecond
	start := time.Now()
	results, err := dbc.BatchCreate("timeout_batch").
		Operation("test.batch.slow", "s1", easyjson.NewJSONObject()).
		Timeout(perCall).
		Commit()
	elapsed := time.Since(start)

	// Must fail on the timeout (not return an OK result on the 60s default) ...
	s.Require().Error(err, "batch must fail on the per-call timeout, not complete on the runtime default")
	s.Contains(err.Error(), "timeout", "error should be a request timeout: %v", err)
	// ... genuinely after ~perCall (rules out an instant fast-fail making the ceiling
	// below vacuous) ...
	s.Require().GreaterOrEqual(elapsed, perCall-50*time.Millisecond,
		"returned before the per-call timeout could elapse (elapsed=%v) — not a real timeout", elapsed)
	// ... and bounded by the per-call Timeout(), NOT the 60s runtime default. Without
	// the forwarding this blocks until the 3s op reply and blows past the ceiling.
	s.Require().Less(elapsed, 1500*time.Millisecond,
		"Batch.Timeout() was not honored; fell back to the runtime default (elapsed=%v)", elapsed)
	s.Empty(results, "no sub-batch results expected on a transport timeout")
}
