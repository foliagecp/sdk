package db

import (
	"time"

	"github.com/foliagecp/easyjson"

	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfp "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
)

const batchFunctionTypename = "functions.graph.api.batch"

// batchSubBatchSize bounds how many operations a single batch request carries. Commit
// splits a larger batch into sequential sub-batch requests of at most this size, so an
// arbitrarily large client-built batch never becomes one oversized request. Configured
// via DB_BATCH_SUBBATCH_SIZE (default 100).
var batchSubBatchSize = system.GetEnvMustProceed[int]("DB_BATCH_SUBBATCH_SIZE", 100)

// SetBatchSubBatchSizeForTest overrides the sub-batch size. Test-only: the value is
// normally resolved once from DB_BATCH_SUBBATCH_SIZE at package init.
func SetBatchSubBatchSizeForTest(n int) { batchSubBatchSize = n }

// BatchResult is one operation's outcome from a committed Batch, in input order.
type BatchResult struct {
	Index    int
	Typename string
	ID       string
	Status   string // "ok" | "idle" | "incomplete" | "failed"
	Data     easyjson.JSON
}

// OK reports whether the operation succeeded.
func (r BatchResult) OK() bool {
	return r.Status == sfMediators.OpStatusNames[sfMediators.SYNC_OP_STATUS_OK]
}

type batchEntry struct {
	typename string
	id       string
	payload  easyjson.JSON
	options  *easyjson.JSON
}

// Batch accumulates sub-operations to run in a SINGLE round-trip via the
// functions.graph.api.batch executor — collapsing N external request-reply calls into
// one. Build it with DBSyncClient.BatchCreate, queue operations with Operation (any
// number), then send with Commit. Operations run in the order added (sequential,
// preserving dependencies such as create-then-link) unless Parallel is set.
//
// It is not CRUD-specific: Operation takes any function typename.
type Batch struct {
	request      sfp.SFRequestFunc
	id           string
	ops          []batchEntry
	parallel     bool
	stopOnErr    bool
	subBatchSize int           // per-batch override of batchSubBatchSize; <= 0 means use the package default
	timeout      time.Duration // per-sub-batch request timeout; <= 0 means use the request func's default
}

// BatchCreate starts a new batch. batchID is the id the executor is invoked on; it is
// irrelevant to execution (any id works) and defaults to "batch". Pass a distinct id
// per concurrently-committed batch if you do not want them to serialize on the
// executor's per-id instance.
func (c DBSyncClient) BatchCreate(batchID ...string) *Batch {
	id := "batch"
	if len(batchID) > 0 && len(batchID[0]) > 0 {
		id = batchID[0]
	}
	return &Batch{request: c.Request, id: id}
}

// Operation queues one sub-call: function `typename` on target `id`, with `payload`
// and optional `options`. Pass easyjson.NewJSONObject() for an empty payload. Returns
// the batch for chaining.
func (b *Batch) Operation(typename, id string, payload easyjson.JSON, options ...easyjson.JSON) *Batch {
	e := batchEntry{typename: typename, id: id, payload: payload}
	if len(options) > 0 {
		o := options[0]
		e.options = &o
	}
	b.ops = append(b.ops, e)
	return b
}

// Parallel makes the executor run the queued operations concurrently instead of in
// input order. Use only for independent operations (no create-then-use dependency).
// Returns the batch for chaining.
func (b *Batch) Parallel() *Batch { b.parallel = true; return b }

// StopOnError makes a sequential batch halt after the first operation that does not
// return status "ok" (failed/incomplete/idle). Ignored when Parallel is set. Returns
// the batch for chaining.
func (b *Batch) StopOnError() *Batch { b.stopOnErr = true; return b }

// SubBatchSize overrides, for THIS batch only, the maximum number of operations per
// sub-batch request, bypassing the package default (DB_BATCH_SUBBATCH_SIZE). A value
// <= 0 is ignored and the package default is used. Returns the batch for chaining.
func (b *Batch) SubBatchSize(n int) *Batch { b.subBatchSize = n; return b }

// Timeout bounds EACH sub-batch request-reply round-trip (not the aggregate Commit
// across sub-batches). A batch is exactly where the fixed request-timeout default is
// most likely to bite: one sub-batch carries up to SubBatchSize operations in a
// single round-trip, so it can legitimately take far longer than any single call and
// would otherwise time out on the caller's default while the executor is still
// working. Raise it for large/slow batches (or lower it to fail fast). A value <= 0
// is ignored and the request travels on the request func's default timeout. Returns
// the batch for chaining.
func (b *Batch) Timeout(d time.Duration) *Batch { b.timeout = d; return b }

// Len returns the number of queued operations.
func (b *Batch) Len() int { return len(b.ops) }

// Commit sends the batch and returns the per-operation results in input order. A batch
// larger than the sub-batch size (this batch's SubBatchSize if set, else the package
// default DB_BATCH_SUBBATCH_SIZE, default 100) is split into several sequential
// sub-batch requests automatically; sub-batches are sent in order so the global
// operation order (and StopOnError) is preserved across the split. The returned error
// is non-nil only for a transport error or a malformed sub-batch (the results gathered
// so far are still returned with it); individual operation failures are reported in
// each BatchResult.Status (inspect it, or BatchResult.OK).
func (b *Batch) Commit() ([]BatchResult, error) {
	size := batchSubBatchSize
	if b.subBatchSize > 0 {
		size = b.subBatchSize
	}
	if size < 1 {
		size = 1
	}

	out := make([]BatchResult, 0, len(b.ops))
	for start := 0; start < len(b.ops); start += size {
		end := start + size
		if end > len(b.ops) {
			end = len(b.ops)
		}

		results, err := b.commitChunk(b.ops[start:end], start)
		out = append(out, results...)
		if err != nil {
			return out, err
		}

		// StopOnError is a sequential concept and is ignored when Parallel is set
		// (matching both the executor and the StopOnError doc) — so it must NOT abort
		// later sub-batches in parallel mode. In sequential mode: if this chunk reported
		// a non-ok op (the executor stopped on it), stop sending further sub-batches too.
		if b.stopOnErr && !b.parallel {
			for _, r := range results {
				if !r.OK() {
					return out, nil
				}
			}
		}
	}
	return out, nil
}

// commitChunk sends one sub-batch and re-maps each result's Index from sub-batch-local
// to global by adding indexOffset.
func (b *Batch) commitChunk(ops []batchEntry, indexOffset int) ([]BatchResult, error) {
	operations := easyjson.NewJSONArray()
	for _, op := range ops {
		e := easyjson.NewJSONObject()
		e.SetByPath("typename", easyjson.NewJSON(op.typename))
		e.SetByPath("id", easyjson.NewJSON(op.id))
		e.SetByPath("payload", op.payload)
		if op.options != nil {
			e.SetByPath("options", *op.options)
		}
		operations.AddToArray(e)
	}
	payload := easyjson.NewJSONObject()
	payload.SetByPath("operations", operations)
	if b.parallel {
		payload.SetByPath("parallel", easyjson.NewJSON(true))
	}
	if b.stopOnErr {
		payload.SetByPath("stop_on_error", easyjson.NewJSON(true))
	}

	// Forward the per-batch timeout (if any) into the request. Empty variadic when
	// unset, so the request func applies its own default exactly as before.
	var to []time.Duration
	if b.timeout > 0 {
		to = []time.Duration{b.timeout}
	}
	om := sfMediators.OpMsgFromSfReply(b.request(sfp.AutoRequestSelect, batchFunctionTypename, b.id, &payload, nil, to...))
	if err := OpErrorFromOpMsg(om); err != nil {
		return nil, err
	}

	resultsJson := om.Data.GetByPath("results")
	out := make([]BatchResult, 0, resultsJson.ArraySize())
	for i := 0; i < resultsJson.ArraySize(); i++ {
		r := resultsJson.ArrayElement(i)
		out = append(out, BatchResult{
			Index:    indexOffset + int(r.GetByPath("index").AsNumericDefault(0)),
			Typename: r.GetByPath("typename").AsStringDefault(""),
			ID:       r.GetByPath("id").AsStringDefault(""),
			Status:   r.GetByPath("status").AsStringDefault(""),
			Data:     r.GetByPath("data"),
		})
	}
	return out, nil
}
