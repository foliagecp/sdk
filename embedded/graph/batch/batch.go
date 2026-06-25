// Package batch provides a generic batch executor function for Foliage: a single
// request carrying many sub-operations (each a typename + id + payload + options),
// run inside the runtime where the target functions live. The target functions are
// reached via AutoRequestSelect, so co-located ones run in-process (Go channel, no
// NATS) — collapsing N external request-reply round-trips into one. It is not
// CRUD-specific: any registered function type can be invoked.
package batch

import (
	"sync"

	"github.com/foliagecp/easyjson"

	"github.com/foliagecp/sdk/statefun"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
)

const MAX_ACK_WAIT_MS = 60 * 1000

func RegisterAllFunctionTypes(runtime *statefun.Runtime) {
	statefun.NewFunctionType(
		runtime,
		"functions.graph.api.batch",
		BatchExecute,
		*statefun.NewFunctionTypeConfig().
			SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).
			SetMsgAckWaitMs(MAX_ACK_WAIT_MS),
	)
}

type batchOp struct {
	index    int
	typename string
	id       string
	payload  easyjson.JSON
	options  *easyjson.JSON
	errMsg   string // non-empty => malformed op, no sub-request issued
}

/*
BatchExecute runs a list of sub-operations and returns their per-operation results.
It can be invoked on ANY id — its own id is irrelevant; it only reads the payload.

Request payload:

	operations: array - required
		[
			{
				typename: string - required  // function type to call, e.g. functions.cmdb.api.object.update
				id:       string - required  // target vertex/object id the call runs on
				payload:  json   - optional  // forwarded as the call's payload
				options:  json   - optional  // forwarded as the call's options
			},
			...
		]
	parallel:      bool - optional // default false. false = run in input order (preserves
	                               // dependencies; e.g. create-then-link); true = run concurrently
	stop_on_error: bool - optional // default false. Sequential only: halt after the first op
	                               // that does NOT return status "ok" (failed/incomplete/idle)

Reply data:

	results: array  // one entry per executed operation, in input order
		[
			{
				index:    int     // position in the input operations array
				typename: string
				id:       string
				status:   "ok"|"idle"|"incomplete"|"failed"  // the sub-call's mediator status
				data:     json    // the sub-call's reply data (or an error string for a malformed op)
			},
			...
		]
	count:  int  // number of results (== executed ops; fewer than len(operations) if stopped early)
	failed: int  // number of results with status "failed"

The overall batch status is "ok" whenever the batch executed (individual failures are
reported per-op, NOT aggregated into the overall status), so the caller always gets the
results back. A non-ok overall status is reserved for a malformed request (no operations).
*/
func BatchExecute(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	om := sfMediators.NewOpMediator(ctx)

	operations := ctx.Payload.GetByPath("operations")
	if !operations.IsArray() {
		om.AggregateOpMsg(sfMediators.OpMsgFailed("batch request requires an \"operations\" array")).Reply()
		return
	}
	n := operations.ArraySize()
	parallel := ctx.Payload.GetByPath("parallel").AsBoolDefault(false)
	stopOnError := ctx.Payload.GetByPath("stop_on_error").AsBoolDefault(false)

	// Extract every operation up front (single-goroutine, cloned) so the concurrent
	// path never reads the shared request payload from multiple goroutines.
	parsed := make([]batchOp, n)
	for i := 0; i < n; i++ {
		e := operations.ArrayElement(i)
		o := batchOp{
			index:    i,
			typename: e.GetByPath("typename").AsStringDefault(""),
			id:       e.GetByPath("id").AsStringDefault(""),
		}
		switch {
		case len(o.typename) == 0:
			o.errMsg = "operation requires a non-empty \"typename\""
		case len(o.id) == 0:
			o.errMsg = "operation requires a non-empty \"id\""
		default:
			if e.PathExists("payload") {
				o.payload = e.GetByPath("payload").Clone()
			} else {
				o.payload = easyjson.NewJSONObject()
			}
			if e.PathExists("options") {
				opt := e.GetByPath("options").Clone()
				o.options = &opt
			}
		}
		parsed[i] = o
	}

	results := make([]easyjson.JSON, n)
	run := func(o batchOp) (ok bool) {
		entry := easyjson.NewJSONObject()
		entry.SetByPath("index", easyjson.NewJSON(o.index))
		entry.SetByPath("typename", easyjson.NewJSON(o.typename))
		entry.SetByPath("id", easyjson.NewJSON(o.id))
		if o.errMsg != "" {
			entry.SetByPath("status", easyjson.NewJSON(sfMediators.OpStatusNames[sfMediators.SYNC_OP_STATUS_FAILED]))
			entry.SetByPath("data", easyjson.NewJSON(o.errMsg))
			results[o.index] = entry
			return false
		}
		reply := sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, o.typename, o.id, &o.payload, o.options))
		entry.SetByPath("status", easyjson.NewJSON(sfMediators.OpStatusNames[reply.Status]))
		entry.SetByPath("data", reply.Data)
		results[o.index] = entry
		return reply.Status == sfMediators.SYNC_OP_STATUS_OK
	}

	executed := n
	if parallel {
		var wg sync.WaitGroup
		for i := range parsed {
			wg.Add(1)
			go func(o batchOp) { defer wg.Done(); run(o) }(parsed[i])
		}
		wg.Wait()
	} else {
		for i := range parsed {
			if ok := run(parsed[i]); stopOnError && !ok {
				executed = i + 1
				break
			}
		}
	}

	resultsArray := easyjson.NewJSONArray()
	failedCount := 0
	for i := 0; i < executed; i++ {
		resultsArray.AddToArray(results[i])
		if results[i].GetByPath("status").AsStringDefault("") == sfMediators.OpStatusNames[sfMediators.SYNC_OP_STATUS_FAILED] {
			failedCount++
		}
	}

	reply := easyjson.NewJSONObject()
	reply.SetByPath("results", resultsArray)
	reply.SetByPath("count", easyjson.NewJSON(resultsArray.ArraySize()))
	reply.SetByPath("failed", easyjson.NewJSON(failedCount))
	om.AggregateOpMsg(sfMediators.OpMsgOk(reply)).Reply()
}
