package db

import (
	"fmt"

	"github.com/foliagecp/easyjson"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfp "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/nats-io/nats.go"
)

type QuerySyncClient struct {
	request sfp.SFRequestFunc
}

func NewQuerySyncClient(NatsURL string, NatsRequestTimeoutSec int, HubDomainName string) (QuerySyncClient, error) {
	var err error
	nc, err := nats.Connect(NatsURL)
	if err != nil {
		return QuerySyncClient{}, err
	}
	request := getRequestFunc(nc, NatsRequestTimeoutSec, HubDomainName)
	return NewQuerySyncClientFromRequestFunction(request)
}

/*
ctx.Request
// or
runtime.Request
*/
func NewQuerySyncClientFromRequestFunction(request sfp.SFRequestFunc) (QuerySyncClient, error) {
	if request == nil {
		return QuerySyncClient{}, fmt.Errorf("request must not be nil")
	}
	return QuerySyncClient{request: request}, nil
}

func (qc QuerySyncClient) JPGQLCtraQuery(id, query string) ([]string, error) {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("query", easyjson.NewJSON(query))

	om := sfMediators.OpMsgFromSfReply(qc.request(sfp.AutoRequestSelect, "functions.graph.api.query.jpgql.ctra", id, &payload, nil))

	return om.Data.GetByPath("uuids").ObjectKeys(), OpErrorFromOpMsg(om)
}

// JPGQLCtraQueryEx is JPGQLCtraQuery that also tells the caller whether the
// answer is whole.
//
// A traversal cut short by its own timeout or by backpressure answers OK: the
// uuids it did collect are real, there are simply fewer of them than the query
// asked for. JPGQLCtraQuery hands back those uuids and drops the rest of the
// reply, so a caller cannot tell a complete answer from a truncated one — and
// an inventory that treats "not returned" as "not there" deletes live data on a
// slow day. This returns the flag and the stats behind it.
//
// Pass strict=true to be told by status instead: the call then fails with an
// *OpError rather than returning a partial result.
func (qc QuerySyncClient) JPGQLCtraQueryEx(id, query string, strict ...bool) (uuids []string, partial bool, stats easyjson.JSON, err error) {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("query", easyjson.NewJSON(query))
	if len(strict) > 0 && strict[0] {
		payload.SetByPath("strict", easyjson.NewJSON(true))
	}

	om := sfMediators.OpMsgFromSfReply(qc.request(sfp.AutoRequestSelect, "functions.graph.api.query.jpgql.ctra", id, &payload, nil))
	return om.Data.GetByPath("uuids").ObjectKeys(),
		om.Data.GetByPath("partial").AsBoolDefault(false),
		om.Data.GetByPath("stats"),
		OpErrorFromOpMsg(om)
}

// FPLQueryEx is FPLQuery that also tells the caller whether the answer is whole:
// FPL intersects only the sub-queries that succeeded, so one that failed or was
// truncated leaves the result narrower than asked — which used to be visible
// only to a caller who walked stats.jpgql_calls itself.
func (qc QuerySyncClient) FPLQueryEx(id, queryStringOfJSON string, strict ...bool) (data easyjson.JSON, partial bool, err error) {
	payload, ok := easyjson.JSONFromString(queryStringOfJSON)
	if !ok {
		return easyjson.NewJSONObject(), false, fmt.Errorf("cannot unmarshal json from provided query")
	}
	if len(strict) > 0 && strict[0] {
		payload.SetByPath("strict", easyjson.NewJSON(true))
	}

	om := sfMediators.OpMsgFromSfReply(qc.request(sfp.AutoRequestSelect, "functions.graph.api.query.fpl", id, &payload, nil))
	return om.Data, om.Data.GetByPath("partial").AsBoolDefault(false), OpErrorFromOpMsg(om)
}

func (qc QuerySyncClient) FPLQuery(id, queryStringOfJSON string) (easyjson.JSON, error) {
	if payload, ok := easyjson.JSONFromString(queryStringOfJSON); ok {
		om := sfMediators.OpMsgFromSfReply(qc.request(sfp.AutoRequestSelect, "functions.graph.api.query.fpl", id, &payload, nil))
		if om.Status == sfMediators.SYNC_OP_STATUS_OK {
			return om.Data, nil
		}
		return easyjson.NewJSONObject(), OpErrorFromOpMsg(om)
	}
	return easyjson.NewJSONObject(), fmt.Errorf("cannot unmarshal json from provided query")
}
