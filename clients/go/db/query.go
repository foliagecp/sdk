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

	return om.Data.ObjectKeys(), OpErrorFromOpMsg(om)
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
