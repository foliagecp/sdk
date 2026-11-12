package db

import (
	"fmt"

	sfp "github.com/foliagecp/sdk/statefun/plugins"
)

type DBSyncClient struct {
	Request sfp.SFRequestFunc
	Graph   GraphSyncClient
	CMDB    CMDBSyncClient
	Query   QuerySyncClient
}

// NewDBSyncClient connects to NATS at NatsURL. Like the runtime's connection, it
// reconnects after an outage of any length, so the client works again once
// NATS is back; the first connection is a single attempt and fails at once
// if NATS is unreachable.
func NewDBSyncClient(NatsURL string, NatsRequestTimeoutSec int, HubDomainName string) (DBSyncClient, error) {
	var err error
	nc, err := connectClient(NatsURL)
	if err != nil {
		return DBSyncClient{}, err
	}
	request := getRequestFunc(nc, NatsRequestTimeoutSec, HubDomainName)
	return NewDBSyncClientFromRequestFunction(request)
}

/*
ctx.Request
// or
runtime.Request
*/
func NewDBSyncClientFromRequestFunction(request sfp.SFRequestFunc) (DBSyncClient, error) {
	if request == nil {
		return DBSyncClient{}, fmt.Errorf("request must not be nil")
	}
	graph, err := NewGraphSyncClientFromRequestFunction(request)
	if err != nil {
		return DBSyncClient{}, err
	}
	cmdb, err := NewCMDBSyncClientFromRequestFunction(request)
	if err != nil {
		return DBSyncClient{}, err
	}
	query, err := NewQuerySyncClientFromRequestFunction(request)
	if err != nil {
		return DBSyncClient{}, err
	}
	return DBSyncClient{
		Request: request,
		Graph:   graph,
		CMDB:    cmdb,
		Query:   query,
	}, nil
}
