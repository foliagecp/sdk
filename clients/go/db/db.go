package db

import (
	"fmt"

	sfp "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/nats-io/nats.go"
)

type DBSyncClient struct {
	Request sfp.SFRequestFunc
	Graph   GraphSyncClient
	CMDB    CMDBSyncClient
	Query   QuerySyncClient
}

func NewDBSyncClient(NatsURL string, NatsRequestTimeoutSec int, HubDomainName string) (DBSyncClient, error) {
	var err error
	nc, err := nats.Connect(NatsURL)
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
