package db

import (
	"fmt"

	sfp "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/nats-io/nats.go"
)

type DBSyncClient struct {
	graph GraphSyncClient
	cmdb  CMDBSyncClient
	query QuerySyncClient
}

func NewDBClient(NatsURL string, NatsRequestTimeout int) (DBSyncClient, error) {
	var err error
	nc, err := nats.Connect(NatsURL)
	if err != nil {
		return DBSyncClient{}, err
	}
	request := getRequestFunc(nc, NatsRequestTimeout)
	return NewDBClientFromRequestFunction(request)
}

/*
ctx.Request
// or
runtime.Request
*/
func NewDBClientFromRequestFunction(request sfp.SFRequestFunc) (DBSyncClient, error) {
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
		graph: graph,
		cmdb:  cmdb,
		query: query,
	}, nil
}
