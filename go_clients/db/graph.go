package db

import (
	"fmt"

	sfp "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/nats-io/nats.go"
)

type GraphSyncClient struct {
	request sfp.SFRequestFunc
}

func NewGraphSyncClient(NatsURL string, NatsRequestTimeout int) (GraphSyncClient, error) {
	var err error
	nc, err := nats.Connect(NatsURL)
	if err != nil {
		return GraphSyncClient{}, err
	}
	request := getRequestFunc(nc, NatsRequestTimeout)
	return NewGraphSyncClientFromRequestFunction(request)
}

/*
ctx.Request
// or
runtime.Request
*/
func NewGraphSyncClientFromRequestFunction(request sfp.SFRequestFunc) (GraphSyncClient, error) {
	if request == nil {
		return GraphSyncClient{}, fmt.Errorf("request must not be nil")
	}
	return GraphSyncClient{request: request}, nil
}
