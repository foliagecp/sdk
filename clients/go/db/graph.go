package db

import (
	"fmt"

	"github.com/foliagecp/easyjson"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfp "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/nats-io/nats.go"
)

type GraphSyncClient struct {
	request sfp.SFRequestFunc
}

func NewGraphSyncClient(NatsURL string, NatsRequestTimeoutSec int, HubDomainName string) (GraphSyncClient, error) {
	var err error
	nc, err := nats.Connect(NatsURL)
	if err != nil {
		return GraphSyncClient{}, err
	}
	request := getRequestFunc(nc, NatsRequestTimeoutSec, HubDomainName)
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

/*
nats -s nats://nats:foliage@nats:4222 req request.hub.functions.graph.api.crud.a "{\"payload\":{\"operation\":{\"target\":\"vertex\",\"type\":\"create\"}},\"options\":{\"op_stack\":true}}"
nats -s nats://nats:foliage@nats:4222 req request.hub.functions.graph.api.crud.a "{\"payload\":{\"operation\":{\"target\":\"vertex\",\"type\":\"update\"},\"data\":{\"body\":{\"foo\":\"bar\"}}},\"options\":{\"op_stack\":true}}"
nats -s nats://nats:foliage@nats:4222 req request.hub.functions.graph.api.crud.a "{\"payload\":{\"operation\":{\"target\":\"vertex\",\"type\":\"delete\"}},\"options\":{\"op_stack\":true}}"

nats -s nats://nats:foliage@nats:4222 req request.hub.functions.graph.api.dirty.vertex.read.a "{\"payload\":{\"details\":true},\"options\":{\"op_stack\":true}}"

nats -s nats://nats:foliage@nats:4222 req request.hub.functions.graph.api.crud.a "{\"payload\":{\"operation\":{\"target\":\"vertex.link\",\"type\":\"create\"},\"data\":{\"to\":\"b\",\"name\":\"a2b\",\"type\":\"ta2b\",\"body\":{\"foo\":\"bar\"},\"tags\":[\"tagA\",\"tagB\"]}},\"options\":{\"op_stack\":true}}"
nats -s nats://nats:foliage@nats:4222 req request.hub.functions.graph.api.crud.a "{\"payload\":{\"operation\":{\"target\":\"vertex.link\",\"type\":\"update\"},\"data\":{\"name\":\"a2b\",\"body\":{\"foo\":\"bar\"},\"tags\":[\"tagA\",\"tagB\"]}},\"options\":{\"op_stack\":true}}"
nats -s nats://nats:foliage@nats:4222 req request.hub.functions.graph.api.crud.a "{\"payload\":{\"operation\":{\"target\":\"vertex.link\",\"type\":\"delete\"},\"data\":{\"name\":\"a2b\"}},\"options\":{\"op_stack\":true}}"

nats -s nats://nats:foliage@nats:4222 req request.hub.functions.graph.api.dirty.vertex.link.read.a "{\"payload\":{\"name\":\"a2b\",\"details\":true},\"options\":{\"op_stack\":true}}"
*/

func (gc GraphSyncClient) VertexCreate(id string, body ...easyjson.JSON) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("operation.target", easyjson.NewJSON("vertex"))
	payload.SetByPath("operation.type", easyjson.NewJSON("create"))
	if len(body) > 0 {
		payload.SetByPath("data.body", body[0])
	}
	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(gc.request(sfp.AutoRequestSelect, "functions.graph.api.crud", id, &payload, nil)))
}

func (gc GraphSyncClient) VertexUpdate(id string, body easyjson.JSON, replace bool, upsert ...bool) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("operation.target", easyjson.NewJSON("vertex"))
	payload.SetByPath("operation.type", easyjson.NewJSON("update"))
	if len(upsert) > 0 {
		payload.SetByPath("data.upsert", easyjson.NewJSON(upsert[0]))
	}
	payload.SetByPath("data.replace", easyjson.NewJSON(replace))
	payload.SetByPath("data.body", body)

	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(gc.request(sfp.AutoRequestSelect, "functions.graph.api.crud", id, &payload, nil)))
}

func (gc GraphSyncClient) VertexDelete(id string) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("operation.target", easyjson.NewJSON("vertex"))
	payload.SetByPath("operation.type", easyjson.NewJSON("delete"))
	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(gc.request(sfp.AutoRequestSelect, "functions.graph.api.crud", id, &payload, nil)))
}

func (gc GraphSyncClient) VertexRead(id string, details ...bool) (easyjson.JSON, error) {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("operation.target", easyjson.NewJSON("vertex"))
	payload.SetByPath("operation.type", easyjson.NewJSON("read"))
	if len(details) > 0 {
		payload.SetByPath("data.details", easyjson.NewJSON(details[0]))
	}
	om := sfMediators.OpMsgFromSfReply(gc.request(sfp.AutoRequestSelect, "functions.graph.api.crud", id, &payload, nil))
	return om.Data, OpErrorFromOpMsg(om)
}

func (gc GraphSyncClient) VertexLinkCreate(from, to, linkName, linkType string, tags []string, body ...easyjson.JSON) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("operation.target", easyjson.NewJSON("vertex.link"))
	payload.SetByPath("operation.type", easyjson.NewJSON("create"))
	payload.SetByPath("data.to", easyjson.NewJSON(to))
	payload.SetByPath("data.name", easyjson.NewJSON(linkName))
	payload.SetByPath("data.type", easyjson.NewJSON(linkType))
	if len(body) > 0 {
		payload.SetByPath("data.body", body[0])
	} else {
		payload.SetByPath("data.body", easyjson.NewJSONObject())
	}
	if len(tags) > 0 {
		payload.SetByPath("tags", easyjson.JSONFromArray(tags))
	}

	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(gc.request(sfp.AutoRequestSelect, "functions.graph.api.crud", from, &payload, nil)))
}

func (gc GraphSyncClient) VertexLinkUpdate(from, linkName string, tags []string, body easyjson.JSON, replace bool, toAndType4Upsert ...string) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("operation.target", easyjson.NewJSON("vertex.link"))
	payload.SetByPath("operation.type", easyjson.NewJSON("update"))
	payload.SetByPath("data.name", easyjson.NewJSON(linkName))
	payload.SetByPath("data.body", body)
	if len(tags) > 0 {
		payload.SetByPath("data.tags", easyjson.JSONFromArray(tags))
	}
	if len(toAndType4Upsert) > 0 {
		if len(toAndType4Upsert) == 2 {
			payload.SetByPath("data.upsert", easyjson.NewJSON(true))
			payload.SetByPath("data.to", easyjson.NewJSON(toAndType4Upsert[0]))
			payload.SetByPath("data.type", easyjson.NewJSON(toAndType4Upsert[1]))
		} else {
			return fmt.Errorf("toAndType4Upsert must consist of 2 string values: \"to\" and \"type\"")
		}
	}

	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(gc.request(sfp.AutoRequestSelect, "functions.graph.api.crud", from, &payload, nil)))
}

func (gc GraphSyncClient) VertexLinkUpdateByToAndType(from, to, linkType string, tags []string, body easyjson.JSON, replace bool, name4Upsert ...string) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("operation.target", easyjson.NewJSON("vertex.link"))
	payload.SetByPath("operation.type", easyjson.NewJSON("update"))
	payload.SetByPath("data.to", easyjson.NewJSON(to))
	payload.SetByPath("data.type", easyjson.NewJSON(linkType))

	payload.SetByPath("data.body", body)
	if len(tags) > 0 {
		payload.SetByPath("data.tags", easyjson.JSONFromArray(tags))
	}
	if len(name4Upsert) > 0 {
		payload.SetByPath("data.upsert", easyjson.NewJSON(true))
		payload.SetByPath("data.name", easyjson.NewJSON(name4Upsert[0]))
	}

	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(gc.request(sfp.AutoRequestSelect, "functions.graph.api.crud", from, &payload, nil)))
}

func (gc GraphSyncClient) VertexLinkDelete(from, linkName string) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("operation.target", easyjson.NewJSON("vertex.link"))
	payload.SetByPath("operation.type", easyjson.NewJSON("delete"))
	payload.SetByPath("data.name", easyjson.NewJSON(linkName))

	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(gc.request(sfp.AutoRequestSelect, "functions.graph.api.crud", from, &payload, nil)))
}

func (gc GraphSyncClient) VertexLinkDeleteByToAndType(from, to, linkType string) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("operation.target", easyjson.NewJSON("vertex.link"))
	payload.SetByPath("operation.type", easyjson.NewJSON("delete"))
	payload.SetByPath("data.to", easyjson.NewJSON(to))
	payload.SetByPath("data.type", easyjson.NewJSON(linkType))

	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(gc.request(sfp.AutoRequestSelect, "functions.graph.api.crud", from, &payload, nil)))
}

func (gc GraphSyncClient) VertexLinkRead(from, linkName string, details ...bool) (easyjson.JSON, error) {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("operation.target", easyjson.NewJSON("vertex.link"))
	payload.SetByPath("operation.type", easyjson.NewJSON("read"))
	payload.SetByPath("data.name", easyjson.NewJSON(linkName))
	if len(details) > 0 {
		payload.SetByPath("data.details", easyjson.NewJSON(details[0]))
	}

	om := sfMediators.OpMsgFromSfReply(gc.request(sfp.AutoRequestSelect, "functions.graph.api.crud", from, &payload, nil))
	return om.Data, OpErrorFromOpMsg(om)
}

func (gc GraphSyncClient) VertexLinkReadByToAndType(from, to, linkType string, details ...bool) (easyjson.JSON, error) {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("operation.target", easyjson.NewJSON("vertex.link"))
	payload.SetByPath("operation.type", easyjson.NewJSON("read"))
	payload.SetByPath("data.to", easyjson.NewJSON(to))
	payload.SetByPath("data.type", easyjson.NewJSON(linkType))
	if len(details) > 0 {
		payload.SetByPath("data.details", easyjson.NewJSON(details[0]))
	}

	om := sfMediators.OpMsgFromSfReply(gc.request(sfp.AutoRequestSelect, "functions.graph.api.crud", from, &payload, nil))
	return om.Data, OpErrorFromOpMsg(om)
}
