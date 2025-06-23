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

func (gc GraphSyncClient) VertexCreate(id string, body ...easyjson.JSON) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("body", easyjson.NewJSONObject())
	if len(body) > 0 {
		payload.SetByPath("body", body[0])
	}
	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(gc.request(sfp.AutoRequestSelect, "functions.graph.api.vertex.create", seqFree(id), &payload, nil)))
}

func (gc GraphSyncClient) VertexUpdate(id string, body easyjson.JSON, replace bool, upsert ...bool) error {
	payload := easyjson.NewJSONObject()
	if len(upsert) > 0 {
		payload.SetByPath("upsert", easyjson.NewJSON(upsert[0]))
	}
	payload.SetByPath("replace", easyjson.NewJSON(replace))
	payload.SetByPath("body", body)

	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(gc.request(sfp.AutoRequestSelect, "functions.graph.api.vertex.update", seqFree(id), &payload, nil)))
}

func (gc GraphSyncClient) VertexDelete(id string) error {
	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(gc.request(sfp.AutoRequestSelect, "functions.graph.api.vertex.delete", seqFree(id), nil, nil)))
}

func (gc GraphSyncClient) VertexRead(id string, details ...bool) (easyjson.JSON, error) {
	payload := easyjson.NewJSONObject()
	if len(details) > 0 {
		payload.SetByPath("details", easyjson.NewJSON(details[0]))
	}
	om := sfMediators.OpMsgFromSfReply(gc.request(sfp.AutoRequestSelect, "functions.graph.api.vertex.read", seqFree(id), &payload, nil))
	return om.Data, OpErrorFromOpMsg(om)
}

func (gc GraphSyncClient) VerticesLinkCreate(from, to, linkName, linkType string, tags []string, body ...easyjson.JSON) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("to", easyjson.NewJSON(to))
	payload.SetByPath("name", easyjson.NewJSON(linkName))
	payload.SetByPath("type", easyjson.NewJSON(linkType))
	payload.SetByPath("body", easyjson.NewJSONObject())
	if len(body) > 0 {
		payload.SetByPath("body", body[0])
	}
	if len(tags) > 0 {
		payload.SetByPath("tags", easyjson.JSONFromArray(tags))
	}

	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(gc.request(sfp.AutoRequestSelect, "functions.graph.api.link.create", seqFree(from), &payload, nil)))
}

func (gc GraphSyncClient) VerticesLinkUpdate(from, linkName string, tags []string, body easyjson.JSON, replace bool, toAndType4Upsert ...string) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("name", easyjson.NewJSON(linkName))
	payload.SetByPath("body", body)
	payload.SetByPath("replace", easyjson.NewJSON(replace))
	if len(tags) > 0 {
		payload.SetByPath("tags", easyjson.JSONFromArray(tags))
	}
	if len(toAndType4Upsert) > 0 {
		if len(toAndType4Upsert) == 2 {
			payload.SetByPath("upsert", easyjson.NewJSON(true))
			payload.SetByPath("to", easyjson.NewJSON(toAndType4Upsert[0]))
			payload.SetByPath("type", easyjson.NewJSON(toAndType4Upsert[1]))
		} else {
			return fmt.Errorf("toAndType4Upsert must consist of 2 string values: \"to\" and \"type\"")
		}
	}

	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(gc.request(sfp.AutoRequestSelect, "functions.graph.api.link.update", seqFree(from), &payload, nil)))
}

func (gc GraphSyncClient) VerticesLinkUpdateByToAndType(from, to, linkType string, tags []string, body easyjson.JSON, replace bool, name4Upsert ...string) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("to", easyjson.NewJSON(to))
	payload.SetByPath("type", easyjson.NewJSON(linkType))

	payload.SetByPath("body", body)
	if len(tags) > 0 {
		payload.SetByPath("tags", easyjson.JSONFromArray(tags))
	}
	if len(name4Upsert) > 0 {
		payload.SetByPath("upsert", easyjson.NewJSON(true))
		payload.SetByPath("name", easyjson.NewJSON(name4Upsert[0]))
	}

	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(gc.request(sfp.AutoRequestSelect, "functions.graph.api.link.update", seqFree(from), &payload, nil)))
}

func (gc GraphSyncClient) VerticesLinkDelete(from, linkName string) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("name", easyjson.NewJSON(linkName))

	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(gc.request(sfp.AutoRequestSelect, "functions.graph.api.link.delete", seqFree(from), &payload, nil)))
}

func (gc GraphSyncClient) VerticesLinkDeleteByToAndType(from, to, linkType string) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("to", easyjson.NewJSON(to))
	payload.SetByPath("type", easyjson.NewJSON(linkType))

	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(gc.request(sfp.AutoRequestSelect, "functions.graph.api.link.delete", seqFree(from), &payload, nil)))
}

func (gc GraphSyncClient) VerticesLinkRead(from, linkName string, details ...bool) (easyjson.JSON, error) {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("name", easyjson.NewJSON(linkName))
	if len(details) > 0 {
		payload.SetByPath("details", easyjson.NewJSON(details[0]))
	}

	om := sfMediators.OpMsgFromSfReply(gc.request(sfp.AutoRequestSelect, "functions.graph.api.link.read", seqFree(from), &payload, nil))
	return om.Data, OpErrorFromOpMsg(om)
}

func (gc GraphSyncClient) VerticesLinkReadByToAndType(from, to, linkType string, details ...bool) (easyjson.JSON, error) {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("to", easyjson.NewJSON(to))
	payload.SetByPath("type", easyjson.NewJSON(linkType))
	if len(details) > 0 {
		payload.SetByPath("details", easyjson.NewJSON(details[0]))
	}

	om := sfMediators.OpMsgFromSfReply(gc.request(sfp.AutoRequestSelect, "functions.graph.api.link.read", seqFree(from), &payload, nil))
	return om.Data, OpErrorFromOpMsg(om)
}
