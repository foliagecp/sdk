package db

import (
	"fmt"
	"strconv"

	"github.com/foliagecp/easyjson"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfp "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
	"github.com/nats-io/nats.go"
	"golang.org/x/sync/singleflight"
)

type GraphSyncClient struct {
	request sfp.SFRequestFunc

	// readFlight deduplicates concurrent identical Read calls. See the
	// twin field on CMDBSyncClient for the full rationale and the
	// reason this is a pointer rather than a value.
	readFlight *singleflight.Group
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
	return GraphSyncClient{request: request, readFlight: &singleflight.Group{}}, nil
}

func (gc GraphSyncClient) VertexCreate(id string, body ...easyjson.JSON) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("op_time", easyjson.NewJSON(system.GetCurrentTimeNs()))
	payload.SetByPath("body", easyjson.NewJSONObject())
	if len(body) > 0 {
		payload.SetByPath("body", body[0])
	}
	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(gc.request(sfp.AutoRequestSelect, "functions.graph.api.vertex.create", id, &payload, nil)))
}

func (gc GraphSyncClient) VertexUpdate(id string, body easyjson.JSON, replace bool, upsert ...bool) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("op_time", easyjson.NewJSON(system.GetCurrentTimeNs()))
	if len(upsert) > 0 {
		payload.SetByPath("upsert", easyjson.NewJSON(upsert[0]))
	}
	payload.SetByPath("replace", easyjson.NewJSON(replace))
	payload.SetByPath("body", body)

	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(gc.request(sfp.AutoRequestSelect, "functions.graph.api.vertex.update", id, &payload, nil)))
}

func (gc GraphSyncClient) VertexDelete(id string) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("op_time", easyjson.NewJSON(system.GetCurrentTimeNs()))
	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(gc.request(sfp.AutoRequestSelect, "functions.graph.api.vertex.delete", id, &payload, nil)))
}

// VertexRead reads a vertex. Optional flags, in order:
//   - details[0] — legacy details: body plus links info (as before);
//   - details[1] — link content: each out-link additionally carries its body
//     and tags. Link content exists only in the details_v2 reply format, so
//     passing details[1]=true switches the request to details_v2 — links.out
//     comes back as an array of {to, name, type, body?, tags?} objects
//     instead of the legacy parallel arrays (same as VertexReadDetailsV2Full).
func (gc GraphSyncClient) VertexRead(id string, details ...bool) (easyjson.JSON, error) {
	// Details flags affect response shape — fold them into the key so readers
	// requesting different shapes do not collapse into one in-flight call.
	withDetails := len(details) > 0 && details[0]
	withLinkContent := len(details) > 1 && details[1]
	if withLinkContent {
		return gc.VertexReadDetailsV2Full(id, true)
	}
	key := "VertexRead:" + strconv.FormatBool(withDetails) + ":" + id
	return doRead(gc.readFlight, key, func() (any, error) {
		payload := easyjson.NewJSONObject()
		if len(details) > 0 {
			payload.SetByPath("details", easyjson.NewJSON(details[0]))
		}
		om := sfMediators.OpMsgFromSfReply(gc.request(sfp.AutoRequestSelect, "functions.graph.api.vertex.read", id, &payload, nil))
		return readResult{data: om.Data, err: OpErrorFromOpMsgStrict(om)}, nil
	})
}

// VertexReadDetailsV2 reads a vertex with the structured links format.
// The signature is frozen: downstream applications pin it in their own
// interfaces, so new capabilities go into separate methods
// (VertexReadDetailsV2Full), never into new parameters here.
func (gc GraphSyncClient) VertexReadDetailsV2(id string) (easyjson.JSON, error) {
	return doRead(gc.readFlight, "VertexRead:v2:"+id, func() (any, error) {
		payload := easyjson.NewJSONObjectWithKeyValue("details_v2", easyjson.NewJSON(true))
		om := sfMediators.OpMsgFromSfReply(gc.request(sfp.AutoRequestSelect, "functions.graph.api.vertex.read", id, &payload, nil))
		return readResult{data: om.Data, err: OpErrorFromOpMsgStrict(om)}, nil
	})
}

// VertexReadDetailsV2Full is VertexReadDetailsV2 with optional out-link
// content: linkContent[0]=true adds with_link_content to the request, and
// each links.out element then also carries the link's body and tags — fields
// are omitted for links with an empty body / no tags, and the reply grows by
// the total size of the vertex's out-link bodies, so batch readers should
// size their sub-batches accordingly. On a runtime without with_link_content
// support the reply degrades to the plain VertexReadDetailsV2 shape, without
// errors. Without arguments it behaves exactly like VertexReadDetailsV2 —
// that method's signature is frozen for downstream interfaces, which is why
// the variadic capabilities live here.
func (gc GraphSyncClient) VertexReadDetailsV2Full(id string, linkContent ...bool) (easyjson.JSON, error) {
	withLinkContent := len(linkContent) > 0 && linkContent[0]
	// Without content this is the same request VertexReadDetailsV2 sends —
	// share its in-flight key; the content shape gets its own.
	key := "VertexRead:v2:" + id
	if withLinkContent {
		key = "VertexRead:v2full:" + id
	}
	return doRead(gc.readFlight, key, func() (any, error) {
		payload := easyjson.NewJSONObjectWithKeyValue("details_v2", easyjson.NewJSON(true))
		if withLinkContent {
			payload.SetByPath("with_link_content", easyjson.NewJSON(true))
		}
		om := sfMediators.OpMsgFromSfReply(gc.request(sfp.AutoRequestSelect, "functions.graph.api.vertex.read", id, &payload, nil))
		return readResult{data: om.Data, err: OpErrorFromOpMsgStrict(om)}, nil
	})
}

func (gc GraphSyncClient) VerticesLinkCreate(from, to, linkName, linkType string, tags []string, body ...easyjson.JSON) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("op_time", easyjson.NewJSON(system.GetCurrentTimeNs()))
	payload.SetByPath("to", easyjson.NewJSON(to))
	payload.SetByPath("name", easyjson.NewJSON(linkName))
	payload.SetByPath("type", easyjson.NewJSON(linkType))
	payload.SetByPath("body", easyjson.NewJSONObject())
	if len(body) > 0 {
		payload.SetByPath("body", body[0])
	}
	if len(tags) > 0 {
		payload.SetByPath("tags", easyjson.NewJSON(tags))
	}

	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(gc.request(sfp.AutoRequestSelect, "functions.graph.api.link.create", SeqFree(from), &payload, nil)))
}

func (gc GraphSyncClient) VerticesLinkUpdate(from, linkName string, tags []string, body easyjson.JSON, replace bool, toAndType4Upsert ...string) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("op_time", easyjson.NewJSON(system.GetCurrentTimeNs()))
	payload.SetByPath("name", easyjson.NewJSON(linkName))
	payload.SetByPath("body", body)
	payload.SetByPath("replace", easyjson.NewJSON(replace))
	if len(tags) > 0 {
		payload.SetByPath("tags", easyjson.NewJSON(tags))
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

	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(gc.request(sfp.AutoRequestSelect, "functions.graph.api.link.update", SeqFree(from), &payload, nil)))
}

func (gc GraphSyncClient) VerticesLinkUpdateByToAndType(from, to, linkType string, tags []string, body easyjson.JSON, replace bool, name4Upsert ...string) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("op_time", easyjson.NewJSON(system.GetCurrentTimeNs()))
	payload.SetByPath("to", easyjson.NewJSON(to))
	payload.SetByPath("type", easyjson.NewJSON(linkType))
	payload.SetByPath("replace", easyjson.NewJSON(replace))

	payload.SetByPath("body", body)
	if len(tags) > 0 {
		payload.SetByPath("tags", easyjson.NewJSON(tags))
	}
	if len(name4Upsert) > 0 {
		payload.SetByPath("upsert", easyjson.NewJSON(true))
		payload.SetByPath("name", easyjson.NewJSON(name4Upsert[0]))
	}

	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(gc.request(sfp.AutoRequestSelect, "functions.graph.api.link.update", SeqFree(from), &payload, nil)))
}

func (gc GraphSyncClient) VerticesLinkDelete(from, linkName string) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("op_time", easyjson.NewJSON(system.GetCurrentTimeNs()))
	payload.SetByPath("name", easyjson.NewJSON(linkName))

	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(gc.request(sfp.AutoRequestSelect, "functions.graph.api.link.delete", SeqFree(from), &payload, nil)))
}

func (gc GraphSyncClient) VerticesLinkDeleteByToAndType(from, to, linkType string) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("op_time", easyjson.NewJSON(system.GetCurrentTimeNs()))
	payload.SetByPath("to", easyjson.NewJSON(to))
	payload.SetByPath("type", easyjson.NewJSON(linkType))

	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(gc.request(sfp.AutoRequestSelect, "functions.graph.api.link.delete", SeqFree(from), &payload, nil)))
}

func (gc GraphSyncClient) VerticesLinkRead(from, linkName string, details ...bool) (easyjson.JSON, error) {
	withDetails := len(details) > 0 && details[0]
	key := "VerticesLinkRead:" + strconv.FormatBool(withDetails) + ":" + from + "|" + linkName
	return doRead(gc.readFlight, key, func() (any, error) {
		payload := easyjson.NewJSONObject()
		payload.SetByPath("name", easyjson.NewJSON(linkName))
		if len(details) > 0 {
			payload.SetByPath("details", easyjson.NewJSON(details[0]))
		}
		om := sfMediators.OpMsgFromSfReply(gc.request(sfp.AutoRequestSelect, "functions.graph.api.link.read", SeqFree(from), &payload, nil))
		return readResult{data: om.Data, err: OpErrorFromOpMsgStrict(om)}, nil
	})
}

func (gc GraphSyncClient) VerticesLinkReadByToAndType(from, to, linkType string, details ...bool) (easyjson.JSON, error) {
	withDetails := len(details) > 0 && details[0]
	key := "VerticesLinkReadByToAndType:" + strconv.FormatBool(withDetails) + ":" + from + "|" + to + "|" + linkType
	return doRead(gc.readFlight, key, func() (any, error) {
		payload := easyjson.NewJSONObject()
		payload.SetByPath("to", easyjson.NewJSON(to))
		payload.SetByPath("type", easyjson.NewJSON(linkType))
		if len(details) > 0 {
			payload.SetByPath("details", easyjson.NewJSON(details[0]))
		}
		om := sfMediators.OpMsgFromSfReply(gc.request(sfp.AutoRequestSelect, "functions.graph.api.link.read", SeqFree(from), &payload, nil))
		return readResult{data: om.Data, err: OpErrorFromOpMsgStrict(om)}, nil
	})
}
