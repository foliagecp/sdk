package db

import (
	"fmt"

	"github.com/foliagecp/easyjson"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfp "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/nats-io/nats.go"
)

type CMDBSyncClient struct {
	request sfp.SFRequestFunc
}

func NewCMDBSyncClient(NatsURL string, NatsRequestTimeout int) (CMDBSyncClient, error) {
	var err error
	nc, err := nats.Connect(NatsURL)
	if err != nil {
		return CMDBSyncClient{}, err
	}
	request := getRequestFunc(nc, NatsRequestTimeout)
	return NewCMDBSyncClientFromRequestFunction(request)
}

/*
ctx.Request
// or
runtime.Request
*/
func NewCMDBSyncClientFromRequestFunction(request sfp.SFRequestFunc) (CMDBSyncClient, error) {
	if request == nil {
		return CMDBSyncClient{}, fmt.Errorf("request must not be nil")
	}
	return CMDBSyncClient{request: request}, nil
}

// ------------------------------------------------------------------------------------------------

func (cc CMDBSyncClient) TypeCreate(name string, body ...easyjson.JSON) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("body", easyjson.NewJSONObject())
	if len(body) > 0 {
		payload.SetByPath("body", body[0])
	}
	om := sfMediators.OpMsgFromSfReply(cc.request(sfp.AutoRequestSelect, "functions.cmdb.api.type.create", name, &payload, nil))
	return &OpError{om.Status, om.Details}
}

func (cc CMDBSyncClient) TypeUpdate(name string, body easyjson.JSON, replace bool, upsert ...bool) error {
	payload := easyjson.NewJSONObject()
	if len(upsert) > 0 {
		payload.SetByPath("upsert", easyjson.NewJSON(upsert[0]))
	}
	payload.SetByPath("replace", easyjson.NewJSON(replace))
	payload.SetByPath("body", body)

	om := sfMediators.OpMsgFromSfReply(cc.request(sfp.AutoRequestSelect, "functions.cmdb.api.type.update", name, &payload, nil))
	return &OpError{om.Status, om.Details}
}

func (cc CMDBSyncClient) TypeDelete(name string) error {
	om := sfMediators.OpMsgFromSfReply(cc.request(sfp.AutoRequestSelect, "functions.cmdb.api.type.delete", name, nil, nil))
	return &OpError{om.Status, om.Details}
}

func (cc CMDBSyncClient) TypeRead(name string) (easyjson.JSON, error) {
	om := sfMediators.OpMsgFromSfReply(cc.request(sfp.AutoRequestSelect, "functions.cmdb.api.type.read", name, nil, nil))
	return om.Data, &OpError{om.Status, om.Details}
}

func (cc CMDBSyncClient) ObjectCreate(objectID, originType string, body ...easyjson.JSON) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("origin_type", easyjson.NewJSON(originType))
	payload.SetByPath("body", easyjson.NewJSONObject())
	if len(body) > 0 {
		payload.SetByPath("body", body[0])
	}

	om := sfMediators.OpMsgFromSfReply(cc.request(sfp.AutoRequestSelect, "functions.cmdb.api.object.create", objectID, &payload, nil))
	return &OpError{om.Status, om.Details}
}

func (cc CMDBSyncClient) ObjectUdate(objectID string, body easyjson.JSON, replace bool, originType4Upsert ...string) error {
	payload := easyjson.NewJSONObject()
	if len(originType4Upsert) > 0 {
		payload.SetByPath("usert", easyjson.NewJSON(true))
		payload.SetByPath("origin_type", easyjson.NewJSON(originType4Upsert[0]))
	}
	payload.SetByPath("replace", easyjson.NewJSON(replace))
	payload.SetByPath("body", body)

	om := sfMediators.OpMsgFromSfReply(cc.request(sfp.AutoRequestSelect, "functions.cmdb.api.object.update", objectID, &payload, nil))
	return &OpError{om.Status, om.Details}
}

func (cc CMDBSyncClient) ObjectDelete(id string) error {
	om := sfMediators.OpMsgFromSfReply(cc.request(sfp.AutoRequestSelect, "functions.cmdb.api.object.delete", id, nil, nil))
	return &OpError{om.Status, om.Details}
}

func (cc CMDBSyncClient) ObjectRead(name string) (easyjson.JSON, error) {
	om := sfMediators.OpMsgFromSfReply(cc.request(sfp.AutoRequestSelect, "functions.cmdb.api.object.read", name, nil, nil))
	return om.Data, &OpError{om.Status, om.Details}
}

// ------------------------------------------------------------------------------------------------

func (cc CMDBSyncClient) TypesLinkCreate(from, to, objectLinkType string, tags []string, body ...easyjson.JSON) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("to", easyjson.NewJSON(to))
	payload.SetByPath("body", easyjson.NewJSONObject())
	if len(body) > 0 {
		payload.SetByPath("body", body[0])
	}
	if tags != nil && len(tags) > 0 {
		payload.SetByPath("tags", easyjson.JSONFromArray(tags))
	}
	payload.SetByPath("object_type", easyjson.NewJSON(objectLinkType))

	om := sfMediators.OpMsgFromSfReply(cc.request(sfp.AutoRequestSelect, "functions.cmdb.api.types.link.create", from, &payload, nil))
	return &OpError{om.Status, om.Details}
}

func (cc CMDBSyncClient) TypesLinkUpdate(from, to string, tags []string, body easyjson.JSON, replace bool, upsert ...bool) error {
	payload := easyjson.NewJSONObject()
	if len(upsert) > 0 {
		payload.SetByPath("upsert", easyjson.NewJSON(upsert[0]))
	}
	payload.SetByPath("to", easyjson.NewJSON(to))
	payload.SetByPath("body", body)
	if tags != nil && len(tags) > 0 {
		payload.SetByPath("tags", easyjson.JSONFromArray(tags))
	}
	payload.SetByPath("replace", easyjson.NewJSON(replace))

	om := sfMediators.OpMsgFromSfReply(cc.request(sfp.AutoRequestSelect, "functions.cmdb.api.types.link.update", from, &payload, nil))
	return &OpError{om.Status, om.Details}
}

func (cc CMDBSyncClient) TypesLinkDelete(from, to string) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("to", easyjson.NewJSON(to))

	om := sfMediators.OpMsgFromSfReply(cc.request(sfp.AutoRequestSelect, "functions.cmdb.api.types.link.delete", from, &payload, nil))
	return &OpError{om.Status, om.Details}
}

func (cc CMDBSyncClient) TypesLinkRead(from, to string) (easyjson.JSON, error) {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("to", easyjson.NewJSON(to))

	om := sfMediators.OpMsgFromSfReply(cc.request(sfp.AutoRequestSelect, "functions.cmdb.api.types.link.read", from, &payload, nil))
	return om.Data, &OpError{om.Status, om.Details}
}

func (cc CMDBSyncClient) ObjectsLinkCreate(from, to, name string, tags []string, body ...easyjson.JSON) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("to", easyjson.NewJSON(to))
	payload.SetByPath("name", easyjson.NewJSON(name))
	payload.SetByPath("body", easyjson.NewJSONObject())
	if len(body) > 0 {
		payload.SetByPath("body", body[0])
	}
	if tags != nil && len(tags) > 0 {
		payload.SetByPath("tags", easyjson.JSONFromArray(tags))
	}

	om := sfMediators.OpMsgFromSfReply(cc.request(sfp.AutoRequestSelect, "functions.cmdb.api.objects.link.create", from, &payload, nil))
	return &OpError{om.Status, om.Details}
}

func (cc CMDBSyncClient) ObjectsLinkUpdate(from, to string, tags []string, body easyjson.JSON, replace bool, name4Upsert string) error {
	payload := easyjson.NewJSONObject()
	if len(name4Upsert) > 0 {
		payload.SetByPath("upsert", easyjson.NewJSON(true))
		payload.SetByPath("name", easyjson.NewJSON(name4Upsert[0]))
	}
	payload.SetByPath("to", easyjson.NewJSON(to))
	payload.SetByPath("body", body)
	if tags != nil && len(tags) > 0 {
		payload.SetByPath("tags", easyjson.JSONFromArray(tags))
	}
	payload.SetByPath("replace", easyjson.NewJSON(replace))

	om := sfMediators.OpMsgFromSfReply(cc.request(sfp.AutoRequestSelect, "functions.cmdb.api.objects.link.update", from, &payload, nil))
	return &OpError{om.Status, om.Details}
}

func (cc CMDBSyncClient) ObjectsLinkDelete(from, to string) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("to", easyjson.NewJSON(to))

	om := sfMediators.OpMsgFromSfReply(cc.request(sfp.AutoRequestSelect, "functions.cmdb.api.objects.link.delete", from, &payload, nil))
	return &OpError{om.Status, om.Details}
}

func (cc CMDBSyncClient) ObjectsLinkRead(from, to string) (easyjson.JSON, error) {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("to", easyjson.NewJSON(to))

	om := sfMediators.OpMsgFromSfReply(cc.request(sfp.AutoRequestSelect, "functions.cmdb.api.objects.link.read", from, &payload, nil))
	return om.Data, &OpError{om.Status, om.Details}
}

// ------------------------------------------------------------------------------------------------
