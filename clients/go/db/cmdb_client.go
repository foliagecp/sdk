package db

import (
	"github.com/foliagecp/easyjson"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfp "github.com/foliagecp/sdk/statefun/plugins"
)

// CMDBClient extends GraphSyncClient with high-level CMDB operations
type CMDBClient struct {
	GraphSyncClient
}

// NewCMDBClient creates a new CMDB client
func NewCMDBClient(NatsURL string, NatsRequestTimeoutSec int, HubDomainName string) (CMDBClient, error) {
	graphClient, err := NewGraphSyncClient(NatsURL, NatsRequestTimeoutSec, HubDomainName)
	if err != nil {
		return CMDBClient{}, err
	}
	return CMDBClient{GraphSyncClient: graphClient}, nil
}

// NewCMDBClientFromRequestFunction creates a CMDB client from an existing request function
func NewCMDBClientFromRequestFunction(request sfp.SFRequestFunc) (CMDBClient, error) {
	graphClient, err := NewGraphSyncClientFromRequestFunction(request)
	if err != nil {
		return CMDBClient{}, err
	}
	return CMDBClient{GraphSyncClient: graphClient}, nil
}

// Type Operations

// TypeCreate creates a new type in the CMDB
func (cc CMDBClient) TypeCreate(typeId string, body easyjson.JSON) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("body", body)

	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(
		cc.request(sfp.AutoRequestSelect, "functions.cmdb.api.type.create", typeId, &payload, nil)))
}

// TypeUpdate updates an existing type
func (cc CMDBClient) TypeUpdate(typeId string, body easyjson.JSON, upsert bool, replace bool) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("body", body)
	payload.SetByPath("upsert", easyjson.NewJSON(upsert))
	payload.SetByPath("replace", easyjson.NewJSON(replace))

	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(
		cc.request(sfp.AutoRequestSelect, "functions.cmdb.api.type.update", typeId, &payload, nil)))
}

// TypeDelete deletes a type
func (cc CMDBClient) TypeDelete(typeId string) error {
	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(
		cc.request(sfp.AutoRequestSelect, "functions.cmdb.api.type.delete", typeId, nil, nil)))
}

// TypeRead reads a type
func (cc CMDBClient) TypeRead(typeId string) (easyjson.JSON, error) {
	om := sfMediators.OpMsgFromSfReply(
		cc.request(sfp.AutoRequestSelect, "functions.cmdb.api.type.read", typeId, nil, nil))
	return om.Data, OpErrorFromOpMsg(om)
}

// Types Link Operations

// TypesLinkCreate creates a link between two types
func (cc CMDBClient) TypesLinkCreate(fromTypeId, toTypeId, objectType string, body easyjson.JSON, tags []string) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("to", easyjson.NewJSON(toTypeId))
	payload.SetByPath("object_type", easyjson.NewJSON(objectType))
	payload.SetByPath("body", body)
	if len(tags) > 0 {
		payload.SetByPath("tags", easyjson.JSONFromArray(tags))
	}

	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(
		cc.request(sfp.AutoRequestSelect, "functions.cmdb.api.types.link.create", fromTypeId, &payload, nil)))
}

// TypesLinkUpdate updates a link between types
func (cc CMDBClient) TypesLinkUpdate(fromTypeId, toTypeId string, body easyjson.JSON, tags []string, upsert bool, replace bool) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("to", easyjson.NewJSON(toTypeId))
	payload.SetByPath("body", body)
	payload.SetByPath("upsert", easyjson.NewJSON(upsert))
	payload.SetByPath("replace", easyjson.NewJSON(replace))
	if len(tags) > 0 {
		payload.SetByPath("tags", easyjson.JSONFromArray(tags))
	}

	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(
		cc.request(sfp.AutoRequestSelect, "functions.cmdb.api.types.link.update", fromTypeId, &payload, nil)))
}

// TypesLinkDelete deletes a link between types
func (cc CMDBClient) TypesLinkDelete(fromTypeId, toTypeId string) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("to", easyjson.NewJSON(toTypeId))

	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(
		cc.request(sfp.AutoRequestSelect, "functions.cmdb.api.types.link.delete", fromTypeId, &payload, nil)))
}

// TypesLinkRead reads a link between types
func (cc CMDBClient) TypesLinkRead(fromTypeId, toTypeId string) (easyjson.JSON, error) {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("to", easyjson.NewJSON(toTypeId))

	om := sfMediators.OpMsgFromSfReply(
		cc.request(sfp.AutoRequestSelect, "functions.cmdb.api.types.link.read", fromTypeId, &payload, nil))
	return om.Data, OpErrorFromOpMsg(om)
}

// Object Operations

// ObjectCreate creates a new object of a specific type
func (cc CMDBClient) ObjectCreate(objectId, originType string, body easyjson.JSON) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("origin_type", easyjson.NewJSON(originType))
	payload.SetByPath("body", body)

	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(
		cc.request(sfp.AutoRequestSelect, "functions.cmdb.api.object.create", objectId, &payload, nil)))
}

// ObjectUpdate updates an existing object
func (cc CMDBClient) ObjectUpdate(objectId string, body easyjson.JSON, originType string, upsert bool, replace bool) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("body", body)
	payload.SetByPath("upsert", easyjson.NewJSON(upsert))
	payload.SetByPath("replace", easyjson.NewJSON(replace))
	if originType != "" {
		payload.SetByPath("origin_type", easyjson.NewJSON(originType))
	}

	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(
		cc.request(sfp.AutoRequestSelect, "functions.cmdb.api.object.update", objectId, &payload, nil)))
}

// ObjectDelete deletes an object
func (cc CMDBClient) ObjectDelete(objectId string) error {
	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(
		cc.request(sfp.AutoRequestSelect, "functions.cmdb.api.object.delete", objectId, nil, nil)))
}

// ObjectRead reads an object
func (cc CMDBClient) ObjectRead(objectId string) (easyjson.JSON, error) {
	om := sfMediators.OpMsgFromSfReply(
		cc.request(sfp.AutoRequestSelect, "functions.cmdb.api.object.read", objectId, nil, nil))
	return om.Data, OpErrorFromOpMsg(om)
}

// Objects Link Operations

// ObjectsLinkCreate creates a link between two objects
func (cc CMDBClient) ObjectsLinkCreate(fromObjectId, toObjectId string, name string, body easyjson.JSON, tags []string) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("to", easyjson.NewJSON(toObjectId))
	if name != "" {
		payload.SetByPath("name", easyjson.NewJSON(name))
	}
	payload.SetByPath("body", body)
	if len(tags) > 0 {
		payload.SetByPath("tags", easyjson.JSONFromArray(tags))
	}

	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(
		cc.request(sfp.AutoRequestSelect, "functions.cmdb.api.objects.link.create", fromObjectId, &payload, nil)))
}

// ObjectsLinkUpdate updates a link between objects
func (cc CMDBClient) ObjectsLinkUpdate(fromObjectId, toObjectId string, name string, body easyjson.JSON, tags []string, upsert bool, replace bool) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("to", easyjson.NewJSON(toObjectId))
	if name != "" {
		payload.SetByPath("name", easyjson.NewJSON(name))
	}
	payload.SetByPath("body", body)
	payload.SetByPath("upsert", easyjson.NewJSON(upsert))
	payload.SetByPath("replace", easyjson.NewJSON(replace))
	if len(tags) > 0 {
		payload.SetByPath("tags", easyjson.JSONFromArray(tags))
	}

	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(
		cc.request(sfp.AutoRequestSelect, "functions.cmdb.api.objects.link.update", fromObjectId, &payload, nil)))
}

// ObjectsLinkDelete deletes a link between objects
func (cc CMDBClient) ObjectsLinkDelete(fromObjectId, toObjectId string) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("to", easyjson.NewJSON(toObjectId))

	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(
		cc.request(sfp.AutoRequestSelect, "functions.cmdb.api.objects.link.delete", fromObjectId, &payload, nil)))
}

// ObjectsLinkRead reads a link between objects
func (cc CMDBClient) ObjectsLinkRead(fromObjectId, toObjectId string) (easyjson.JSON, error) {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("to", easyjson.NewJSON(toObjectId))

	om := sfMediators.OpMsgFromSfReply(
		cc.request(sfp.AutoRequestSelect, "functions.cmdb.api.objects.link.read", fromObjectId, &payload, nil))
	return om.Data, OpErrorFromOpMsg(om)
}
