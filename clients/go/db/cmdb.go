package db

import (
	"fmt"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/statefun"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfp "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/nats-io/nats.go"
)

type TriggerType = string

const (
	CreateTrigger TriggerType = "create"
	UpdateTrigger TriggerType = "update"
	DeleteTrigger TriggerType = "delete"
	ReadTrigger   TriggerType = "read"
)

type CMDBSyncClient struct {
	request                   sfp.SFRequestFunc
	ShadowObjectCanBeRecevier bool
}

func NewCMDBSyncClient(NatsURL string, NatsRequestTimeoutSec int, HubDomainName string) (CMDBSyncClient, error) {
	var err error
	nc, err := nats.Connect(NatsURL)
	if err != nil {
		return CMDBSyncClient{}, err
	}
	request := getRequestFunc(nc, NatsRequestTimeoutSec, HubDomainName)
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

func (cmdb CMDBSyncClient) commonTriggerDelete(body easyjson.JSON, triggerType TriggerType, statefunName ...string) easyjson.JSON {
	triggerPath := fmt.Sprintf("triggers.%s", triggerType)
	var bodyTriggers easyjson.JSON
	if body.GetByPath(triggerPath).IsNonEmptyObject() {
		newTriggers := []string{}
		if arr, ok := body.GetByPath(triggerPath).AsArrayString(); ok {
			for _, sf := range arr {
				toRemove := false
				for _, sf2Remove := range statefunName {
					if sf == sf2Remove {
						toRemove = true
					}
				}
				if !toRemove {
					newTriggers = append(newTriggers, sf)
				}
			}
		}
		bodyTriggers = easyjson.NewJSONObjectWithKeyValue(triggerPath, easyjson.JSONFromArray(newTriggers))
	} else {
		bodyTriggers = easyjson.NewJSONObjectWithKeyValue(triggerPath, easyjson.NewJSONArray())
	}

	body.SetByPath(triggerPath, bodyTriggers)
	newBody := body.GetByPath("body")
	if newBody.IsNull() {
		newBody = easyjson.NewJSONObject()
	}

	return newBody
}

func (cmdb CMDBSyncClient) commonTriggersDrop(body easyjson.JSON, triggerType TriggerType) easyjson.JSON {
	triggerPath := fmt.Sprintf("triggers.%s", triggerType)

	body.SetByPath(triggerPath, easyjson.NewJSONArray())
	newBody := body.GetByPath("body")
	if newBody.IsNull() {
		newBody = easyjson.NewJSONObject()
	}

	return newBody
}

func (cmdb CMDBSyncClient) TriggerObjectSet(typeName string, triggerType TriggerType, statefunName ...string) error {
	if len(statefunName) == 0 {
		return fmt.Errorf("at least 1 statefun name is required")
	}

	body := easyjson.NewJSONObject()
	body.SetByPath(fmt.Sprintf("triggers.%s", triggerType), easyjson.JSONFromArray(statefunName))
	return cmdb.TypeUpdate(
		typeName,
		body,
		false,
		true,
	)
}

func (cmdb CMDBSyncClient) TriggerObjectDelete(typeName string, triggerType TriggerType, statefunName ...string) error {
	if len(statefunName) == 0 {
		return fmt.Errorf("at least 1 statefun name is required")
	}

	data, err := cmdb.TypeRead(typeName)
	if err != nil {
		return err
	}

	body := data.GetByPath("body")
	if !body.IsNull() {
		body = cmdb.commonTriggerDelete(body, triggerType, statefunName...)
	}

	return cmdb.TypeUpdate(
		typeName,
		body,
		true,
	)
}

func (cmdb CMDBSyncClient) TriggerObjectDrop(typeName string, triggerType TriggerType) error {
	data, err := cmdb.TypeRead(typeName)
	if err != nil {
		return err
	}

	body := data.GetByPath("body")
	if !body.IsNull() {
		body = cmdb.commonTriggersDrop(body, triggerType)
	}

	return cmdb.TypeUpdate(
		typeName,
		body,
		true,
	)
}

func (cmdb CMDBSyncClient) TriggerLinkSet(fromTypeName, toTypeName string, triggerType TriggerType, statefunName ...string) error {
	if len(statefunName) == 0 {
		return fmt.Errorf("at least 1 statefun name is required")
	}
	body := easyjson.NewJSONObject()
	body.SetByPath(fmt.Sprintf("triggers.%s", triggerType), easyjson.JSONFromArray(statefunName))
	return cmdb.TypesLinkUpdate(
		fromTypeName,
		toTypeName,
		nil,
		body,
		false,
		true,
	)
}

func (cmdb CMDBSyncClient) TriggerLinkRemove(fromTypeName, toTypeName string, triggerType TriggerType, statefunName ...string) error {
	if len(statefunName) == 0 {
		return fmt.Errorf("at least 1 statefun name is required")
	}

	data, err := cmdb.TypesLinkRead(fromTypeName, toTypeName)
	if err != nil {
		return err
	}

	tags := []string{}
	if arr, ok := data.GetByPath("tags").AsArrayString(); ok {
		tags = arr
	}

	body := data.GetByPath("body")
	if !body.IsNull() {
		body = cmdb.commonTriggerDelete(body, triggerType, statefunName...)
	}

	return cmdb.TypesLinkUpdate(
		fromTypeName,
		toTypeName,
		tags,
		body,
		true,
	)
}

func (cmdb CMDBSyncClient) TriggerLinkDrop(fromTypeName, toTypeName string, triggerType TriggerType) error {
	data, err := cmdb.TypesLinkRead(fromTypeName, toTypeName)
	if err != nil {
		return err
	}

	tags := []string{}
	if arr, ok := data.GetByPath("tags").AsArrayString(); ok {
		tags = arr
	}

	body := data.GetByPath("body")
	if !body.IsNull() {
		body = cmdb.commonTriggersDrop(body, triggerType)
	}

	return cmdb.TypesLinkUpdate(
		fromTypeName,
		toTypeName,
		tags,
		body,
		true,
	)
}

func (cmdb CMDBSyncClient) TypeCreate(name string, body ...easyjson.JSON) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("body", easyjson.NewJSONObject())
	if len(body) > 0 {
		payload.SetByPath("body", body[0])
	}

	options := easyjson.NewJSONObject()
	options.SetByPath(statefun.ShadowObjectCallParamOptionPath, easyjson.NewJSON(cmdb.ShadowObjectCanBeRecevier))
	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(cmdb.request(sfp.AutoRequestSelect, "functions.cmdb.api.type.create", name, &payload, &options)))
}

func (cmdb CMDBSyncClient) TypeUpdate(name string, body easyjson.JSON, replace bool, upsert ...bool) error {
	payload := easyjson.NewJSONObject()
	if len(upsert) > 0 {
		payload.SetByPath("upsert", easyjson.NewJSON(upsert[0]))
	}
	payload.SetByPath("replace", easyjson.NewJSON(replace))
	payload.SetByPath("body", body)

	options := easyjson.NewJSONObject()
	options.SetByPath(statefun.ShadowObjectCallParamOptionPath, easyjson.NewJSON(cmdb.ShadowObjectCanBeRecevier))
	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(cmdb.request(sfp.AutoRequestSelect, "functions.cmdb.api.type.update", name, &payload, &options)))
}

func (cmdb CMDBSyncClient) TypeDelete(name string) error {
	options := easyjson.NewJSONObject()
	options.SetByPath(statefun.ShadowObjectCallParamOptionPath, easyjson.NewJSON(cmdb.ShadowObjectCanBeRecevier))
	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(cmdb.request(sfp.AutoRequestSelect, "functions.cmdb.api.type.delete", name, nil, &options)))
}

func (cmdb CMDBSyncClient) TypeRead(name string) (easyjson.JSON, error) {
	options := easyjson.NewJSONObject()
	options.SetByPath(statefun.ShadowObjectCallParamOptionPath, easyjson.NewJSON(cmdb.ShadowObjectCanBeRecevier))
	om := sfMediators.OpMsgFromSfReply(cmdb.request(sfp.AutoRequestSelect, "functions.cmdb.api.type.read", name, nil, &options))
	return om.Data, OpErrorFromOpMsg(om)
}

func (cmdb CMDBSyncClient) ObjectCreate(objectID, originType string, body ...easyjson.JSON) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("origin_type", easyjson.NewJSON(originType))
	payload.SetByPath("body", easyjson.NewJSONObject())
	if len(body) > 0 {
		payload.SetByPath("body", body[0])
	}

	options := easyjson.NewJSONObject()
	options.SetByPath(statefun.ShadowObjectCallParamOptionPath, easyjson.NewJSON(cmdb.ShadowObjectCanBeRecevier))
	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(cmdb.request(sfp.AutoRequestSelect, "functions.cmdb.api.object.create", objectID, &payload, &options)))
}

func (cmdb CMDBSyncClient) ObjectUpdate(objectID string, body easyjson.JSON, replace bool, originType4Upsert ...string) error {
	payload := easyjson.NewJSONObject()
	if len(originType4Upsert) > 0 {
		payload.SetByPath("upsert", easyjson.NewJSON(true))
		payload.SetByPath("origin_type", easyjson.NewJSON(originType4Upsert[0]))
	}
	payload.SetByPath("replace", easyjson.NewJSON(replace))
	payload.SetByPath("body", body)

	options := easyjson.NewJSONObject()
	options.SetByPath(statefun.ShadowObjectCallParamOptionPath, easyjson.NewJSON(cmdb.ShadowObjectCanBeRecevier))
	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(cmdb.request(sfp.AutoRequestSelect, "functions.cmdb.api.object.update", objectID, &payload, &options)))
}

func (cmdb CMDBSyncClient) ObjectUpdateWithDetails(objectID string, body easyjson.JSON, replace bool, originType4Upsert ...string) (easyjson.JSON, error) {
	payload := easyjson.NewJSONObject()
	if len(originType4Upsert) > 0 {
		payload.SetByPath("upsert", easyjson.NewJSON(true))
		payload.SetByPath("origin_type", easyjson.NewJSON(originType4Upsert[0]))
	}
	payload.SetByPath("replace", easyjson.NewJSON(replace))
	payload.SetByPath("body", body)

	options := easyjson.NewJSONObjectWithKeyValue("op_stack", easyjson.NewJSON(true))
	options.SetByPath(statefun.ShadowObjectCallParamOptionPath, easyjson.NewJSON(cmdb.ShadowObjectCanBeRecevier))
	msg := sfMediators.OpMsgFromSfReply(cmdb.request(sfp.AutoRequestSelect, "functions.cmdb.api.object.update", objectID, &payload, &options))
	return msg.Data, OpErrorFromOpMsg(msg)
}

func (cmdb CMDBSyncClient) ObjectDelete(id string) error {
	options := easyjson.NewJSONObject()
	options.SetByPath(statefun.ShadowObjectCallParamOptionPath, easyjson.NewJSON(cmdb.ShadowObjectCanBeRecevier))
	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(cmdb.request(sfp.AutoRequestSelect, "functions.cmdb.api.object.delete", id, nil, &options)))
}

func (cmdb CMDBSyncClient) ObjectDeleteWithDetails(id string) (easyjson.JSON, error) {
	options := easyjson.NewJSONObjectWithKeyValue("op_stack", easyjson.NewJSON(true))
	options.SetByPath(statefun.ShadowObjectCallParamOptionPath, easyjson.NewJSON(cmdb.ShadowObjectCanBeRecevier))
	msg := sfMediators.OpMsgFromSfReply(cmdb.request(sfp.AutoRequestSelect, "functions.cmdb.api.object.delete", id, nil, &options))
	return msg.Data, OpErrorFromOpMsg(msg)
}

func (cmdb CMDBSyncClient) ObjectRead(name string) (easyjson.JSON, error) {
	options := easyjson.NewJSONObject()
	options.SetByPath(statefun.ShadowObjectCallParamOptionPath, easyjson.NewJSON(cmdb.ShadowObjectCanBeRecevier))
	om := sfMediators.OpMsgFromSfReply(cmdb.request(sfp.AutoRequestSelect, "functions.cmdb.api.object.read", name, nil, &options))
	return om.Data, OpErrorFromOpMsg(om)
}

// ------------------------------------------------------------------------------------------------

func (cmdb CMDBSyncClient) TypesLinkCreate(from, to, objectLinkType string, tags []string, body ...easyjson.JSON) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("to", easyjson.NewJSON(to))
	payload.SetByPath("body", easyjson.NewJSONObject())
	if len(body) > 0 {
		payload.SetByPath("body", body[0])
	}
	if len(tags) > 0 {
		payload.SetByPath("tags", easyjson.JSONFromArray(tags))
	}
	payload.SetByPath("object_type", easyjson.NewJSON(objectLinkType))

	options := easyjson.NewJSONObject()
	options.SetByPath(statefun.ShadowObjectCallParamOptionPath, easyjson.NewJSON(cmdb.ShadowObjectCanBeRecevier))
	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(cmdb.request(sfp.AutoRequestSelect, "functions.cmdb.api.types.link.create", from, &payload, &options)))
}

func (cmdb CMDBSyncClient) TypesLinkUpdate(from, to string, tags []string, body easyjson.JSON, replace bool, upsert ...bool) error {
	payload := easyjson.NewJSONObject()
	if len(upsert) > 0 {
		payload.SetByPath("upsert", easyjson.NewJSON(upsert[0]))
	}
	payload.SetByPath("to", easyjson.NewJSON(to))
	payload.SetByPath("body", body)
	if len(tags) > 0 {
		payload.SetByPath("tags", easyjson.JSONFromArray(tags))
	}
	payload.SetByPath("replace", easyjson.NewJSON(replace))

	options := easyjson.NewJSONObject()
	options.SetByPath(statefun.ShadowObjectCallParamOptionPath, easyjson.NewJSON(cmdb.ShadowObjectCanBeRecevier))
	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(cmdb.request(sfp.AutoRequestSelect, "functions.cmdb.api.types.link.update", from, &payload, &options)))
}

func (cmdb CMDBSyncClient) TypesLinkDelete(from, to string) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("to", easyjson.NewJSON(to))

	options := easyjson.NewJSONObject()
	options.SetByPath(statefun.ShadowObjectCallParamOptionPath, easyjson.NewJSON(cmdb.ShadowObjectCanBeRecevier))
	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(cmdb.request(sfp.AutoRequestSelect, "functions.cmdb.api.types.link.delete", from, &payload, &options)))
}

func (cmdb CMDBSyncClient) TypesLinkRead(from, to string) (easyjson.JSON, error) {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("to", easyjson.NewJSON(to))

	options := easyjson.NewJSONObject()
	options.SetByPath(statefun.ShadowObjectCallParamOptionPath, easyjson.NewJSON(cmdb.ShadowObjectCanBeRecevier))
	om := sfMediators.OpMsgFromSfReply(cmdb.request(sfp.AutoRequestSelect, "functions.cmdb.api.types.link.read", from, &payload, &options))
	return om.Data, OpErrorFromOpMsg(om)
}

func (cmdb CMDBSyncClient) ObjectsLinkCreate(from, to, name string, tags []string, body ...easyjson.JSON) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("to", easyjson.NewJSON(to))
	payload.SetByPath("name", easyjson.NewJSON(name))
	payload.SetByPath("body", easyjson.NewJSONObject())
	if len(body) > 0 {
		payload.SetByPath("body", body[0])
	}
	if len(tags) > 0 {
		payload.SetByPath("tags", easyjson.JSONFromArray(tags))
	}

	options := easyjson.NewJSONObject()
	options.SetByPath(statefun.ShadowObjectCallParamOptionPath, easyjson.NewJSON(cmdb.ShadowObjectCanBeRecevier))
	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(cmdb.request(sfp.AutoRequestSelect, "functions.cmdb.api.objects.link.create", from, &payload, &options)))
}

func (cmdb CMDBSyncClient) ObjectsLinkUpdate(from, to string, tags []string, body easyjson.JSON, replace bool, name4Upsert ...string) error {
	payload := easyjson.NewJSONObject()
	if len(name4Upsert) > 0 {
		payload.SetByPath("upsert", easyjson.NewJSON(true))
		payload.SetByPath("name", easyjson.NewJSON(name4Upsert[0]))
	}
	payload.SetByPath("to", easyjson.NewJSON(to))
	payload.SetByPath("body", body)
	if len(tags) > 0 {
		payload.SetByPath("tags", easyjson.JSONFromArray(tags))
	}
	payload.SetByPath("replace", easyjson.NewJSON(replace))

	options := easyjson.NewJSONObject()
	options.SetByPath(statefun.ShadowObjectCallParamOptionPath, easyjson.NewJSON(cmdb.ShadowObjectCanBeRecevier))
	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(cmdb.request(sfp.AutoRequestSelect, "functions.cmdb.api.objects.link.update", from, &payload, &options)))
}

func (cmdb CMDBSyncClient) ObjectsLinkUpdateWithDetails(from, to string, tags []string, body easyjson.JSON, replace bool, name4Upsert ...string) (easyjson.JSON, error) {
	payload := easyjson.NewJSONObject()
	if len(name4Upsert) > 0 {
		payload.SetByPath("upsert", easyjson.NewJSON(true))
		payload.SetByPath("name", easyjson.NewJSON(name4Upsert[0]))
	}
	payload.SetByPath("to", easyjson.NewJSON(to))
	payload.SetByPath("body", body)
	if len(tags) > 0 {
		payload.SetByPath("tags", easyjson.JSONFromArray(tags))
	}
	payload.SetByPath("replace", easyjson.NewJSON(replace))

	options := easyjson.NewJSONObjectWithKeyValue("op_stack", easyjson.NewJSON(true))
	options.SetByPath(statefun.ShadowObjectCallParamOptionPath, easyjson.NewJSON(cmdb.ShadowObjectCanBeRecevier))
	msg := sfMediators.OpMsgFromSfReply(cmdb.request(sfp.AutoRequestSelect, "functions.cmdb.api.objects.link.update", from, &payload, &options))
	return msg.Data, OpErrorFromOpMsg(msg)
}

func (cmdb CMDBSyncClient) ObjectsLinkDelete(from, to string) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("to", easyjson.NewJSON(to))

	options := easyjson.NewJSONObject()
	options.SetByPath(statefun.ShadowObjectCallParamOptionPath, easyjson.NewJSON(cmdb.ShadowObjectCanBeRecevier))
	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(cmdb.request(sfp.AutoRequestSelect, "functions.cmdb.api.objects.link.delete", from, &payload, &options)))
}

func (cmdb CMDBSyncClient) ObjectsLinkDeleteWithDetails(from, to string) (easyjson.JSON, error) {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("to", easyjson.NewJSON(to))

	options := easyjson.NewJSONObjectWithKeyValue("op_stack", easyjson.NewJSON(true))
	options.SetByPath(statefun.ShadowObjectCallParamOptionPath, easyjson.NewJSON(cmdb.ShadowObjectCanBeRecevier))
	msg := sfMediators.OpMsgFromSfReply(cmdb.request(sfp.AutoRequestSelect, "functions.cmdb.api.objects.link.delete", from, &payload, &options))
	return msg.Data, OpErrorFromOpMsg(msg)
}

func (cmdb CMDBSyncClient) ObjectsLinkRead(from, to string) (easyjson.JSON, error) {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("to", easyjson.NewJSON(to))

	options := easyjson.NewJSONObject()
	options.SetByPath(statefun.ShadowObjectCallParamOptionPath, easyjson.NewJSON(cmdb.ShadowObjectCanBeRecevier))
	om := sfMediators.OpMsgFromSfReply(cmdb.request(sfp.AutoRequestSelect, "functions.cmdb.api.objects.link.read", from, &payload, &options))
	return om.Data, OpErrorFromOpMsg(om)
}

// ------------------------------------------------------------------------------------------------
