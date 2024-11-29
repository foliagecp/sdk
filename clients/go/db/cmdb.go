package db

import (
	"fmt"
	"time"

	"github.com/foliagecp/easyjson"
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
	request  sfp.SFRequestFunc
	isolated bool
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
	return CMDBSyncClient{request: request, isolated: true}, nil
}

// ------------------------------------------------------------------------------------------------

func (cmdb *CMDBSyncClient) SetIsolated(isolated bool) {
	cmdb.isolated = isolated
}

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

func (cmdb CMDBSyncClient) TriggerObjectRelationSet(fromTypeName, toTypeName string, triggerType TriggerType, statefunName ...string) error {
	if len(statefunName) == 0 {
		return fmt.Errorf("at least 1 statefun name is required")
	}
	body := easyjson.NewJSONObject()
	body.SetByPath(fmt.Sprintf("triggers.%s", triggerType), easyjson.JSONFromArray(statefunName))
	return cmdb.TypeRelationUpdate(
		fromTypeName,
		toTypeName,
		nil,
		body,
		false,
	)
}

func (cmdb CMDBSyncClient) TriggerObjectRelationRemove(fromTypeName, toTypeName string, triggerType TriggerType, statefunName ...string) error {
	if len(statefunName) == 0 {
		return fmt.Errorf("at least 1 statefun name is required")
	}

	data, err := cmdb.TypeRelationRead(fromTypeName, toTypeName)
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

	return cmdb.TypeRelationUpdate(
		fromTypeName,
		toTypeName,
		tags,
		body,
		true,
	)
}

func (cmdb CMDBSyncClient) TriggerObjectRelationDrop(fromTypeName, toTypeName string, triggerType TriggerType) error {
	data, err := cmdb.TypeRelationRead(fromTypeName, toTypeName)
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

	return cmdb.TypeRelationUpdate(
		fromTypeName,
		toTypeName,
		tags,
		body,
		true,
	)
}

func (cmdb CMDBSyncClient) cmdbRequestWrapper(rp sfp.RequestProvider, ft string, id string, payload *easyjson.JSON, options *easyjson.JSON, t ...time.Duration) sfMediators.OpMsg {
	if cmdb.isolated {
		p := payload.Clone()
		p.SetByPath("uuid", easyjson.NewJSON(id))
		return sfMediators.OpMsgFromSfReply(cmdb.request(sfp.AutoRequestSelect, "functions.cmdb.api.crud.isolated", "i", &p, options, t...))
	}
	return sfMediators.OpMsgFromSfReply(cmdb.request(sfp.AutoRequestSelect, "functions.cmdb.api.crud", id, payload, options, t...))
}

func (cmdb CMDBSyncClient) TypeCreate(name string, body ...easyjson.JSON) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("operation.target", easyjson.NewJSON("type"))
	payload.SetByPath("operation.type", easyjson.NewJSON("create"))
	if len(body) > 0 {
		payload.SetByPath("data.body", body[0])
	} else {
		payload.SetByPath("data.body", easyjson.NewJSONObject())
	}
	return OpErrorFromOpMsg(cmdb.cmdbRequestWrapper(sfp.AutoRequestSelect, "functions.cmdb.api.crud", name, &payload, nil))
}

func (cmdb CMDBSyncClient) TypeUpdate(name string, body easyjson.JSON, replace bool, upsert ...bool) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("operation.target", easyjson.NewJSON("type"))
	payload.SetByPath("operation.type", easyjson.NewJSON("update"))
	if len(upsert) > 0 {
		payload.SetByPath("data.upsert", easyjson.NewJSON(upsert[0]))
	}
	payload.SetByPath("data.replace", easyjson.NewJSON(replace))
	payload.SetByPath("data.body", body)
	if body.PathExists("triggers") {
		payload.SetByPath("data.triggers", body.GetByPath("triggers"))
	}
	return OpErrorFromOpMsg(cmdb.cmdbRequestWrapper(sfp.AutoRequestSelect, "functions.cmdb.api.crud", name, &payload, nil))
}

func (cmdb CMDBSyncClient) TypeDelete(name string) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("operation.target", easyjson.NewJSON("type"))
	payload.SetByPath("operation.type", easyjson.NewJSON("delete"))
	return OpErrorFromOpMsg(cmdb.cmdbRequestWrapper(sfp.AutoRequestSelect, "functions.cmdb.api.crud", name, &payload, nil))
}

func (cmdb CMDBSyncClient) TypeRead(name string) (easyjson.JSON, error) {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("operation.target", easyjson.NewJSON("type"))
	payload.SetByPath("operation.type", easyjson.NewJSON("read"))
	msg := cmdb.cmdbRequestWrapper(sfp.AutoRequestSelect, "functions.cmdb.api.crud", name, &payload, nil)
	return msg.Data, OpErrorFromOpMsg(msg)
}

func (cmdb CMDBSyncClient) ObjectCreate(objectID, originType string, body ...easyjson.JSON) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("operation.target", easyjson.NewJSON("object"))
	payload.SetByPath("operation.type", easyjson.NewJSON("create"))
	payload.SetByPath("data.type", easyjson.NewJSON(originType))
	if len(body) > 0 {
		payload.SetByPath("data.body", body[0])
	} else {
		payload.SetByPath("data.body", easyjson.NewJSONObject())
	}
	return OpErrorFromOpMsg(cmdb.cmdbRequestWrapper(sfp.AutoRequestSelect, "functions.cmdb.api.crud", objectID, &payload, nil))
}

func (cmdb CMDBSyncClient) ObjectUpdate(objectID string, body easyjson.JSON, replace bool, originType4Upsert ...string) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("operation.target", easyjson.NewJSON("object"))
	payload.SetByPath("operation.type", easyjson.NewJSON("update"))
	if len(originType4Upsert) > 0 {
		payload.SetByPath("data.upsert", easyjson.NewJSON(true))
		payload.SetByPath("data.type", easyjson.NewJSON(originType4Upsert[0]))
	}
	payload.SetByPath("data.replace", easyjson.NewJSON(replace))
	payload.SetByPath("data.body", body)
	return OpErrorFromOpMsg(cmdb.cmdbRequestWrapper(sfp.AutoRequestSelect, "functions.cmdb.api.crud", objectID, &payload, nil))
}

func (cmdb CMDBSyncClient) ObjectUpdateWithDetails(objectID string, body easyjson.JSON, replace bool, originType4Upsert ...string) (easyjson.JSON, error) {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("operation.target", easyjson.NewJSON("object"))
	payload.SetByPath("operation.type", easyjson.NewJSON("update"))
	if len(originType4Upsert) > 0 {
		payload.SetByPath("data.upsert", easyjson.NewJSON(true))
		payload.SetByPath("data.type", easyjson.NewJSON(originType4Upsert[0]))
	}
	payload.SetByPath("data.replace", easyjson.NewJSON(replace))
	payload.SetByPath("data.body", body)

	options := easyjson.NewJSONObjectWithKeyValue("op_stack", easyjson.NewJSON(true))
	msg := cmdb.cmdbRequestWrapper(sfp.AutoRequestSelect, "functions.cmdb.api.crud", objectID, &payload, &options)
	return msg.Data, OpErrorFromOpMsg(msg)
}

func (cmdb CMDBSyncClient) ObjectDelete(objectID string) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("operation.target", easyjson.NewJSON("object"))
	payload.SetByPath("operation.type", easyjson.NewJSON("delete"))
	return OpErrorFromOpMsg(cmdb.cmdbRequestWrapper(sfp.AutoRequestSelect, "functions.cmdb.api.crud", objectID, &payload, nil))
}

func (cmdb CMDBSyncClient) ObjectDeleteWithDetails(id string) (easyjson.JSON, error) {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("operation.target", easyjson.NewJSON("object"))
	payload.SetByPath("operation.type", easyjson.NewJSON("delete"))
	options := easyjson.NewJSONObjectWithKeyValue("op_stack", easyjson.NewJSON(true))
	msg := cmdb.cmdbRequestWrapper(sfp.AutoRequestSelect, "functions.cmdb.api.crud", id, &payload, &options)
	return msg.Data, OpErrorFromOpMsg(msg)
}

func (cmdb CMDBSyncClient) ObjectRead(name string) (easyjson.JSON, error) {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("operation.target", easyjson.NewJSON("object"))
	payload.SetByPath("operation.type", easyjson.NewJSON("read"))
	msg := cmdb.cmdbRequestWrapper(sfp.AutoRequestSelect, "functions.cmdb.api.crud", name, &payload, nil)
	return msg.Data, OpErrorFromOpMsg(msg)
}

// ------------------------------------------------------------------------------------------------

func (cmdb CMDBSyncClient) TypeRelationCreate(from, to, objectLinkType string, tags []string, body ...easyjson.JSON) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("operation.target", easyjson.NewJSON("type.relation"))
	payload.SetByPath("operation.type", easyjson.NewJSON("create"))
	payload.SetByPath("data.to", easyjson.NewJSON(to))
	payload.SetByPath("data.body", easyjson.NewJSONObject())
	if len(body) > 0 {
		payload.SetByPath("data.body", body[0])
	}
	if len(tags) > 0 {
		payload.SetByPath("data.tags", easyjson.JSONFromArray(tags))
	}
	payload.SetByPath("data.object_relation_type", easyjson.NewJSON(objectLinkType))
	return OpErrorFromOpMsg(cmdb.cmdbRequestWrapper(sfp.AutoRequestSelect, "functions.cmdb.api.crud", from, &payload, nil))
}

func (cmdb CMDBSyncClient) TypeRelationUpdate(from, to string, tags []string, body easyjson.JSON, replace bool, upsert ...bool) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("operation.target", easyjson.NewJSON("type.relation"))
	payload.SetByPath("operation.type", easyjson.NewJSON("update"))
	if len(upsert) > 0 {
		payload.SetByPath("data.upsert", easyjson.NewJSON(upsert[0]))
	}
	payload.SetByPath("data.to", easyjson.NewJSON(to))
	payload.SetByPath("data.body", body)
	if len(tags) > 0 {
		payload.SetByPath("data.tags", easyjson.JSONFromArray(tags))
	}
	if body.PathExists("triggers") {
		payload.SetByPath("data.triggers", body.GetByPath("triggers"))
	}
	payload.SetByPath("data.replace", easyjson.NewJSON(replace))
	return OpErrorFromOpMsg(cmdb.cmdbRequestWrapper(sfp.AutoRequestSelect, "functions.cmdb.api.crud", from, &payload, nil))
}

func (cmdb CMDBSyncClient) TypeRelationDelete(from, to string) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("operation.target", easyjson.NewJSON("type.relation"))
	payload.SetByPath("operation.type", easyjson.NewJSON("delete"))
	payload.SetByPath("data.to", easyjson.NewJSON(to))
	return OpErrorFromOpMsg(cmdb.cmdbRequestWrapper(sfp.AutoRequestSelect, "functions.cmdb.api.crud", from, &payload, nil))
}

func (cmdb CMDBSyncClient) TypeRelationRead(from, to string) (easyjson.JSON, error) {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("operation.target", easyjson.NewJSON("type.relation"))
	payload.SetByPath("operation.type", easyjson.NewJSON("read"))
	payload.SetByPath("data.to", easyjson.NewJSON(to))
	msg := cmdb.cmdbRequestWrapper(sfp.AutoRequestSelect, "functions.cmdb.api.crud", from, &payload, nil)
	return msg.Data, OpErrorFromOpMsg(msg)
}

func (cmdb CMDBSyncClient) ObjectRelationCreate(from, to, name string, tags []string, body ...easyjson.JSON) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("operation.target", easyjson.NewJSON("object.relation"))
	payload.SetByPath("operation.type", easyjson.NewJSON("create"))
	payload.SetByPath("data.to", easyjson.NewJSON(to))
	payload.SetByPath("data.name", easyjson.NewJSON(name))
	if len(body) > 0 {
		payload.SetByPath("data.body", body[0])
	} else {
		payload.SetByPath("data.body", easyjson.NewJSONObject())
	}
	if len(tags) > 0 {
		payload.SetByPath("data.tags", easyjson.JSONFromArray(tags))
	}
	return OpErrorFromOpMsg(cmdb.cmdbRequestWrapper(sfp.AutoRequestSelect, "functions.cmdb.api.crud", from, &payload, nil))
}

func (cmdb CMDBSyncClient) ObjectRelationUpdate(from, to string, tags []string, body easyjson.JSON, replace bool, name4Upsert ...string) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("operation.target", easyjson.NewJSON("object.relation"))
	payload.SetByPath("operation.type", easyjson.NewJSON("update"))
	if len(name4Upsert) > 0 {
		payload.SetByPath("data.upsert", easyjson.NewJSON(true))
		payload.SetByPath("data.name", easyjson.NewJSON(name4Upsert[0]))
	}
	payload.SetByPath("data.to", easyjson.NewJSON(to))
	payload.SetByPath("data.body", body)
	if len(tags) > 0 {
		payload.SetByPath("data.tags", easyjson.JSONFromArray(tags))
	}
	payload.SetByPath("data.replace", easyjson.NewJSON(replace))
	return OpErrorFromOpMsg(cmdb.cmdbRequestWrapper(sfp.AutoRequestSelect, "functions.cmdb.api.crud", from, &payload, nil))
}

func (cmdb CMDBSyncClient) ObjectRelationUpdateWithDetails(from, to string, tags []string, body easyjson.JSON, replace bool, name4Upsert ...string) (easyjson.JSON, error) {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("operation.target", easyjson.NewJSON("object.relation"))
	payload.SetByPath("operation.type", easyjson.NewJSON("update"))
	if len(name4Upsert) > 0 {
		payload.SetByPath("data.upsert", easyjson.NewJSON(true))
		payload.SetByPath("data.name", easyjson.NewJSON(name4Upsert[0]))
	}
	payload.SetByPath("data.to", easyjson.NewJSON(to))
	payload.SetByPath("data.body", body)
	if len(tags) > 0 {
		payload.SetByPath("data.tags", easyjson.JSONFromArray(tags))
	}
	payload.SetByPath("data.replace", easyjson.NewJSON(replace))
	options := easyjson.NewJSONObjectWithKeyValue("op_stack", easyjson.NewJSON(true))
	msg := cmdb.cmdbRequestWrapper(sfp.AutoRequestSelect, "functions.cmdb.api.crud", from, &payload, &options)
	return msg.Data, OpErrorFromOpMsg(msg)
}

func (cmdb CMDBSyncClient) ObjectRelationDelete(from, to string) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("operation.target", easyjson.NewJSON("object.relation"))
	payload.SetByPath("operation.type", easyjson.NewJSON("delete"))
	payload.SetByPath("data.to", easyjson.NewJSON(to))
	return OpErrorFromOpMsg(cmdb.cmdbRequestWrapper(sfp.AutoRequestSelect, "functions.cmdb.api.crud", from, &payload, nil))
}

func (cmdb CMDBSyncClient) ObjectRelationDeleteWithDetails(from, to string) (easyjson.JSON, error) {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("operation.target", easyjson.NewJSON("object.relation"))
	payload.SetByPath("operation.type", easyjson.NewJSON("delete"))
	payload.SetByPath("data.to", easyjson.NewJSON(to))
	options := easyjson.NewJSONObjectWithKeyValue("op_stack", easyjson.NewJSON(true))
	msg := cmdb.cmdbRequestWrapper(sfp.AutoRequestSelect, "functions.cmdb.api.crud", from, &payload, &options)
	return msg.Data, OpErrorFromOpMsg(msg)
}

func (cmdb CMDBSyncClient) ObjectRelationRead(from, to string) (easyjson.JSON, error) {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("operation.target", easyjson.NewJSON("object.relation"))
	payload.SetByPath("operation.type", easyjson.NewJSON("read"))
	payload.SetByPath("data.to", easyjson.NewJSON(to))
	msg := cmdb.cmdbRequestWrapper(sfp.AutoRequestSelect, "functions.cmdb.api.crud", from, &payload, nil)
	return msg.Data, OpErrorFromOpMsg(msg)
}

// ------------------------------------------------------------------------------------------------
