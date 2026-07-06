package db

import (
	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/statefun"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfp "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
)

func (cmdb CMDBSyncClient) TypeSetSubType(baseType, childType string) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("sub_type", easyjson.NewJSON(childType))

	options := easyjson.NewJSONObject()
	options.SetByPath(statefun.ShadowObjectCallParamOptionPath, easyjson.NewJSON(cmdb.ShadowObjectCanBeRecevier))
	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(cmdb.request(sfp.AutoRequestSelect, "functions.cmdb.api.type.subtype.set", baseType, &payload, &options)))
}

func (cmdb CMDBSyncClient) TypeRemoveSubType(baseType, childType string) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("sub_type", easyjson.NewJSON(childType))

	options := easyjson.NewJSONObject()
	options.SetByPath(statefun.ShadowObjectCallParamOptionPath, easyjson.NewJSON(cmdb.ShadowObjectCanBeRecevier))
	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(cmdb.request(sfp.AutoRequestSelect, "functions.cmdb.api.type.subtype.remove", baseType, &payload, &options)))
}

func (cmdb CMDBSyncClient) ObjectsLinkSuperTypeCreate(from, to, fromClaimType, toClaimType, name string, tags []string, body ...easyjson.JSON) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("op_time", easyjson.NewJSON(system.GetCurrentTimeNs()))
	payload.SetByPath("to", easyjson.NewJSON(to))
	payload.SetByPath("from_super_type", easyjson.NewJSON(fromClaimType))
	payload.SetByPath("to_super_type", easyjson.NewJSON(toClaimType))
	payload.SetByPath("body", easyjson.NewJSONObject())

	if len(name) > 0 {
		payload.SetByPath("name", easyjson.NewJSON(name))
	}
	if len(body) > 0 {
		payload.SetByPath("body", body[0])
	}
	if len(tags) > 0 {
		payload.SetByPath("tags", easyjson.NewJSON(tags))
	}

	options := easyjson.NewJSONObject()
	options.SetByPath(statefun.ShadowObjectCallParamOptionPath, easyjson.NewJSON(cmdb.ShadowObjectCanBeRecevier))
	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(cmdb.request(sfp.AutoRequestSelect, "functions.cmdb.api.objects.link.supertype.create", SeqFree(from), &payload, &options)))
}

func (cmdb CMDBSyncClient) ObjectsLinkSuperTypeUpdate(from, to, fromClaimType, toClaimType, name string, tags []string, body easyjson.JSON, replace bool) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("op_time", easyjson.NewJSON(system.GetCurrentTimeNs()))
	payload.SetByPath("to", easyjson.NewJSON(to))
	payload.SetByPath("from_super_type", easyjson.NewJSON(fromClaimType))
	payload.SetByPath("to_super_type", easyjson.NewJSON(toClaimType))
	payload.SetByPath("body", body)
	payload.SetByPath("replace", easyjson.NewJSON(replace))
	if len(name) > 0 {
		payload.SetByPath("name", easyjson.NewJSON(name))
	}
	if len(tags) > 0 {
		payload.SetByPath("tags", easyjson.NewJSON(tags))
	}

	options := easyjson.NewJSONObject()
	options.SetByPath(statefun.ShadowObjectCallParamOptionPath, easyjson.NewJSON(cmdb.ShadowObjectCanBeRecevier))
	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(cmdb.request(sfp.AutoRequestSelect, "functions.cmdb.api.objects.link.supertype.update", SeqFree(from), &payload, &options)))
}

func (cmdb CMDBSyncClient) ObjectsLinkSuperTypeDelete(from, to, fromClaimType, toClaimType string) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("op_time", easyjson.NewJSON(system.GetCurrentTimeNs()))
	payload.SetByPath("to", easyjson.NewJSON(to))
	payload.SetByPath("from_super_type", easyjson.NewJSON(fromClaimType))
	payload.SetByPath("to_super_type", easyjson.NewJSON(toClaimType))

	options := easyjson.NewJSONObject()
	options.SetByPath(statefun.ShadowObjectCallParamOptionPath, easyjson.NewJSON(cmdb.ShadowObjectCanBeRecevier))
	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(cmdb.request(sfp.AutoRequestSelect, "functions.cmdb.api.objects.link.supertype.delete", SeqFree(from), &payload, &options)))
}
