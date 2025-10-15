package db

import (
	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/statefun"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfp "github.com/foliagecp/sdk/statefun/plugins"
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
	payload.SetByPath("to", easyjson.NewJSON(to))
	payload.SetByPath("from_super_type", easyjson.NewJSON(fromClaimType))
	payload.SetByPath("to_super_type", easyjson.NewJSON(toClaimType))
	payload.SetByPath("name", easyjson.NewJSON(name))
	payload.SetByPath("body", easyjson.NewJSONObject())

	if len(body) > 0 {
		payload.SetByPath("body", body[0])
	}
	if len(tags) > 0 {
		payload.SetByPath("tags", easyjson.NewJSON(tags))
	}

	options := easyjson.NewJSONObject()
	options.SetByPath(statefun.ShadowObjectCallParamOptionPath, easyjson.NewJSON(cmdb.ShadowObjectCanBeRecevier))
	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(cmdb.request(sfp.AutoRequestSelect, "functions.cmdb.api.objects.link.supertype.create", from, &payload, &options)))
}

func (cmdb CMDBSyncClient) ObjectsLinkSuperTypeDelete(from, to, fromClaimType, toClaimType string) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("to", easyjson.NewJSON(to))
	payload.SetByPath("from_super_type", easyjson.NewJSON(fromClaimType))
	payload.SetByPath("to_super_type", easyjson.NewJSON(toClaimType))

	options := easyjson.NewJSONObject()
	options.SetByPath(statefun.ShadowObjectCallParamOptionPath, easyjson.NewJSON(cmdb.ShadowObjectCanBeRecevier))
	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(cmdb.request(sfp.AutoRequestSelect, "functions.cmdb.api.objects.link.supertype.delete", from, &payload, &options)))
}
