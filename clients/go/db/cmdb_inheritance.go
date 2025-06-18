package db

import (
	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/statefun"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfp "github.com/foliagecp/sdk/statefun/plugins"
)

func (cmdb CMDBSyncClient) TypeAddChild(baseType, childType string) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("child_type", easyjson.NewJSON(childType))

	options := easyjson.NewJSONObject()
	options.SetByPath(statefun.ShadowObjectCallParamOptionPath, easyjson.NewJSON(cmdb.ShadowObjectCanBeRecevier))
	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(cmdb.request(sfp.AutoRequestSelect, "functions.cmdb.api.inherit.type.add.child", baseType, &payload, &options)))
}

func (cmdb CMDBSyncClient) TypeRemoveChild(baseType, childType string) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("child_type", easyjson.NewJSON(childType))

	options := easyjson.NewJSONObject()
	options.SetByPath(statefun.ShadowObjectCallParamOptionPath, easyjson.NewJSON(cmdb.ShadowObjectCanBeRecevier))
	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(cmdb.request(sfp.AutoRequestSelect, "functions.cmdb.api.inherit.type.remove.child", baseType, &payload, &options)))
}

func (cmdb CMDBSyncClient) ObjectsLinkFromClaimedTypesCreate(from, to, fromClaimType, toClaimType, name string, tags []string, body ...easyjson.JSON) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("to", easyjson.NewJSON(to))
	payload.SetByPath("from_claim_type", easyjson.NewJSON(fromClaimType))
	payload.SetByPath("to_claim_type", easyjson.NewJSON(toClaimType))
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
	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(cmdb.request(sfp.AutoRequestSelect, "functions.cmdb.api.inherit.objects.link.create", from, &payload, &options)))
}

func (cmdb CMDBSyncClient) ObjectsLinkFromClaimedTypesDelete(from, to, fromClaimType, toClaimType string) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("to", easyjson.NewJSON(to))
	payload.SetByPath("from_claim_type", easyjson.NewJSON(fromClaimType))
	payload.SetByPath("to_claim_type", easyjson.NewJSON(toClaimType))

	options := easyjson.NewJSONObject()
	options.SetByPath(statefun.ShadowObjectCallParamOptionPath, easyjson.NewJSON(cmdb.ShadowObjectCanBeRecevier))
	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(cmdb.request(sfp.AutoRequestSelect, "functions.cmdb.api.inherit.objects.link.delete", from, &payload, &options)))
}
