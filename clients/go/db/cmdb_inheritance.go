package db

import (
	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/statefun"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfp "github.com/foliagecp/sdk/statefun/plugins"
)

func (cmdb CMDBSyncClient) TypeSetChild(baseType, childType string) error {
	payload := easyjson.NewJSONObject()
	payload.SetByPath("child_type", easyjson.NewJSON(childType))

	options := easyjson.NewJSONObject()
	options.SetByPath(statefun.ShadowObjectCallParamOptionPath, easyjson.NewJSON(cmdb.ShadowObjectCanBeRecevier))
	return OpErrorFromOpMsg(sfMediators.OpMsgFromSfReply(cmdb.request(sfp.AutoRequestSelect, "functions.cmdb.api.inherit.type.child", baseType, &payload, &options)))
}
