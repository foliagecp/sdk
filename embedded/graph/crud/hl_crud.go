package crud

import (
	"fmt"
	"strings"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/embedded/graph/common"
	sfplugins "github.com/foliagecp/sdk/statefun/plugins"
)

/*
	{
		"body": json
	}

create types -> type link
*/
func CreateType(executor sfplugins.StatefunExecutor, contextProcessor *sfplugins.StatefunContextProcessor) {
	selfID := contextProcessor.Self.ID
	payload := contextProcessor.Payload
	body := payload.GetByPath("body")
	qid := common.GetQueryID(contextProcessor)

	_, err := contextProcessor.GolangCallSync("functions.graph.ll.api.object.create", selfID, &body, nil)
	if err != nil {
		reply := easyjson.NewJSONObject()
		reply.SetByPath("status", easyjson.NewJSON("failed"))
		reply.SetByPath("result", easyjson.NewJSON(err.Error()))
		common.ReplyQueryID(qid, easyjson.NewJSONObjectWithKeyValue("payload", reply).GetPtr(), contextProcessor)
		return
	}

	link := easyjson.NewJSONObject()
	link.SetByPath("descendant_uuid", easyjson.NewJSON(selfID))
	link.SetByPath("link_type", easyjson.NewJSON("__type"))
	link.SetByPath("link_body", easyjson.NewJSONObject())

	_, err = contextProcessor.GolangCallSync("functions.graph.ll.api.link.create", "types", &link, nil)
	if err != nil {
		reply := easyjson.NewJSONObject()
		reply.SetByPath("status", easyjson.NewJSON("failed"))
		reply.SetByPath("result", easyjson.NewJSON(err.Error()))
		common.ReplyQueryID(qid, easyjson.NewJSONObjectWithKeyValue("payload", reply).GetPtr(), contextProcessor)
		return
	}

	reply := easyjson.NewJSONObject()
	reply.SetByPath("status", easyjson.NewJSON("ok"))
	common.ReplyQueryID(qid, easyjson.NewJSONObjectWithKeyValue("payload", reply).GetPtr(), contextProcessor)
}

/*
	{
		"originType": string,
		"body": json
	}

create objects -> object link

create type -> object link

create object -> type link
*/
func CreateObject(executor sfplugins.StatefunExecutor, contextProcessor *sfplugins.StatefunContextProcessor) {
	selfID := contextProcessor.Self.ID
	payload := contextProcessor.Payload

	originType, ok := payload.GetByPath("originType").AsString()
	if !ok {
		return
	}

	body := payload.GetByPath("body")
	qid := common.GetQueryID(contextProcessor)

	_, err := contextProcessor.GolangCallSync("functions.graph.ll.api.object.create", selfID, &body, nil)
	if err != nil {
		reply := easyjson.NewJSONObject()
		reply.SetByPath("status", easyjson.NewJSON("failed"))
		reply.SetByPath("result", easyjson.NewJSON(err.Error()))
		common.ReplyQueryID(qid, easyjson.NewJSONObjectWithKeyValue("payload", reply).GetPtr(), contextProcessor)
		return
	}

	type _link struct {
		from, to, lt string
	}

	needLinks := []_link{
		{from: "objects", to: selfID, lt: "__object"},
		{from: selfID, to: originType, lt: "__type"},
		{from: originType, to: selfID, lt: "__object"},
	}

	for _, l := range needLinks {
		link := easyjson.NewJSONObject()
		link.SetByPath("descendant_uuid", easyjson.NewJSON(l.to))
		link.SetByPath("link_type", easyjson.NewJSON(l.lt))
		link.SetByPath("link_body", easyjson.NewJSONObject())

		_, err = contextProcessor.GolangCallSync("functions.graph.ll.api.link.create", l.from, &link, nil)
		if err != nil {
			reply := easyjson.NewJSONObject()
			reply.SetByPath("status", easyjson.NewJSON("failed"))
			reply.SetByPath("result", easyjson.NewJSON(err.Error()))
			common.ReplyQueryID(qid, easyjson.NewJSONObjectWithKeyValue("payload", reply).GetPtr(), contextProcessor)
			return
		}
	}

	reply := easyjson.NewJSONObject()
	reply.SetByPath("status", easyjson.NewJSON("ok"))
	common.ReplyQueryID(qid, easyjson.NewJSONObjectWithKeyValue("payload", reply).GetPtr(), contextProcessor)
}

/*
	{
		"to": string,
		"objectLinkType": string
	}

create type -> type link
*/
func CreateTypesLink(executor sfplugins.StatefunExecutor, contextProcessor *sfplugins.StatefunContextProcessor) {
	selfID := contextProcessor.Self.ID
	payload := contextProcessor.Payload

	objectLinkType, ok := payload.GetByPath("objectLinkType").AsString()
	if !ok {
		return
	}

	to, ok := payload.GetByPath("to").AsString()
	if !ok {
		return
	}

	qid := common.GetQueryID(contextProcessor)

	link := easyjson.NewJSONObject()
	link.SetByPath("descendant_uuid", easyjson.NewJSON(to))
	link.SetByPath("link_type", easyjson.NewJSON(to))
	link.SetByPath("link_body.link_type", easyjson.NewJSON(objectLinkType))

	_, err := contextProcessor.GolangCallSync("functions.graph.ll.api.link.create", selfID, &link, nil)
	if err != nil {
		reply := easyjson.NewJSONObject()
		reply.SetByPath("status", easyjson.NewJSON("failed"))
		reply.SetByPath("result", easyjson.NewJSON(err.Error()))
		common.ReplyQueryID(qid, easyjson.NewJSONObjectWithKeyValue("payload", reply).GetPtr(), contextProcessor)
		return
	}

	reply := easyjson.NewJSONObject()
	reply.SetByPath("status", easyjson.NewJSON("ok"))
	common.ReplyQueryID(qid, easyjson.NewJSONObjectWithKeyValue("payload", reply).GetPtr(), contextProcessor)
}

/*
	{
		"to": string,
	}

create object -> object link
*/
func CreateObjectsLink(executor sfplugins.StatefunExecutor, contextProcessor *sfplugins.StatefunContextProcessor) {
	selfID := contextProcessor.Self.ID
	payload := contextProcessor.Payload

	to, ok := payload.GetByPath("to").AsString()
	if !ok {
		return
	}

	selfType := findObjectType(contextProcessor, selfID)
	toType := findObjectType(contextProcessor, to)

	linkBody, err := getLinkBody(contextProcessor, selfType, toType)
	if err != nil {
		return
	}

	linkType, ok := linkBody.GetByPath("link_type").AsString()
	if !ok {
		return
	}

	qid := common.GetQueryID(contextProcessor)

	objectLink := easyjson.NewJSONObject()
	objectLink.SetByPath("descendant_uuid", easyjson.NewJSON(to))
	objectLink.SetByPath("link_type", easyjson.NewJSON(linkType))
	objectLink.SetByPath("link_body", easyjson.NewJSONObject())

	_, err = contextProcessor.GolangCallSync("functions.graph.ll.api.link.create", selfID, &objectLink, nil)
	if err != nil {
		reply := easyjson.NewJSONObject()
		reply.SetByPath("status", easyjson.NewJSON("failed"))
		reply.SetByPath("result", easyjson.NewJSON(err.Error()))
		common.ReplyQueryID(qid, easyjson.NewJSONObjectWithKeyValue("payload", reply).GetPtr(), contextProcessor)
		return
	}

	reply := easyjson.NewJSONObject()
	reply.SetByPath("status", easyjson.NewJSON("ok"))
	common.ReplyQueryID(qid, easyjson.NewJSONObjectWithKeyValue("payload", reply).GetPtr(), contextProcessor)
}

func findObjectType(ctx *sfplugins.StatefunContextProcessor, id string) string {
	pattern := id + ".out.ltp_oid-bdy.__type.>"
	keys := ctx.GlobalCache.GetKeysByPattern(pattern)
	if len(keys) == 0 {
		return ""
	}

	split := strings.Split(keys[0], ".")

	return split[len(split)-1]
}

func getLinkBody(ctx *sfplugins.StatefunContextProcessor, from, to string) (*easyjson.JSON, error) {
	id := fmt.Sprintf("%s.out.ltp_oid-bdy.%s.%s", from, to, to)
	return ctx.GlobalCache.GetValueAsJSON(id)
}
