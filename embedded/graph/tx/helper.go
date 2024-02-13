package tx

import (
	"errors"
	"fmt"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/embedded/graph/common"
	"github.com/foliagecp/sdk/embedded/graph/crud"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
)

type beginTxType struct {
	Mode    string              `json:"mode"`
	Objects map[string]struct{} `json:"objects,omitempty"`
}

func cloneTypeFromMainGraphToTx(ctx *sfPlugins.StatefunContextProcessor, txID, src, dst string) error {
	originBody, err := ctx.GlobalCache.GetValueAsJSON(src)
	if err != nil {
		return err
	}

	createPayload := easyjson.NewJSONObject()
	createPayload.SetByPath("id", easyjson.NewJSON(dst))
	createPayload.SetByPath("body", *originBody)

	if _, err := ctx.Request(sfPlugins.AutoSelect, "functions.cmdb.tx.type.create", txID, &createPayload, nil); err != nil {
		return err
	}

	return nil
}

func cloneObjectFromMainGraphToTx(ctx *sfPlugins.StatefunContextProcessor, txID, src, dst, originType string) error {
	originBody, err := ctx.GlobalCache.GetValueAsJSON(src)
	if err != nil {
		return err
	}

	createPayload := easyjson.NewJSONObject()
	createPayload.SetByPath("id", easyjson.NewJSON(dst))
	createPayload.SetByPath("body", *originBody)
	createPayload.SetByPath("origin_type", easyjson.NewJSON(originType))

	if _, err := ctx.Request(sfPlugins.AutoSelect, "functions.cmdb.tx.object.create", txID, &createPayload, nil); err != nil {
		return err
	}

	return nil
}

func cloneLinkFromMainGraphToTx(ctx *sfPlugins.StatefunContextProcessor, originFrom, originLt, originTo, txFrom, txLt, txTo string) error {
	linkID := fmt.Sprintf(crud.OutLinkBodyKeyPrefPattern+crud.LinkKeySuff2Pattern, originFrom, originLt, originTo)
	originBody, err := ctx.GlobalCache.GetValueAsJSON(linkID)
	if err != nil {
		return err
	}

	if err := createLowLevelLink(ctx, txFrom, txTo, txLt, "", *originBody); err != nil {
		return err
	}

	return nil
}

func createLowLevelLink(ctx *sfPlugins.StatefunContextProcessor, from, to, lt, objectLt string, body easyjson.JSON) error {
	const op = "functions.graph.api.link.create"

	link := easyjson.NewJSONObject()
	link.SetByPath("descendant_uuid", easyjson.NewJSON(to))
	link.SetByPath("link_type", easyjson.NewJSON(lt))
	link.SetByPath("link_body", body)
	if objectLt != "" {
		link.SetByPath("link_body.link_type", easyjson.NewJSON(objectLt))
	}

	if _, err := ctx.Request(sfPlugins.AutoSelect, op, from, &link, nil); err != nil {
		return err
	}

	return nil
}

func updateLowLevelLink(ctx *sfPlugins.StatefunContextProcessor, from, to, lt string, body easyjson.JSON) error {
	const op = "functions.graph.api.link.update"

	link := easyjson.NewJSONObject()
	link.SetByPath("descendant_uuid", easyjson.NewJSON(to))
	link.SetByPath("link_type", easyjson.NewJSON(lt))
	link.SetByPath("link_body", body)

	if _, err := ctx.Request(sfPlugins.AutoSelect, op, from, &link, nil); err != nil {
		return err
	}

	return nil
}

func deleteLowLevelLink(ctx *sfPlugins.StatefunContextProcessor, from, to, linkType string) error {
	const op = "functions.graph.api.link.delete"

	payload := easyjson.NewJSONObject()
	payload.SetByPath("descendant_uuid", easyjson.NewJSON(to))
	payload.SetByPath("link_type", easyjson.NewJSON(linkType))

	if _, err := ctx.Request(sfPlugins.AutoSelect, op, from, &payload, nil); err != nil {
		return err
	}

	return nil
}

func createLowLevelObject(ctx *sfPlugins.StatefunContextProcessor, id string, body *easyjson.JSON) error {
	const op = "functions.graph.api.vertex.create"

	payload := easyjson.NewJSONObject()
	payload.SetByPath("body", *body)

	if _, err := ctx.Request(sfPlugins.AutoSelect, op, id, &payload, nil); err != nil {
		return err
	}

	return nil
}

func updateLowLevelObject(ctx *sfPlugins.StatefunContextProcessor, mode, id string, body *easyjson.JSON) error {
	const op = "functions.graph.api.vertex.update"

	payload := easyjson.NewJSONObject()
	payload.SetByPath("body", *body)
	payload.SetByPath("mode", easyjson.NewJSON(mode))

	if _, err := ctx.Request(sfPlugins.AutoSelect, op, id, &payload, nil); err != nil {
		return err
	}

	return nil
}

func deleteLowLevelObject(ctx *sfPlugins.StatefunContextProcessor, id string) error {
	const op = "functions.graph.api.vertex.delete"

	payload := easyjson.NewJSONObject()

	if _, err := ctx.Request(sfPlugins.AutoSelect, op, id, &payload, nil); err != nil {
		return err
	}

	return nil
}

func replyOk(ctx *sfPlugins.StatefunContextProcessor, msg ...string) {
	reply(ctx, "ok", msg)
}

func replyError(ctx *sfPlugins.StatefunContextProcessor, err error) {
	reply(ctx, "failed", err.Error())
}

func reply(ctx *sfPlugins.StatefunContextProcessor, status string, data any) {
	qid := common.GetQueryID(ctx)
	reply := easyjson.NewJSONObject()
	reply.SetByPath("status", easyjson.NewJSON(status))
	if data != nil {
		reply.SetByPath("result", easyjson.NewJSON(data))
	}
	common.ReplyQueryID(qid, easyjson.NewJSONObjectWithKeyValue("payload", reply).GetPtr(), ctx)
}

func checkRequestError(result *easyjson.JSON, err error) error {
	if err != nil {
		return err
	}

	if result.GetByPath("payload.status").AsStringDefault("failed") == "failed" {
		return errors.New(result.GetByPath("payload.result").AsStringDefault("unknown error"))
	}

	return nil
}

func generateDeletedMeta() easyjson.JSON {
	now := system.GetCurrentTimeNs()

	metaBody := easyjson.NewJSONObject()
	metaBody.SetByPath("status", easyjson.NewJSON("deleted"))
	metaBody.SetByPath("time", easyjson.NewJSON(now))

	return easyjson.NewJSONObjectWithKeyValue("__meta", metaBody)
}
