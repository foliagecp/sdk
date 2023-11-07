package tx

import (
	"errors"
	"fmt"
	"strings"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/embedded/graph/common"
	sfplugins "github.com/foliagecp/sdk/statefun/plugins"
)

func cloneTypeFromMainGraphToTx(ctx *sfplugins.StatefunContextProcessor, txID, src, dst string) error {
	originBody, err := ctx.GlobalCache.GetValueAsJSON(src)
	if err != nil {
		return err
	}

	createPayload := easyjson.NewJSONObject()
	createPayload.SetByPath("id", easyjson.NewJSON(dst))
	createPayload.SetByPath("body", *originBody)

	if _, err := ctx.Request(sfplugins.GolangLocalRequest, "functions.graph.tx.type.create", txID, &createPayload, nil); err != nil {
		return err
	}

	return nil
}

func cloneObjectFromMainGraphToTx(ctx *sfplugins.StatefunContextProcessor, txID, src, dst, originType string) error {
	originBody, err := ctx.GlobalCache.GetValueAsJSON(src)
	if err != nil {
		return err
	}

	createPayload := easyjson.NewJSONObject()
	createPayload.SetByPath("id", easyjson.NewJSON(dst))
	createPayload.SetByPath("body", *originBody)
	createPayload.SetByPath("origin_type", easyjson.NewJSON(originType))

	if _, err := ctx.Request(sfplugins.GolangLocalRequest, "functions.graph.tx.object.create", txID, &createPayload, nil); err != nil {
		return err
	}

	return nil
}

func cloneLinkFromMainGraphToTx(ctx *sfplugins.StatefunContextProcessor, originFrom, originLt, originTo, txFrom, txLt, txTo string) error {
	linkID := fmt.Sprintf("%s.out.ltp_oid-bdy.%s.%s", originFrom, originLt, originTo)
	originBody, err := ctx.GlobalCache.GetValueAsJSON(linkID)
	if err != nil {
		return err
	}

	if err := createLowLevelLink(ctx, txFrom, txTo, txLt, "", *originBody); err != nil {
		return err
	}

	return nil
}

func createLowLevelLink(ctx *sfplugins.StatefunContextProcessor, from, to, lt, objectLt string, body easyjson.JSON) error {
	const op = "functions.graph.ll.api.link.create"

	link := easyjson.NewJSONObject()
	link.SetByPath("descendant_uuid", easyjson.NewJSON(to))
	link.SetByPath("link_type", easyjson.NewJSON(lt))
	link.SetByPath("link_body", body)
	if objectLt != "" {
		link.SetByPath("link_body.link_type", easyjson.NewJSON(objectLt))
	}

	if _, err := ctx.Request(sfplugins.GolangLocalRequest, op, from, &link, nil); err != nil {
		return err
	}

	return nil
}

func updateLowLevelLink(ctx *sfplugins.StatefunContextProcessor, from, to, lt string, body easyjson.JSON) error {
	const op = "functions.graph.ll.api.link.update"

	link := easyjson.NewJSONObject()
	link.SetByPath("descendant_uuid", easyjson.NewJSON(to))
	link.SetByPath("link_type", easyjson.NewJSON(lt))
	link.SetByPath("link_body", body)

	if _, err := ctx.Request(sfplugins.GolangLocalRequest, op, from, &link, nil); err != nil {
		return err
	}

	return nil
}

func createLowLevelObject(ctx *sfplugins.StatefunContextProcessor, id string, body *easyjson.JSON) error {
	const op = "functions.graph.ll.api.object.create"

	payload := easyjson.NewJSONObject()
	payload.SetByPath("body", *body)

	if _, err := ctx.Request(sfplugins.GolangLocalRequest, op, id, body, nil); err != nil {
		return err
	}

	return nil
}

func updateLowLevelObject(ctx *sfplugins.StatefunContextProcessor, mode, id string, body *easyjson.JSON) error {
	const op = "functions.graph.ll.api.object.update"

	payload := easyjson.NewJSONObject()
	payload.SetByPath("body", *body)
	payload.SetByPath("mode", easyjson.NewJSON(mode))

	if _, err := ctx.Request(sfplugins.GolangLocalRequest, op, id, body, nil); err != nil {
		return err
	}

	return nil
}

func replyOk(ctx *sfplugins.StatefunContextProcessor, msg ...string) {
	reply(ctx, "ok", msg)
}

func replyError(ctx *sfplugins.StatefunContextProcessor, err error) {
	reply(ctx, "failed", err.Error())
}

func reply(ctx *sfplugins.StatefunContextProcessor, status string, data any) {
	qid := common.GetQueryID(ctx)
	reply := easyjson.NewJSONObject()
	reply.SetByPath("status", easyjson.NewJSON(status))
	if data != nil {
		reply.SetByPath("result", easyjson.NewJSON(data))
	}
	common.ReplyQueryID(qid, easyjson.NewJSONObjectWithKeyValue("payload", reply).GetPtr(), ctx)
}

func findObjectType(ctx *sfplugins.StatefunContextProcessor, objectID string) string {
	pattern := objectID + ".out.ltp_oid-bdy.__type.>"

	keys := ctx.GlobalCache.GetKeysByPattern(pattern)
	if len(keys) == 0 {
		return ""
	}

	split := strings.Split(keys[0], ".")

	return split[len(split)-1]
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
