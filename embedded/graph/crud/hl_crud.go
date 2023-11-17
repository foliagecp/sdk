package crud

import (
	"errors"
	"fmt"
	"strings"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/embedded/graph/common"
	sfplugins "github.com/foliagecp/sdk/statefun/plugins"
)

/*
	{
		"prefix": string, optional
		"body": json
	}

create types -> type link
*/
func CreateType(_ sfplugins.StatefunExecutor, contextProcessor *sfplugins.StatefunContextProcessor) {
	selfID := contextProcessor.Self.ID
	payload := contextProcessor.Payload

	prefix := payload.GetByPath("prefix").AsStringDefault("")

	_, err := contextProcessor.Request(sfplugins.GolangLocalRequest, "functions.graph.ll.api.object.create", selfID, payload, nil)
	if err != nil {
		replyError(contextProcessor, err)
		return
	}

	link := easyjson.NewJSONObject()
	link.SetByPath("descendant_uuid", easyjson.NewJSON(selfID))
	link.SetByPath("link_type", easyjson.NewJSON("__type"))
	link.SetByPath("link_body.tags", easyjson.JSONFromArray([]string{"TYPE_" + selfID}))

	_, err = contextProcessor.Request(sfplugins.GolangLocalRequest, "functions.graph.ll.api.link.create", prefix+"types", &link, nil)
	if err != nil {
		replyError(contextProcessor, err)
		return
	}

	replyOk(contextProcessor)
}

/*
	{
		"strategy": string, optional, default: DeepMerge
		"body": json
	}
*/
func UpdateType(_ sfplugins.StatefunExecutor, contextProcessor *sfplugins.StatefunContextProcessor) {
	selfID := contextProcessor.Self.ID

	payload := contextProcessor.Payload

	result, err := contextProcessor.Request(sfplugins.GolangLocalRequest, "functions.graph.ll.api.object.update", selfID, payload, nil)
	if err := checkRequestError(result, err); err != nil {
		replyError(contextProcessor, err)
		return
	}

	replyOk(contextProcessor)
}

func DeleteType(_ sfplugins.StatefunExecutor, contextProcessor *sfplugins.StatefunContextProcessor) {
	replyOk(contextProcessor)
}

/*
	{
		"prefix": string, optional,
		"origin_type": string,
		"body": json
	}

create objects -> object link

create type -> object link

create object -> type link

TODO: Add origin type check
*/
func CreateObject(_ sfplugins.StatefunExecutor, contextProcessor *sfplugins.StatefunContextProcessor) {
	selfID := contextProcessor.Self.ID
	payload := contextProcessor.Payload

	originType, ok := payload.GetByPath("origin_type").AsString()
	if !ok {
		return
	}

	prefix := payload.GetByPath("prefix").AsStringDefault("")

	result, err := contextProcessor.Request(sfplugins.GolangLocalRequest, "functions.graph.ll.api.object.create", selfID, payload, nil)
	if err := checkRequestError(result, err); err != nil {
		replyError(contextProcessor, err)
		return
	}

	type _link struct {
		from, to, lt string
	}

	needLinks := []_link{
		{from: prefix + "objects", to: selfID, lt: "__object"},
		{from: selfID, to: prefix + originType, lt: "__type"},
		{from: prefix + originType, to: selfID, lt: "__object"},
	}

	for _, l := range needLinks {
		link := easyjson.NewJSONObject()
		link.SetByPath("descendant_uuid", easyjson.NewJSON(l.to))
		link.SetByPath("link_type", easyjson.NewJSON(l.lt))
		link.SetByPath("link_body", easyjson.NewJSONObject())

		switch l.lt {
		case "__type":
			link.SetByPath("link_body.tags", easyjson.JSONFromArray([]string{"TYPE_" + l.to}))
		}

		result, err = contextProcessor.Request(sfplugins.GolangLocalRequest, "functions.graph.ll.api.link.create", l.from, &link, nil)
		if err := checkRequestError(result, err); err != nil {
			replyError(contextProcessor, err)
			return
		}
	}

	replyOk(contextProcessor)
}

/*
	{
		"mode": string, optional, default: merge
		"body": json
	}
*/
func UpdateObject(_ sfplugins.StatefunExecutor, contextProcessor *sfplugins.StatefunContextProcessor) {
	selfID := contextProcessor.Self.ID
	payload := contextProcessor.Payload

	result, err := contextProcessor.Request(sfplugins.GolangLocalRequest, "functions.graph.ll.api.object.update", selfID, payload, nil)
	if err := checkRequestError(result, err); err != nil {
		replyError(contextProcessor, err)
		return
	}

	replyOk(contextProcessor)
}

/*
	{
		"mode": "vertex" | "cascade", optional, default: vertex
	}
*/
func DeleteObject(_ sfplugins.StatefunExecutor, contextProcessor *sfplugins.StatefunContextProcessor) {
	selfID := contextProcessor.Self.ID
	payload := contextProcessor.Payload

	mode := payload.GetByPath("mode").AsStringDefault("vertex")
	switch mode {
	case "cascade":
		visited := map[string]struct{}{
			selfID: {},
		}
		queue := []string{selfID}

		for len(queue) > 0 {
			elem := queue[0]
			pattern := elem + ".out.ltp_oid-bdy.>"
			children := contextProcessor.GlobalCache.GetKeysByPattern(pattern)

			for _, v := range children {
				split := strings.Split(v, ".")
				if len(split) == 0 {
					continue
				}

				id := split[len(split)-1]

				if _, ok := visited[id]; ok {
					continue
				}

				visited[id] = struct{}{}
				queue = append(queue, id)
			}

			queue = queue[1:]
			if len(queue) == 0 {
				break
			}

			empty := easyjson.NewJSONObject()
			result, err := contextProcessor.Request(sfplugins.GolangLocalRequest, "functions.graph.ll.api.object.delete", elem, &empty, nil)
			if err := checkRequestError(result, err); err != nil {
				replyError(contextProcessor, err)
				return
			}
		}
	case "vertex":
	}

	replyOk(contextProcessor)
}

/*
	{
		"to": string,
		"object_link_type": string
		"body": json
	}

create type -> type link
*/
func CreateTypesLink(_ sfplugins.StatefunExecutor, contextProcessor *sfplugins.StatefunContextProcessor) {
	selfID := contextProcessor.Self.ID
	payload := contextProcessor.Payload

	objectLinkType, ok := payload.GetByPath("object_link_type").AsString()
	if !ok {
		return
	}

	to, ok := payload.GetByPath("to").AsString()
	if !ok {
		return
	}

	link := easyjson.NewJSONObject()
	link.SetByPath("descendant_uuid", easyjson.NewJSON(to))
	link.SetByPath("link_type", easyjson.NewJSON("__type"))
	link.SetByPath("link_body.link_type", easyjson.NewJSON(objectLinkType))
	link.SetByPath("link_body.tags", easyjson.JSONFromArray([]string{"TYPE_" + to}))

	result, err := contextProcessor.Request(sfplugins.GolangLocalRequest, "functions.graph.ll.api.link.create", selfID, &link, nil)
	if err := checkRequestError(result, err); err != nil {
		replyError(contextProcessor, err)
		return
	}

	replyOk(contextProcessor)
}

/*
	{
		"to": string,
		"object_link_type": string, optional
		"body": json, optional
	}

if object_link_type not empty
  - prepare link_body.link_type = object_link_type
  - after success updating, find all objects with certain types and change link_type
*/
func UpdateTypesLink(_ sfplugins.StatefunExecutor, contextProcessor *sfplugins.StatefunContextProcessor) {
	selfID := contextProcessor.Self.ID
	payload := contextProcessor.Payload

	to, ok := payload.GetByPath("to").AsString()
	if !ok {
		replyError(contextProcessor, errors.New("to undefined"))
		return
	}

	objectLinkType := payload.GetByPath("object_link_type").AsStringDefault("")
	body := payload.GetByPath("body")

	if objectLinkType == "" && !body.IsNonEmptyObject() {
		replyError(contextProcessor, errors.New("nothing to update"))
		return
	}

	updateLinkPayload := easyjson.NewJSONObject()
	updateLinkPayload.SetByPath("descendant_uuid", easyjson.NewJSON(to))
	updateLinkPayload.SetByPath("link_type", easyjson.NewJSON("__type"))
	updateLinkPayload.SetByPath("link_body", body)
	updateLinkPayload.SetByPath("link_body.tags", easyjson.JSONFromArray([]string{"TYPE_" + to}))

	needUpdateObjectLinkType := objectLinkType != ""
	currentObjectLinkType := ""

	if needUpdateObjectLinkType {
		updateLinkPayload.SetByPath("link_body.link_type", easyjson.NewJSON(objectLinkType))

		currentBody, err := getTypesLinkBody(contextProcessor, selfID, to)
		if err != nil {
			replyError(contextProcessor, err)
			return
		}

		currentObjectLinkType, _ = currentBody.GetByPath("link_body.link_type").AsString()
	}

	result, err := contextProcessor.Request(sfplugins.GolangLocalRequest, "functions.graph.ll.api.link.update", selfID, &updateLinkPayload, nil)
	if err := checkRequestError(result, err); err != nil {
		replyError(contextProcessor, err)
		return
	}

	// update link type of objects with certain types
	if needUpdateObjectLinkType {
		objects := findTypeObjects(contextProcessor, selfID)
		for _, objectID := range objects {
			pattern := fmt.Sprintf("%s.out.ltp_oid-bdy.%s.>", objectID, currentObjectLinkType)
			keys := contextProcessor.GlobalCache.GetKeysByPattern(pattern)

			for _, key := range keys {
				split := strings.Split(key, ".")
				toObjectID := split[len(split)-1]

				// update link_type
				updateObjectLinkPayload := easyjson.NewJSONObject()
				updateObjectLinkPayload.SetByPath("descendant_uuid", easyjson.NewJSON(toObjectID))
				updateObjectLinkPayload.SetByPath("link_type", easyjson.NewJSON(objectLinkType))
				updateObjectLinkPayload.SetByPath("link_body", easyjson.NewJSONObject())

				result, err := contextProcessor.Request(sfplugins.GolangLocalRequest, "functions.graph.ll.api.link.update", objectID, &updateObjectLinkPayload, nil)
				if err := checkRequestError(result, err); err != nil {
					replyError(contextProcessor, err)
					return
				}
			}
		}
	}

	replyOk(contextProcessor)
}

func DeleteTypesLink(_ sfplugins.StatefunExecutor, contextProcessor *sfplugins.StatefunContextProcessor) {
	replyOk(contextProcessor)
}

/*
	{
		"to": string,
		"body": json
	}

create object -> object link
*/
func CreateObjectsLink(_ sfplugins.StatefunExecutor, contextProcessor *sfplugins.StatefunContextProcessor) {
	selfID := contextProcessor.Self.ID
	payload := contextProcessor.Payload

	to, ok := payload.GetByPath("to").AsString()
	if !ok {
		replyError(contextProcessor, errors.New("to undefined"))
		return
	}

	selfType := findObjectType(contextProcessor, selfID)
	toType := findObjectType(contextProcessor, to)

	linkBody, err := getTypesLinkBody(contextProcessor, selfType, toType)
	if err != nil {
		replyError(contextProcessor, err)
		return
	}

	linkType, ok := linkBody.GetByPath("link_type").AsString()
	if !ok {
		return
	}

	objectLink := easyjson.NewJSONObject()
	objectLink.SetByPath("descendant_uuid", easyjson.NewJSON(to))
	objectLink.SetByPath("link_type", easyjson.NewJSON(linkType))
	objectLink.SetByPath("link_body", easyjson.NewJSONObject())

	result, err := contextProcessor.Request(sfplugins.GolangLocalRequest, "functions.graph.ll.api.link.create", selfID, &objectLink, nil)
	if err := checkRequestError(result, err); err != nil {
		replyError(contextProcessor, err)
		return
	}

	replyOk(contextProcessor)
}

/*
	{
		"to": string,
		"body": json
	}
*/
func UpdateObjectsLink(_ sfplugins.StatefunExecutor, contextProcessor *sfplugins.StatefunContextProcessor) {
	selfID := contextProcessor.Self.ID
	payload := contextProcessor.Payload

	objectToID, ok := payload.GetByPath("to").AsString()
	if !ok {
		return
	}

	fromTypeID := findObjectType(contextProcessor, selfID)
	toTypeID := findObjectType(contextProcessor, objectToID)

	linkBody, err := getTypesLinkBody(contextProcessor, fromTypeID, toTypeID)
	if err != nil {
		replyError(contextProcessor, err)
		return
	}

	linkType, ok := linkBody.GetByPath("link_type").AsString()
	if !ok {
		replyError(contextProcessor, err)
		return
	}

	objectLink := easyjson.NewJSONObject()
	objectLink.SetByPath("descendant_uuid", easyjson.NewJSON(objectToID))
	objectLink.SetByPath("link_type", easyjson.NewJSON(linkType))
	objectLink.SetByPath("link_body", payload.GetByPath("body"))

	result, err := contextProcessor.Request(sfplugins.GolangLocalRequest, "functions.graph.ll.api.link.update", selfID, &objectLink, nil)
	if err := checkRequestError(result, err); err != nil {
		replyError(contextProcessor, err)
		return
	}

	replyOk(contextProcessor)
}

func DeleteObejectsLink(_ sfplugins.StatefunExecutor, contextProcessor *sfplugins.StatefunContextProcessor) {
	replyOk(contextProcessor)
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

func findTypeObjects(ctx *sfplugins.StatefunContextProcessor, typeID string) []string {
	pattern := typeID + ".out.ltp_oid-bdy.__object.>"

	keys := ctx.GlobalCache.GetKeysByPattern(pattern)
	if len(keys) == 0 {
		return []string{}
	}

	out := make([]string, 0, len(keys))
	for _, v := range keys {
		split := strings.Split(v, ".")
		out = append(out, split[len(split)-1])
	}

	return out
}

func getTypesLinkBody(ctx *sfplugins.StatefunContextProcessor, from, to string) (*easyjson.JSON, error) {
	id := fmt.Sprintf("%s.out.ltp_oid-bdy.__type.%s", from, to)

	body, err := ctx.GlobalCache.GetValueAsJSON(id)
	if err != nil {
		return nil, fmt.Errorf("link %s, %s not found: %w", from, to, err)
	}

	return body, nil
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
	reply.SetByPath("result", easyjson.NewJSON(data))
	common.ReplyQueryID(qid, easyjson.NewJSONObjectWithKeyValue("payload", reply).GetPtr(), ctx)
}

func checkRequestError(result *easyjson.JSON, err error) error {
	if err != nil {
		return err
	}

	if result.GetByPath("status").AsStringDefault("failed") == "failed" {
		return errors.New(result.GetByPath("result").AsStringDefault("unknown error"))
	}

	return nil
}
