package tx

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/embedded/graph/common"
	"github.com/foliagecp/sdk/statefun"
	sfplugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
)

const _TX_MASTER = "txmaster"
const _TX_SEPARATOR = "="

const (
	OBJECTS_TYPELINK         = "__objects"
	TYPES_TYPELINK           = "__types"
	TYPE_TYPELINK            = "__type"
	OBJECT_TYPELINK          = "__object"
	OBJECT_2_OBJECT_TYPELINK = "obj"
	BUILT_IN_TYPES           = "types"
	BUILT_IN_OBJECTS         = "objects"
	BUILT_IN_ROOT            = "root"
	BUILT_IN_GROUP           = "group"
	BUILT_IN_NAV             = "nav"
	GROUP_TYPELINK           = "group"
)

var (
	errInvalidArgument = errors.New("invalid argument")
)

func RegisterAllFunctionTypes(runtime *statefun.Runtime) {
	statefun.NewFunctionType(runtime, "functions.graph.tx.begin", Begin, *statefun.NewFunctionTypeConfig().SetServiceState(true))

	statefun.NewFunctionType(runtime, "functions.graph.tx.type.create", CreateType, *statefun.NewFunctionTypeConfig().SetServiceState(true))
	statefun.NewFunctionType(runtime, "functions.graph.tx.type.update", UpdateType, *statefun.NewFunctionTypeConfig().SetServiceState(true))
	statefun.NewFunctionType(runtime, "functions.graph.tx.type.delete", nil, *statefun.NewFunctionTypeConfig().SetServiceState(true))

	statefun.NewFunctionType(runtime, "functions.graph.tx.object.create", CreateObject, *statefun.NewFunctionTypeConfig().SetServiceState(true))
	statefun.NewFunctionType(runtime, "functions.graph.tx.object.update", UpdateObject, *statefun.NewFunctionTypeConfig().SetServiceState(true))
	statefun.NewFunctionType(runtime, "functions.graph.tx.object.delete", nil, *statefun.NewFunctionTypeConfig().SetServiceState(true))

	statefun.NewFunctionType(runtime, "functions.graph.tx.types.link.create", CreateTypesLink, *statefun.NewFunctionTypeConfig().SetServiceState(true))
	statefun.NewFunctionType(runtime, "functions.graph.tx.types.link.update", UpdateTypesLink, *statefun.NewFunctionTypeConfig().SetServiceState(true))
	statefun.NewFunctionType(runtime, "functions.graph.tx.types.link.delete", nil, *statefun.NewFunctionTypeConfig().SetServiceState(true))

	statefun.NewFunctionType(runtime, "functions.graph.tx.objects.link.create", CreateObjectsLink, *statefun.NewFunctionTypeConfig().SetServiceState(true))
	statefun.NewFunctionType(runtime, "functions.graph.tx.objects.link.update", UpdateObjectsLink, *statefun.NewFunctionTypeConfig().SetServiceState(true))
	statefun.NewFunctionType(runtime, "functions.graph.tx.objects.link.delete", nil, *statefun.NewFunctionTypeConfig().SetServiceState(true))

	statefun.NewFunctionType(runtime, "functions.graph.tx.commit", Commit, *statefun.NewFunctionTypeConfig().SetServiceState(true))
	statefun.NewFunctionType(runtime, "functions.graph.tx.push", Push, *statefun.NewFunctionTypeConfig().SetServiceState(true))
}

// exec only on txmaster
// create tx_id, clone exist graph with tx_id prefix, return tx_id to client
// tx_id = sha256(txmaster + nonce.String() + unixnano.String()).String()

/*
	payload:{
		"clone": "min" | "full" | "with_types", optional, default: full
		"types": []string, only with "clone":"with_types"
	}
*/
func Begin(_ sfplugins.StatefunExecutor, contextProcessor *sfplugins.StatefunContextProcessor) {
	selfID := contextProcessor.Self.ID
	if selfID != _TX_MASTER {
		replyError(contextProcessor, errors.New("only on txmaster"))
		return
	}

	payload := contextProcessor.Payload
	cloneMod := payload.GetByPath("clone").AsStringDefault("full")
	cloneWithTypes, _ := payload.GetByPath("types").AsArrayString()

	body := contextProcessor.GetObjectContext()

	nonce := int(body.GetByPath("nonce").AsNumericDefault(0))
	nonce++
	body.SetByPath("nonce", easyjson.NewJSON(nonce))

	contextProcessor.SetObjectContext(body)

	now := system.GetCurrentTimeNs()

	// create tx
	txID := generateTxID(nonce, now)

	txBody := easyjson.NewJSONObject()
	txBody.SetByPath("created_at", easyjson.NewJSON(now))

	if _, err := contextProcessor.Request(sfplugins.GolangLocalRequest, "functions.graph.ll.api.object.create", txID, &txBody, nil); err != nil {
		replyError(contextProcessor, err)
		return
	}

	if err := createLowLevelLink(contextProcessor, selfID, txID, "tx", "", easyjson.NewJSONObject()); err != nil {
		replyError(contextProcessor, err)
		return
	}

	if err := cloneGraph(contextProcessor, txID, cloneMod, cloneWithTypes...); err != nil {
		replyError(contextProcessor, err)
		return
	}

	qid := common.GetQueryID(contextProcessor)

	reply := easyjson.NewJSONObject()
	reply.SetByPath("status", easyjson.NewJSON("ok"))
	reply.SetByPath("id", easyjson.NewJSON(txID))
	common.ReplyQueryID(qid, easyjson.NewJSONObjectWithKeyValue("payload", reply).GetPtr(), contextProcessor)
}

/*
	payload:{
		"mode": "merge" | "replace", optional, default: "merge"
	}
*/
// exec on transaction
func Commit(_ sfplugins.StatefunExecutor, contextProcessor *sfplugins.StatefunContextProcessor) {
	// add validating stage
	mode := contextProcessor.Payload.GetByPath("mode").AsStringDefault("merge")
	payload := easyjson.NewJSONObjectWithKeyValue("mode", easyjson.NewJSON(mode))

	system.MsgOnErrorReturn(contextProcessor.Request(sfplugins.GolangLocalRequest, "functions.graph.tx.push", _TX_MASTER, payload.GetPtr(), nil))

	qid := common.GetQueryID(contextProcessor)
	reply := easyjson.NewJSONObject()
	reply.SetByPath("status", easyjson.NewJSON("ok"))
	common.ReplyQueryID(qid, easyjson.NewJSONObjectWithKeyValue("payload", reply).GetPtr(), contextProcessor)
}

/*
	payload:{
		"mode": "merge" | "replace", optional, default: "merge"
	}
*/
func Push(_ sfplugins.StatefunExecutor, contextProcessor *sfplugins.StatefunContextProcessor) {
	selfID := contextProcessor.Self.ID
	if selfID != _TX_MASTER {
		return
	}

	mode := contextProcessor.Payload.GetByPath("mode").AsStringDefault("merge")

	// TODO: check tx id
	txID := contextProcessor.Caller.ID

	if err := merge(contextProcessor, txID, mode); err != nil {
		replyError(contextProcessor, err)
		return
	}

	// delete success tx
	if _, err := contextProcessor.Request(sfplugins.GolangLocalRequest, "functions.graph.ll.api.object.delete", txID, easyjson.NewJSONObject().GetPtr(), nil); err != nil {
		replyError(contextProcessor, err)
		return
	}

	fmt.Println("[INFO] Merge Done!")

	replyOk(contextProcessor)
}

/*
	{
		"id": string,
		"body": json
	}

create types -> type link
*/
func CreateType(_ sfplugins.StatefunExecutor, contextProcessor *sfplugins.StatefunContextProcessor) {
	txID := contextProcessor.Self.ID
	payload := contextProcessor.Payload

	prefix := generatePrefix(txID)

	typeID := payload.GetByPath("id").AsStringDefault("")
	txTypeID := prefix + typeID

	createTypePayload := easyjson.NewJSONObject()
	createTypePayload.SetByPath("prefix", easyjson.NewJSON(prefix))
	createTypePayload.SetByPath("body", payload.GetByPath("body"))

	result, err := contextProcessor.Request(sfplugins.GolangLocalRequest, "functions.graph.api.type.create", txTypeID, &createTypePayload, nil)
	if err := checkRequestError(result, err); err != nil {
		replyError(contextProcessor, err)
		return
	}

	replyOk(contextProcessor)
}

/*
	{
		"id": string,
		"body": json
		"strategy": string, not impl
	}

clone type from main graph if not exists

update type body
*/
func UpdateType(_ sfplugins.StatefunExecutor, contextProcessor *sfplugins.StatefunContextProcessor) {
	txID := contextProcessor.Self.ID
	payload := contextProcessor.Payload

	typeID, ok := payload.GetByPath("id").AsString()
	if !ok {
		replyError(contextProcessor, errInvalidArgument)
		return
	}

	prefix := generatePrefix(txID)

	txTypes := prefix + BUILT_IN_TYPES
	txType := prefix + typeID

	pattern := fmt.Sprintf("%s.out.ltp_oid-bdy.%s.%s", txTypes, TYPE_TYPELINK, txType)
	keys := contextProcessor.GlobalCache.GetKeysByPattern(pattern)

	// tx type doesn't created yet
	if len(keys) == 0 {
		originBody, err := contextProcessor.GlobalCache.GetValueAsJSON(typeID)
		if err != nil {
			replyError(contextProcessor, err)
			return
		}

		createPayload := easyjson.NewJSONObject()
		createPayload.SetByPath("id", easyjson.NewJSON(typeID))
		createPayload.SetByPath("body", *originBody)

		result, err := contextProcessor.Request(sfplugins.GolangLocalRequest, "functions.graph.tx.type.create", txID, &createPayload, nil)
		if err := checkRequestError(result, err); err != nil {
			replyError(contextProcessor, err)
			return
		}
	}

	updatePayload := easyjson.NewJSONObject()
	updatePayload.SetByPath("body", payload.GetByPath("body"))

	result, err := contextProcessor.Request(sfplugins.GolangLocalRequest, "functions.graph.api.type.update", txType, &updatePayload, nil)
	if err := checkRequestError(result, err); err != nil {
		replyError(contextProcessor, err)
		return
	}

	replyOk(contextProcessor)
}

/*
	{
		"id": string,
		"origin_type": string,
		"body": json
	}

create objects -> object link

create type -> object link

create object -> type link
*/
func CreateObject(_ sfplugins.StatefunExecutor, contextProcessor *sfplugins.StatefunContextProcessor) {
	txID := contextProcessor.Self.ID
	payload := contextProcessor.Payload

	prefix := generatePrefix(txID)

	objID := payload.GetByPath("id").AsStringDefault("")
	txObjID := prefix + objID

	createObjPayload := easyjson.NewJSONObject()
	createObjPayload.SetByPath("prefix", easyjson.NewJSON(prefix))
	createObjPayload.SetByPath("origin_type", payload.GetByPath("origin_type"))
	createObjPayload.SetByPath("body", payload.GetByPath("body"))

	result, err := contextProcessor.Request(sfplugins.GolangLocalRequest, "functions.graph.api.object.create", txObjID, &createObjPayload, nil)
	if err := checkRequestError(result, err); err != nil {
		replyError(contextProcessor, err)
		return
	}

	replyOk(contextProcessor)
}

/*
	{
		"id": string,
		"body": json
		"strategy": string, not impl
	}

clone object from main graph if not exists

update object body
*/
func UpdateObject(_ sfplugins.StatefunExecutor, contextProcessor *sfplugins.StatefunContextProcessor) {
	txID := contextProcessor.Self.ID
	payload := contextProcessor.Payload

	objectID, ok := payload.GetByPath("id").AsString()
	if !ok {
		replyError(contextProcessor, errInvalidArgument)
		return
	}

	prefix := generatePrefix(txID)

	txObjects := prefix + BUILT_IN_OBJECTS
	txObject := prefix + objectID

	pattern := fmt.Sprintf("%s.out.ltp_oid-bdy.%s.%s", txObjects, OBJECT_TYPELINK, txObject)
	keys := contextProcessor.GlobalCache.GetKeysByPattern(pattern)

	// tx object doesn't created yet
	if len(keys) == 0 {
		originBody, err := contextProcessor.GlobalCache.GetValueAsJSON(objectID)
		if err != nil {
			replyError(contextProcessor, err)
			return
		}

		linkPattern := fmt.Sprintf("%s.out.ltp_oid-bdy.%s.>", objectID, TYPE_TYPELINK)
		typeKeys := contextProcessor.GlobalCache.GetKeysByPattern(linkPattern)
		if len(typeKeys) == 0 {
			return
		}

		createPayload := easyjson.NewJSONObject()
		createPayload.SetByPath("id", easyjson.NewJSON(objectID))
		createPayload.SetByPath("origin_type", easyjson.NewJSON(typeKeys[0]))
		createPayload.SetByPath("body", *originBody)

		result, err := contextProcessor.Request(sfplugins.GolangLocalRequest, "functions.graph.tx.object.create", txID, &createPayload, nil)
		if err := checkRequestError(result, err); err != nil {
			replyError(contextProcessor, err)
			return
		}
	}

	updatePayload := easyjson.NewJSONObject()
	updatePayload.SetByPath("body", payload.GetByPath("body"))

	result, err := contextProcessor.Request(sfplugins.GolangLocalRequest, "functions.graph.api.object.update", txObject, &updatePayload, nil)
	if err := checkRequestError(result, err); err != nil {
		replyError(contextProcessor, err)
		return
	}

	replyOk(contextProcessor)
}

/*
	{
		"from": string,
		"to": string,
		"object_link_type": string
		"body": json
	}

create type -> type link
*/
func CreateTypesLink(_ sfplugins.StatefunExecutor, contextProcessor *sfplugins.StatefunContextProcessor) {
	txID := contextProcessor.Self.ID
	payload := contextProcessor.Payload

	from, ok := payload.GetByPath("from").AsString()
	if !ok {
		replyError(contextProcessor, errInvalidArgument)
		return
	}

	to, ok := payload.GetByPath("to").AsString()
	if !ok {
		replyError(contextProcessor, errInvalidArgument)
		return
	}

	prefix := generatePrefix(txID)
	txFrom := prefix + from
	txTo := prefix + to

	createLinkPayload := easyjson.NewJSONObject()
	createLinkPayload.SetByPath("to", easyjson.NewJSON(txTo))
	createLinkPayload.SetByPath("object_link_type", payload.GetByPath("object_link_type"))

	result, err := contextProcessor.Request(sfplugins.GolangLocalRequest, "functions.graph.api.types.link.create", txFrom, &createLinkPayload, nil)
	if err := checkRequestError(result, err); err != nil {
		replyError(contextProcessor, err)
		return
	}

	replyOk(contextProcessor)
}

/*
	{
		"from": string,
		"to": string,
		"object_link_type": string, optional
		"body": json, optional
	}
*/
func UpdateTypesLink(_ sfplugins.StatefunExecutor, contextProcessor *sfplugins.StatefunContextProcessor) {
	txID := contextProcessor.Self.ID
	payload := contextProcessor.Payload

	from, ok := payload.GetByPath("from").AsString()
	if !ok {
		replyError(contextProcessor, errInvalidArgument)
		return
	}

	to, ok := payload.GetByPath("to").AsString()
	if !ok {
		replyError(contextProcessor, errInvalidArgument)
		return
	}

	prefix := generatePrefix(txID)
	txFrom := prefix + from
	txTo := prefix + to

	updatePayload := easyjson.NewJSONObject()
	updatePayload.SetByPath("to", easyjson.NewJSON(txTo))

	//needObjects := false

	if payload.PathExists("object_link_type") {
		//needObjects = true
		updatePayload.SetByPath("object_link_type", payload.GetByPath("object_link_type"))
	}

	if payload.PathExists("body") {
		updatePayload.SetByPath("body", payload.GetByPath("body"))
	}

	linkID := fmt.Sprintf("%s.out.ltp_oid-bdy.%s.%s", txFrom, txTo, txTo)

	// if link to update doesn't exists, so we need to clone area from main graph
	if _, err := contextProcessor.GlobalCache.GetValue(linkID); err != nil {

		// if from doesn't exists, clone from main graph
		if _, err := contextProcessor.GlobalCache.GetValue(txFrom); err != nil {
			// clone
			if err := cloneTypeFromMainGraphToTx(contextProcessor, txID, from, txFrom); err != nil {
				replyError(contextProcessor, err)
				return
			}
		}

		// if to doesn't exists, clone from main graph
		if _, err := contextProcessor.GlobalCache.GetValue(txTo); err != nil {
			// clone
			if err := cloneTypeFromMainGraphToTx(contextProcessor, txID, to, txTo); err != nil {
				replyError(contextProcessor, err)
				return
			}
		}

		// clone link
		if err := cloneLinkFromMainGraphToTx(contextProcessor, from, to, to, txFrom, txTo, txTo); err != nil {
			replyError(contextProcessor, err)
			return
		}

		//if needObjects {
		// clone objects
		//}
	}

	result, err := contextProcessor.Request(sfplugins.GolangLocalRequest, "functions.graph.api.types.link.update", txFrom, &updatePayload, nil)
	if err := checkRequestError(result, err); err != nil {
		replyError(contextProcessor, err)
		return
	}

	replyOk(contextProcessor)
}

/*
	{
		"from": string,
		"to": string,
		"body": json
	}

create object -> object link
*/
func CreateObjectsLink(_ sfplugins.StatefunExecutor, contextProcessor *sfplugins.StatefunContextProcessor) {
	txID := contextProcessor.Self.ID
	payload := contextProcessor.Payload

	prefix := generatePrefix(txID)

	from, ok := payload.GetByPath("from").AsString()
	if !ok {
		replyError(contextProcessor, errInvalidArgument)
		return
	}

	to, ok := payload.GetByPath("to").AsString()
	if !ok {
		replyError(contextProcessor, errInvalidArgument)
		return
	}

	txFrom := prefix + from
	txTo := prefix + to

	createLinkPayload := easyjson.NewJSONObject()
	createLinkPayload.SetByPath("to", easyjson.NewJSON(txTo))

	result, err := contextProcessor.Request(sfplugins.GolangLocalRequest, "functions.graph.api.objects.link.create", txFrom, &createLinkPayload, nil)
	if err := checkRequestError(result, err); err != nil {
		replyError(contextProcessor, err)
		return
	}

	replyOk(contextProcessor)
}

/*
	{
		"from": string,
		"to": string,
		"body": json, optional
	}
*/
func UpdateObjectsLink(_ sfplugins.StatefunExecutor, contextProcessor *sfplugins.StatefunContextProcessor) {
	txID := contextProcessor.Self.ID
	payload := contextProcessor.Payload

	prefix := generatePrefix(txID)

	from, ok := payload.GetByPath("from").AsString()
	if !ok {
		replyError(contextProcessor, errInvalidArgument)
		return
	}

	to, ok := payload.GetByPath("to").AsString()
	if !ok {
		replyError(contextProcessor, errInvalidArgument)
		return
	}

	fromType := findObjectType(contextProcessor, from)
	toType := findObjectType(contextProcessor, to)

	typesLink := fmt.Sprintf("%s.out.ltp_oid-bdy.__type.%s", fromType, toType)
	typesLinkBody, err := contextProcessor.GlobalCache.GetValueAsJSON(typesLink)
	if err != nil {
		replyError(contextProcessor, err)
		return
	}

	txFrom := prefix + from
	txTo := prefix + to

	objectLinkType := typesLinkBody.GetByPath("link_type").AsStringDefault("")
	linkID := fmt.Sprintf("%s.out.ltp_oid-bdy.%s.%s", txFrom, objectLinkType, txTo)

	if _, err := contextProcessor.GlobalCache.GetValue(linkID); err != nil {
		if _, err := contextProcessor.GlobalCache.GetValue(txFrom); err != nil {
			if err := cloneObjectFromMainGraphToTx(contextProcessor, txID, from, txFrom, fromType); err != nil {
				replyError(contextProcessor, err)
				return
			}
		}

		if _, err := contextProcessor.GlobalCache.GetValue(txTo); err != nil {
			if err := cloneObjectFromMainGraphToTx(contextProcessor, txID, to, txTo, toType); err != nil {
				replyError(contextProcessor, err)
				return
			}
		}

		if err := cloneLinkFromMainGraphToTx(contextProcessor, from, objectLinkType, to, txFrom, objectLinkType, txTo); err != nil {
			replyError(contextProcessor, err)
			return
		}
	}

	updatePayload := easyjson.NewJSONObject()
	updatePayload.SetByPath("to", easyjson.NewJSON(txTo))

	if payload.PathExists("body") {
		updatePayload.SetByPath("body", payload.GetByPath("body"))
	}

	result, err := contextProcessor.Request(sfplugins.GolangLocalRequest, "functions.graph.api.objects.link.update", txFrom, &updatePayload, nil)
	if err := checkRequestError(result, err); err != nil {
		replyError(contextProcessor, err)
		return
	}

	replyOk(contextProcessor)
}

func cloneGraph(ctx *sfplugins.StatefunContextProcessor, txID, cloneMod string, types ...string) error {
	switch cloneMod {
	case "min":
		if err := cloneGraphWithTypes(ctx, txID); err != nil {
			return err
		}
	case "full":
		if err := fullClone(ctx, txID); err != nil {
			return err
		}
	case "with_types":
		if err := cloneGraphWithTypes(ctx, txID, types...); err != nil {
			return err
		}
	}

	return nil
}

func initBuilInObjects(ctx *sfplugins.StatefunContextProcessor, txID string) error {
	prefix := generatePrefix(txID)

	// create root
	root := prefix + BUILT_IN_ROOT
	_, err := ctx.Request(sfplugins.GolangLocalRequest, "functions.graph.ll.api.object.create", root, easyjson.NewJSONObject().GetPtr(), nil)
	if err != nil {
		return err
	}

	if err := createLowLevelLink(ctx, txID, root, "graph", "", easyjson.NewJSONObject()); err != nil {
		return err
	}

	// create objects and types
	objects := prefix + BUILT_IN_OBJECTS
	_, err = ctx.Request(sfplugins.GolangLocalRequest, "functions.graph.ll.api.object.create", objects, easyjson.NewJSONObject().GetPtr(), nil)
	if err != nil {
		return err
	}

	types := prefix + BUILT_IN_TYPES
	_, err = ctx.Request(sfplugins.GolangLocalRequest, "functions.graph.ll.api.object.create", types, easyjson.NewJSONObject().GetPtr(), nil)
	if err != nil {
		return err
	}

	// create root -> objects link
	if err := createLowLevelLink(ctx, root, objects, OBJECTS_TYPELINK, "", easyjson.NewJSONObject()); err != nil {
		return err
	}

	// create root -> types link
	if err := createLowLevelLink(ctx, root, types, TYPES_TYPELINK, "", easyjson.NewJSONObject()); err != nil {
		return err
	}

	// create group type ----------------------------------------
	group := prefix + "group"
	_, err = ctx.Request(sfplugins.GolangLocalRequest, "functions.graph.ll.api.object.create", group, easyjson.NewJSONObject().GetPtr(), nil)
	if err != nil {
		return err
	}

	if err := createLowLevelLink(ctx, types, group, TYPE_TYPELINK, "", easyjson.NewJSONObject()); err != nil {
		return err
	}

	// link from group -> group, need for define "group" link type
	if err := createLowLevelLink(ctx, group, group, TYPE_TYPELINK, GROUP_TYPELINK, easyjson.NewJSONObject()); err != nil {
		return err
	}
	//-----------------------------------------------------------

	// create NAV ------------------------------------------------
	nav := prefix + "nav"
	_, err = ctx.Request(sfplugins.GolangLocalRequest, "functions.graph.ll.api.object.create", nav, easyjson.NewJSONObject().GetPtr(), nil)
	if err != nil {
		return err
	}

	if err := createLowLevelLink(ctx, objects, nav, OBJECT_TYPELINK, "", easyjson.NewJSONObject()); err != nil {
		return err
	}

	if err := createLowLevelLink(ctx, nav, group, TYPE_TYPELINK, "", easyjson.NewJSONObject()); err != nil {
		return err
	}

	if err := createLowLevelLink(ctx, group, nav, OBJECT_TYPELINK, "", easyjson.NewJSONObject()); err != nil {
		return err
	}
	// -----------------------------------------------------------

	return nil
}

func fullClone(ctx *sfplugins.StatefunContextProcessor, txID string) error {
	prefix := generatePrefix(txID)
	state := graphState(ctx, BUILT_IN_ROOT)

	state.initBuiltIn()

	for id := range state.objects {
		body, err := ctx.GlobalCache.GetValueAsJSON(id)
		if err != nil {
			body = easyjson.NewJSONObject().GetPtr()
		}

		if err := createLowLevelObject(ctx, prefix+id, body); err != nil {
			continue
		}
	}

	for _, l := range state.links {
		body, err := ctx.GlobalCache.GetValueAsJSON(l.cacheID)
		if err != nil {
			body = easyjson.NewJSONObject().GetPtr()
		}

		from := prefix + l.from
		to := prefix + l.to

		if err := createLowLevelLink(ctx, from, to, l.lt, l.objectLt, *body); err != nil {
			continue
		}
	}

	if err := createLowLevelLink(ctx, txID, prefix+BUILT_IN_ROOT, "graph", "", easyjson.NewJSONObject()); err != nil {
		return err
	}

	return nil
}

func cloneGraphWithTypes(ctx *sfplugins.StatefunContextProcessor, txID string, types ...string) error {
	if err := initBuilInObjects(ctx, txID); err != nil {
		return err
	}

	prefix := generatePrefix(txID)

	uniqTypeObjects := make(map[string]map[string]struct{})
	for _, v := range types {
		uniqTypeObjects[v] = make(map[string]struct{})
	}

	links := make(map[string]link)
	objects := make(map[string]struct{})

	for v := range uniqTypeObjects {
		// if type doesn't exists, continue
		if _, err := ctx.GlobalCache.GetValue(v); err != nil {
			continue
		}

		// create type
		objects[v] = struct{}{}

		// create types -> type link
		links[BUILT_IN_TYPES+v+TYPE_TYPELINK] = link{
			from: BUILT_IN_TYPES,
			to:   v,
			lt:   TYPE_TYPELINK,
		}

		pattern := v + ".out.ltp_oid-bdy.>"
		outLinks := ctx.GlobalCache.GetKeysByPattern(pattern)

		for _, outLink := range outLinks {
			split := strings.Split(outLink, ".")
			if len(split) == 0 {
				continue
			}

			outLinkID := split[len(split)-1]
			outLinkLt := split[len(split)-2]

			switch outLinkLt {
			case OBJECT_TYPELINK:
				uniqTypeObjects[v][outLinkID] = struct{}{}

				objects[outLinkID] = struct{}{}

				// create type -> object link
				links[v+outLinkID+OBJECT_TYPELINK] = link{
					from: v,
					to:   outLinkID,
					lt:   OBJECT_TYPELINK,
				}

				// create object -> type link
				links[outLinkID+v+TYPE_TYPELINK] = link{
					from: outLinkID,
					to:   v,
					lt:   TYPE_TYPELINK,
				}

				// create objects -> object link
				links[BUILT_IN_OBJECTS+outLinkID+OBJECT_TYPELINK] = link{
					from: BUILT_IN_OBJECTS,
					to:   outLinkID,
					lt:   OBJECT_TYPELINK,
				}
			case TYPE_TYPELINK:
				if _, ok := uniqTypeObjects[outLinkID]; !ok {
					continue
				}

				objects[outLinkID] = struct{}{}

				// create type -> type link
				links[v+outLinkID+TYPE_TYPELINK] = link{
					from: v,
					to:   outLinkID,
					lt:   TYPE_TYPELINK,
				}
			}
		}
	}

	for _, l := range links {
		if l.lt != TYPE_TYPELINK {
			continue
		}

		objectsFrom := uniqTypeObjects[l.from]
		objectsTo := uniqTypeObjects[l.to]

		if len(objectsFrom) == 0 || len(objectsTo) == 0 {
			continue
		}

		typesLink, err := ctx.GlobalCache.GetValueAsJSON(fmt.Sprintf("%s.out.ltp_oid-bdy.__type.%s", l.from, l.to))
		if err != nil {
			continue
		}

		linkType, ok := typesLink.GetByPath("link_type").AsString()
		if !ok {
			continue
		}

		for objectFrom := range objectsFrom {
			out := ctx.GlobalCache.GetKeysByPattern(fmt.Sprintf("%s.out.ltp_oid-bdy.%s.>", objectFrom, linkType))
			for _, objectTo := range out {
				links[objectFrom+objectTo+linkType] = link{
					from: objectFrom,
					to:   objectTo,
					lt:   linkType,
				}
			}
		}
	}

	for id := range objects {
		body, err := ctx.GlobalCache.GetValueAsJSON(id)
		if err != nil {
			body = easyjson.NewJSONObject().GetPtr()
		}

		if err := createLowLevelObject(ctx, prefix+id, body); err != nil {
			continue
		}
	}

	for _, l := range links {
		body, err := ctx.GlobalCache.GetValueAsJSON(fmt.Sprintf("%s.out.ltp_oid-bdy.%s.%s", l.from, l.lt, l.to))
		if err != nil {
			body = easyjson.NewJSONObject().GetPtr()
		}

		from := prefix + l.from
		to := prefix + l.to

		if err := createLowLevelLink(ctx, from, to, l.lt, l.objectLt, *body); err != nil {
			continue
		}
	}

	if err := createLowLevelLink(ctx, txID, prefix+BUILT_IN_ROOT, "graph", "", easyjson.NewJSONObject()); err != nil {
		return err
	}

	return nil
}

func generateTxID(nonce int, unix int64) string {
	hash := sha256.Sum256([]byte(_TX_MASTER + strconv.Itoa(nonce) + strconv.Itoa(int(unix))))
	return hex.EncodeToString(hash[:8])
}

func generatePrefix(txID string) string {
	b := strings.Builder{}
	b.WriteString(txID)
	b.WriteString(_TX_SEPARATOR)
	return b.String()
}
