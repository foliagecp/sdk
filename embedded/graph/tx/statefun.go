package tx

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/embedded/graph/common"
	"github.com/foliagecp/sdk/embedded/graph/crud"
	"github.com/foliagecp/sdk/statefun"
	"github.com/foliagecp/sdk/statefun/logger"
	sfplugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
	"github.com/prometheus/client_golang/prometheus"
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
	statefun.NewFunctionType(runtime, "functions.cmdb.tx.begin", Begin, *statefun.NewFunctionTypeConfig().SetServiceState(true))

	statefun.NewFunctionType(runtime, "functions.cmdb.tx.type.create", CreateType, *statefun.NewFunctionTypeConfig().SetServiceState(true))
	statefun.NewFunctionType(runtime, "functions.cmdb.tx.type.update", UpdateType, *statefun.NewFunctionTypeConfig().SetServiceState(true))
	statefun.NewFunctionType(runtime, "functions.cmdb.tx.type.delete", DeleteType, *statefun.NewFunctionTypeConfig().SetServiceState(true))

	statefun.NewFunctionType(runtime, "functions.cmdb.tx.object.create", CreateObject, *statefun.NewFunctionTypeConfig().SetServiceState(true))
	statefun.NewFunctionType(runtime, "functions.cmdb.tx.object.update", UpdateObject, *statefun.NewFunctionTypeConfig().SetServiceState(true))
	statefun.NewFunctionType(runtime, "functions.cmdb.tx.object.delete", DeleteObject, *statefun.NewFunctionTypeConfig().SetServiceState(true))

	statefun.NewFunctionType(runtime, "functions.cmdb.tx.types.link.create", CreateTypesLink, *statefun.NewFunctionTypeConfig().SetServiceState(true))
	statefun.NewFunctionType(runtime, "functions.cmdb.tx.types.link.update", UpdateTypesLink, *statefun.NewFunctionTypeConfig().SetServiceState(true))
	statefun.NewFunctionType(runtime, "functions.cmdb.tx.types.link.delete", DeleteTypesLink, *statefun.NewFunctionTypeConfig().SetServiceState(true))

	statefun.NewFunctionType(runtime, "functions.cmdb.tx.objects.link.create", CreateObjectsLink, *statefun.NewFunctionTypeConfig().SetServiceState(true))
	statefun.NewFunctionType(runtime, "functions.cmdb.tx.objects.link.update", UpdateObjectsLink, *statefun.NewFunctionTypeConfig().SetServiceState(true))
	statefun.NewFunctionType(runtime, "functions.cmdb.tx.objects.link.delete", DeleteObjectsLink, *statefun.NewFunctionTypeConfig().SetServiceState(true))

	statefun.NewFunctionType(runtime, "functions.cmdb.tx.commit", Commit, *statefun.NewFunctionTypeConfig().SetServiceState(true))
	statefun.NewFunctionType(runtime, "functions.cmdb.tx.push", Push, *statefun.NewFunctionTypeConfig().SetServiceState(true))
}

// exec on arbitrary id=txid,
// id must not belong to an existing object in graph! otherwise object will be rewritten
// create tx_id, clone exist graph with tx_id prefix, return tx_id to client

func replyTxError(ctx *sfplugins.StatefunContextProcessor, err error) {
	system.MsgOnErrorReturn(ctx.ObjectMutexUnlock())
	reply(ctx, "failed", err.Error())
}

/*
	payload:{
		"clone": "min" | "full" | "with_types", optional, default: full
		"types": map[string]beginTxType, only with "clone":"with_types"
	}
*/
func Begin(_ sfplugins.StatefunExecutor, contextProcessor *sfplugins.StatefunContextProcessor) {
	if err := contextProcessor.ObjectMutexLock(false); err != nil {
		replyError(contextProcessor, err)
		return
	}

	txID := contextProcessor.Self.ID

	payload := contextProcessor.Payload
	cloneMod := payload.GetByPath("clone").AsStringDefault("full")
	cloneWithTypes := payload.GetByPath("types")

	body := contextProcessor.GetObjectContext()

	nonce := int(body.GetByPath("nonce").AsNumericDefault(0))
	nonce++
	body.SetByPath("nonce", easyjson.NewJSON(nonce))

	contextProcessor.SetObjectContext(body)

	now := system.GetCurrentTimeNs()

	txBody := easyjson.NewJSONObject()
	txBody.SetByPath("created_at", easyjson.NewJSON(now))

	if err := createLowLevelLink(contextProcessor, _TX_MASTER, txID, "tx", "", easyjson.NewJSONObject()); err != nil {
		replyTxError(contextProcessor, err)
		return
	}

	types := make(map[string]beginTxType)
	bytes := cloneWithTypes.ToBytes()

	if len(bytes) > 0 {
		if err := json.Unmarshal(bytes, &types); err != nil {
			replyTxError(contextProcessor, err)
			return
		}
	}

	cloneStart := time.Now()

	if err := cloneGraph(contextProcessor, txID, cloneMod, types); err != nil {
		replyTxError(contextProcessor, err)
		return
	}

	// Measure cloning duration ---------------------------
	measureName := fmt.Sprintf("%sclone_execution_time", strings.ReplaceAll(contextProcessor.Self.Typename, ".", ""))
	if gaugeVec, err := system.GlobalPrometrics.EnsureGaugeVecSimple(measureName, "", []string{"type"}); err == nil {
		gaugeVec.With(prometheus.Labels{"type": cloneMod}).Set(float64(time.Since(cloneStart).Microseconds()))
	}
	// ----------------------------------------------------

	qid := common.GetQueryID(contextProcessor)

	reply := easyjson.NewJSONObject()
	reply.SetByPath("status", easyjson.NewJSON("ok"))
	common.ReplyQueryID(qid, easyjson.NewJSONObjectWithKeyValue("payload", reply).GetPtr(), contextProcessor)
}

/*
	payload:{
		"debug": bool, optional, default: "false"
		"mode": "merge" | "replace", optional, default: "merge"
	}
*/
// exec on transaction
func Commit(_ sfplugins.StatefunExecutor, contextProcessor *sfplugins.StatefunContextProcessor) {
	// add validating stage
	payload := contextProcessor.Payload
	push := easyjson.NewJSONObject()

	if mode, ok := payload.GetByPath("mode").AsString(); ok {
		push.SetByPath("mode", easyjson.NewJSON(mode))
	}

	if debug, ok := payload.GetByPath("debug").AsBool(); ok {
		push.SetByPath("debug", easyjson.NewJSON(debug))
	}

	system.MsgOnErrorReturn(contextProcessor.Request(sfplugins.GolangLocalRequest, "functions.cmdb.tx.push", _TX_MASTER, &push, nil))

	qid := common.GetQueryID(contextProcessor)
	reply := easyjson.NewJSONObject()
	reply.SetByPath("status", easyjson.NewJSON("ok"))
	common.ReplyQueryID(qid, easyjson.NewJSONObjectWithKeyValue("payload", reply).GetPtr(), contextProcessor)

	system.MsgOnErrorReturn(contextProcessor.ObjectMutexUnlock())
}

/*
	payload:{
		"debug": bool, optional, default: "false"
		"mode": "merge" | "replace", optional, default: "merge"
	}
*/
func Push(_ sfplugins.StatefunExecutor, contextProcessor *sfplugins.StatefunContextProcessor) {
	selfID := contextProcessor.Self.ID
	if selfID != _TX_MASTER {
		return
	}

	opts := make([]mergerOpt, 0)

	if mode, ok := contextProcessor.Payload.GetByPath("mode").AsString(); ok {
		opts = append(opts, withMode(mode))
	}

	if debug, ok := contextProcessor.Payload.GetByPath("debug").AsBool(); ok && debug {
		opts = append(opts, withDebug())
	}

	// TODO: check tx id
	txID := contextProcessor.Caller.ID
	merger := newMerger(txID, opts...)

	if err := merger.Merge(contextProcessor); err != nil {
		logger.Logln(logger.ErrorLevel, err)
		replyTxError(contextProcessor, err)
		return
	}

	// delete success tx
	delete := easyjson.NewJSONObject()
	delete.SetByPath("mode", easyjson.NewJSON("cascade"))

	if _, err := contextProcessor.Request(sfplugins.GolangLocalRequest, "functions.cmdb.api.object.delete", txID, &delete, nil); err != nil {
		replyTxError(contextProcessor, err)
		return
	}

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

	result, err := contextProcessor.Request(sfplugins.GolangLocalRequest, "functions.cmdb.api.type.create", txTypeID, &createTypePayload, nil)
	if err := checkRequestError(result, err); err != nil {
		replyTxError(contextProcessor, err)
		return
	}

	replyOk(contextProcessor)
}

/*
	{
		"id": string,
		"body": json
	}

clone type from main graph if not exists

update type body
*/
func UpdateType(_ sfplugins.StatefunExecutor, contextProcessor *sfplugins.StatefunContextProcessor) {
	txID := contextProcessor.Self.ID
	payload := contextProcessor.Payload

	typeID, ok := payload.GetByPath("id").AsString()
	if !ok {
		replyTxError(contextProcessor, errInvalidArgument)
		return
	}

	prefix := generatePrefix(txID)

	txTypes := prefix + BUILT_IN_TYPES
	txType := prefix + typeID

	pattern := fmt.Sprintf(crud.OutLinkBodyKeyPrefPattern+crud.LinkKeySuff2Pattern, txTypes, TYPE_TYPELINK, txType)
	keys := contextProcessor.GlobalCache.GetKeysByPattern(pattern)

	// tx type doesn't created yet
	if len(keys) == 0 {
		originBody, err := contextProcessor.GlobalCache.GetValueAsJSON(typeID)
		if err != nil {
			replyTxError(contextProcessor, err)
			return
		}

		createPayload := easyjson.NewJSONObject()
		createPayload.SetByPath("id", easyjson.NewJSON(typeID))
		createPayload.SetByPath("body", *originBody)

		result, err := contextProcessor.Request(sfplugins.GolangLocalRequest, "functions.cmdb.tx.type.create", txID, &createPayload, nil)
		if err := checkRequestError(result, err); err != nil {
			replyTxError(contextProcessor, err)
			return
		}
	}

	updatePayload := easyjson.NewJSONObject()
	updatePayload.SetByPath("body", payload.GetByPath("body"))

	result, err := contextProcessor.Request(sfplugins.GolangLocalRequest, "functions.cmdb.api.type.update", txType, &updatePayload, nil)
	if err := checkRequestError(result, err); err != nil {
		replyTxError(contextProcessor, err)
		return
	}

	replyOk(contextProcessor)
}

/*
	{
		"id": string
	}
*/
func DeleteType(_ sfplugins.StatefunExecutor, contextProcessor *sfplugins.StatefunContextProcessor) {
	txID := contextProcessor.Self.ID
	payload := contextProcessor.Payload

	typeID, ok := payload.GetByPath("id").AsString()
	if !ok {
		replyTxError(contextProcessor, errInvalidArgument)
		return
	}

	prefix := generatePrefix(txID)
	txType := prefix + typeID
	meta := generateDeletedMeta()

	// delete objects which implement this type
	for _, v := range findTypeObjects(contextProcessor, txType) {
		updatePayload := easyjson.NewJSONObject()
		updatePayload.SetByPath("body", meta)

		result, err := contextProcessor.Request(sfplugins.GolangLocalRequest, "functions.cmdb.api.object.update", v, &updatePayload, nil)
		if err := checkRequestError(result, err); err != nil {
			replyTxError(contextProcessor, err)
			return
		}

	}

	updatePayload := easyjson.NewJSONObject()
	updatePayload.SetByPath("body", meta)

	result, err := contextProcessor.Request(sfplugins.GolangLocalRequest, "functions.cmdb.api.type.update", txType, &updatePayload, nil)
	if err := checkRequestError(result, err); err != nil {
		replyTxError(contextProcessor, err)
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

	result, err := contextProcessor.Request(sfplugins.GolangLocalRequest, "functions.cmdb.api.object.create", txObjID, &createObjPayload, nil)
	if err := checkRequestError(result, err); err != nil {
		replyTxError(contextProcessor, err)
		return
	}

	replyOk(contextProcessor)
}

/*
	{
		"id": string,
		"body": json
		"mode": "merge" | "replace", optional, default: "merge"
	}

clone object from main graph if not exists

update object body
*/
func UpdateObject(_ sfplugins.StatefunExecutor, contextProcessor *sfplugins.StatefunContextProcessor) {
	txID := contextProcessor.Self.ID
	payload := contextProcessor.Payload

	objectID, ok := payload.GetByPath("id").AsString()
	if !ok {
		replyTxError(contextProcessor, errInvalidArgument)
		return
	}

	prefix := generatePrefix(txID)

	txObjects := prefix + BUILT_IN_OBJECTS
	txObject := prefix + objectID

	pattern := fmt.Sprintf(crud.OutLinkBodyKeyPrefPattern+crud.LinkKeySuff2Pattern, txObjects, OBJECT_TYPELINK, txObject)
	keys := contextProcessor.GlobalCache.GetKeysByPattern(pattern)

	// tx object doesn't created yet
	if len(keys) == 0 {
		originBody, err := contextProcessor.GlobalCache.GetValueAsJSON(objectID)
		if err != nil {
			replyTxError(contextProcessor, err)
			return
		}

		linkPattern := fmt.Sprintf(crud.OutLinkBodyKeyPrefPattern+crud.LinkKeySuff2Pattern, objectID, TYPE_TYPELINK, ">")
		typeKeys := contextProcessor.GlobalCache.GetKeysByPattern(linkPattern)
		if len(typeKeys) == 0 {
			replyTxError(contextProcessor, fmt.Errorf("missing links: %s", linkPattern))
			return
		}

		createPayload := easyjson.NewJSONObject()
		createPayload.SetByPath("id", easyjson.NewJSON(objectID))
		createPayload.SetByPath("origin_type", easyjson.NewJSON(typeKeys[0]))
		createPayload.SetByPath("body", *originBody)

		result, err := contextProcessor.Request(sfplugins.GolangLocalRequest, "functions.cmdb.tx.object.create", txID, &createPayload, nil)
		if err := checkRequestError(result, err); err != nil {
			replyTxError(contextProcessor, err)
			return
		}
	}

	updatePayload := easyjson.NewJSONObject()
	updatePayload.SetByPath("body", payload.GetByPath("body"))

	if mode, ok := payload.GetByPath("mode").AsString(); ok {
		updatePayload.SetByPath("mode", easyjson.NewJSON(mode))
	}

	result, err := contextProcessor.Request(sfplugins.GolangLocalRequest, "functions.cmdb.api.object.update", txObject, &updatePayload, nil)
	if err := checkRequestError(result, err); err != nil {
		replyTxError(contextProcessor, err)
		return
	}

	replyOk(contextProcessor)
}

/*
	{
		"id":string
	}

TODO: mark for delete all link from/in object
*/
func DeleteObject(_ sfplugins.StatefunExecutor, contextProcessor *sfplugins.StatefunContextProcessor) {
	txID := contextProcessor.Self.ID
	payload := contextProcessor.Payload

	objectID, ok := payload.GetByPath("id").AsString()
	if !ok {
		replyTxError(contextProcessor, errInvalidArgument)
		return
	}

	prefix := generatePrefix(txID)
	txObject := prefix + objectID
	meta := generateDeletedMeta()

	updatePayload := easyjson.NewJSONObject()
	updatePayload.SetByPath("body", meta)

	result, err := contextProcessor.Request(sfplugins.GolangLocalRequest, "functions.cmdb.api.object.update", txObject, &updatePayload, nil)
	if err := checkRequestError(result, err); err != nil {
		replyTxError(contextProcessor, err)
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
		replyTxError(contextProcessor, errInvalidArgument)
		return
	}

	to, ok := payload.GetByPath("to").AsString()
	if !ok {
		replyTxError(contextProcessor, errInvalidArgument)
		return
	}

	prefix := generatePrefix(txID)
	txFrom := prefix + from
	txTo := prefix + to

	createLinkPayload := easyjson.NewJSONObject()
	createLinkPayload.SetByPath("to", easyjson.NewJSON(txTo))
	createLinkPayload.SetByPath("object_link_type", payload.GetByPath("object_link_type"))
	createLinkPayload.SetByPath("body", payload.GetByPath("body"))

	result, err := contextProcessor.Request(sfplugins.GolangLocalRequest, "functions.cmdb.api.types.link.create", txFrom, &createLinkPayload, nil)
	if err := checkRequestError(result, err); err != nil {
		replyTxError(contextProcessor, err)
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
		replyTxError(contextProcessor, errInvalidArgument)
		return
	}

	to, ok := payload.GetByPath("to").AsString()
	if !ok {
		replyTxError(contextProcessor, errInvalidArgument)
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

	linkID := fmt.Sprintf(crud.OutLinkBodyKeyPrefPattern+crud.LinkKeySuff2Pattern, txFrom, "__type", txTo)

	// if link to update doesn't exists, so we need to clone area from main graph
	if _, err := contextProcessor.GlobalCache.GetValue(linkID); err != nil {

		// if from doesn't exists, clone from main graph
		if _, err := contextProcessor.GlobalCache.GetValue(txFrom); err != nil {
			// clone
			if err := cloneTypeFromMainGraphToTx(contextProcessor, txID, from, txFrom); err != nil {
				replyTxError(contextProcessor, err)
				return
			}
		}

		// if to doesn't exists, clone from main graph
		if _, err := contextProcessor.GlobalCache.GetValue(txTo); err != nil {
			// clone
			if err := cloneTypeFromMainGraphToTx(contextProcessor, txID, to, txTo); err != nil {
				replyTxError(contextProcessor, err)
				return
			}
		}

		// clone link
		if err := cloneLinkFromMainGraphToTx(contextProcessor, from, to, to, txFrom, txTo, txTo); err != nil {
			replyTxError(contextProcessor, err)
			return
		}

		//if needObjects {
		// clone objects
		//}
	}

	result, err := contextProcessor.Request(sfplugins.GolangLocalRequest, "functions.cmdb.api.types.link.update", txFrom, &updatePayload, nil)
	if err := checkRequestError(result, err); err != nil {
		replyTxError(contextProcessor, err)
		return
	}

	replyOk(contextProcessor)
}

/*
	{
		"from": string,
		"to": string,
	}
*/
func DeleteTypesLink(_ sfplugins.StatefunExecutor, contextProcessor *sfplugins.StatefunContextProcessor) {
	txID := contextProcessor.Self.ID
	payload := contextProcessor.Payload

	from, ok := payload.GetByPath("from").AsString()
	if !ok {
		replyTxError(contextProcessor, errInvalidArgument)
		return
	}

	to, ok := payload.GetByPath("to").AsString()
	if !ok {
		replyTxError(contextProcessor, errInvalidArgument)
		return
	}

	prefix := generatePrefix(txID)
	meta := generateDeletedMeta()

	txFrom := prefix + from
	txTo := prefix + to
	linkID := fmt.Sprintf(crud.OutLinkBodyKeyPrefPattern+crud.LinkKeySuff2Pattern, txFrom, "__type", txTo)

	linkBody, err := contextProcessor.GlobalCache.GetValueAsJSON(linkID)
	if err != nil {
		replyTxError(contextProcessor, err)
		return
	}

	linkType, ok := linkBody.GetByPath("link_type").AsString()
	if !ok {
		replyTxError(contextProcessor, errInvalidArgument)
		return
	}

	// delete objects links
	for _, objectID := range findTypeObjects(contextProcessor, txFrom) {
		links := contextProcessor.GlobalCache.GetKeysByPattern(fmt.Sprintf(crud.OutLinkBodyKeyPrefPattern+crud.LinkKeySuff2Pattern, objectID, linkType, ">"))
		for _, v := range links {
			split := strings.Split(v, ".")
			if len(split) == 0 {
				continue
			}

			id := split[len(split)-1]

			updatePayload := easyjson.NewJSONObject()
			updatePayload.SetByPath("to", easyjson.NewJSON(id))
			updatePayload.SetByPath("body", meta)

			result, err := contextProcessor.Request(sfplugins.GolangLocalRequest, "functions.cmdb.api.objects.link.update", objectID, &updatePayload, nil)
			if err := checkRequestError(result, err); err != nil {
				replyTxError(contextProcessor, err)
				return
			}
		}
	}

	updatePayload := easyjson.NewJSONObject()
	updatePayload.SetByPath("to", easyjson.NewJSON(txTo))
	updatePayload.SetByPath("body", meta)

	result, err := contextProcessor.Request(sfplugins.GolangLocalRequest, "functions.cmdb.api.types.link.update", txFrom, &updatePayload, nil)
	if err := checkRequestError(result, err); err != nil {
		replyTxError(contextProcessor, err)
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
		replyTxError(contextProcessor, errInvalidArgument)
		return
	}

	to, ok := payload.GetByPath("to").AsString()
	if !ok {
		replyTxError(contextProcessor, errInvalidArgument)
		return
	}

	txFrom := prefix + from
	txTo := prefix + to

	createLinkPayload := easyjson.NewJSONObject()
	createLinkPayload.SetByPath("to", easyjson.NewJSON(txTo))
	createLinkPayload.SetByPath("body", payload.GetByPath("body"))

	result, err := contextProcessor.Request(sfplugins.GolangLocalRequest, "functions.cmdb.api.objects.link.create", txFrom, &createLinkPayload, nil)
	if err := checkRequestError(result, err); err != nil {
		replyTxError(contextProcessor, err)
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
		replyTxError(contextProcessor, errInvalidArgument)
		return
	}

	to, ok := payload.GetByPath("to").AsString()
	if !ok {
		replyTxError(contextProcessor, errInvalidArgument)
		return
	}

	fromType := findObjectType(contextProcessor, from)
	toType := findObjectType(contextProcessor, to)

	typesLink := fmt.Sprintf(crud.OutLinkBodyKeyPrefPattern+crud.LinkKeySuff2Pattern, fromType, "__type", toType)
	typesLinkBody, err := contextProcessor.GlobalCache.GetValueAsJSON(typesLink)
	if err != nil {
		replyTxError(contextProcessor, err)
		return
	}

	txFrom := prefix + from
	txTo := prefix + to

	objectLinkType := typesLinkBody.GetByPath("link_type").AsStringDefault("")
	linkID := fmt.Sprintf(crud.OutLinkBodyKeyPrefPattern+crud.LinkKeySuff2Pattern, txFrom, objectLinkType, txTo)

	if _, err := contextProcessor.GlobalCache.GetValue(linkID); err != nil {
		if _, err := contextProcessor.GlobalCache.GetValue(txFrom); err != nil {
			if err := cloneObjectFromMainGraphToTx(contextProcessor, txID, from, txFrom, fromType); err != nil {
				replyTxError(contextProcessor, err)
				return
			}
		}

		if _, err := contextProcessor.GlobalCache.GetValue(txTo); err != nil {
			if err := cloneObjectFromMainGraphToTx(contextProcessor, txID, to, txTo, toType); err != nil {
				replyTxError(contextProcessor, err)
				return
			}
		}

		if err := cloneLinkFromMainGraphToTx(contextProcessor, from, objectLinkType, to, txFrom, objectLinkType, txTo); err != nil {
			replyTxError(contextProcessor, err)
			return
		}
	}

	updatePayload := easyjson.NewJSONObject()
	updatePayload.SetByPath("to", easyjson.NewJSON(txTo))

	if payload.PathExists("body") {
		updatePayload.SetByPath("body", payload.GetByPath("body"))
	}

	result, err := contextProcessor.Request(sfplugins.GolangLocalRequest, "functions.cmdb.api.objects.link.update", txFrom, &updatePayload, nil)
	if err := checkRequestError(result, err); err != nil {
		replyTxError(contextProcessor, err)
		return
	}

	replyOk(contextProcessor)
}

/*
	{
		"from": string,
		"to": string,
	}
*/
func DeleteObjectsLink(_ sfplugins.StatefunExecutor, contextProcessor *sfplugins.StatefunContextProcessor) {
	txID := contextProcessor.Self.ID
	payload := contextProcessor.Payload

	from, ok := payload.GetByPath("from").AsString()
	if !ok {
		replyTxError(contextProcessor, errInvalidArgument)
		return
	}

	to, ok := payload.GetByPath("to").AsString()
	if !ok {
		replyTxError(contextProcessor, errInvalidArgument)
		return
	}

	prefix := generatePrefix(txID)
	txFrom := prefix + from
	txTo := prefix + to
	meta := generateDeletedMeta()

	updatePayload := easyjson.NewJSONObject()
	updatePayload.SetByPath("to", easyjson.NewJSON(txTo))
	updatePayload.SetByPath("body", meta)

	result, err := contextProcessor.Request(sfplugins.GolangLocalRequest, "functions.cmdb.api.objects.link.update", txFrom, &updatePayload, nil)
	if err := checkRequestError(result, err); err != nil {
		replyTxError(contextProcessor, err)
		return
	}

	replyOk(contextProcessor)
}

func cloneGraph(ctx *sfplugins.StatefunContextProcessor, txID, cloneMod string, types map[string]beginTxType) error {
	switch cloneMod {
	case "min":
		if err := cloneGraphWithTypes(ctx, txID, types); err != nil {
			return err
		}
	case "full":
		if err := fullClone(ctx, txID); err != nil {
			return err
		}
	case "with_types":
		if err := cloneGraphWithTypes(ctx, txID, types); err != nil {
			return err
		}
	}

	return nil
}

func initBuilInObjects(ctx *sfplugins.StatefunContextProcessor, txID string) error {
	prefix := generatePrefix(txID)

	// create root
	root := prefix + BUILT_IN_ROOT
	_, err := ctx.Request(sfplugins.GolangLocalRequest, "functions.graph.api.vertex.create", root, easyjson.NewJSONObject().GetPtr(), nil)
	if err != nil {
		return err
	}

	if err := createLowLevelLink(ctx, txID, root, "graph", "", easyjson.NewJSONObject()); err != nil {
		return err
	}

	// create objects and types
	objects := prefix + BUILT_IN_OBJECTS
	_, err = ctx.Request(sfplugins.GolangLocalRequest, "functions.graph.api.vertex.create", objects, easyjson.NewJSONObject().GetPtr(), nil)
	if err != nil {
		return err
	}

	types := prefix + BUILT_IN_TYPES
	_, err = ctx.Request(sfplugins.GolangLocalRequest, "functions.graph.api.vertex.create", types, easyjson.NewJSONObject().GetPtr(), nil)
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
	_, err = ctx.Request(sfplugins.GolangLocalRequest, "functions.graph.api.vertex.create", group, easyjson.NewJSONObject().GetPtr(), nil)
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
	_, err = ctx.Request(sfplugins.GolangLocalRequest, "functions.graph.api.vertex.create", nav, easyjson.NewJSONObject().GetPtr(), nil)
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

func cloneGraphWithTypes(ctx *sfplugins.StatefunContextProcessor, txID string, types map[string]beginTxType) error {
	if err := initBuilInObjects(ctx, txID); err != nil {
		return err
	}

	prefix := generatePrefix(txID)

	uniqTypeObjects := make(map[string]map[string]struct{})

	links := make(map[string]link)
	objects := make(map[string]struct{})

	for v, policy := range types {
		// if type doesn't exists, continue
		if _, err := ctx.GlobalCache.GetValue(v); err != nil {
			continue
		}

		uniqTypeObjects[v] = make(map[string]struct{})

		// create type
		objects[v] = struct{}{}

		// create types -> type link
		links[BUILT_IN_TYPES+v+TYPE_TYPELINK] = link{
			from: BUILT_IN_TYPES,
			to:   v,
			lt:   TYPE_TYPELINK,
		}

		pattern := fmt.Sprintf(crud.OutLinkBodyKeyPrefPattern+crud.LinkKeySuff1Pattern, v, ">")
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

				switch policy.Mode {
				case "none":
					continue
				case "only":
					if _, ok := policy.Objects[outLinkID]; !ok {
						continue
					}
				}

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
				if _, ok := types[outLinkID]; !ok {
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

		typesLink, err := ctx.GlobalCache.GetValueAsJSON(fmt.Sprintf(crud.OutLinkBodyKeyPrefPattern+crud.LinkKeySuff2Pattern, l.from, "__type", l.to))
		if err != nil {
			continue
		}

		linkType, ok := typesLink.GetByPath("link_type").AsString()
		if !ok {
			continue
		}

		for objectFrom := range objectsFrom {
			out := ctx.GlobalCache.GetKeysByPattern(fmt.Sprintf(crud.OutLinkBodyKeyPrefPattern+crud.LinkKeySuff2Pattern, objectFrom, linkType, ">"))
			for _, v := range out {
				split := strings.Split(v, ".")
				if len(split) == 0 {
					continue
				}

				objectTo := split[len(split)-1]

				if _, ok := objectsTo[objectTo]; !ok {
					continue
				}

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
		body, err := ctx.GlobalCache.GetValueAsJSON(fmt.Sprintf(crud.OutLinkBodyKeyPrefPattern+crud.LinkKeySuff2Pattern, l.from, l.lt, l.to))
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

func generatePrefix(txID string) string {
	b := strings.Builder{}
	b.WriteString(txID)
	b.WriteString(_TX_SEPARATOR)
	return b.String()
}
