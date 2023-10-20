package tx

import (
	"container/list"
	"crypto/sha256"
	"encoding/hex"
	"log/slog"
	"strconv"
	"strings"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/embedded/graph/common"
	"github.com/foliagecp/sdk/statefun"
	sfplugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
)

const _TX_MASTER = "txmaster"

const (
	OBJECTS_TYPELINK         = "__objects"
	TYPES_TYPELINK           = "__types"
	TYPE_TYPELINK            = "__type"
	OBJECT_TYPELINK          = "__object"
	OBJECT_2_OBJECT_TYPELINK = "obj"
	BUILT_IN_TYPES           = "types"
	BUILT_IN_OBJECTS         = "objects"
	BUILT_IN_ROOT            = "root"
	GROUP_TYPELINK           = "group"
)

func RegisterAllFunctionTypes(runtime *statefun.Runtime) {
	statefun.NewFunctionType(runtime, "functions.graph.tx.begin", begin, *statefun.NewFunctionTypeConfig())
	statefun.NewFunctionType(runtime, "functions.graph.tx.type.create", createType, *statefun.NewFunctionTypeConfig())
	statefun.NewFunctionType(runtime, "functions.graph.tx.object.create", createObject, *statefun.NewFunctionTypeConfig())
	statefun.NewFunctionType(runtime, "functions.graph.tx.types.link.create", createTypesLink, *statefun.NewFunctionTypeConfig())
	statefun.NewFunctionType(runtime, "functions.graph.tx.objects.link.create", createObjectsLink, *statefun.NewFunctionTypeConfig())
	statefun.NewFunctionType(runtime, "functions.graph.tx.commit", commit, *statefun.NewFunctionTypeConfig())
	statefun.NewFunctionType(runtime, "functions.graph.tx.push", push, *statefun.NewFunctionTypeConfig())
}

// exec only on txmaster
// create tx_id, clone exist graph with tx_id prefix, return tx_id to client
// tx_id = sha256(txmaster + nonce.toString() + unixnano.toString()).toString()
func begin(_ sfplugins.StatefunExecutor, contextProcessor *sfplugins.StatefunContextProcessor) {
	selfID := contextProcessor.Self.ID
	if selfID != _TX_MASTER {
		return
	}

	body := contextProcessor.GetObjectContext()

	nonce := int(body.GetByPath("nonce").AsNumericDefault(0))
	nonce++
	body.SetByPath("nonce", easyjson.NewJSON(nonce))

	contextProcessor.SetObjectContext(body)

	now := system.GetCurrentTimeNs()
	txID := generateTxID(nonce, now)

	txBody := easyjson.NewJSONObject()
	txBody.SetByPath("created_at", easyjson.NewJSON(now))

	if _, err := contextProcessor.Request(sfplugins.GolangLocalRequest, "functions.graph.ll.api.object.create", txID, &txBody, nil); err != nil {
		return
	}

	link := easyjson.NewJSONObject()
	link.SetByPath("descendant_uuid", easyjson.NewJSON(txID))
	link.SetByPath("link_type", easyjson.NewJSON("tx"))
	link.SetByPath("link_body", easyjson.NewJSONObject())

	if _, err := contextProcessor.Request(sfplugins.GolangLocalRequest, "functions.graph.ll.api.link.create", selfID, &link, nil); err != nil {
		return
	}

	if err := cloneGraph(contextProcessor, txID); err != nil {
		return
	}

	qid := common.GetQueryID(contextProcessor)

	reply := easyjson.NewJSONObject()
	reply.SetByPath("status", easyjson.NewJSON("ok"))
	reply.SetByPath("id", easyjson.NewJSON(txID))
	common.ReplyQueryID(qid, easyjson.NewJSONObjectWithKeyValue("payload", reply).GetPtr(), contextProcessor)
}

/*
	{
		"id": string,
		"body": json
	}

create types -> type link
*/
func createType(_ sfplugins.StatefunExecutor, contextProcessor *sfplugins.StatefunContextProcessor) {
	txID := contextProcessor.Self.ID
	payload := contextProcessor.Payload

	prefix := txID + "="

	typeID := payload.GetByPath("id").AsStringDefault("")
	typeID = prefix + typeID

	createTypePayload := easyjson.NewJSONObject()
	createTypePayload.SetByPath("prefix", easyjson.NewJSON(prefix))
	createTypePayload.SetByPath("body", payload.GetByPath("body"))

	if _, err := contextProcessor.Request(sfplugins.GolangLocalRequest, "functions.graph.api.type.create", typeID, &createTypePayload, nil); err != nil {
		return
	}

	qid := common.GetQueryID(contextProcessor)

	reply := easyjson.NewJSONObject()
	reply.SetByPath("status", easyjson.NewJSON("ok"))
	common.ReplyQueryID(qid, easyjson.NewJSONObjectWithKeyValue("payload", reply).GetPtr(), contextProcessor)
}

/*
	{
		"id": string,
		"originType": string,
		"body": json
	}

create objects -> object link

create type -> object link

create object -> type link
*/
func createObject(_ sfplugins.StatefunExecutor, contextProcessor *sfplugins.StatefunContextProcessor) {
	txID := contextProcessor.Self.ID
	payload := contextProcessor.Payload

	prefix := txID + "="

	objID := payload.GetByPath("id").AsStringDefault("")
	objID = prefix + objID

	createObjPayload := easyjson.NewJSONObject()
	createObjPayload.SetByPath("prefix", easyjson.NewJSON(prefix))
	createObjPayload.SetByPath("originType", payload.GetByPath("originType"))
	createObjPayload.SetByPath("body", payload.GetByPath("body"))

	if _, err := contextProcessor.Request(sfplugins.GolangLocalRequest, "functions.graph.api.object.create", objID, &createObjPayload, nil); err != nil {
		return
	}

	qid := common.GetQueryID(contextProcessor)

	reply := easyjson.NewJSONObject()
	reply.SetByPath("status", easyjson.NewJSON("ok"))
	common.ReplyQueryID(qid, easyjson.NewJSONObjectWithKeyValue("payload", reply).GetPtr(), contextProcessor)
}

/*
	{
		"from": string,
		"to": string,
		"objectLinkType": string
	}

create type -> type link
*/
func createTypesLink(_ sfplugins.StatefunExecutor, contextProcessor *sfplugins.StatefunContextProcessor) {
	txID := contextProcessor.Self.ID
	payload := contextProcessor.Payload

	prefix := txID + "="

	from := payload.GetByPath("from").AsStringDefault("")
	from = prefix + from

	to := payload.GetByPath("to").AsStringDefault("")
	to = prefix + to

	createLinkPayload := easyjson.NewJSONObject()
	createLinkPayload.SetByPath("to", easyjson.NewJSON(to))
	createLinkPayload.SetByPath("objectLinkType", payload.GetByPath("objectLinkType"))

	if _, err := contextProcessor.Request(sfplugins.GolangLocalRequest, "functions.graph.api.types.link.create", from, &createLinkPayload, nil); err != nil {
		return
	}

	qid := common.GetQueryID(contextProcessor)

	reply := easyjson.NewJSONObject()
	reply.SetByPath("status", easyjson.NewJSON("ok"))
	common.ReplyQueryID(qid, easyjson.NewJSONObjectWithKeyValue("payload", reply).GetPtr(), contextProcessor)
}

/*
	{
		"from": string,
		"to": string,
	}

create object -> object link
*/
func createObjectsLink(_ sfplugins.StatefunExecutor, contextProcessor *sfplugins.StatefunContextProcessor) {
	txID := contextProcessor.Self.ID
	payload := contextProcessor.Payload

	prefix := txID + "="

	from := payload.GetByPath("from").AsStringDefault("")
	from = prefix + from

	to := payload.GetByPath("to").AsStringDefault("")
	to = prefix + to

	createLinkPayload := easyjson.NewJSONObject()
	createLinkPayload.SetByPath("to", easyjson.NewJSON(to))

	if _, err := contextProcessor.Request(sfplugins.GolangLocalRequest, "functions.graph.api.objects.link.create", from, &createLinkPayload, nil); err != nil {
		return
	}

	qid := common.GetQueryID(contextProcessor)

	reply := easyjson.NewJSONObject()
	reply.SetByPath("status", easyjson.NewJSON("ok"))
	common.ReplyQueryID(qid, easyjson.NewJSONObjectWithKeyValue("payload", reply).GetPtr(), contextProcessor)
}

func commit(_ sfplugins.StatefunExecutor, contextProcessor *sfplugins.StatefunContextProcessor) {
	empty := easyjson.NewJSONObject().GetPtr()
	contextProcessor.Request(sfplugins.GolangLocalRequest, "functions.graph.tx.push", _TX_MASTER, empty, empty)

	qid := common.GetQueryID(contextProcessor)
	reply := easyjson.NewJSONObject()
	reply.SetByPath("status", easyjson.NewJSON("ok"))
	common.ReplyQueryID(qid, easyjson.NewJSONObjectWithKeyValue("payload", reply).GetPtr(), contextProcessor)
}

func push(_ sfplugins.StatefunExecutor, contextProcessor *sfplugins.StatefunContextProcessor) {
	selfID := contextProcessor.Self.ID
	if selfID != _TX_MASTER {
		return
	}

	// TODO: check tx id
	txID := contextProcessor.Caller.ID

	if err := merge(contextProcessor, txID); err != nil {
		slog.Error(err.Error())
		return
	}

	// delete success tx
	if _, err := contextProcessor.Request(sfplugins.GolangLocalRequest, "functions.graph.ll.api.object.delete", txID, easyjson.NewJSONObject().GetPtr(), nil); err != nil {
		return
	}

	slog.Info("Merge Done!")

	qid := common.GetQueryID(contextProcessor)

	reply := easyjson.NewJSONObject()
	reply.SetByPath("status", easyjson.NewJSON("ok"))
	common.ReplyQueryID(qid, easyjson.NewJSONObjectWithKeyValue("payload", reply).GetPtr(), contextProcessor)
}

func cloneGraph(ctx *sfplugins.StatefunContextProcessor, txID string) error {
	return initBuilInObjects(ctx, txID)
}

func initBuilInObjects(ctx *sfplugins.StatefunContextProcessor, txID string) error {
	prefix := txID + "="

	// create root
	root := prefix + BUILT_IN_ROOT
	_, err := ctx.Request(sfplugins.GolangLocalRequest, "functions.graph.ll.api.object.create", root, easyjson.NewJSONObject().GetPtr(), nil)
	if err != nil {
		return err
	}

	if err := createLowLevelLink(ctx, txID, root, "graph", ""); err != nil {
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
	if err := createLowLevelLink(ctx, root, objects, OBJECTS_TYPELINK, ""); err != nil {
		return err
	}

	// create root -> types link
	if err := createLowLevelLink(ctx, root, types, TYPES_TYPELINK, ""); err != nil {
		return err
	}

	// create group type ----------------------------------------
	group := prefix + "group"
	_, err = ctx.Request(sfplugins.GolangLocalRequest, "functions.graph.ll.api.object.create", group, easyjson.NewJSONObject().GetPtr(), nil)
	if err != nil {
		return err
	}

	if err := createLowLevelLink(ctx, types, group, TYPE_TYPELINK, ""); err != nil {
		return err
	}

	// link from group -> group, need for define "group" link type
	if err := createLowLevelLink(ctx, group, group, GROUP_TYPELINK, GROUP_TYPELINK); err != nil {
		return err
	}
	//-----------------------------------------------------------

	// create NAV ------------------------------------------------
	nav := prefix + "nav"
	_, err = ctx.Request(sfplugins.GolangLocalRequest, "functions.graph.ll.api.object.create", nav, easyjson.NewJSONObject().GetPtr(), nil)
	if err != nil {
		return err
	}

	if err := createLowLevelLink(ctx, objects, nav, OBJECT_TYPELINK, ""); err != nil {
		return err
	}

	if err := createLowLevelLink(ctx, nav, group, TYPE_TYPELINK, ""); err != nil {
		return err
	}

	if err := createLowLevelLink(ctx, group, nav, OBJECT_TYPELINK, ""); err != nil {
		return err
	}
	// -----------------------------------------------------------

	return nil
}

func generateTxID(nonce int, unix int64) string {
	hash := sha256.Sum256([]byte(_TX_MASTER + strconv.Itoa(nonce) + strconv.Itoa(int(unix))))
	return hex.EncodeToString(hash[:8])
}

// merge v0
func merge(ctx *sfplugins.StatefunContextProcessor, txGraphID string) error {
	slog.Info("Start merging", "tx", txGraphID)

	prefix := txGraphID + "="
	txGraphRoot := prefix + BUILT_IN_ROOT

	main := treeToMap(ctx, BUILT_IN_ROOT)
	txGraph := treeToMap(ctx, txGraphRoot)

	created := make(map[string]struct{})
	for _, n := range main {
		created[n.parent] = struct{}{}
		created[n.child] = struct{}{}
		created[n.NormalID(prefix)] = struct{}{}
	}

	new := make(map[string]node)

	for _, n := range txGraph {
		normalID := n.NormalID(prefix)
		if _, ok := main[normalID]; ok {
			continue
		}

		new[normalID] = n
	}

	// create new elements
	for _, n := range new {
		// create parent if need
		normalParentID := strings.TrimPrefix(n.parent, prefix)
		if _, ok := created[normalParentID]; !ok {
			body, err := ctx.GlobalCache.GetValueAsJSON(n.parent)
			if err != nil {
				return err
			}

			payload := easyjson.NewJSONObjectWithKeyValue("body", *body)

			// TODO: use high level api?
			if _, err = ctx.Request(sfplugins.GolangLocalRequest, "functions.graph.ll.api.object.create", normalParentID, &payload, nil); err != nil {
				return err
			}

			created[normalParentID] = struct{}{}
		}

		// create child if need
		normalChildID := strings.TrimPrefix(n.child, prefix)
		if _, ok := created[normalChildID]; !ok {
			body, err := ctx.GlobalCache.GetValueAsJSON(n.child)
			if err != nil {
				return err
			}

			payload := easyjson.NewJSONObjectWithKeyValue("body", *body)

			// TODO: use high level api?
			if _, err = ctx.Request(sfplugins.GolangLocalRequest, "functions.graph.ll.api.object.create", normalChildID, &payload, nil); err != nil {
				return err
			}

			created[normalChildID] = struct{}{}
		}

		// create link if need
		normalLinkTypeID := n.NormalID(prefix)
		if _, ok := created[normalLinkTypeID]; !ok {
			err := createLowLevelLink(ctx, normalParentID, normalChildID, strings.TrimPrefix(n.lt, prefix), "")
			if err != nil {
				return err
			}

			created[normalLinkTypeID] = struct{}{}
		}
	}

	return nil
}

type node struct {
	parent string
	child  string
	lt     string
}

func (n node) ID() string {
	return n.parent + n.child + n.lt
}

func (n node) NormalID(prefix string) string {
	return strings.TrimPrefix(n.parent, prefix) + strings.TrimPrefix(n.child, prefix) + strings.TrimPrefix(n.lt, prefix)
}

func treeToMap(ctx *sfplugins.StatefunContextProcessor, startPoint string) map[string]node {
	visited := make(map[string]node)

	root := node{
		child: startPoint,
	}

	queue := list.New()
	queue.PushBack(root)

	for queue.Len() > 0 {
		e := queue.Front()
		queue.Remove(e)

		node, ok := e.Value.(node)
		if !ok {
			continue
		}

		if _, exists := visited[node.ID()]; !exists {
			visited[node.ID()] = node
		}

		for _, n := range getChildren(ctx, node.child) {
			if _, ok := visited[n.ID()]; !ok {
				queue.PushBack(n)
			}
		}
	}

	delete(visited, root.ID())

	return visited
}

func getChildren(ctx *sfplugins.StatefunContextProcessor, id string) []node {
	pattern := id + ".out.ltp_oid-bdy.>"
	children := ctx.GlobalCache.GetKeysByPattern(pattern)

	nodes := make([]node, 0, len(children))

	for _, v := range children {
		split := strings.Split(v, ".")
		if len(split) == 0 {
			continue
		}

		nodes = append(nodes, node{
			parent: id,
			child:  split[len(split)-1],
			lt:     split[len(split)-2],
		})
	}

	return nodes
}
