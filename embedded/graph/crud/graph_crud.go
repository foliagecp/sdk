package crud

import (
	"fmt"
	"strings"

	"github.com/foliagecp/easyjson"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
)

/*
GraphCUDGateway. Garanties sequential order for all graph api calls
This function works via signals and request-reply.

Request:

	payload: json - optional
		target: string - requred // supported values (case insensitive): "vertex", "link"
		operation: string - requred // supported values (case insensitive): "create", "update", "delete", "read"
		data: json - required // operation data

	options: json - optional
		return_stack: bool - optional

Reply:

	payload: json
		status: string
		details: string - optional, if any exists
		data: json - optional, id any exists
			target: string - required // "vertex", "link"
			operation: string - required // "create", "update", "delete"
			op_stack: json array - optional
*/
func GraphCRUDGateway(sfExec sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	om := sfMediators.NewOpMediator(ctx)

	meta := om.GetMeta(ctx)
	target := meta.GetByPath("target").AsStringDefault("")
	operation := meta.GetByPath("operation").AsStringDefault("")

	if len(target) == 0 {
		target = ctx.Payload.GetByPath("target").AsStringDefault("")
		operation = ctx.Payload.GetByPath("operation").AsStringDefault("")
		meta := easyjson.NewJSONObjectWithKeyValue("target", easyjson.NewJSON(target))
		meta.SetByPath("operation", easyjson.NewJSON(operation))
		om.SetMeta(ctx, meta)
	}
	switch strings.ToLower(target) {
	case "vertex":
		GraphVertexCRUD(sfExec, ctx, om, operation)
	case "link":
		GraphLinkCRUD(sfExec, ctx, om, operation)
	default:

	}
}

// Prevents execution of CRUD's block of intructions (when there is younger operation that already changed the same block) to remain data consistency
func FixateOperationIdTime(ctx *sfPlugins.StatefunContextProcessor, id string, opTime int64) bool {
	funcContext := ctx.GetFunctionContext()
	path := fmt.Sprintf("op.%s-%s-%s", system.GetHashStr(ctx.Self.Typename), ctx.Self.ID, id)
	alreadyFixatedTime := system.Str2Int(funcContext.GetByPath(path).AsStringDefault(""))
	if alreadyFixatedTime > opTime {
		return false
	}
	funcContext.SetByPath(path, easyjson.NewJSON(system.IntToStr(opTime)))
	ctx.SetFunctionContext(funcContext)
	return true
}

func GraphVertexReadLoose(sfExec sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	om := sfMediators.NewOpMediator(ctx)
	GraphVertexRead(ctx, om, ctx.Payload, system.GetCurrentTimeNs())
}

func GraphLinkReadLoose(sfExec sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	om := sfMediators.NewOpMediator(ctx)
	GraphLinkRead(ctx, om, ctx.Payload, system.GetCurrentTimeNs())
}
