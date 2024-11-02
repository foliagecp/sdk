package crud

import (
	"fmt"
	"strings"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/statefun/mediator"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
)

type GraphCRUDDispatcher func(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, operation string, opTime int64, data *easyjson.JSON)

var (
	graphCRUDDispatcherFromTarget = map[string]GraphCRUDDispatcher{
		"vertex":      GraphVertexCRUD_Dispatcher,
		"vertex.link": GraphVertexLinkCRUD_Dispatcher,
	}
)

/*
GraphCRUDGateway. Garanties sequential order for all graph api calls
This function works via signals and request-reply.

Request:

	payload: json - optional
		operation: json
			type: string - requred // supported values (case insensitive): "create", "update", "delete", "read"
			target: string - requred // supported values (case insensitive): "vertex", "vertex.link"
		data: json - required // operation data

	options: json - optional
		return_stack: bool - optional

Reply:

	payload: json
		status: string
		details: string - optional, if any exists
		data: json - optional, if any exists
			operation: json
				type: string - required // "create", "update", "delete", "read"
				target: string - required // "vertex", "vertex.link"
			op_stack: json array - optional
*/
func GraphCRUDGateway(sfExec sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	om := sfMediators.NewOpMediator(ctx)

	meta := om.GetMeta(ctx)
	target := strings.ToLower(meta.GetByPath("operation.target").AsStringDefault(""))
	operation := strings.ToLower(meta.GetByPath("operation.type").AsStringDefault(""))

	if len(target) == 0 {
		target = ctx.Payload.GetByPath("operation.target").AsStringDefault("")
		operation = ctx.Payload.GetByPath("operation.type").AsStringDefault("")
		meta := easyjson.NewJSONObject()
		meta.SetByPath("operation.target", easyjson.NewJSON(target))
		meta.SetByPath("operation.type", easyjson.NewJSON(operation))
		om.SetMeta(ctx, meta)
	}
	GraphCRUDController(ctx, om, target, operation)
}

/* All operations start executing in the same order they were initiated (signalled).
 * If all perations have the same execution stages (depth) they all will also end working in the same order */
func GraphCRUDController(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, target string, operation string) {
	var dispatcher *GraphCRUDDispatcher
	if d, ok := graphCRUDDispatcherFromTarget[target]; ok {
		dispatcher = &d
	} else {
		// TODO: Return error msg
		return
	}

	switch om.GetOpType() {
	case mediator.MereOp:
		if len(ctx.Options.GetByPath("op_time").AsStringDefault("")) == 0 {
			forwardOptions := ctx.Options.Clone()
			forwardOptions.SetByPath("op_time", easyjson.NewJSON(fmt.Sprintf("%d", system.GetCurrentTimeNs())))
			om.SignalWithAggregation(sfPlugins.AutoSignalSelect, ctx.Self.Typename, ctx.Self.ID, ctx.Payload, &forwardOptions)
			return
		}
		fallthrough
	case mediator.WorkerIsTaskedByAggregatorOp:
		data := ctx.Payload.GetByPath("data")
		optTimeStr := ctx.Options.GetByPath("op_time").AsStringDefault("")
		if len(optTimeStr) > 0 {
			(*dispatcher)(ctx, om, operation, system.Str2Int(optTimeStr), &data)
		} else {
			om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("GraphCRUDController for target %s detected no op_time", target))).Reply()
		}
	case mediator.AggregatedWorkersOp:
		opInfo := easyjson.NewJSONObject()
		opInfo.SetByPath("operation.type", easyjson.NewJSON(operation))
		opInfo.SetByPath("operation.target", easyjson.NewJSON(target))
		om.SetAdditionalReplyData(&opInfo)

		aggregatedData := unifiedCRUDDataAggregator(om)
		system.MsgOnErrorReturn(om.ReplyWithData(&aggregatedData))
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

/*func GraphVertexReadLoose(sfExec sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	om := sfMediators.NewOpMediator(ctx)
	GraphVertexRead(ctx, om, system.GetCurrentTimeNs(), ctx.Payload)
}

func GraphLinkReadLoose(sfExec sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	om := sfMediators.NewOpMediator(ctx)
	GraphVertexLinkRead(ctx, om, system.GetCurrentTimeNs(), ctx.Payload)
}*/

func getVertexLinkNameTypeTargetFromVariousIdentifiers(ctx *sfPlugins.StatefunContextProcessor, linkDataContainer *easyjson.JSON) (linkName string, linkType string, linkTargetId string, err error) {
	linkName = linkDataContainer.GetByPath("name").AsStringDefault("")
	linkType = linkDataContainer.GetByPath("type").AsStringDefault("")
	linkTargetId = ctx.Domain.CreateObjectIDWithThisDomain(linkDataContainer.GetByPath("to").AsStringDefault(""), false)

	if len(linkName) > 0 {
		linkTargetBytes, err := ctx.Domain.Cache().GetValue(fmt.Sprintf(OutLinkTargetKeyPrefPattern+LinkKeySuff1Pattern, ctx.Self.ID, linkName))
		if err != nil {
			return "", "", "", fmt.Errorf("link from=%s with name=%s does not exist", ctx.Self.ID, linkName)
		}
		linkTargetStr := string(linkTargetBytes)
		linkTargetTokens := strings.Split(linkTargetStr, ".")
		if len(linkTargetTokens) != 2 || len(linkTargetTokens[0]) == 0 || len(linkTargetTokens[1]) == 0 {
			return "", "", "", fmt.Errorf("link from=%s with name=%s, has invalid target: %s", ctx.Self.ID, linkName, linkTargetStr)
		}
		return linkName, linkTargetTokens[0], linkTargetTokens[1], nil
	} else {
		if len(linkTargetId) > 0 {
			if len(linkType) > 0 {
				linkNameBytes, err := ctx.Domain.Cache().GetValue(fmt.Sprintf(OutLinkTypeKeyPrefPattern+LinkKeySuff2Pattern, ctx.Self.ID, linkType, linkTargetId))
				if err != nil {
					return "", "", "", fmt.Errorf("link from=%s to=%s with type=%s does not exist", ctx.Self.ID, linkTargetId, linkType)
				}
				return string(linkNameBytes), linkType, linkTargetId, nil
			}
		}
	}
	return "", "", "", fmt.Errorf("not enough information about link, link name or link type with link target id are needed")
}
