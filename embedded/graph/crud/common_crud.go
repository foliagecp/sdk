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

const (
	// crud_op.<op_target>.<op_id>
	opRegisterPrefixTemplate = "crud_op.%s.%s"
	// crud_op.<op_target>.<op_id>.<op_type>.<op_time>
	opRegisterTemplate = opRegisterPrefixTemplate + ".%s.%d"
)

func crudRegisterOperation(ctx *sfPlugins.StatefunContextProcessor, target, operation string, opTime int64) {
	ctx.Domain.Cache().SetValueKVSync(fmt.Sprintf(opRegisterTemplate, target, ctx.Self.ID, operation, opTime), []byte{0}, -1)
}

func crudUnregisterOperation(ctx *sfPlugins.StatefunContextProcessor, target, operation string, opTime int64) {
	ctx.Domain.Cache().DeleteValueKVSync(fmt.Sprintf(opRegisterTemplate, target, ctx.Self.ID, operation, opTime), -1)
}

func dirtyReadAppendMeta(ctx *sfPlugins.StatefunContextProcessor, om *sfMediators.OpMediator, target, operation string, opTime int64) {
	olderCUDOps, allCUDOps := 0, 0

	regOpKeys := ctx.Domain.Cache().GetKeysByPattern(fmt.Sprintf(opRegisterPrefixTemplate, target, ctx.Self.ID) + ".>")
	for _, regOpKey := range regOpKeys {
		tokens := strings.Split(regOpKey, ".")
		opType := tokens[3]
		if opType == "create" || opType == "update" || opType == "delete" {
			opTimeStr := tokens[4]
			if len(opTimeStr) > 0 {
				if system.Str2Int(opTimeStr) < opTime {
					olderCUDOps++
				}
				allCUDOps++
			}
		}
	}

	readInfo := easyjson.NewJSONObject()
	readInfo.SetByPath("not_finished_cud.older", easyjson.NewJSON(olderCUDOps))
	readInfo.SetByPath("not_finished_cud.all", easyjson.NewJSON(allCUDOps))

	detailsStr := ""
	if olderCUDOps > 0 {
		detailsStr += fmt.Sprintf("not_finished_cud.older=%d ", olderCUDOps)
	}
	if allCUDOps > 0 {
		detailsStr += fmt.Sprintf("not_finished_cud.all=%d ", allCUDOps)
	}

	msg := mediator.MakeOpMsg(mediator.SYNC_OP_STATUS_IDLE, detailsStr, "", easyjson.NewJSONObject())
	om.AddIntermediateResultMsg(ctx, msg)

	om.SetAdditionalReplyData(ctx, &readInfo)
}

func unifiedCRUDDataAggregator(om *sfMediators.OpMediator) easyjson.JSON {
	aggregatedData := easyjson.NewJSONNull()
	for _, opMsg := range om.GetAggregatedOpMsgs() {
		if opMsg.Data.IsNonEmptyObject() {
			if aggregatedData.IsNull() {
				aggregatedData = opMsg.Data.Clone()
			} else {
				aggregatedData.DeepMerge(opMsg.Data)
			}
		}
	}
	return aggregatedData
}
