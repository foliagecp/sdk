package mediator

import (
	"fmt"
	"strings"

	"github.com/foliagecp/easyjson"

	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
)

type OpType = uint8

const (
	MereOp OpType = iota
	WorkerIsTaskedByAggregatorOp
	AggregatorRepliedByWorkerOp
	AggregatedWorkersOp
)

const (
	aggrPackTempl = "__mAggrPack.%s"
)

type OpMediator struct {
	ctx        *sfPlugins.StatefunContextProcessor
	opMsgs     []OpMsg
	mediatorId string
	opType     OpType
}

func NewOpMediator(ctx *sfPlugins.StatefunContextProcessor) *OpMediator {
	mediatorId := system.GetUniqueStrID()
	opType := MereOp
	opMsgs := []OpMsg{}
	if ctx.Payload != nil && ctx.Payload.IsNonEmptyObject() {
		if s, ok := ctx.Payload.GetByPath("__mAggregationId").AsString(); ok {
			mediatorId = s
			opType = WorkerIsTaskedByAggregatorOp
		}
		if s, ok := ctx.Payload.GetByPath("__mAggregationIdReply").AsString(); ok {
			mediatorId = s
			opType = AggregatorRepliedByWorkerOp

			funcContext := ctx.GetFunctionContext()
			// Update the aggreagtion pack --------------------------
			aggrPackPath := fmt.Sprintf(aggrPackTempl, mediatorId)
			aggregationPack := funcContext.GetByPath(aggrPackPath)
			if aggregationPack.IsNonEmptyObject() {
				aggregationPack = easyjson.NewJSONObject()
			}
			registeredCallbacks := int(aggregationPack.GetByPath("callbacks").AsNumericDefault(0))
			registeredCallbacks--
			if registeredCallbacks <= 0 {
				opType = AggregatedWorkersOp
			}
			aggregationPack.SetByPath("callbacks", easyjson.NewJSON(registeredCallbacks))

			var registeredResults easyjson.JSON
			if aggregationPack.PathExists("results") {
				registeredResults = aggregationPack.GetByPath("results")
			} else {
				registeredResults = easyjson.NewJSONObject()
			}
			msg := OpMsgFromJson(ctx.Payload)
			registeredResults.SetByPath(fmt.Sprintf("%s:%s", ctx.Caller.Typename, ctx.Caller.ID), msg.Data)

			for _, key := range registeredResults.ObjectKeys() {
				opMsg := OpMsgFromJson(registeredResults.GetByPath(key).GetPtr())
				opMsg.Meta = key
				opMsgs = append(opMsgs)
			}
			// ------------------------------------------------------
			funcContext.SetByPath(aggrPackPath, aggregationPack)
		}
	}
	return &OpMediator{ctx, opMsgs, mediatorId, opType}
}

func (sosc *OpMediator) AggregateOpMsg(som OpMsg) *OpMediator {
	sosc.opMsgs = append(sosc.opMsgs, som)
	return sosc
}

func (sosc *OpMediator) GetOpType() OpType {
	return sosc.opType
}

func (sosc *OpMediator) GetAggregatedOpMsgs() []OpMsg {
	return sosc.opMsgs
}

func (sosc *OpMediator) GetLastSyncOp() OpMsg {
	if len(sosc.opMsgs) > 0 {
		return sosc.opMsgs[len(sosc.opMsgs)-1]
	}
	return OpMsgIdle("unknown")
}

func (sosc *OpMediator) GetStatus() OpStatus {
	var status *OpStatus = nil
	for _, som := range sosc.opMsgs {
		if status == nil {
			status = &som.Status
		} else {
			*status = OpStatusMatrix[*status][som.Status]
		}
	}
	if status != nil {
		return *status
	}
	return GetSyncOpIntegratedStatusWithDefault(sosc.opMsgs, SYNC_OP_STATUS_IDLE)
}

func (sosc *OpMediator) GetDetails() string {
	allDetails := []string{}
	for _, som := range sosc.opMsgs {
		if len(som.Details) > 0 {
			allDetails = append(allDetails, som.Details)
		}
	}
	return strings.Join(allDetails, ";")
}

func (sosc *OpMediator) GetData() easyjson.JSON {
	if len(sosc.opMsgs) > 0 {
		return sosc.opMsgs[len(sosc.opMsgs)-1].Data
	}
	return easyjson.NewJSONNull()
}

func (sosc *OpMediator) releaseAggPackAndGetParentAggregator() (typename string, id string, aggrId string, ok bool) {
	if sosc.opType == AggregatedWorkersOp {
		funcContext := sosc.ctx.GetFunctionContext()

		aggrPackPath := fmt.Sprintf(aggrPackTempl, sosc.mediatorId)
		if funcContext.PathExists(aggrPackPath) {
			aggregationPack := funcContext.GetByPath(aggrPackPath)
			if aggregationPack.IsNonEmptyObject() {
				funcContext.RemoveByPath(aggrPackPath)
				sosc.ctx.SetFunctionContext(funcContext)

				typename = funcContext.GetByPath("parentAggregator.typename").AsStringDefault("")
				id = funcContext.GetByPath("parentAggregator.id").AsStringDefault("")
				aggrId = funcContext.GetByPath("parentAggregator.aggrid").AsStringDefault("")
				ok = true
				return
			}
		}
	}
	return "", "", "", false
}

func (sosc *OpMediator) ReplyWithData(data *easyjson.JSON) error {
	var reply *easyjson.JSON
	if data == nil {
		reply = MakeOpMsg(sosc.GetStatus(), sosc.GetDetails(), "", sosc.GetData()).ToJson()
	} else {
		sosc.ctx.Reply.With(MakeOpMsg(sosc.GetStatus(), sosc.GetDetails(), "", *data).ToJson())
	}

	if sosc.ctx.Reply != nil {
		sosc.ctx.Reply.With(reply)
	} else {
		if paTypename, paId, aggrId, ok := sosc.releaseAggPackAndGetParentAggregator(); ok {
			if len(aggrId) > 0 {
				reply.SetByPath("__mAggregationIdReply", easyjson.NewJSON(aggrId))
			}
			return sosc.ctx.Signal(sfPlugins.JetstreamGlobalSignal, paTypename, paId, reply, nil)
		} else {
			if len(sosc.ctx.Caller.Typename) == 0 || len(sosc.ctx.Caller.ID) == 0 {
				return sosc.ctx.Egress(sfPlugins.NatsCoreEgress, reply)
			} else {
				return sosc.ctx.Signal(sfPlugins.JetstreamGlobalSignal, sosc.ctx.Caller.Typename, sosc.ctx.Caller.ID, easyjson.NewJSONObjectWithKeyValue("__mData", *reply).GetPtr(), nil)
			}
		}
	}
	return nil
}

func (sosc *OpMediator) Reply() {
	sosc.ReplyWithData(nil)
}

func (sosc *OpMediator) SignalWithAggregation(provider sfPlugins.SignalProvider, typename string, id string, payload *easyjson.JSON, options *easyjson.JSON) error {
	// Create payload to send for a descendant ------------
	if payload == nil || !payload.IsNonEmptyObject() {
		payload = easyjson.NewJSONObject().GetPtr()
	}
	payload.SetByPath("__mAggregationId", easyjson.NewJSON(sosc.mediatorId))
	// ----------------------------------------------------
	if err := sosc.ctx.Signal(provider, typename, id, payload, options); err != nil {
		return err
	}

	funcContext := sosc.ctx.GetFunctionContext()
	// Create an aggreagtion pack -------------------------
	aggrPackPath := fmt.Sprintf(aggrPackTempl, sosc.mediatorId)
	aggregationPack := funcContext.GetByPath(aggrPackPath)
	if aggregationPack.IsNonEmptyObject() {
		aggregationPack = easyjson.NewJSONObject()
	}
	registeredCallbacks := int(aggregationPack.GetByPath("callbacks").AsNumericDefault(0))
	registeredCallbacks++
	aggregationPack.SetByPath("callbacks", easyjson.NewJSON(registeredCallbacks))

	// If this function in its turn should send singal to parent aggregator -------------
	if !aggregationPack.PathExists("parentAggregator") {
		aggregationPack.SetByPath("parentAggregator.typename", easyjson.NewJSON(sosc.ctx.Caller.Typename))
		aggregationPack.SetByPath("parentAggregator.id", easyjson.NewJSON(sosc.ctx.Caller.ID))
		if sosc.opType == WorkerIsTaskedByAggregatorOp {
			aggregationPack.SetByPath("parentAggregator.aggrid", sosc.ctx.Payload.GetByPath("__mAggregationId")) // Store this __mAggregationId for future
		}
	}
	// ----------------------------------------------------------------------------------
	funcContext.SetByPath(aggrPackPath, aggregationPack)
	// ----------------------------------------------------
	sosc.ctx.SetFunctionContext(funcContext)

	return nil
}
