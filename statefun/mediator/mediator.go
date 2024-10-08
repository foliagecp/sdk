package mediator

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/foliagecp/easyjson"

	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
)

type OpType = uint8

const (
	MereOp                       OpType = iota // Call came from an initial client
	WorkerIsTaskedByAggregatorOp               // Call came from a higher function
	AggregatorRepliedByWorkerOp                // Call came from a subordinate function
	AggregatedWorkersOp                        // Call came from the last subordinate function, aggregated everything
)

const (
	aggrPack                       = "__mAggrPack"
	aggrPackTempl                  = aggrPack + ".%s"
	gcIntervalSec                  = 60
	replyStoreRecordExpirationSecs = 120
)

var (
	replyStore           map[string]SyncReplyPack = map[string]SyncReplyPack{}
	replyStoreMutex      sync.Mutex
	replyStoreLastGCTime time.Time
)

type OpMediator struct {
	ctx        *sfPlugins.StatefunContextProcessor
	opMsgs     []OpMsg
	mediatorId string
	opType     OpType
	meta1      string
}

type SyncReplyPack struct {
	syncReply *sfPlugins.SyncReply
	created   time.Time
}

func NewOpMediator(ctx *sfPlugins.StatefunContextProcessor) *OpMediator {
	opMediator, _ := NewOpMediatorWithUniquenessControl(ctx, system.GetUniqueStrID)
	return opMediator
}

func NewOpMediatorWithUniquenessControl(ctx *sfPlugins.StatefunContextProcessor, uniqueIdGenerator func() string) (om *OpMediator, unque bool) {
	unque = true
	mediatorId := uniqueIdGenerator()
	opType := MereOp
	opMsgs := []OpMsg{}
	meta1 := ""
	if ctx.Payload != nil && ctx.Payload.IsNonEmptyObject() {
		if aggrId, ok := ctx.Payload.GetByPath("__mAggregationId").AsString(); ok {
			opType = WorkerIsTaskedByAggregatorOp
			meta1 = aggrId

			funcContext := ctx.GetFunctionContext()
			aggrPackPath := fmt.Sprintf(aggrPackTempl, mediatorId)
			if funcContext.PathExists(aggrPackPath) { // Mediator with this mediatorId on current object was already executed, terminate cause maybe a loop
				unque = false
			}
		}
		if s, ok := ctx.Payload.GetByPath("__mAggregationIdReply").AsString(); ok {
			mediatorId = s
			opType = AggregatorRepliedByWorkerOp

			funcContext := ctx.GetFunctionContext()
			// Update the aggregation pack --------------------------
			aggrPackPath := fmt.Sprintf(aggrPackTempl, mediatorId)
			aggregationPack := funcContext.GetByPath(aggrPackPath)
			if !aggregationPack.IsNonEmptyObject() {
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
			registeredResults.SetByPath(fmt.Sprintf("%s:%s", strings.ReplaceAll(ctx.Caller.Typename, ".", "_"), ctx.Caller.ID), *ctx.Payload)
			aggregationPack.SetByPath("results", registeredResults)

			for _, key := range registeredResults.ObjectKeys() {
				opMsg := OpMsgFromJson(registeredResults.GetByPath(key).GetPtr())
				opMsg.Meta = key
				opMsgs = append(opMsgs, opMsg)
			}
			// ------------------------------------------------------
			funcContext.SetByPath(aggrPackPath, aggregationPack)
			ctx.SetFunctionContext(funcContext)

			ctx.SetContextExpirationAfter(time.Duration(gcIntervalSec) * time.Second)
		}
	}
	om = &OpMediator{ctx, opMsgs, mediatorId, opType, meta1}
	return
}

func (om *OpMediator) AddIntermediateResult(ctx *sfPlugins.StatefunContextProcessor, intermediateResult *easyjson.JSON) {
	msg := MakeOpMsg(SYNC_OP_STATUS_OK, "", "", *intermediateResult)

	funcContext := ctx.GetFunctionContext()

	// Update the aggregation pack --------------------------
	aggrPackPath := fmt.Sprintf(aggrPackTempl, om.mediatorId)
	aggregationPack := funcContext.GetByPath(aggrPackPath)
	if !aggregationPack.IsNonEmptyObject() {
		aggregationPack = easyjson.NewJSONObject()
	}

	var registeredResults easyjson.JSON
	if aggregationPack.PathExists("results") {
		registeredResults = aggregationPack.GetByPath("results")
	} else {
		registeredResults = easyjson.NewJSONObject()
	}

	registeredResults.SetByPath(fmt.Sprintf("%s:%s", strings.ReplaceAll(ctx.Self.Typename, ".", "_"), fmt.Sprintf("%s-%d", ctx.Self.ID, registeredResults.KeysCount())), *msg.ToJson())
	aggregationPack.SetByPath("results", registeredResults)

	om.opMsgs = append(om.opMsgs, msg)
	// ------------------------------------------------------

	funcContext.SetByPath(aggrPackPath, aggregationPack)
	ctx.SetFunctionContext(funcContext)
}

func (om *OpMediator) AggregateOpMsg(som OpMsg) *OpMediator {
	om.opMsgs = append(om.opMsgs, som)
	return om
}

func (om *OpMediator) GetOpType() OpType {
	return om.opType
}

func (om *OpMediator) GetAggregatedOpMsgs() []OpMsg {
	return om.opMsgs
}

func (om *OpMediator) GetLastSyncOp() OpMsg {
	if len(om.opMsgs) > 0 {
		return om.opMsgs[len(om.opMsgs)-1]
	}
	return OpMsgIdle("unknown")
}

func (om *OpMediator) GetStatus() OpStatus {
	var status *OpStatus = nil
	for _, som := range om.opMsgs {
		if status == nil {
			status = &som.Status
		} else {
			*status = OpStatusMatrix[*status][som.Status]
		}
	}
	if status != nil {
		return *status
	}
	return GetSyncOpIntegratedStatusWithDefault(om.opMsgs, SYNC_OP_STATUS_IDLE)
}

func (om *OpMediator) GetDetails() string {
	allDetails := []string{}
	for _, som := range om.opMsgs {
		if len(som.Details) > 0 {
			allDetails = append(allDetails, som.Details)
		}
	}
	return strings.Join(allDetails, ";")
}

func (om *OpMediator) GetData() easyjson.JSON {
	if len(om.opMsgs) > 0 {
		return om.opMsgs[len(om.opMsgs)-1].Data
	}
	return easyjson.NewJSONNull()
}

func (om *OpMediator) releaseAggPackOnAggregated() *easyjson.JSON {
	funcContext := om.ctx.GetFunctionContext()
	aggrPackPath := fmt.Sprintf(aggrPackTempl, om.mediatorId)
	if funcContext.PathExists(aggrPackPath) {
		aggregationPack := funcContext.GetByPath(aggrPackPath)
		if aggregationPack.IsNonEmptyObject() {
			funcContext.SetByPath(aggrPackPath, easyjson.NewJSONObject()) // Do not delete, just make empty - for loops to be detected
			om.ctx.SetFunctionContext(funcContext)
			om.ctx.SetContextExpirationAfter(time.Duration(gcIntervalSec) * time.Second)
			return &aggregationPack
		}
	}
	return nil
}

func (om *OpMediator) releaseAggPackAndGetParentReplyAggregator(aggregationPack *easyjson.JSON) (syncReply *sfPlugins.SyncReply, ok bool) {
	if aggregationPack == nil {
		return nil, false
	}

	if time.Since(replyStoreLastGCTime).Seconds() > gcIntervalSec {
		go func() {
			replyStoreMutex.Lock()
			defer replyStoreMutex.Unlock()
			// replyStore GC ------------------------------------------------------------
			for srpKey, srp := range replyStore {
				if time.Since(srp.created).Seconds() > replyStoreRecordExpirationSecs {
					delete(replyStore, srpKey)
				}
			}
			// --------------------------------------------------------------------------
		}()
		replyStoreLastGCTime = time.Now()
	}

	syncReplyId := aggregationPack.GetByPath("responseContext.replyId").AsStringDefault("")
	replyStoreMutex.Lock()
	defer replyStoreMutex.Unlock()
	if syncReplyPack, ok := replyStore[syncReplyId]; ok {
		delete(replyStore, syncReplyId)
		return syncReplyPack.syncReply, true
	}
	return nil, false
}

func (om *OpMediator) releaseAggPackAndGetParentSignalAggregator(aggregationPack *easyjson.JSON) (typename string, id string, aggrId string, ok bool) {
	if aggregationPack == nil {
		return "", "", "", false
	}

	typename = aggregationPack.GetByPath("responseContext.typename").AsStringDefault("")
	id = aggregationPack.GetByPath("responseContext.id").AsStringDefault("")
	aggrId = aggregationPack.GetByPath("responseContext.aggrid").AsStringDefault("")
	ok = true
	return
}

func (om *OpMediator) ReplyWithData(data *easyjson.JSON) error {
	var reply *easyjson.JSON
	if data == nil {
		reply = MakeOpMsg(om.GetStatus(), om.GetDetails(), "", om.GetData()).ToJson()
	} else {
		reply = MakeOpMsg(om.GetStatus(), om.GetDetails(), "", *data).ToJson()
	}

	if om.opType == AggregatedWorkersOp {
		if aggregationPack := om.releaseAggPackOnAggregated(); aggregationPack != nil {
			if syncReply, ok := om.releaseAggPackAndGetParentReplyAggregator(aggregationPack); ok {
				syncReply.With(reply)
			} else {
				if paTypename, paId, aggrId, ok := om.releaseAggPackAndGetParentSignalAggregator(aggregationPack); ok {
					if len(paTypename) == 0 || len(paId) == 0 {
						return om.ctx.Egress(sfPlugins.NatsCoreEgress, reply)
					} else {
						if len(aggrId) > 0 {
							reply.SetByPath("__mAggregationIdReply", easyjson.NewJSON(aggrId))
						}
						return om.ctx.Signal(sfPlugins.JetstreamGlobalSignal, paTypename, paId, reply, nil)
					}
				}
			}
		}
	} else {
		if om.opType == MereOp || om.opType == WorkerIsTaskedByAggregatorOp {
			if om.ctx.Reply != nil {
				om.ctx.Reply.With(reply)
			} else {
				if len(om.ctx.Caller.Typename) == 0 || len(om.ctx.Caller.ID) == 0 {
					return om.ctx.Egress(sfPlugins.NatsCoreEgress, reply)
				} else {
					if om.opType == WorkerIsTaskedByAggregatorOp {
						reply.SetByPath("__mAggregationIdReply", easyjson.NewJSON(om.meta1))
					}
					return om.ctx.Signal(sfPlugins.JetstreamGlobalSignal, om.ctx.Caller.Typename, om.ctx.Caller.ID, reply, nil)
				}
			}
		}
	}

	return nil
}

func (om *OpMediator) Reply() {
	system.MsgOnErrorReturn(om.ReplyWithData(nil))
}

func (om *OpMediator) SignalWithAggregation(provider sfPlugins.SignalProvider, typename string, id string, payload *easyjson.JSON, options *easyjson.JSON) error {
	// Create payload to send for a descendant ------------
	if payload == nil || !payload.IsNonEmptyObject() {
		payload = easyjson.NewJSONObject().GetPtr()
	}
	payload.SetByPath("__mAggregationId", easyjson.NewJSON(om.mediatorId))
	// ----------------------------------------------------
	if err := om.ctx.Signal(provider, typename, id, payload, options); err != nil {
		return err
	}

	funcContext := om.ctx.GetFunctionContext()

	// Create an aggreagtion pack -------------------------
	aggrPackPath := fmt.Sprintf(aggrPackTempl, om.mediatorId)
	aggregationPack := funcContext.GetByPath(aggrPackPath)
	if !aggregationPack.IsNonEmptyObject() {
		aggregationPack = easyjson.NewJSONObject()
	}
	registeredCallbacks := int(aggregationPack.GetByPath("callbacks").AsNumericDefault(0))
	registeredCallbacks++
	aggregationPack.SetByPath("callbacks", easyjson.NewJSON(registeredCallbacks))

	// Create an aggreagtion pack -------------------------
	if !aggregationPack.PathExists("responseContext") {
		if om.ctx.Reply != nil {
			// If this function in its turn should send sync reply to parent aggregator -
			aggregationPack.SetByPath("responseContext.replyId", easyjson.NewJSON(om.mediatorId))
			replyStoreMutex.Lock()
			replyStore[om.mediatorId] = SyncReplyPack{om.ctx.Reply.OverrideRequestCallback(), time.Now()}
			replyStoreMutex.Unlock()
			// --------------------------------------------------------------------------
		} else {
			// If this function in its turn should send singal to parent aggregator -----
			aggregationPack.SetByPath("responseContext.typename", easyjson.NewJSON(om.ctx.Caller.Typename))
			aggregationPack.SetByPath("responseContext.id", easyjson.NewJSON(om.ctx.Caller.ID))
			aggregationPack.SetByPath("responseContext.aggrid", om.ctx.Payload.GetByPath("__mAggregationId")) // Store this __mAggregationId for future
			// --------------------------------------------------------------------------
		}
	}
	// ----------------------------------------------------

	funcContext.SetByPath(aggrPackPath, aggregationPack)
	// ----------------------------------------------------
	om.ctx.SetFunctionContext(funcContext)
	om.ctx.SetContextExpirationAfter(time.Duration(gcIntervalSec) * time.Second)

	return nil
}
