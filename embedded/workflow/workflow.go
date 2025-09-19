package workflow

import (
	"fmt"
	"strings"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/statefun"
	lg "github.com/foliagecp/sdk/statefun/logger"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
)

const (
	CTX_CALLBACK_RESULT_PATH  = "workflow.callback.%s.result"
	WORKFLOW_PREFIX_OF_SECRET = "slave"

	ctxSecretPath = "workflow.secret"
	ctxPausedPath = "workflow.paused"
)

const (
	WF_STATE_RUNNING = "RUNNING"
	WF_STATE_PAUSED  = "PAUSED"
	WF_STATE_STOPPED = "STOPPED"
)

type WorkflowTools struct {
	ctx                   *sfPlugins.StatefunContextProcessor
	secret                string
	workflow              *WorkflowEngine
	callbackUUIDGenerator int
}

type ActivityOptions struct {
	Timeout time.Duration
}

func (wt *WorkflowTools) ExecActivity(activity *WorkflowActivity, data easyjson.JSON, activityOptions *ActivityOptions) *easyjson.JSON {
	wt.callbackUUIDGenerator++

	strUUID := fmt.Sprint(wt.callbackUUIDGenerator)
	if existingResult, ok := wt.workflow.getActivityResultFromStatefunCtx(strUUID, wt.ctx); ok {
		return &existingResult
	}

	// id = <callback_uud>-<secret>
	// secret = slave_<ustr>
	wt.ctx.Signal(sfPlugins.AutoSignalSelect, activity.statefunName, strUUID+"-"+wt.secret, &data, wt.ctx.Options)
	if activityOptions != nil {
		payload := easyjson.NewJSONObject()
		payload.SetByPath("cmd", easyjson.NewJSON("schedule_once"))
		payload.SetByPath("task.caller_typename", easyjson.NewJSON(activity.statefunName))
		payload.SetByPath("task.caller_id", easyjson.NewJSON(strUUID+"-"+wt.secret))
		payload.SetByPath("task.target_typename", easyjson.NewJSON(wt.ctx.Self.Typename))
		payload.SetByPath("task.target_id", easyjson.NewJSON(wt.ctx.Self.ID))
		payload.SetByPath("task.due_in_ms", easyjson.NewJSON(activityOptions.Timeout.Milliseconds()))

		_ = wt.ctx.Signal(sfPlugins.JetstreamGlobalSignal, DelayedSignalGeneratorTypename, "timer", &payload, nil)
	}

	panic(workflowStop{}) // Soft workflow termination
}

type workflowStop struct{}

type WorkflowLogicHandler func(tools WorkflowTools)

type WorkflowEngine struct {
	statefunName string
	logicHandler WorkflowLogicHandler
}

func NewWorkflowEngine(logicHandler WorkflowLogicHandler, statefunName string) *WorkflowEngine {
	return &WorkflowEngine{
		statefunName: statefunName,
		logicHandler: logicHandler,
	}
}

func (w *WorkflowEngine) RegisterStatefun(runtime *statefun.Runtime) {
	statefun.NewFunctionType(
		runtime,
		w.statefunName,
		w.workflowStatefun,
		*statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).SetMultipleInstancesAllowance(false).SetMaxIdHandlers(-1),
	)
}

func (w *WorkflowEngine) workflowStatefun(_ sfPlugins.StatefunExecutor, sfctx *sfPlugins.StatefunContextProcessor) {
	isRunning := false

	ctxData := sfctx.GetFunctionContext()
	secret := ctxData.GetByPath(ctxSecretPath).AsStringDefault("")
	if len(secret) != 0 { // Secret is not initialized, workflow is not stared
		isRunning = true
	}

	isPaused := ctxData.GetByPath(ctxPausedPath).AsBoolDefault(false)

	// Processing commands --------------------------------
	if sfctx.Payload.PathExists("cmd") {
		cmd := sfctx.Payload.GetByPath("cmd").AsStringDefault("")

		om := sfMediators.NewOpMediator(sfctx)
		cmdReply := easyjson.NewJSONObject()
		cmdUnknown := true
		defer func() {
			if cmdUnknown {
				om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("command \"%s\" is unknown", cmd))).Reply()
			} else {
				om.AggregateOpMsg(sfMediators.OpMsgOk(cmdReply)).Reply()
			}
		}()

		if cmd == "status" {
			cmdUnknown = false
			status := easyjson.NewJSONObject()

			state := WF_STATE_STOPPED
			if isRunning {
				if isPaused {
					state = WF_STATE_PAUSED
				} else {
					state = WF_STATE_RUNNING
				}
			}
			status.SetByPath("state", easyjson.NewJSON(state))
			cmdReply = status
			return
		}

		if !isRunning {
			if cmd == "start" {
				cmdUnknown = false
				secret = WORKFLOW_PREFIX_OF_SECRET + "_" + system.GetUniqueStrID()
				ctxData.SetByPath(ctxSecretPath, easyjson.NewJSON(secret))
				sfctx.SetFunctionContextImmediately(ctxData)
				isRunning = true
			}
		} else {
			if cmd == "pause" {
				cmdUnknown = false
				ctxData.SetByPath(ctxPausedPath, easyjson.NewJSON(true))
				sfctx.SetFunctionContextImmediately(ctxData)
				return
			}
			if cmd == "resume" {
				cmdUnknown = false
				ctxData.SetByPath(ctxPausedPath, easyjson.NewJSON(false))
				sfctx.SetFunctionContextImmediately(ctxData)
				isPaused = false
			}
			if cmd == "stop" {
				cmdUnknown = false
				sfctx.SetFunctionContextImmediately(easyjson.NewJSONObject().GetPtr())
				return
			}
		}
	}
	// ----------------------------------------------------

	if isRunning {
		callerIdTokens := strings.Split(sfctx.Domain.GetObjectIDWithoutDomain(sfctx.Caller.ID), "-")
		callback := len(callerIdTokens) == 2 && callerIdTokens[1] == secret

		if callback {
			if err := w.setActivityResultIntoStatefunCtx(callerIdTokens[0], *sfctx.Payload, sfctx); err != nil {
				lg.Logln(lg.WarnLevel, "Workflow %s received activity callback, but could not process it: %s", sfctx.Self.ID, err.Error())
				return // cannot continue
			}
		}

		if isPaused { // If paused
			return
		}

		tools := WorkflowTools{
			ctx:                   sfctx,
			secret:                secret,
			workflow:              w,
			callbackUUIDGenerator: 0,
		}

		defer func() {
			if r := recover(); r != nil {
				if _, ok := r.(workflowStop); ok {
					// soft stop
					return
				}
				// real panic
				panic(r)
			}
		}()

		w.logicHandler(tools)

		// clean function context when workflow has reached its end
		sfctx.SetFunctionContextImmediately(easyjson.NewJSONObject().GetPtr())
	}
}

func (w *WorkflowEngine) getActivityResultFromStatefunCtx(activityUUID string, sfctx *sfPlugins.StatefunContextProcessor) (res easyjson.JSON, exists bool) {
	funcContext := sfctx.GetFunctionContext()

	resPath := fmt.Sprintf(CTX_CALLBACK_RESULT_PATH, activityUUID)

	if funcContext.PathExists(resPath) {
		return funcContext.GetByPath(resPath), true
	}

	return easyjson.NewJSONObject(), false
}

func (w *WorkflowEngine) setActivityResultIntoStatefunCtx(activityUUID string, data easyjson.JSON, sfctx *sfPlugins.StatefunContextProcessor) error {
	funcContext := sfctx.GetFunctionContext()

	resPath := fmt.Sprintf(CTX_CALLBACK_RESULT_PATH, activityUUID)

	if funcContext.PathExists(resPath) {
		return fmt.Errorf("data for this activityId already exists")
	}
	if ok := funcContext.SetByPath(resPath, data); !ok {
		return fmt.Errorf("could not set data by path '%s'", resPath)
	}

	sfctx.SetFunctionContextImmediately(funcContext)

	return nil
}
