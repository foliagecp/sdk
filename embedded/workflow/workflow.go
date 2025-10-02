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
	ctxStagePath  = "workflow.info"
	ctxTaskPath   = "workflow.task"
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
	Retries int
}

func (wt *WorkflowTools) SetStageProgressInfo(name string) {
	ctxData := wt.ctx.GetFunctionContext()
	ctxData.SetByPath(ctxStagePath, easyjson.NewJSON(name))
	wt.ctx.SetFunctionContextImmediately(ctxData)
}

func (wt *WorkflowTools) setTaskDetails(taskData easyjson.JSON) {
	ctxData := wt.ctx.GetFunctionContext()
	ctxData.SetByPath(ctxTaskPath, taskData)
	wt.ctx.SetFunctionContextImmediately(ctxData)
}

func (wt *WorkflowTools) ExecActivity(activity *WorkflowActivity, data easyjson.JSON, activityOptions *ActivityOptions) *easyjson.JSON {
	timerTimeoutMs := max(activityOptions.Timeout.Milliseconds(), 1000)
	retries := max(activityOptions.Retries, 0)

	wt.callbackUUIDGenerator++

	strUUID := fmt.Sprint(wt.callbackUUIDGenerator)

	existingResult, retry := wt.workflow.getActivityResultFromStatefunCtx(strUUID, wt.ctx)
	if retry == -1 || (retry > 0 && retry == retries) {
		return &existingResult
	}

	// id = <callback_uud>-<secret>
	// secret = slave_<ustr>
	opts := wt.ctx.Options.Clone()
	opts.RemoveByPath("retry")
	wt.ctx.Signal(sfPlugins.AutoSignalSelect, activity.statefunName, strUUID+"-"+wt.secret, &data, &opts)
	if activityOptions != nil {
		optionsFromTimer := wt.ctx.Options.Clone()
		optionsFromTimer.SetByPath("retry", easyjson.NewJSON(retry+1))

		payload := easyjson.NewJSONObject()
		payload.SetByPath("cmd", easyjson.NewJSON("schedule_once"))
		payload.SetByPath("task.caller_typename", easyjson.NewJSON(activity.statefunName))
		payload.SetByPath("task.caller_id", easyjson.NewJSON(strUUID+"-"+wt.secret))
		payload.SetByPath("task.target_typename", easyjson.NewJSON(wt.ctx.Self.Typename))
		payload.SetByPath("task.target_id", easyjson.NewJSON(wt.ctx.Self.ID))
		payload.SetByPath("task.due_in_ms", easyjson.NewJSON(timerTimeoutMs))
		payload.SetByPath("task.options", optionsFromTimer)

		_ = wt.ctx.Signal(sfPlugins.JetstreamGlobalSignal, DelayedSignalGeneratorTypename, "timer", &payload, nil)
	}
	taskData := easyjson.NewJSONObject()
	taskData.SetByPath("activity", easyjson.NewJSON(activity.statefunName))
	if retry > 0 {
		taskData.SetByPath("retry", easyjson.NewJSON(retry))
	}
	wt.setTaskDetails(taskData)

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
		errorMsg := ""
		defer func() {
			if len(errorMsg) > 0 {
				om.AggregateOpMsg(sfMediators.OpMsgFailed(errorMsg)).Reply()
			} else {
				om.AggregateOpMsg(sfMediators.OpMsgOk(cmdReply)).Reply()
			}
		}()

		switch cmd {
		case "status":
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
			stage := ctxData.GetByPath(ctxStagePath).AsStringDefault("")
			if len(stage) > 0 {
				status.SetByPath("stage", easyjson.NewJSON(stage))
			}
			taskData := ctxData.GetByPath(ctxTaskPath)
			if taskData.IsObject() {
				status.SetByPath("task", taskData)
			}

			cmdReply = status
			return
		case "start":
			if isRunning {
				errorMsg = fmt.Sprintf("workflow is already running")
				return
			}
			secret = WORKFLOW_PREFIX_OF_SECRET + "_" + system.GetUniqueStrID()
			ctxData.SetByPath(ctxSecretPath, easyjson.NewJSON(secret))
			sfctx.SetFunctionContextImmediately(ctxData)
			isRunning = true
		case "pause":
			if !isRunning {
				errorMsg = fmt.Sprintf("workflow is not running")
				return
			}
			ctxData.SetByPath(ctxPausedPath, easyjson.NewJSON(true))
			sfctx.SetFunctionContextImmediately(ctxData)
			return
		case "resume":
			if !isRunning {
				errorMsg = fmt.Sprintf("workflow is not running")
				return
			}
			ctxData.SetByPath(ctxPausedPath, easyjson.NewJSON(false))
			sfctx.SetFunctionContextImmediately(ctxData)
			isPaused = false
		case "stop":
			if !isRunning {
				errorMsg = fmt.Sprintf("workflow is not running")
				return
			}
			sfctx.SetFunctionContextImmediately(easyjson.NewJSONObject().GetPtr())
			return
		default:
			errorMsg = fmt.Sprintf("command \"%s\" is unknown", cmd)
		}
	}
	// ----------------------------------------------------

	if isRunning {
		callerIdTokens := strings.Split(sfctx.Domain.GetObjectIDWithoutDomain(sfctx.Caller.ID), "-")
		callback := len(callerIdTokens) == 2 && callerIdTokens[1] == secret

		if callback {
			if err := w.setActivityResultIntoStatefunCtx(callerIdTokens[0], sfctx); err != nil {
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

func (w *WorkflowEngine) getActivityResultFromStatefunCtx(activityUUID string, sfctx *sfPlugins.StatefunContextProcessor) (res easyjson.JSON, retry int) {
	funcContext := sfctx.GetFunctionContext()

	resPath := fmt.Sprintf(CTX_CALLBACK_RESULT_PATH, activityUUID)

	if funcContext.PathExists(resPath) {
		obj := funcContext.GetByPath(resPath)
		if v, ok := obj.AsNumeric(); ok {
			return easyjson.NewJSONObject(), int(v)
		}
		return funcContext.GetByPath(resPath), -1
	}

	return easyjson.NewJSONObject(), 0
}

func (w *WorkflowEngine) setActivityResultIntoStatefunCtx(activityUUID string, sfctx *sfPlugins.StatefunContextProcessor) error {
	funcContext := sfctx.GetFunctionContext()

	resPath := fmt.Sprintf(CTX_CALLBACK_RESULT_PATH, activityUUID)

	inConextDataType := 0 // nothing

	obj := funcContext.GetByPath(resPath)
	if obj.IsObject() {
		inConextDataType = 1 // json object
	}
	if obj.IsNumeric() {
		inConextDataType = 2 // int
	}

	// retry data -----------------------------------------
	if v, ok := sfctx.Options.GetByPath("retry").AsNumeric(); sfctx.Options != nil && ok {
		if inConextDataType == 0 || inConextDataType == 2 {
			if ok := funcContext.SetByPath(resPath, easyjson.NewJSON(int(v))); !ok {
				return fmt.Errorf("could not set retry data by path '%s'", resPath)
			}
			sfctx.SetFunctionContextImmediately(funcContext)
			return nil
		}
	}
	// ----------------------------------------------------

	if inConextDataType == 1 {
		return fmt.Errorf("data for this activityId already exists")
	}
	if ok := funcContext.SetByPath(resPath, *sfctx.Payload); !ok {
		return fmt.Errorf("could not set data by path '%s'", resPath)
	}

	sfctx.SetFunctionContextImmediately(funcContext)

	return nil
}
