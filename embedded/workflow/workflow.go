package workflow

import (
	"fmt"
	"strings"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/statefun"
	lg "github.com/foliagecp/sdk/statefun/logger"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
)

const (
	CTX_CALLBACK_RESULT_PATH  = "workflow.callback.%s.result"
	WORKFLOW_PREFIX_OF_SECRET = "slave"
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
		*statefun.NewFunctionTypeConfig().SetMultipleInstancesAllowance(false).SetMaxIdHandlers(-1),
	)
}

func (w *WorkflowEngine) workflowStatefun(_ sfPlugins.StatefunExecutor, sfctx *sfPlugins.StatefunContextProcessor) {
	starting := true

	callerIdTokens := strings.Split(sfctx.Domain.GetObjectIDWithoutDomain(sfctx.Caller.ID), "-")

	ctxData := sfctx.GetFunctionContext()
	secret := ctxData.GetByPath("secret").AsStringDefault("")
	if len(secret) == 0 {
		if len(callerIdTokens) == 2 {
			secretTokens := strings.Split(callerIdTokens[1], "_")
			if len(secretTokens) == 2 && secretTokens[0] == WORKFLOW_PREFIX_OF_SECRET {
				return // slave (for e.g. activity) replied after workflow has finished its job
			}
		}
		secret = WORKFLOW_PREFIX_OF_SECRET + "_" + system.GetUniqueStrID()
		ctxData.SetByPath("secret", easyjson.NewJSON(secret))
		sfctx.SetFunctionContextImmediately(ctxData)
	} else {
		starting = false
	}

	if len(callerIdTokens) == 2 && callerIdTokens[1] == secret {
		if err := w.setActivityResultIntoStatefunCtx(callerIdTokens[0], *sfctx.Payload, sfctx); err != nil {
			lg.Logln(lg.WarnLevel, "Workflow %s received activity callback, but could not process it: %s", sfctx.Self.ID, err.Error())
			return // cannot continue
		}
	} else if !starting {
		return // cannot continue
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
