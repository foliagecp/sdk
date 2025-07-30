package workflow

import (
	"fmt"
	"strings"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/statefun"
	lg "github.com/foliagecp/sdk/statefun/logger"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
)

const (
	CTX_CALLBACK_RESULT_PATH = "workflow.callback.%s.result"
)

type WorkflowTools struct {
	ctx                   *sfPlugins.StatefunContextProcessor
	secret                string
	workflow              *WorkflowEngine
	callbackUUIDGenerator int
}

func (wt *WorkflowTools) ExecActivity(activity *WorkflowActivity, data easyjson.JSON) *easyjson.JSON {
	wt.callbackUUIDGenerator++

	strUUID := fmt.Sprint(wt.callbackUUIDGenerator)
	if existingResult, ok := wt.workflow.getActivityResultFromStatefunCtx(strUUID, wt.ctx); ok {
		return &existingResult
	}

	wt.ctx.Signal(sfPlugins.AutoSignalSelect, activity.statefunName, strUUID+"-"+wt.secret, &data, wt.ctx.Options)

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
	ctxData := sfctx.GetFunctionContext()
	secret := ctxData.GetByPath("secret").AsStringDefault("")
	if len(secret) == 0 {
		secret = system.GetUniqueStrID()
		ctxData.SetByPath("secret", easyjson.NewJSON(secret))
		sfctx.SetFunctionContext(ctxData)
	}

	callerIdTokens := strings.Split(sfctx.Domain.GetObjectIDWithoutDomain(sfctx.Caller.ID), "-")
	if len(callerIdTokens) == 2 && callerIdTokens[1] == secret {
		if err := w.setActivityResultIntoStatefunCtx(callerIdTokens[0], *sfctx.Payload, sfctx); err != nil {
			lg.Logln(lg.WarnLevel, "Workflow %s received activity callback, but could not process it: %s", sfctx.Self.ID, err.Error())
		}
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
	sfctx.SetFunctionContext(easyjson.NewJSONObject().GetPtr())
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

	sfctx.SetFunctionContext(funcContext)

	return nil
}
