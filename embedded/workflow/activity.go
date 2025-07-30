package workflow

import (
	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/statefun"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
)

type ActivityTools struct {
	SFctx  *sfPlugins.StatefunContextProcessor
	result *easyjson.JSON
}

func (at *ActivityTools) ReplyWith(result easyjson.JSON) {
	*at.result = result
}

type WorkflowActivityLogicHandler func(tools ActivityTools)

type WorkflowActivity struct {
	statefunName string
	logicHandler WorkflowActivityLogicHandler
}

func NewWorkflowActivity(logicHandler WorkflowActivityLogicHandler, statefunName string) *WorkflowActivity {
	return &WorkflowActivity{
		statefunName: statefunName,
		logicHandler: logicHandler,
	}
}

func (a *WorkflowActivity) RegisterStatefun(runtime *statefun.Runtime) {
	statefun.NewFunctionType(
		runtime,
		a.statefunName,
		a.activityStatefun,
		*statefun.NewFunctionTypeConfig().SetMultipleInstancesAllowance(false).SetMaxIdHandlers(-1),
	)
}

func (a *WorkflowActivity) activityStatefun(_ sfPlugins.StatefunExecutor, sfctx *sfPlugins.StatefunContextProcessor) {
	result := easyjson.NewJSONObject().GetPtr()

	tools := ActivityTools{
		SFctx:  sfctx,
		result: result,
	}

	a.logicHandler(tools)

	sfctx.Signal(sfPlugins.AutoSignalSelect, sfctx.Caller.Typename, sfctx.Caller.ID, result, sfctx.Options)
}
