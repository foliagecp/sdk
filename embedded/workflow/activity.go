package workflow

import (
	"context"

	"github.com/foliagecp/easyjson"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
)

type ActivityTools struct {
	Signal sfPlugins.SFSignalFunc
}

type WorkflowActivityLogicHandler func(ctx context.Context, cancel context.CancelFunc, tools ActivityTools)

type WorkflowActivity struct {
	logicHandler WorkflowActivityLogicHandler
	sfctx        *sfPlugins.StatefunContextProcessor
}

func (a *WorkflowActivity) SetLogicHandler(logicHandler WorkflowActivityLogicHandler) {
	a.logicHandler = logicHandler
}

func (a *WorkflowActivity) Execute(ctx context.Context, cancel context.CancelFunc, sfctx *sfPlugins.StatefunContextProcessor) {
	a.sfctx = sfctx

	// Generate UID for this activity whithin its parent workflow
	// Check whether result for this activity exection already exists in sfctx
	// If exist - return result as it is from sfctx

	tools := ActivityTools{
		Signal: a.signal,
	}

	a.logicHandler(ctx, cancel, tools)

	//cancel()
}

func (a *WorkflowActivity) signal(signalProvider sfPlugins.SignalProvider, typename string, id string, payload *easyjson.JSON, options *easyjson.JSON) error {
	// a.sfctx - save the fact of signalling
	// signal with a callback
	return nil
}
