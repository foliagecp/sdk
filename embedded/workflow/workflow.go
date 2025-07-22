package workflow

import (
	"context"

	"github.com/foliagecp/sdk/statefun"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
)

type execActivityFunction func(ctx context.Context, cancel context.CancelFunc, activity WorkflowActivity)

type WorkflowTools struct {
	ExecActivity execActivityFunction
}

// type WorkflowLogicHandler func(context context.Context, cancel context.CancelFunc)
type WorkflowLogicHandler func(ctx context.Context, cancel context.CancelFunc, tools WorkflowTools)

type WorkflowEngine struct {
	logicHandler WorkflowLogicHandler
	sfctx        *sfPlugins.StatefunContextProcessor
}

func (w *WorkflowEngine) SetLogicHandler(logicHandler WorkflowLogicHandler) {
	w.logicHandler = logicHandler
}

func (w *WorkflowEngine) GetStatefunHandler() statefun.FunctionLogicHandler {
	return w.workflowStatefun
}

/*
	{
		"timeout_sec": int, not requred, default: -1 (infinite)
		"upsert": bool - optional, default: false
		"replace": bool - optional, default: false
		"body": json
	}
*/
func (w *WorkflowEngine) workflowStatefun(_ sfPlugins.StatefunExecutor, sfctx *sfPlugins.StatefunContextProcessor) {
	// Check whether instantiated via callback or not
	// If via callback -

	w.sfctx = sfctx

	ctx, cancel := context.WithCancel(context.Background())

	tools := WorkflowTools{
		ExecActivity: w.execActivityFunction,
	}

	w.logicHandler(ctx, cancel, tools)

	/*
		w.logicHandler must be implemented like this:


		{
			done := make(chan struct{})

			go func() {
				defer close(done)

				!!!!!! Workflow engine code here !!!!!!!
			}()

			select {
			case <-ctx.Done():
				fmt.Println("parent: received cancel signal, exiting")
			case <-done:
				fmt.Println("logicHandler finished")
			case <-time.After(5 * time.Second):
				fmt.Println("parent: done waiting")
			}
		}
	*/
}

func (w *WorkflowEngine) execActivityFunction(ctx context.Context, cancel context.CancelFunc, activity WorkflowActivity) {
	activity.Execute(ctx, cancel, w.sfctx)
}
