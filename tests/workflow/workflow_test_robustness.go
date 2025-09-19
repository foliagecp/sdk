package main

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/embedded/workflow"
	lg "github.com/foliagecp/sdk/statefun/logger"
)

func TestWorkflowRobustness(tools workflow.WorkflowTools) {
	le := lg.GetLogger()
	ctx := context.Background()

	le.Info(ctx, "==TEST============= Starting 5-step workflow...")

	for i := 1; i <= 5; i++ {
		data := easyjson.NewJSONObjectWithKeyValue("step", easyjson.NewJSON(i))
		result := tools.ExecActivity(stepActivity, data, &workflow.ActivityOptions{Timeout: 2 * time.Second})

		if !result.GetByPath("ok").AsBoolDefault(false) {
			le.Errorf(ctx, "==TEST============= Step %d failed", i)
			return
		}

		le.Infof(ctx, "Step %d completed", i)

		if i%2 != 0 {
			le.Info(ctx, "==TEST============= Simulating crash after step 4...")
			go restartNATSContainer()
			//os.Exit(1)
		}

		time.Sleep(3 * time.Second)
	}

	le.Info(ctx, "==TEST============= All 5 steps completed!")
}

func StepActivity(tools workflow.ActivityTools) {
	step := int(tools.SFctx.Payload.GetByPath("step").AsNumericDefault(0))

	funcCtx := tools.SFctx.GetFunctionContext()
	stepKey := fmt.Sprintf("step_%d_completed", step)

	if funcCtx.GetByPath(stepKey).AsBoolDefault(false) {
		tools.ReplyWith(easyjson.NewJSONObjectWithKeyValue("ok", easyjson.NewJSON(true)))
		return
	}

	f, _ := os.OpenFile("workflow_progress.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	fmt.Fprintf(f, "%d", step)
	f.Close()

	funcCtx.SetByPath(stepKey, easyjson.NewJSON(true))
	tools.SFctx.SetFunctionContextImmediately(funcCtx)

	tools.ReplyWith(easyjson.NewJSONObjectWithKeyValue("ok", easyjson.NewJSON(true)))
}

func restartNATSContainer() {
	cmd := exec.Command("docker", "restart", "workflow-nats-1")
	cmd.Run()
}
