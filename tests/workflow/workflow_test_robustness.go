package main

import (
	"context"
	"os"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/embedded/workflow"
	lg "github.com/foliagecp/sdk/statefun/logger"
)

func TestWorkflowRobustness(tools workflow.WorkflowTools) {
	le := lg.GetLogger()
	logCtx := context.Background()

	le.Info(logCtx, "==========>TestWorkflowRobustness started")

	testFile := "/tmp/test_config.json"
	testContent := `{"version": "1.0", "status": "active"}`
	os.WriteFile(testFile, []byte(testContent), 0644)

	data1 := easyjson.NewJSONObjectWithKeyValue("file", easyjson.NewJSON(testFile))
	result1 := tools.ExecActivity(workflowActivity1, data1, &workflow.ActivityOptions{Timeout: 3 * time.Second})

	if !result1.GetByPath("ok").AsBoolDefault(false) {
		le.Error(logCtx, "Backup failed: %s", result1.GetByPath("error").AsStringDefault(""))
		return
	}

	data2 := easyjson.NewJSONObjectWithKeyValue("file", easyjson.NewJSON(testFile))
	data2.SetByPath("backup_file", result1.GetByPath("backup_file"))
	result2 := tools.ExecActivity(workflowActivity2, data2, &workflow.ActivityOptions{Timeout: 3 * time.Second})

	if !result2.GetByPath("ok").AsBoolDefault(false) {
		le.Error(logCtx, "File processing failed: %s", result2.GetByPath("error").AsStringDefault(""))
	}

	le.Info(logCtx, "<==========TestWorkflowRobustness finished")
}

func ActivityOne(tools workflow.ActivityTools) {
	le := lg.GetLogger()
	ctx := context.Background()

	le.Info(ctx, "ActivityOne: Creating backup...")

	originalFile := tools.SFctx.Payload.GetByPath("file").AsStringDefault("")
	if originalFile == "" {
		reply := easyjson.NewJSONObject()
		reply.SetByPath("ok", easyjson.NewJSON(false))
		reply.SetByPath("error", easyjson.NewJSON("file parameter required"))
		tools.ReplyWith(reply)
		return
	}

	if _, err := os.Stat(originalFile); os.IsNotExist(err) {
		reply := easyjson.NewJSONObject()
		reply.SetByPath("ok", easyjson.NewJSON(false))
		reply.SetByPath("error", easyjson.NewJSON("original file does not exist"))
		tools.ReplyWith(reply)
		return
	}

	timestamp := time.Now().Format("20060102_150405")
	backupFile := originalFile + ".backup." + timestamp

	data, err := os.ReadFile(originalFile)
	if err != nil {
		reply := easyjson.NewJSONObject()
		reply.SetByPath("ok", easyjson.NewJSON(false))
		reply.SetByPath("error", easyjson.NewJSON(err.Error()))
		tools.ReplyWith(reply)
		return
	}

	err = os.WriteFile(backupFile, data, 0644)
	if err != nil {
		reply := easyjson.NewJSONObject()
		reply.SetByPath("ok", easyjson.NewJSON(false))
		reply.SetByPath("error", easyjson.NewJSON(err.Error()))
		tools.ReplyWith(reply)
		return
	}

	le.Info(ctx, "ActivityOne: Backup created: %s", backupFile)

	result := easyjson.NewJSONObjectWithKeyValue("ok", easyjson.NewJSON(true))
	result.SetByPath("backup_file", easyjson.NewJSON(backupFile))
	tools.ReplyWith(result)
}

func ActivityTwo(tools workflow.ActivityTools) {
	le := lg.GetLogger()
	ctx := context.Background()

	le.Info(ctx, "ActivityTwo: Processing file...")

	originalFile := tools.SFctx.Payload.GetByPath("file").AsStringDefault("")
	backupFile := tools.SFctx.Payload.GetByPath("backup_file").AsStringDefault("")

	if originalFile == "" || backupFile == "" {
		reply := easyjson.NewJSONObject()
		reply.SetByPath("ok", easyjson.NewJSON(false))
		reply.SetByPath("error", easyjson.NewJSON("file and backup_file parameters required"))
		tools.ReplyWith(reply)
		return
	}

	if _, err := os.Stat(backupFile); os.IsNotExist(err) {
		reply := easyjson.NewJSONObject()
		reply.SetByPath("ok", easyjson.NewJSON(false))
		reply.SetByPath("error", easyjson.NewJSON("backup file does not exist"))
		tools.ReplyWith(reply)
		return
	}

	err := os.Remove(originalFile)
	if err != nil {
		reply := easyjson.NewJSONObject()
		reply.SetByPath("ok", easyjson.NewJSON(false))
		reply.SetByPath("error", easyjson.NewJSON(err.Error()))
		tools.ReplyWith(reply)
		return
	}

	le.Info(ctx, "ActivityTwo: Original file removed, backup preserved")

	result := easyjson.NewJSONObjectWithKeyValue("ok", easyjson.NewJSON(true))
	result.SetByPath("removed_file", easyjson.NewJSON(originalFile))
	result.SetByPath("backup_preserved", easyjson.NewJSON(backupFile))
	tools.ReplyWith(result)
}
