// Foliage basic test package.
// Provides the basic example of usage of the SDK.
package main

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"time"

	"github.com/foliagecp/easyjson"
	graphCRUD "github.com/foliagecp/sdk/embedded/graph/crud"
	"github.com/foliagecp/sdk/embedded/graph/fpl"
	"github.com/foliagecp/sdk/embedded/graph/search"
	"github.com/foliagecp/sdk/embedded/workflow"
	lg "github.com/foliagecp/sdk/statefun/logger"

	graphDebug "github.com/foliagecp/sdk/embedded/graph/debug"
	"github.com/foliagecp/sdk/embedded/graph/jpgql"
	"github.com/foliagecp/sdk/statefun"
	"github.com/foliagecp/sdk/statefun/cache"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
)

var (
	// NatsURL - nats server url
	NatsURL string = system.GetEnvMustProceed("NATS_URL", "nats://nats:foliage@nats:4222")

	workflowEngine = workflow.NewWorkflowEngine(TestWorkflow, "functions.workflow.engine")
	// Create Database
	workflowActivity1 = workflow.NewWorkflowActivity(Activity1, "functions.workflow.activity1")
	// Create Schema
	workflowActivity2 = workflow.NewWorkflowActivity(Activity2, "functions.workflow.activity2")
	// Migrations
	workflowActivity3 = workflow.NewWorkflowActivity(Activity3, "functions.workflow.activity3")
	// new task
	workflowActivity4 = workflow.NewWorkflowActivity(Activity4, "functions.workflow.activity4")
	// new task 2
	workflowActivity5 = workflow.NewWorkflowActivity(Activity5, "functions.workflow.activity5")
)

func RegisterFunctionTypes(runtime *statefun.Runtime) {
	graphCRUD.RegisterAllFunctionTypes(runtime)
	graphDebug.RegisterAllFunctionTypes(runtime)
	jpgql.RegisterAllFunctionTypes(runtime)
	fpl.RegisterAllFunctionTypes(runtime)
	search.RegisterAllFunctionTypes(runtime)
}

func TestWorkflow(tools workflow.WorkflowTools) {
	le := lg.GetLogger()

	le.Info(context.TODO(), "===== Workflow started =====")

	le.Info(context.TODO(), "Step 1: Creating database...")
	result1 := tools.ExecActivity(workflowActivity1, easyjson.NewJSONObject(), &workflow.ActivityOptions{Timeout: 10 * time.Second})
	if !result1.GetByPath("ok").AsBoolDefault(false) {
		le.Error(context.TODO(), "✗ Failed to create database")
		return
	}
	le.Info(context.TODO(), "✓ Database created")

	le.Info(context.TODO(), "Step 2: Creating schema...")
	result2 := tools.ExecActivity(workflowActivity2, easyjson.NewJSONObject(), &workflow.ActivityOptions{Timeout: 10 * time.Second})
	if !result2.GetByPath("ok").AsBoolDefault(false) {
		le.Error(context.TODO(), "✗ Failed to create schema")
		return
	}
	le.Info(context.TODO(), "✓ Schema created")

	le.Info(context.TODO(), "Step 3: Migrations...")
	result3 := tools.ExecActivity(workflowActivity3, easyjson.NewJSONObject(), &workflow.ActivityOptions{Timeout: 10 * time.Second})
	if !result3.GetByPath("ok").AsBoolDefault(false) {
		le.Error(context.TODO(), "x Failed to run migrations")
		return
	}
	le.Info(context.TODO(), "✓ Migrations completed")

	le.Info(context.TODO(), "Step 4: Creating test order...")
	result4 := tools.ExecActivity(workflowActivity4, easyjson.NewJSONObject(), &workflow.ActivityOptions{Timeout: 15 * time.Second})
	if !result4.GetByPath("ok").AsBoolDefault(false) {
		le.Error(context.TODO(), "Failed to create test order")
		return
	}
	le.Info(context.TODO(), "✓ Order created")

	le.Info(context.TODO(), "Step 5: Processing orders...")
	result5 := tools.ExecActivity(workflowActivity5, easyjson.NewJSONObject(), &workflow.ActivityOptions{Timeout: 15 * time.Second})
	if !result5.GetByPath("ok").AsBoolDefault(false) {
		le.Error(context.TODO(), "Failed to process orders")
		return
	}
	le.Info(context.TODO(), "✓ Orders processed")

	le.Info(context.TODO(), "===== Workflow completed successfully =====")
}

func PeriodicBackup(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	le := lg.GetLogger()
	funcCtx := ctx.GetFunctionContext()
	if funcCtx.PathExists("backup_timestamp") {
		lastBackupTime := funcCtx.GetByPath("backup_timestamp").AsStringDefault("")
		lastBackupTS, err := time.ParseInLocation(time.DateTime, lastBackupTime, time.Local)
		if err != nil {
			le.Errorf(context.TODO(), "---- PeriodicTask: Failed to parse backup timestamp: %v", err)
			return
		}
		if time.Since(lastBackupTS) < 30*time.Second {
			le.Info(context.TODO(), "---- PeriodicTask: Backup already created, skipping")
			return
		}
	}

	le.Info(context.TODO(), "---- PeriodicTask: Creating backup...")

	db, err := sql.Open("postgres", getConnString(system.GetEnvMustProceed("DB_NAME", "workflow_db")))
	if err != nil {
		le.Errorf(context.TODO(), "---- PeriodicTask: Failed to connect: %v", err)
		return
	}
	defer db.Close()

	var userCount, orderCount int
	db.QueryRow("SELECT COUNT(*) FROM users").Scan(&userCount)
	db.QueryRow("SELECT COUNT(*) FROM orders").Scan(&orderCount)

	timestamp := time.Now().Format("2006-01-02 15:04:05")
	le.Infof(context.TODO(), "---- PeriodicTask: Backup stats - Users: %d, Orders: %d, Timestamp: %s", userCount, orderCount, timestamp)

	funcCtx.SetByPath("backup_created", easyjson.NewJSON(true))
	funcCtx.SetByPath("backup_timestamp", easyjson.NewJSON(timestamp))
	funcCtx.SetByPath("backup_stats.users", easyjson.NewJSON(userCount))
	funcCtx.SetByPath("backup_stats.products", easyjson.NewJSON(orderCount))
	ctx.SetFunctionContextImmediately(funcCtx)

	le.Info(context.TODO(), "---- PeriodicTask: Backup completed successfully")
}

func PeriodicOrderCreate(_ sfPlugins.StatefunExecutor, _ *sfPlugins.StatefunContextProcessor) {
	le := lg.GetLogger()

	le.Info(context.TODO(), "---- PeriodicOrderCreate: Creating random orders...")

	db, err := sql.Open("postgres", getConnString(system.GetEnvMustProceed("DB_NAME", "workflow_db")))
	if err != nil {
		le.Errorf(context.TODO(), "---- PeriodicOrderCreate: Failed to connect: %v", err)
		return
	}
	defer db.Close()

	rand.Seed(time.Now().UnixNano())

	userCount := rand.Intn(5) + 1
	orderCount := rand.Intn(11) + 10

	var createdUsers []int
	for i := 0; i < userCount; i++ {
		username := fmt.Sprintf("user_%d_%d", time.Now().Unix(), i)
		email := fmt.Sprintf("%s@example.com", username)

		var userID int
		err := db.QueryRow("INSERT INTO users (username, email) VALUES ($1, $2) RETURNING id",
			username, email).Scan(&userID)
		if err != nil {
			le.Errorf(context.TODO(), "---- PeriodicOrderCreate: Failed to create user: %v", err)
			continue
		}
		createdUsers = append(createdUsers, userID)
	}

	if len(createdUsers) == 0 {
		le.Error(context.TODO(), "---- PeriodicOrderCreate: No users created, skipping orders")
		return
	}

	for i := 0; i < orderCount; i++ {
		userID := createdUsers[rand.Intn(len(createdUsers))]
		total := rand.Float64()*500 + 10

		_, err := db.Exec("INSERT INTO orders (user_id, total) VALUES ($1, $2)",
			userID, total)
		if err != nil {
			le.Errorf(context.TODO(), "---- PeriodicOrderCreate: Failed to create order: %v", err)
			continue
		}
	}

	le.Infof(context.TODO(), "---- PeriodicOrderCreate: Created %d users and %d orders successfully",
		len(createdUsers), orderCount)
}

func periodicTest(runtime *statefun.Runtime) {
	//backup task
	plBackup := easyjson.NewJSONObject()
	plBackup.SetByPath("cmd", easyjson.NewJSON("schedule_every"))
	plBackup.SetByPath("task.id", easyjson.NewJSON("every:30s"))
	plBackup.SetByPath("task.target_typename", easyjson.NewJSON("functions.tests.backup"))
	plBackup.SetByPath("task.target_id", easyjson.NewJSON("backup-p30"))
	plBackup.SetByPath("task.period_ms", easyjson.NewJSON(30000))
	plBackup.SetByPath("task.first_in_ms", easyjson.NewJSON(20000))
	system.MsgOnErrorReturn(runtime.Signal(sfPlugins.JetstreamGlobalSignal, workflow.DelayedSignalGeneratorTypename, "wheel1", &plBackup, nil))

	//create-order task
	plCreate := easyjson.NewJSONObject()
	plCreate.SetByPath("cmd", easyjson.NewJSON("schedule_every"))
	plCreate.SetByPath("task.id", easyjson.NewJSON("every:50s"))
	plCreate.SetByPath("task.target_typename", easyjson.NewJSON("functions.tests.create"))
	plCreate.SetByPath("task.target_id", easyjson.NewJSON("create-p50"))
	plCreate.SetByPath("task.period_ms", easyjson.NewJSON(50000))
	plCreate.SetByPath("task.first_in_ms", easyjson.NewJSON(30000))
	system.MsgOnErrorReturn(runtime.Signal(sfPlugins.JetstreamGlobalSignal, workflow.DelayedSignalGeneratorTypename, "wheel1", &plCreate, nil))
}

func Start() {
	system.GlobalPrometrics = system.NewPrometrics("", ":9901")

	afterStart := func(ctx context.Context, runtime *statefun.Runtime) error {
<<<<<<< HEAD
		//payload := easyjson.NewJSONObject()
		//payload.SetByPath("cmd", easyjson.NewJSON("start"))
		//system.MsgOnErrorReturn(runtime.Signal(sfPlugins.JetstreamGlobalSignal, "functions.workflow.engine", "test", &payload, nil))

		//periodicTest(runtime)
=======
		system.MsgOnErrorReturn(runtime.Signal(sfPlugins.JetstreamGlobalSignal, "functions.workflow.engine", "test", nil, nil))

		periodicTest(runtime)
>>>>>>> origin/feat/workflow-instance-write-to-nats

		return nil
	}

	if runtime, err := statefun.NewRuntime(*statefun.NewRuntimeConfigSimple(NatsURL, "clean").UseJSDomainAsHubDomainName()); err == nil {
		workflow.RegisterDelayedSignalGenerator(runtime)
		workflowEngine.RegisterStatefun(runtime)
		workflowActivity1.RegisterStatefun(runtime)
		workflowActivity2.RegisterStatefun(runtime)
		workflowActivity3.RegisterStatefun(runtime)
		workflowActivity4.RegisterStatefun(runtime)
		workflowActivity5.RegisterStatefun(runtime)

		statefun.NewFunctionType(runtime, "functions.tests.backup", PeriodicBackup, *statefun.NewFunctionTypeConfig())
		statefun.NewFunctionType(runtime, "functions.tests.create", PeriodicOrderCreate, *statefun.NewFunctionTypeConfig())

		RegisterFunctionTypes(runtime)

		runtime.RegisterOnAfterStartFunction(afterStart, true)
		if err := runtime.Start(context.TODO(), cache.NewCacheConfig("main_cache")); err != nil {
			lg.GetLogger().Errorf(context.TODO(), "Cannot start due to an error: %s", err)
		}
	} else {
		lg.GetLogger().Errorf(context.TODO(), "Cannot create statefun runtime due to an error: %s", err)
	}
}
