package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	_ "github.com/lib/pq"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/embedded/workflow"
	lg "github.com/foliagecp/sdk/statefun/logger"
	"github.com/foliagecp/sdk/statefun/system"
)

func Activity1(tools workflow.ActivityTools) {
	le := lg.GetLogger()
	funcCtx := tools.SFctx.GetFunctionContext()
	if funcCtx.GetByPath("db_created").AsBoolDefault(false) {
		tools.ReplyWith(easyjson.NewJSONObjectWithKeyValue("ok", easyjson.NewJSON(true)))
		return
	}

	le.Info(context.TODO(), "---- Activity: Creating database...")

	db, err := sql.Open("postgres", getConnString("postgres"))
	if err != nil {
		le.Errorf(context.TODO(), "---- Activity: Failed to connect: %v", err)
		tools.ReplyWith(easyjson.NewJSONObjectWithKeyValue("ok", easyjson.NewJSON(false)))
		return
	}
	defer db.Close()

	dbName := system.GetEnvMustProceed("DB_NAME", "workflow_db")

	var exists bool
	err = db.QueryRow("SELECT EXISTS(SELECT 1 FROM pg_database WHERE datname = $1)", dbName).Scan(&exists)
	if err != nil {
		le.Errorf(context.TODO(), "---- Activity: Failed to check if DB exists: %v", err)
		tools.ReplyWith(easyjson.NewJSONObjectWithKeyValue("ok", easyjson.NewJSON(false)))
		return
	}
	if !exists {
		_, err = db.Exec(fmt.Sprintf("CREATE DATABASE %s", dbName))
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			le.Errorf(context.TODO(), "---- Activity: Failed to create DB: %v", err)
			tools.ReplyWith(easyjson.NewJSONObjectWithKeyValue("ok", easyjson.NewJSON(false)))
			return
		}

		le.Info(context.TODO(), "---- Activity: Database created successfully")
	}

	funcCtx.SetByPath("db_created", easyjson.NewJSON(true))
	tools.SFctx.SetFunctionContextImmediately(funcCtx)
	tools.ReplyWith(easyjson.NewJSONObjectWithKeyValue("ok", easyjson.NewJSON(true)))
}

func Activity2(tools workflow.ActivityTools) {
	le := lg.GetLogger()
	funcCtx := tools.SFctx.GetFunctionContext()
	if funcCtx.GetByPath("schema_created").AsBoolDefault(false) {
		le.Info(context.TODO(), "---- Activity 2: Schema already created, skipping")
		tools.ReplyWith(easyjson.NewJSONObjectWithKeyValue("ok", easyjson.NewJSON(true)))
		return
	}

	le.Info(context.TODO(), "---- Activity 2: Creating schema...")

	db, err := sql.Open("postgres", getConnString(system.GetEnvMustProceed("DB_NAME", "workflow_db")))
	if err != nil {
		le.Errorf(context.TODO(), "---- Activity 2: Failed to connect: %v", err)
		tools.ReplyWith(easyjson.NewJSONObjectWithKeyValue("ok", easyjson.NewJSON(false)))
		return
	}
	defer db.Close()

	schema := `
	CREATE TABLE IF NOT EXISTS users (
		id SERIAL PRIMARY KEY,
		username VARCHAR(50)  NOT NULL,
		email VARCHAR(100)  NOT NULL,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS orders (
		id SERIAL PRIMARY KEY,
		user_id INTEGER REFERENCES users(id),
		total DECIMAL(10,2) NOT NULL,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);`

	_, err = db.Exec(schema)
	if err != nil {
		le.Errorf(context.TODO(), "---- Activity 2: Failed to create schema: %v", err)
		tools.ReplyWith(easyjson.NewJSONObjectWithKeyValue("ok", easyjson.NewJSON(false)))
		return
	}

	le.Info(context.TODO(), "---- Activity 2: Schema created successfully")

	funcCtx.SetByPath("schema_created", easyjson.NewJSON(true))
	tools.SFctx.SetFunctionContextImmediately(funcCtx)

	tools.ReplyWith(easyjson.NewJSONObjectWithKeyValue("ok", easyjson.NewJSON(true)))
}

func Activity3(tools workflow.ActivityTools) {
	le := lg.GetLogger()
	funcCtx := tools.SFctx.GetFunctionContext()
	if funcCtx.GetByPath("migrations_ran").AsBoolDefault(false) {
		le.Info(context.TODO(), "---- Activity 3: Migrations already ran, skipping")
		tools.ReplyWith(easyjson.NewJSONObjectWithKeyValue("ok", easyjson.NewJSON(true)))
		return
	}

	le.Info(context.TODO(), "---- Activity 3: Running migrations...")

	db, err := sql.Open("postgres", getConnString(system.GetEnvMustProceed("DB_NAME", "workflow_db")))
	if err != nil {
		le.Errorf(context.TODO(), "---- Activity 3: Failed to connect: %v", err)
		tools.ReplyWith(easyjson.NewJSONObjectWithKeyValue("ok", easyjson.NewJSON(false)))
		return
	}
	defer db.Close()

	migrations := `
	ALTER TABLE users ADD COLUMN IF NOT EXISTS last_login TIMESTAMP;
	ALTER TABLE orders ADD COLUMN IF NOT EXISTS description VARCHAR(100);
	`

	_, err = db.Exec(migrations)
	if err != nil {
		le.Errorf(context.TODO(), "---- Activity 3: Failed to run migrations: %v", err)
		tools.ReplyWith(easyjson.NewJSONObjectWithKeyValue("ok", easyjson.NewJSON(false)))
		return
	}

	le.Info(context.TODO(), "---- Activity 3: Migrations completed successfully")

	funcCtx.SetByPath("migrations_ran", easyjson.NewJSON(true))
	tools.SFctx.SetFunctionContextImmediately(funcCtx)

	tools.ReplyWith(easyjson.NewJSONObjectWithKeyValue("ok", easyjson.NewJSON(true)))
}

func Activity4(tools workflow.ActivityTools) {
	le := lg.GetLogger()
	funcCtx := tools.SFctx.GetFunctionContext()
	if funcCtx.GetByPath("test_order_created").AsBoolDefault(false) {
		tools.ReplyWith(easyjson.NewJSONObjectWithKeyValue("ok", easyjson.NewJSON(true)))
		return
	}

	le.Info(context.TODO(), "---- Activity 4: Creating test order...")

	db, err := sql.Open("postgres", getConnString(system.GetEnvMustProceed("DB_NAME", "workflow_db")))
	if err != nil {
		le.Errorf(context.TODO(), "---- Activity 4: Failed to connect: %v", err)
		tools.ReplyWith(easyjson.NewJSONObjectWithKeyValue("ok", easyjson.NewJSON(false)))
		return
	}
	defer db.Close()

	var userID int
	err = db.QueryRow("INSERT INTO users (username, email) VALUES ($1, $2) RETURNING id",
		"testuser", "test@example.com").Scan(&userID)
	if err != nil {
		le.Errorf(context.TODO(), "---- Activity 4: Failed to create user: %v", err)
		tools.ReplyWith(easyjson.NewJSONObjectWithKeyValue("ok", easyjson.NewJSON(false)))
		return
	}

	_, err = db.Exec("INSERT INTO orders (user_id, total, description) VALUES ($1, $2, $3)",
		userID, 99.99, "Test order from workflow")
	if err != nil {
		le.Errorf(context.TODO(), "---- Activity 4: Failed to create order: %v", err)
		tools.ReplyWith(easyjson.NewJSONObjectWithKeyValue("ok", easyjson.NewJSON(false)))
		return
	}

	le.Info(context.TODO(), "---- Activity 4: Test order created successfully")

	funcCtx.SetByPath("test_order_created", easyjson.NewJSON(true))
	tools.SFctx.SetFunctionContextImmediately(funcCtx)

	tools.ReplyWith(easyjson.NewJSONObjectWithKeyValue("ok", easyjson.NewJSON(true)))
}

func Activity5(tools workflow.ActivityTools) {
	le := lg.GetLogger()
	funcCtx := tools.SFctx.GetFunctionContext()
	if funcCtx.GetByPath("order_processed").AsBoolDefault(false) {
		tools.ReplyWith(easyjson.NewJSONObjectWithKeyValue("ok", easyjson.NewJSON(true)))
		return
	}

	le.Info(context.TODO(), "---- Activity 5: Processing orders...")

	db, err := sql.Open("postgres", getConnString(system.GetEnvMustProceed("DB_NAME", "workflow_db")))
	if err != nil {
		le.Errorf(context.TODO(), "---- Activity 5: Failed to connect: %v", err)
		tools.ReplyWith(easyjson.NewJSONObjectWithKeyValue("ok", easyjson.NewJSON(false)))
		return
	}
	defer db.Close()

	var orderCount int
	var totalRevenue float64
	db.QueryRow("SELECT COUNT(*), COALESCE(SUM(total), 0) FROM orders").Scan(&orderCount, &totalRevenue)

	le.Infof(context.TODO(), "---- Activity 5: Found %d orders, total revenue: $%.2f", orderCount, totalRevenue)

	funcCtx.SetByPath("order_processed", easyjson.NewJSON(true))
	tools.SFctx.SetFunctionContextImmediately(funcCtx)

	tools.ReplyWith(easyjson.NewJSONObjectWithKeyValue("ok", easyjson.NewJSON(true)))
}

func getConnString(dbName string) string {
	return fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		system.GetEnvMustProceed("DB_HOST", "db"),
		system.GetEnvMustProceed("DB_PORT", "5432"),
		system.GetEnvMustProceed("DB_USER", "postgres"),
		system.GetEnvMustProceed("DB_PASSWORD", "password"),
		dbName,
	)
}
