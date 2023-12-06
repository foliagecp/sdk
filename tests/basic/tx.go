package basic

import (
	"fmt"
	"runtime"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/statefun"
	lg "github.com/foliagecp/sdk/statefun/logger"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
)

var (
	txRequestProvider = sfPlugins.GolangLocalRequest
)

func IsTransactionOperationOk(j *easyjson.JSON, err error) bool {
	le := lg.GetCustomLogEntry(runtime.Caller(1))
	if err != nil {
		le.Logf(lg.ErrorLevel, "Transaction operation failed: %s\n", err)
		return false
	}
	if s, ok := j.GetByPath("payload.status").AsString(); ok {
		if s != "ok" {
			le.Logf(lg.WarnLevel, "Transaction status is not ok, raw data: %s\n", j.ToString())
			return false
		}
	} else {
		le.Logf(lg.WarnLevel, "Transaction operation status format is unknown, raw data: %s\n", j.ToString())
		return false
	}
	return true
}

func prepareForTXTests(runtime *statefun.Runtime) {
	txId := "init"
	transactionPayload := easyjson.NewJSONObjectWithKeyValue("clone", easyjson.NewJSON("min"))
	if IsTransactionOperationOk(runtime.Request(txRequestProvider, "functions.cmdb.tx.begin", txId, &transactionPayload, nil)) {
		// + T:session ------------------------
		signalPayload := easyjson.NewJSONObjectWithKeyValue("id", easyjson.NewJSON("session"))
		IsTransactionOperationOk(runtime.Request(txRequestProvider, "functions.cmdb.tx.type.create", txId, &signalPayload, nil))
		// ------------------------------------
		// + T:controller ---------------------
		signalPayload = easyjson.NewJSONObjectWithKeyValue("id", easyjson.NewJSON("controller"))
		IsTransactionOperationOk(runtime.Request(txRequestProvider, "functions.cmdb.tx.type.create", txId, &signalPayload, nil))
		// ------------------------------------
		// + T:group -> T:session -------------
		signalPayload = easyjson.NewJSONObjectWithKeyValue("object_link_type", easyjson.NewJSON("session"))
		signalPayload.SetByPath("from", easyjson.NewJSON("group"))
		signalPayload.SetByPath("to", easyjson.NewJSON("session"))
		IsTransactionOperationOk(runtime.Request(txRequestProvider, "functions.cmdb.tx.types.link.create", txId, &signalPayload, nil))
		// ------------------------------------
		// + T:session -> T:controller --------
		signalPayload = easyjson.NewJSONObjectWithKeyValue("object_link_type", easyjson.NewJSON("controller"))
		signalPayload.SetByPath("from", easyjson.NewJSON("session"))
		signalPayload.SetByPath("to", easyjson.NewJSON("controller"))
		IsTransactionOperationOk(runtime.Request(txRequestProvider, "functions.cmdb.tx.types.link.create", txId, &signalPayload, nil))
		// ------------------------------------
		// + T:controller -> T:session --------
		signalPayload = easyjson.NewJSONObjectWithKeyValue("object_link_type", easyjson.NewJSON("session"))
		signalPayload.SetByPath("from", easyjson.NewJSON("controller"))
		signalPayload.SetByPath("to", easyjson.NewJSON("session"))
		IsTransactionOperationOk(runtime.Request(txRequestProvider, "functions.cmdb.tx.types.link.create", txId, &signalPayload, nil))
		// ------------------------------------

		// + O:session_entrypoint -------------
		signalPayload = easyjson.NewJSONObjectWithKeyValue("id", easyjson.NewJSON("session_entrypoint"))
		signalPayload.SetByPath("origin_type", easyjson.NewJSON("group"))
		IsTransactionOperationOk(runtime.Request(txRequestProvider, "functions.cmdb.tx.object.create", txId, &signalPayload, nil))
		// ------------------------------------
		// + O:nav -> O:session_entrypoint ----
		signalPayload = easyjson.NewJSONObject()
		signalPayload.SetByPath("from", easyjson.NewJSON("nav"))
		signalPayload.SetByPath("to", easyjson.NewJSON("session_entrypoint"))
		IsTransactionOperationOk(runtime.Request(txRequestProvider, "functions.cmdb.tx.objects.link.create", txId, &signalPayload, nil))
		// ------------------------------------

		body := easyjson.NewJSONObject()
		body.SetByPath("life_time", easyjson.NewJSON(10))
		body.SetByPath("inactivity_timeout", easyjson.NewJSON(120))
		body.SetByPath("creation_time", easyjson.NewJSON(0))
		body.SetByPath("last_activity_time", easyjson.NewJSON(0))
		body.SetByPath("client_id", easyjson.NewJSON(0))
		/*for i := 0; i < 100000; i++ {
			sessionId := fmt.Sprintf("prep-sess-%d", i)
			// + O:session ------------------------
			signalPayload := easyjson.NewJSONObjectWithKeyValue("id", easyjson.NewJSON(sessionId))
			signalPayload.SetByPath("origin_type", easyjson.NewJSON("session"))
			signalPayload.SetByPath("body", body)
			IsTransactionOperationOk(runtime.Request(txRequestProvider, "functions.cmdb.tx.object.create", txId, &signalPayload, nil))
			// ------------------------------------
			// + O:session_entrypoint -> O:session
			signalPayload = easyjson.NewJSONObject()
			signalPayload.SetByPath("from", easyjson.NewJSON("session_entrypoint"))
			signalPayload.SetByPath("to", easyjson.NewJSON(sessionId))
			IsTransactionOperationOk(runtime.Request(txRequestProvider, "functions.cmdb.tx.objects.link.create", txId, &signalPayload, nil))
			// ------------------------------------
		}*/
		IsTransactionOperationOk(runtime.Request(txRequestProvider, "functions.cmdb.tx.commit", txId, nil, nil))
	}
}

func generateTxID(id string) string {
	//return system.GetHashStr(id)
	return "sameidalways"
}

func doTransactionForSession(runtime *statefun.Runtime, sessionId string) {
	lg.Logf(lg.DebugLevel, "creating session %s using tx\n", sessionId)
	start := time.Now()
	start1 := start

	txId := generateTxID(sessionId)
	//transactionPayload := easyjson.NewJSONObjectWithKeyValue("clone", easyjson.NewJSON("with_types"))
	//transactionPayload.SetByPath("types", easyjson.JSONFromArray([]string{"session", "group"}))
	transactionPayload := easyjson.NewJSONObjectWithKeyValue("clone", easyjson.NewJSON("full"))
	if IsTransactionOperationOk(runtime.Request(txRequestProvider, "functions.cmdb.tx.begin", txId, &transactionPayload, nil)) {
		lg.Logf(lg.InfoLevel, ">> TX(0) took %s", time.Since(start1))
		start1 = time.Now()

		/*// + T:session ------------------------
		signalPayload := easyjson.NewJSONObjectWithKeyValue("id", easyjson.NewJSON("session"))
		IsTransactionOperationOk(runtime.Request(txRequestProvider, "functions.cmdb.tx.type.create", txId, &signalPayload, nil))
		// ------------------------------------
		// + T:controller ---------------------
		signalPayload = easyjson.NewJSONObjectWithKeyValue("id", easyjson.NewJSON("controller"))
		IsTransactionOperationOk(runtime.Request(txRequestProvider, "functions.cmdb.tx.type.create", txId, &signalPayload, nil))
		// ------------------------------------
		// + T:group -> T:session -------------
		signalPayload = easyjson.NewJSONObjectWithKeyValue("object_link_type", easyjson.NewJSON("session"))
		signalPayload.SetByPath("from", easyjson.NewJSON("group"))
		signalPayload.SetByPath("to", easyjson.NewJSON("session"))
		IsTransactionOperationOk(runtime.Request(txRequestProvider, "functions.cmdb.tx.types.link.create", txId, &signalPayload, nil))
		// ------------------------------------
		// + T:session -> T:controller --------
		signalPayload = easyjson.NewJSONObjectWithKeyValue("object_link_type", easyjson.NewJSON("controller"))
		signalPayload.SetByPath("from", easyjson.NewJSON("session"))
		signalPayload.SetByPath("to", easyjson.NewJSON("controller"))
		IsTransactionOperationOk(runtime.Request(txRequestProvider, "functions.cmdb.tx.types.link.create", txId, &signalPayload, nil))
		// ------------------------------------
		// + T:controller -> T:session --------
		signalPayload = easyjson.NewJSONObjectWithKeyValue("object_link_type", easyjson.NewJSON("session"))
		signalPayload.SetByPath("from", easyjson.NewJSON("controller"))
		signalPayload.SetByPath("to", easyjson.NewJSON("session"))
		IsTransactionOperationOk(runtime.Request(txRequestProvider, "functions.cmdb.tx.types.link.create", txId, &signalPayload, nil))
		// ------------------------------------

		// + O:session_entrypoint -------------
		signalPayload = easyjson.NewJSONObjectWithKeyValue("id", easyjson.NewJSON("session_entrypoint"))
		signalPayload.SetByPath("origin_type", easyjson.NewJSON("group"))
		IsTransactionOperationOk(runtime.Request(txRequestProvider, "functions.cmdb.tx.object.create", txId, &signalPayload, nil))
		// ------------------------------------
		// + O:nav -> O:session_entrypoint ----
		signalPayload = easyjson.NewJSONObject()
		signalPayload.SetByPath("from", easyjson.NewJSON("nav"))
		signalPayload.SetByPath("to", easyjson.NewJSON("session_entrypoint"))
		IsTransactionOperationOk(runtime.Request(txRequestProvider, "functions.cmdb.tx.objects.link.create", txId, &signalPayload, nil))
		// ------------------------------------*/

		/*// + O:session_entrypoint -------------
		signalPayload := easyjson.NewJSONObjectWithKeyValue("id", easyjson.NewJSON("session_entrypoint"))
		signalPayload.SetByPath("origin_type", easyjson.NewJSON("group"))
		IsTransactionOperationOk(runtime.Request(txRequestProvider, "functions.cmdb.tx.object.create", txId, &signalPayload, nil))
		// ------------------------------------
		// + O:nav -> O:session_entrypoint ----
		signalPayload = easyjson.NewJSONObject()
		signalPayload.SetByPath("from", easyjson.NewJSON("nav"))
		signalPayload.SetByPath("to", easyjson.NewJSON("session_entrypoint"))
		IsTransactionOperationOk(runtime.Request(txRequestProvider, "functions.cmdb.tx.objects.link.create", txId, &signalPayload, nil))
		// ------------------------------------*/

		body := easyjson.NewJSONObject()
		body.SetByPath("life_time", easyjson.NewJSON(10))
		body.SetByPath("inactivity_timeout", easyjson.NewJSON(120))
		body.SetByPath("creation_time", easyjson.NewJSON(0))
		body.SetByPath("last_activity_time", easyjson.NewJSON(0))
		body.SetByPath("client_id", easyjson.NewJSON(0))

		// + O:session ------------------------
		signalPayload := easyjson.NewJSONObjectWithKeyValue("id", easyjson.NewJSON(sessionId))
		signalPayload.SetByPath("origin_type", easyjson.NewJSON("session"))
		signalPayload.SetByPath("body", body)
		IsTransactionOperationOk(runtime.Request(txRequestProvider, "functions.cmdb.tx.object.create", txId, &signalPayload, nil))
		// ------------------------------------

		lg.Logf(lg.InfoLevel, ">> TX(1) took %s", time.Since(start1))
		start1 = time.Now()

		// + O:session_entrypoint -> O:session
		signalPayload = easyjson.NewJSONObject()
		signalPayload.SetByPath("from", easyjson.NewJSON("session_entrypoint"))
		signalPayload.SetByPath("to", easyjson.NewJSON(sessionId))
		IsTransactionOperationOk(runtime.Request(txRequestProvider, "functions.cmdb.tx.objects.link.create", txId, &signalPayload, nil))
		// ------------------------------------

		lg.Logf(lg.InfoLevel, ">> TX(2) took %s", time.Since(start1))
		start1 = time.Now()

		IsTransactionOperationOk(runtime.Request(txRequestProvider, "functions.cmdb.tx.commit", txId, nil, nil))

		lg.Logf(lg.InfoLevel, ">> TX(3) took %s", time.Since(start1))
	}

	lg.Logf(lg.InfoLevel, "TX took %s", time.Since(start))
}

func TransactionTest(runtime *statefun.Runtime) {
	lg.Logln(lg.DebugLevel, ">>> Test started: transactions")

	prepareForTXTests(runtime)
	lg.Logln(lg.DebugLevel, "----------------------- BASIC TYPES FOR TX TEST CREATED")
	doTransactionForSession(runtime, fmt.Sprintf("sess-%d", 0))

	for i := 0; i < 1000000; i++ {
		doTransactionForSession(runtime, fmt.Sprintf("sess-%d", i))
		time.Sleep(100 * time.Millisecond)
	}

	lg.Logln(lg.DebugLevel, "<<< Test ended: transactions")
}
