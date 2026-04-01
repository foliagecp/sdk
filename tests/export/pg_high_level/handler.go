package main

import (
	"encoding/json"

	"github.com/foliagecp/sdk/statefun"
	lg "github.com/foliagecp/sdk/statefun/logger"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
)

var translator *statefun.SemanticTranslator

// SemanticPGHandler is the Foliage function type handler for the semantic PG dumper.
// It receives ExportEvents from the export pipeline via ctx.Payload, translates
// them into CMDB-level semantic events (OnObjectPut, OnTypePut, …), and writes
// the result to PostgreSQL through SemanticPGDumper.
func SemanticPGHandler(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	if translator == nil {
		lg.Logln(lg.WarnLevel, "SemanticPGHandler: translator not ready yet, skipping event")
		return
	}
	var event statefun.ExportEvent
	if err := json.Unmarshal(ctx.Payload.ToBytes(), &event); err != nil {
		lg.Logf(lg.ErrorLevel, "SemanticPGHandler: unmarshal payload: %s", err)
		return
	}
	if err := translator.ProcessEvent(event); err != nil {
		lg.Logf(lg.ErrorLevel, "SemanticPGHandler: process event tx=%s: %s", event.TxID, err)
		return
	}
	lg.Logf(lg.DebugLevel, "SemanticPGHandler: processed tx=%s (%d ops)", event.TxID, len(event.Ops))
}
