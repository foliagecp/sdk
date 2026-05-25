package main

import (
	"encoding/json"

	"github.com/foliagecp/sdk/statefun"
	lg "github.com/foliagecp/sdk/statefun/logger"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
)

var pgTranslator *statefun.SemanticTranslator
var neo4jTranslator *statefun.SemanticTranslator

func SemanticPGHandler(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	if pgTranslator == nil {
		lg.Logln(lg.WarnLevel, "SemanticPGHandler: translator not ready yet, skipping event")
		return
	}
	var event statefun.ExportEvent
	if err := json.Unmarshal(ctx.Payload.ToBytes(), &event); err != nil {
		lg.Logf(lg.ErrorLevel, "SemanticPGHandler: unmarshal payload: %s", err)
		return
	}
	if err := pgTranslator.ProcessEvent(event); err != nil {
		lg.Logf(lg.ErrorLevel, "SemanticPGHandler: process event tx=%s: %s", event.TxID, err)
		return
	}
	lg.Logf(lg.DebugLevel, "SemanticPGHandler: processed tx=%s (%d ops)", event.TxID, len(event.Ops))
}

func SemanticNeo4jHandler(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	if neo4jTranslator == nil {
		lg.Logln(lg.WarnLevel, "SemanticNeo4jHandler: translator not ready yet, skipping event")
		return
	}
	var event statefun.ExportEvent
	if err := json.Unmarshal(ctx.Payload.ToBytes(), &event); err != nil {
		lg.Logf(lg.ErrorLevel, "SemanticNeo4jHandler: unmarshal payload: %s", err)
		return
	}
	if err := neo4jTranslator.ProcessEvent(event); err != nil {
		lg.Logf(lg.ErrorLevel, "SemanticNeo4jHandler: process event tx=%s: %s", event.TxID, err)
		return
	}
	lg.Logf(lg.DebugLevel, "SemanticNeo4jHandler: processed tx=%s (%d ops)", event.TxID, len(event.Ops))
}
