package main

import (
	"context"
	"encoding/json"

	"github.com/foliagecp/sdk/statefun"
	lg "github.com/foliagecp/sdk/statefun/logger"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/tests/integration/export/pg_low_level/dumper"
)

var pgDumper *dumper.PGDumper

// PGExportHandler is the Foliage function type handler for the PG dumper.
// It receives ExportEvents from the export bridge via ctx.Payload and writes
// them to PostgreSQL.
func PGExportHandler(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	if pgDumper == nil {
		lg.Logln(lg.WarnLevel, "PGExportHandler: pgDumper not ready yet, skipping event")
		return
	}
	var event statefun.ExportEvent
	if err := json.Unmarshal(ctx.Payload.ToBytes(), &event); err != nil {
		lg.Logf(lg.ErrorLevel, "PGExportHandler: unmarshal payload: %s", err)
		return
	}
	if err := pgDumper.ApplyEvent(context.Background(), event); err != nil {
		lg.Logf(lg.ErrorLevel, "PGExportHandler: apply event tx=%s: %s", event.TxID, err)
		return
	}
	lg.Logf(lg.DebugLevel, "PGExportHandler: applied tx=%s (%d ops)", event.TxID, len(event.Ops))
}
