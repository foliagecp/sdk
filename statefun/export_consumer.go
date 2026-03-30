package statefun

import (
	"context"
	"fmt"
	"time"

	lg "github.com/foliagecp/sdk/statefun/logger"
	"github.com/nats-io/nats.go"
)

// ExportDumperStreamNameTmpl is the name template for a per-dumper sourced stream.
// Each dumper gets its own independent copy of the export events stream.
const ExportDumperStreamNameTmpl = "export-%s-%s-events"

// CreateExportDumperStream creates a per-dumper JetStream stream that sources all
// messages from the main export events stream (created by ExportCommitter).
//
// Each dumper application (PostgreSQL, Neo4j, Elasticsearch, …) should call this
// once at startup to obtain its own independent copy of the event stream.
// Because the stream uses LimitsPolicy retention, multiple dumpers never interfere
// with each other's read positions.
//
// The stream is configured with a RePublish rule that re-delivers every ingested
// message to the Foliage signal subject for functionTypeName. The NATS server
// handles delivery directly — no application-level bridge goroutine is needed.
func CreateExportDumperStream(js nats.JetStreamContext, domain, dumperName, functionTypeName string, maxMsgs, maxBytes int64, maxAge time.Duration) error {
	dumperStreamName := fmt.Sprintf(ExportDumperStreamNameTmpl, domain, dumperName)
	sourceStreamName := fmt.Sprintf(ExportStreamNameTmpl, domain)

	if _, err := js.StreamInfo(dumperStreamName); err == nil {
		return nil // already exists
	}

	// Re-publish destination: $SI.<domain>.signal.<domain>.<functionTypeName>.<txID>
	// $1 captures the txID token from the source subject export.<domain>.events.<txID>.
	repub := fmt.Sprintf("%s.%s.%s.%s.%s.$1",
		DomainSubjectsIngressPrefix, domain, SignalPrefix, domain, functionTypeName)

	_, err := js.AddStream(&nats.StreamConfig{
		Name: dumperStreamName,
		Sources: []*nats.StreamSource{{
			Name: sourceStreamName,
		}},
		Retention: nats.LimitsPolicy,
		MaxMsgs:   maxMsgs,
		MaxBytes:  maxBytes,
		MaxAge:    maxAge,
		RePublish: &nats.RePublish{
			Source:      fmt.Sprintf(ExportSubjectFilterTmpl, domain),
			Destination: repub,
		},
	})
	return err
}

// DeleteExportDumperStream removes a per-dumper sourced stream.
func DeleteExportDumperStream(js nats.JetStreamContext, domain, dumperName string) error {
	return js.DeleteStream(fmt.Sprintf(ExportDumperStreamNameTmpl, domain, dumperName))
}

// RegisterExportDumper registers a Foliage function type that receives ExportEvents
// and wires it to the export pipeline.
//
// It performs two steps:
//  1. Registers functionTypeName as a Foliage function type via NewFunctionType.
//  2. After runtime start (sync afterStart): creates a per-dumper sourced stream
//     with a RePublish rule so that the NATS server delivers every export event
//     directly to the function as a Foliage signal — no bridge goroutine required.
//
// The handler receives the full ExportEvent via ctx.Payload:
//
//	func pgHandler(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
//	    var event statefun.ExportEvent
//	    _ = json.Unmarshal(ctx.Payload.ToBytes(), &event)
//	    // apply event to target DB …
//	}
func RegisterExportDumper(
	runtime *Runtime,
	dumperName string,
	functionTypeName string,
	handler FunctionLogicHandler,
	cfg *FunctionTypeConfig,
) {
	NewFunctionType(runtime, functionTypeName, handler, *cfg)

	// Sync afterStart: runs before async afterStart functions, so the stream
	// is ready before any user-level CRUD logic fires.
	runtime.RegisterOnAfterStartFunction(func(ctx context.Context, r *Runtime) error {
		domain := r.Domain.Name()

		if err := CreateExportDumperStream(
			r.Domain.js, domain, dumperName, functionTypeName,
			ExportStreamMaxMsgs, ExportStreamMaxBytes, ExportStreamMaxAge,
		); err != nil {
			return fmt.Errorf("RegisterExportDumper %q: create sourced stream: %w", dumperName, err)
		}
		lg.Logf(lg.InfoLevel, "RegisterExportDumper: sourced stream ready, dumper=%s function=%s",
			dumperName, functionTypeName)
		return nil
	}, false)
}
