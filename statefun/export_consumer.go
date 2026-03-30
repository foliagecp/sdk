package statefun

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/foliagecp/easyjson"
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
func CreateExportDumperStream(js nats.JetStreamContext, domain, dumperName string, maxMsgs, maxBytes int64, maxAge time.Duration) error {
	dumperStreamName := fmt.Sprintf(ExportDumperStreamNameTmpl, domain, dumperName)
	sourceStreamName := fmt.Sprintf(ExportStreamNameTmpl, domain)

	if _, err := js.StreamInfo(dumperStreamName); err == nil {
		return nil // already exists
	}

	_, err := js.AddStream(&nats.StreamConfig{
		Name: dumperStreamName,
		Sources: []*nats.StreamSource{{
			Name: sourceStreamName,
		}},
		Retention: nats.LimitsPolicy,
		MaxMsgs:   maxMsgs,
		MaxBytes:  maxBytes,
		MaxAge:    maxAge,
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
// It performs three steps:
//  1. Registers functionTypeName as a Foliage function type via NewFunctionType.
//  2. After runtime start (sync afterStart): creates a per-dumper sourced stream
//     that independently replicates events from the main export stream.
//  3. Starts a bridge goroutine that pulls messages from the sourced stream and
//     dispatches each ExportEvent as a Foliage signal to functionTypeName.
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
	// and bridge are ready before any user-level CRUD logic fires.
	runtime.RegisterOnAfterStartFunction(func(ctx context.Context, r *Runtime) error {
		domain := r.Domain.Name()

		if err := CreateExportDumperStream(
			r.Domain.js, domain, dumperName,
			ExportStreamMaxMsgs, ExportStreamMaxBytes, ExportStreamMaxAge,
		); err != nil {
			return fmt.Errorf("RegisterExportDumper %q: create sourced stream: %w", dumperName, err)
		}
		lg.Logf(lg.InfoLevel, "RegisterExportDumper: sourced stream ready, dumper=%s function=%s",
			dumperName, functionTypeName)

		go runExportBridge(ctx, r, domain, dumperName, functionTypeName)
		return nil
	}, false)
}

// runExportBridge is the bridge goroutine: it pulls ExportEvents from the
// per-dumper sourced stream and dispatches each one as a Foliage signal so
// that functionTypeName receives it via ctx.Payload like any other function.
func runExportBridge(ctx context.Context, runtime *Runtime, domain, dumperName, functionTypeName string) {
	dumperStreamName := fmt.Sprintf(ExportDumperStreamNameTmpl, domain, dumperName)
	exportSubject := fmt.Sprintf(ExportSubjectTmpl, domain)
	consumerName := dumperName + "-bridge"

	_, err := runtime.Domain.js.AddConsumer(dumperStreamName, &nats.ConsumerConfig{
		Name:          consumerName,
		Durable:       consumerName,
		FilterSubject: exportSubject,
		AckPolicy:     nats.AckExplicitPolicy,
		DeliverPolicy: nats.DeliverAllPolicy,
	})
	if err != nil {
		lg.Logf(lg.ErrorLevel, "ExportBridge[%s]: AddConsumer: %s", dumperName, err)
		return
	}

	// Bind to the already-created consumer on the sourced stream.
	sub, err := runtime.Domain.js.PullSubscribe(exportSubject, "", nats.Bind(dumperStreamName, consumerName))
	if err != nil {
		lg.Logf(lg.ErrorLevel, "ExportBridge[%s]: PullSubscribe: %s", dumperName, err)
		return
	}

	lg.Logf(lg.InfoLevel, "ExportBridge[%s]: running, dispatching to function type %q", dumperName, functionTypeName)

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		msgs, fetchErr := sub.Fetch(32, nats.MaxWait(2*time.Second))
		if fetchErr != nil {
			// Timeout when the stream is idle — keep polling.
			continue
		}

		for _, msg := range msgs {
			var event ExportEvent
			if unmarshalErr := json.Unmarshal(msg.Data, &event); unmarshalErr != nil {
				lg.Logf(lg.ErrorLevel, "ExportBridge[%s]: unmarshal: %s", dumperName, unmarshalErr)
				_ = msg.Ack()
				continue
			}

			if dispatchErr := dispatchExportEventAsSignal(runtime, domain, functionTypeName, &event); dispatchErr != nil {
				lg.Logf(lg.ErrorLevel, "ExportBridge[%s]: dispatch tx=%s: %s", dumperName, event.TxID, dispatchErr)
				_ = msg.Nak()
				continue
			}
			_ = msg.Ack()
		}
	}
}

// dispatchExportEventAsSignal publishes an ExportEvent as a Foliage signal so that
// functionTypeName receives it via ctx.Payload. The transaction ID is used as the
// Foliage object ID, which gives natural per-transaction ordering guarantees.
func dispatchExportEventAsSignal(runtime *Runtime, domain, functionTypeName string, event *ExportEvent) error {
	// Build payload: the handler will unmarshal ctx.Payload into ExportEvent.
	payload := easyjson.NewJSONObject()
	payload.SetByPath("tx_id", easyjson.NewJSON(event.TxID))
	payload.SetByPath("domain", easyjson.NewJSON(event.Domain))
	payload.SetByPath("timestamp", easyjson.NewJSON(event.Timestamp))

	opsBytes, err := json.Marshal(event.Ops)
	if err != nil {
		return fmt.Errorf("marshal ops: %w", err)
	}
	opsJSON, ok := easyjson.JSONFromBytes(opsBytes)
	if !ok {
		return fmt.Errorf("parse ops JSON failed")
	}
	payload.SetByPath("ops", opsJSON)

	// Signal subject: $SI.<domain>.signal.<domain>.<functionType>.<txID>
	// This matches the function type's subscription wildcard:
	// $SI.<domain>.signal.<domain>.<functionType>.*
	subject := fmt.Sprintf(
		DomainIngressSubjectsTmpl,
		domain,
		fmt.Sprintf("%s.%s.%s.%s", SignalPrefix, domain, functionTypeName, event.TxID),
	)

	return runtime.nc.Publish(subject, buildNatsData(domain, "", "", &payload, nil, nil))
}
