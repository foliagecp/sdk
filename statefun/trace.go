package statefun

import (
	"fmt"
	"github.com/foliagecp/easyjson"
	lg "github.com/foliagecp/sdk/statefun/logger"
	"github.com/foliagecp/sdk/statefun/system"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"time"
)

type TraceContext struct {
	TraceID      string
	SpanID       string
	ParentSpanID string
	StartTime    int64
}

func NewTraceContext(traceID, parentSpanID string) *TraceContext {
	return &TraceContext{
		TraceID:      traceID,
		SpanID:       NewSpanID(),
		ParentSpanID: parentSpanID,
		StartTime:    system.GetCurrentTimeNs(),
	}
}

func (tc *TraceContext) ToJSON() *easyjson.JSON {
	data := easyjson.NewJSONObject()
	data.SetByPath("trace_id", easyjson.NewJSON(tc.TraceID))
	data.SetByPath("span_id", easyjson.NewJSON(tc.SpanID))
	data.SetByPath("parent_span_id", easyjson.NewJSON(tc.ParentSpanID))
	data.SetByPath("start_time", easyjson.NewJSON(tc.StartTime))

	return data.GetPtr()
}

func TraceContextFromJSON(j *easyjson.JSON) *TraceContext {
	if j == nil {
		return nil
	}
	st, ok := j.GetByPath("start_time").AsNumeric()
	if !ok {
		lg.Logf(lg.ErrorLevel, "TraceContextFromJSON: start_time not exist")
		st = 0
	}

	tc := &TraceContext{
		TraceID:      j.GetByPath("trace_id").AsStringDefault(""),
		SpanID:       j.GetByPath("span_id").AsStringDefault(""),
		ParentSpanID: j.GetByPath("parent_span_id").AsStringDefault(""),
		StartTime:    int64(st),
	}

	return tc
}

type TraceEvent struct {
	TraceID      string
	SpanID       string
	ParentSpanID string
	EventType    string
	FuncTypename string
	VertexID     string
	Timestamp    int64
	Duration     time.Duration
}

func PublishTraceEvent(nc *nats.Conn, domainName string, event *TraceEvent) {
	go func() {
		subject := fmt.Sprintf("trace.%s.events.%s", domainName, event.TraceID)
		eventJSON := easyjson.NewJSONObject()
		eventJSON.SetByPath("trace_id", easyjson.NewJSON(event.TraceID))
		eventJSON.SetByPath("span_id", easyjson.NewJSON(event.SpanID))
		eventJSON.SetByPath("parent_span_id", easyjson.NewJSON(event.ParentSpanID))
		eventJSON.SetByPath("event_type", easyjson.NewJSON(event.EventType))
		eventJSON.SetByPath("func_typename", easyjson.NewJSON(event.FuncTypename))
		eventJSON.SetByPath("vertex_id", easyjson.NewJSON(event.VertexID))
		eventJSON.SetByPath("timestamp", easyjson.NewJSON(event.Timestamp))
		if event.Duration > 0 {
			eventJSON.SetByPath("duration_ms", easyjson.NewJSON(event.Duration.Milliseconds()))
		}
		if err := nc.Publish(subject, eventJSON.ToBytes()); err != nil {
			lg.Logf(lg.ErrorLevel, "Failed to publish trace event for trace %s: %v", event.TraceID, err)
		}
	}()
}

func NewTraceID() string {
	return generateID()
}

func NewSpanID() string {
	return generateID()
}

func generateID() string {
	return uuid.New().String()
}
