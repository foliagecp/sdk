package statefun

// Benchmarks for the JSON serialization hot path on the NATS message route.
//
// Step 1 of the "speed up ser/de" investigation: quantify where time goes
// BEFORE deciding whether to swap the encoder (jsoniter/sonic) or move
// metadata to NATS headers.
//
// The measured paths mirror production exactly:
//
//   send    → buildNatsData (io.go): NewJSONObject + SetByPath x4-5 +
//             ToBytes (=> encoding/json Marshal over interface{} tree)
//
//   receive → handleNatsMsg (nats_sources.go): JSONFromBytes
//             (=> encoding/json Unmarshal into interface{}) + 5x GetByPath
//             to extract payload/options/trace_context/caller_*
//
// Pure ToBytes/JSONFromBytes benches isolate the easyjson library cost that
// a Tier-1 encoder swap would change, separated from the envelope overhead
// that a Tier-2 headers change would remove.
//
// Run:
//   go test ./statefun/ -run '^$' -bench 'Serde|Envelope|JSONToBytes|JSONFromBytes' -benchmem

import (
	"testing"

	"github.com/foliagecp/easyjson"
)

// --- representative payloads -------------------------------------------------

// tiny: smallest realistic body (e.g. a single-field vertex update).
func payloadTiny() *easyjson.JSON {
	p := easyjson.NewJSONObject()
	p.SetByPath("body.cpu", easyjson.NewJSON(16))
	return &p
}

// link: a typical CRUD link.create payload.
func payloadLink() *easyjson.JSON {
	p := easyjson.NewJSONObject()
	p.SetByPath("to", easyjson.NewJSON("hub/rackA"))
	p.SetByPath("name", easyjson.NewJSON("hosts"))
	p.SetByPath("type", easyjson.NewJSON("__object"))
	p.SetByPath("body.zone", easyjson.NewJSON(1))
	return &p
}

// fat: a heavier object body with nesting and arrays — closer to real CMDB
// object payloads (specs, labels, tags).
func payloadFat() *easyjson.JSON {
	p := easyjson.NewJSONObject()
	p.SetByPath("body.spec.cpu", easyjson.NewJSON(64))
	p.SetByPath("body.spec.mem_gb", easyjson.NewJSON(512))
	p.SetByPath("body.spec.disk_gb", easyjson.NewJSON(4096))
	p.SetByPath("body.spec.nics", easyjson.NewJSON([]string{"eth0", "eth1", "eth2", "eth3"}))
	p.SetByPath("body.labels.rack", easyjson.NewJSON("A12"))
	p.SetByPath("body.labels.zone", easyjson.NewJSON("dc1-row3"))
	p.SetByPath("body.labels.env", easyjson.NewJSON("production"))
	p.SetByPath("body.labels.team", easyjson.NewJSON("platform-infra"))
	p.SetByPath("body.tags", easyjson.NewJSON([]string{"hot", "ssd", "gpu", "reserved", "monitored"}))
	p.SetByPath("body.status.up", easyjson.NewJSON(true))
	p.SetByPath("body.status.uptime_s", easyjson.NewJSON(987654))
	p.SetByPath("body.status.last_seen", easyjson.NewJSON(1779439322556726000))
	return &p
}

// emptyOptions mirrors what the runtime passes most of the time.
func benchOptions() *easyjson.JSON {
	o := easyjson.NewJSONObject()
	o.SetByPath("op_stack", easyjson.NewJSON(true))
	return &o
}

// decodeLikeHandleNatsMsg replays the exact extraction handleNatsMsg does on
// an incoming message: full parse + 5 GetByPath lookups.
func decodeLikeHandleNatsMsg(raw []byte) {
	data, ok := easyjson.JSONFromBytes(raw)
	if !ok {
		return
	}
	if data.GetByPath("payload").IsObject() {
		_ = data.GetByPath("payload")
	}
	if data.GetByPath("options").IsObject() {
		_ = data.GetByPath("options")
	}
	if data.GetByPath("trace_context").IsObject() {
		_ = data.GetByPath("trace_context")
	}
	if data.GetByPath("caller_typename").IsString() {
		_, _ = data.GetByPath("caller_typename").AsString()
	}
	if data.GetByPath("caller_id").IsString() {
		_, _ = data.GetByPath("caller_id").AsString()
	}
}

// --- envelope encode (send side) --------------------------------------------

func benchEnvelopeBuild(b *testing.B, payload *easyjson.JSON) {
	b.ReportAllocs()
	opts := benchOptions()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = buildNatsData("hub", "functions.graph.api.vertex.update", "hub/srv01", payload, opts, nil)
	}
}

func BenchmarkEnvelopeBuild_Tiny(b *testing.B) { benchEnvelopeBuild(b, payloadTiny()) }
func BenchmarkEnvelopeBuild_Link(b *testing.B) { benchEnvelopeBuild(b, payloadLink()) }
func BenchmarkEnvelopeBuild_Fat(b *testing.B)  { benchEnvelopeBuild(b, payloadFat()) }

// --- envelope decode (receive side) -----------------------------------------

func benchEnvelopeParse(b *testing.B, payload *easyjson.JSON) {
	raw := buildNatsData("hub", "functions.graph.api.vertex.update", "hub/srv01", payload, benchOptions(), nil)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		decodeLikeHandleNatsMsg(raw)
	}
}

func BenchmarkEnvelopeParse_Tiny(b *testing.B) { benchEnvelopeParse(b, payloadTiny()) }
func BenchmarkEnvelopeParse_Link(b *testing.B) { benchEnvelopeParse(b, payloadLink()) }
func BenchmarkEnvelopeParse_Fat(b *testing.B)  { benchEnvelopeParse(b, payloadFat()) }

// --- full message round-trip (one hop) --------------------------------------

func benchEnvelopeRoundTrip(b *testing.B, payload *easyjson.JSON) {
	b.ReportAllocs()
	opts := benchOptions()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		raw := buildNatsData("hub", "functions.graph.api.vertex.update", "hub/srv01", payload, opts, nil)
		decodeLikeHandleNatsMsg(raw)
	}
}

func BenchmarkEnvelopeRoundTrip_Tiny(b *testing.B) { benchEnvelopeRoundTrip(b, payloadTiny()) }
func BenchmarkEnvelopeRoundTrip_Link(b *testing.B) { benchEnvelopeRoundTrip(b, payloadLink()) }
func BenchmarkEnvelopeRoundTrip_Fat(b *testing.B)  { benchEnvelopeRoundTrip(b, payloadFat()) }

// --- pure easyjson lib cost (what a Tier-1 encoder swap would change) --------

func benchJSONToBytes(b *testing.B, payload *easyjson.JSON) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = payload.ToBytes()
	}
}

func BenchmarkJSONToBytes_Tiny(b *testing.B) { benchJSONToBytes(b, payloadTiny()) }
func BenchmarkJSONToBytes_Link(b *testing.B) { benchJSONToBytes(b, payloadLink()) }
func BenchmarkJSONToBytes_Fat(b *testing.B)  { benchJSONToBytes(b, payloadFat()) }

func benchJSONFromBytes(b *testing.B, payload *easyjson.JSON) {
	raw := payload.ToBytes()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = easyjson.JSONFromBytes(raw)
	}
}

func BenchmarkJSONFromBytes_Tiny(b *testing.B) { benchJSONFromBytes(b, payloadTiny()) }
func BenchmarkJSONFromBytes_Link(b *testing.B) { benchJSONFromBytes(b, payloadLink()) }
func BenchmarkJSONFromBytes_Fat(b *testing.B)  { benchJSONFromBytes(b, payloadFat()) }
