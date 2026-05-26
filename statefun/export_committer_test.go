package statefun

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/foliagecp/sdk/statefun/cache"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const testStorePrefix = "store"

// WAL values are now the raw user payload — no [time][flag][payload] header.
// DELETE ops carry no value (the committer infers "this is a delete" from
// WALOp.OpType). Helpers kept by name so existing test bodies need no churn.

func makeJSONWALValue(jsonStr string) []byte { return []byte(jsonStr) }

func makeBytesWALValue(data string) []byte { return []byte(data) }

func makeDeletedWALValue() []byte { return nil }

// --- parseKey tests ---

func TestParseKey_VertexBody(t *testing.T) {
	pk := parseKey("store.hub/vertex1", testStorePrefix)
	require.NotNil(t, pk)
	assert.Equal(t, "vertex", pk.entityType)
	assert.Equal(t, "hub/vertex1", pk.vertexID)
	assert.Equal(t, "body", pk.role)
	assert.Equal(t, "hub/vertex1", pk.entityKey)
}

func TestParseKey_VertexBody_NoDomain(t *testing.T) {
	pk := parseKey("store.root", testStorePrefix)
	require.NotNil(t, pk)
	assert.Equal(t, "vertex", pk.entityType)
	assert.Equal(t, "root", pk.vertexID)
}

func TestParseKey_OutLinkTarget(t *testing.T) {
	pk := parseKey("store.hub/srv01.out.to.hosts", testStorePrefix)
	require.NotNil(t, pk)
	assert.Equal(t, "link", pk.entityType)
	assert.Equal(t, "target", pk.role)
	assert.Equal(t, "hub/srv01", pk.from)
	assert.Equal(t, "hosts", pk.linkName)
	assert.Equal(t, "hub/srv01\x00hosts", pk.entityKey)
}

func TestParseKey_OutLinkBody(t *testing.T) {
	pk := parseKey("store.hub/srv01.out.body.hosts", testStorePrefix)
	require.NotNil(t, pk)
	assert.Equal(t, "link", pk.entityType)
	assert.Equal(t, "body", pk.role)
	assert.Equal(t, "hub/srv01", pk.from)
	assert.Equal(t, "hosts", pk.linkName)
}

func TestParseKey_LinkTypeIndex(t *testing.T) {
	pk := parseKey("store.hub/srv01.ltype.__object.hub/rackA", testStorePrefix)
	require.NotNil(t, pk)
	assert.Equal(t, "aux", pk.entityType)
}

func TestParseKey_InLink(t *testing.T) {
	pk := parseKey("store.hub/rackA.in.hub/srv01.hosts", testStorePrefix)
	require.NotNil(t, pk)
	assert.Equal(t, "aux", pk.entityType)
}

func TestParseKey_VertexBodyIndex(t *testing.T) {
	pk := parseKey("store.hub/vertex1.bodyindex.prop1.string", testStorePrefix)
	require.NotNil(t, pk)
	assert.Equal(t, "aux", pk.entityType)
}

func TestParseKey_OutLinkIndex_Tag(t *testing.T) {
	pk := parseKey("store.hub/srv01.out.index.hosts.tag.critical", testStorePrefix)
	require.NotNil(t, pk)
	assert.Equal(t, "link", pk.entityType)
	assert.Equal(t, "tag", pk.role)
	assert.Equal(t, "hub/srv01", pk.from)
	assert.Equal(t, "hosts", pk.linkName)
	assert.Equal(t, "critical", pk.tagValue)
}

func TestParseKey_OutLinkIndex_Type(t *testing.T) {
	pk := parseKey("store.hub/srv01.out.index.hosts.type.__object", testStorePrefix)
	require.NotNil(t, pk)
	assert.Equal(t, "aux", pk.entityType) // type index → aux
}

func TestParseKey_OutLinkBodyIndex(t *testing.T) {
	pk := parseKey("store.hub/srv01.out.bodyindex.hosts.zone.number", testStorePrefix)
	require.NotNil(t, pk)
	assert.Equal(t, "aux", pk.entityType)
}

func TestParseKey_ConsistencyKey(t *testing.T) {
	pk := parseKey("__kv_consistent__", "")
	assert.Nil(t, pk)
}

func TestParseKey_EmptyAfterPrefix(t *testing.T) {
	pk := parseKey("store.", testStorePrefix)
	assert.Nil(t, pk)
}

// --- extractBody tests ---

func TestExtractBody_JSON(t *testing.T) {
	val := makeJSONWALValue(`{"cpu":16,"ram":64}`)
	body := extractBody(val)
	require.NotNil(t, body)
	assert.JSONEq(t, `{"cpu":16,"ram":64}`, string(body))
}

func TestExtractBody_Deleted(t *testing.T) {
	val := makeDeletedWALValue()
	body := extractBody(val)
	assert.Nil(t, body)
}

func TestExtractBody_RawPayload(t *testing.T) {
	// After the cache-as-source-of-truth refactor, WAL values are raw user
	// payload — no header to strip, so a short non-empty value is returned
	// as-is rather than rejected as "too short for header".
	body := extractBody([]byte{1, 2, 3})
	assert.Equal(t, json.RawMessage{1, 2, 3}, body)
}

func TestExtractTargetValue(t *testing.T) {
	val := makeBytesWALValue("__object.hub/rackA")
	target := extractTargetValue(val)
	assert.Equal(t, "__object.hub/rackA", target)
}

// --- deduplicateOps tests ---

func TestDeduplicateOps_SingleVertex(t *testing.T) {
	body := `{"name":"server-01"}`
	ops := []WALOp{
		{OpType: cache.OpTypePUT, Key: "store.hub/srv01", Value: makeJSONWALValue(body)},
		{OpType: cache.OpTypePUT, Key: "store.hub/srv01.bodyindex.name.string", Value: makeBytesWALValue("server-01")},
	}

	result := deduplicateOps(ops, testStorePrefix)
	require.Len(t, result, 1)
	assert.Equal(t, "vertex_put", result[0].Op)
	assert.Equal(t, "hub/srv01", result[0].ID)
	assert.JSONEq(t, body, string(result[0].Body))
}

func TestDeduplicateOps_SingleLink(t *testing.T) {
	linkBody := `{"zone":1}`
	ops := []WALOp{
		{OpType: cache.OpTypePUT, Key: "store.hub/srv01.out.to.hosts", Value: makeBytesWALValue("__object.hub/rackA")},
		{OpType: cache.OpTypePUT, Key: "store.hub/srv01.out.body.hosts", Value: makeJSONWALValue(linkBody)},
		{OpType: cache.OpTypePUT, Key: "store.hub/srv01.ltype.__object.hub/rackA", Value: makeBytesWALValue("hosts")},
		{OpType: cache.OpTypePUT, Key: "store.hub/srv01.out.index.hosts.type.__object", Value: nil},
		{OpType: cache.OpTypePUT, Key: "store.hub/srv01.out.index.hosts.tag.critical", Value: nil},
		{OpType: cache.OpTypePUT, Key: "store.hub/srv01.out.index.hosts.tag.production", Value: nil},
		{OpType: cache.OpTypePUT, Key: "store.hub/rackA.in.hub/srv01.hosts", Value: makeBytesWALValue("__object")},
	}

	result := deduplicateOps(ops, testStorePrefix)
	require.Len(t, result, 1)
	assert.Equal(t, "link_put", result[0].Op)
	assert.Equal(t, "hub/srv01", result[0].From)
	assert.Equal(t, "hub/rackA", result[0].To)
	assert.Equal(t, "hosts", result[0].Name)
	assert.Equal(t, "__object", result[0].LinkType)
	assert.JSONEq(t, linkBody, string(result[0].Body))
	assert.ElementsMatch(t, []string{"critical", "production"}, result[0].Tags)
}

func TestDeduplicateOps_MixedOps(t *testing.T) {
	ops := []WALOp{
		{OpType: cache.OpTypePUT, Key: "store.hub/v1", Value: makeJSONWALValue(`{"a":1}`)},
		{OpType: cache.OpTypePUT, Key: "store.hub/v1.out.to.link1", Value: makeBytesWALValue("type1.hub/v2")},
		{OpType: cache.OpTypePUT, Key: "store.hub/v1.out.body.link1", Value: makeJSONWALValue(`{}`)},
		{OpType: cache.OpTypePUT, Key: "store.hub/v2", Value: makeJSONWALValue(`{"b":2}`)},
	}

	result := deduplicateOps(ops, testStorePrefix)
	require.Len(t, result, 3) // 2 vertices + 1 link

	// Vertices come first, then links (grouped by entity type)
	assert.Equal(t, "vertex_put", result[0].Op)
	assert.Equal(t, "hub/v1", result[0].ID)
	assert.Equal(t, "vertex_put", result[1].Op)
	assert.Equal(t, "hub/v2", result[1].ID)
	assert.Equal(t, "link_put", result[2].Op)
	assert.Equal(t, "hub/v1", result[2].From)
}

func TestDeduplicateOps_DeleteVertex(t *testing.T) {
	ops := []WALOp{
		{OpType: cache.OpTypeDelete, Key: "store.hub/old", Value: makeDeletedWALValue()},
		{OpType: cache.OpTypeDelete, Key: "store.hub/old.bodyindex.name.string", Value: makeDeletedWALValue()},
	}

	result := deduplicateOps(ops, testStorePrefix)
	require.Len(t, result, 1)
	assert.Equal(t, "vertex_delete", result[0].Op)
	assert.Equal(t, "hub/old", result[0].ID)
}

func TestDeduplicateOps_DeleteLink(t *testing.T) {
	ops := []WALOp{
		{OpType: cache.OpTypeDelete, Key: "store.hub/srv01.out.to.hosts", Value: makeDeletedWALValue()},
		{OpType: cache.OpTypeDelete, Key: "store.hub/srv01.out.body.hosts", Value: makeDeletedWALValue()},
		{OpType: cache.OpTypeDelete, Key: "store.hub/srv01.ltype.__object.hub/rackA", Value: makeDeletedWALValue()},
		{OpType: cache.OpTypeDelete, Key: "store.hub/rackA.in.hub/srv01.hosts", Value: makeDeletedWALValue()},
	}

	result := deduplicateOps(ops, testStorePrefix)
	require.Len(t, result, 1)
	assert.Equal(t, "link_delete", result[0].Op)
	assert.Equal(t, "hub/srv01", result[0].From)
	assert.Equal(t, "hosts", result[0].Name)
}

func TestDeduplicateOps_EmptyOps(t *testing.T) {
	result := deduplicateOps(nil, testStorePrefix)
	assert.Empty(t, result)
}

func TestDeduplicateOps_OnlyAuxKeys(t *testing.T) {
	ops := []WALOp{
		{OpType: cache.OpTypePUT, Key: "store.hub/v1.bodyindex.name.string", Value: makeBytesWALValue("test")},
		{OpType: cache.OpTypePUT, Key: "store.hub/v1.ltype.type1.hub/v2", Value: makeBytesWALValue("link1")},
	}

	result := deduplicateOps(ops, testStorePrefix)
	assert.Empty(t, result)
}

// --- ExportEvent JSON serialization ---

func TestExportEvent_Serialization(t *testing.T) {
	event := ExportEvent{
		TxID:      "12345",
		Domain:    "hub",
		Timestamp: 1710700005000000000,
		Ops: []ExportOp{
			{Op: "vertex_put", ID: "hub/srv01", Body: json.RawMessage(`{"cpu":16}`)},
			{Op: "link_put", From: "hub/srv01", To: "hub/rackA", Name: "hosts", LinkType: "__object", Tags: []string{"critical"}},
			{Op: "vertex_delete", ID: "hub/old"},
		},
	}

	data, err := json.Marshal(event)
	require.NoError(t, err)

	var decoded ExportEvent
	err = json.Unmarshal(data, &decoded)
	require.NoError(t, err)

	assert.Equal(t, event.TxID, decoded.TxID)
	assert.Equal(t, event.Domain, decoded.Domain)
	require.Len(t, decoded.Ops, 3)
	assert.Equal(t, "vertex_put", decoded.Ops[0].Op)
	assert.Equal(t, "hub/srv01", decoded.Ops[0].ID)
	assert.JSONEq(t, `{"cpu":16}`, string(decoded.Ops[0].Body))
	assert.Equal(t, "link_put", decoded.Ops[1].Op)
	assert.Equal(t, []string{"critical"}, decoded.Ops[1].Tags)
	assert.Equal(t, "vertex_delete", decoded.Ops[2].Op)
}

// --- Nonfunctional: large transaction ---

func TestDeduplicateOps_LargeTransaction(t *testing.T) {
	// Simulate 1000 vertices, each with 2 bodyindex keys = 3000 WAL ops
	ops := make([]WALOp, 0, 3000)
	for i := 0; i < 1000; i++ {
		id := fmt.Sprintf("hub/v%04d", i)
		body := fmt.Sprintf(`{"i":%d}`, i)
		ops = append(ops,
			WALOp{OpType: cache.OpTypePUT, Key: "store." + id, Value: makeJSONWALValue(body)},
			WALOp{OpType: cache.OpTypePUT, Key: "store." + id + ".bodyindex.i.number", Value: makeBytesWALValue("1")},
			WALOp{OpType: cache.OpTypePUT, Key: "store." + id + ".bodyindex.name.string", Value: makeBytesWALValue("test")},
		)
	}

	result := deduplicateOps(ops, testStorePrefix)
	// Should have exactly 1000 vertex_put ops (bodyindex keys are aux)
	assert.Len(t, result, 1000)
	for _, op := range result {
		assert.Equal(t, "vertex_put", op.Op)
	}
}

// --- Benchmarks ---

func BenchmarkParseKey_Vertex(b *testing.B) {
	for i := 0; i < b.N; i++ {
		parseKey("store.hub/vertex1", testStorePrefix)
	}
}

func BenchmarkParseKey_OutLinkTarget(b *testing.B) {
	for i := 0; i < b.N; i++ {
		parseKey("store.hub/srv01.out.to.hosts", testStorePrefix)
	}
}

func BenchmarkParseKey_InLink(b *testing.B) {
	for i := 0; i < b.N; i++ {
		parseKey("store.hub/rackA.in.hub/srv01.hosts", testStorePrefix)
	}
}

func BenchmarkDeduplicateOps_100Entities(b *testing.B) {
	ops := make([]WALOp, 0, 800)
	for i := 0; i < 100; i++ {
		id := "hub/v" + string(rune('0'+i%10))
		ops = append(ops,
			WALOp{OpType: cache.OpTypePUT, Key: "store." + id, Value: makeJSONWALValue(`{"a":1}`)},
			WALOp{OpType: cache.OpTypePUT, Key: "store." + id + ".bodyindex.a.number", Value: makeBytesWALValue("1")},
			WALOp{OpType: cache.OpTypePUT, Key: "store." + id + ".out.to.link1", Value: makeBytesWALValue("t.hub/v0")},
			WALOp{OpType: cache.OpTypePUT, Key: "store." + id + ".out.body.link1", Value: makeJSONWALValue(`{}`)},
			WALOp{OpType: cache.OpTypePUT, Key: "store." + id + ".ltype.t.hub/v0", Value: makeBytesWALValue("link1")},
			WALOp{OpType: cache.OpTypePUT, Key: "store." + id + ".out.index.link1.type.t", Value: nil},
			WALOp{OpType: cache.OpTypePUT, Key: "store.hub/v0.in." + id + ".link1", Value: makeBytesWALValue("t")},
		)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		deduplicateOps(ops, testStorePrefix)
	}
}
