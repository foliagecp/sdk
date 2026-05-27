package debug

import (
	"os"
	"strings"
	"testing"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/embedded/graph/crud"
	"github.com/foliagecp/sdk/statefun"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
	"github.com/foliagecp/sdk/statefun/test"
	"github.com/stretchr/testify/suite"
)

func TestMain(m *testing.M) {
	if system.GlobalPrometrics == nil {
		system.GlobalPrometrics = system.NewPrometrics("", "127.0.0.1:0")
	}
	os.Exit(m.Run())
}

// ---- suite ------------------------------------------------------------------

type ExportTestSuite struct {
	test.StatefunTestSuite
}

func TestExportTestSuite(t *testing.T) {
	suite.Run(t, new(ExportTestSuite))
}

func (s *ExportTestSuite) setupRuntime() {
	cfg := *statefun.NewFunctionTypeConfig().
		SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).
		SetMaxIdHandlers(-1)

	// Graph CRUD — needed by LLAPIPrintGraph for vertex.read and link.read
	s.RegisterFunction("functions.graph.api.vertex.create", crud.LLAPIVertexCreate, cfg)
	s.RegisterFunction("functions.graph.api.vertex.read", crud.LLAPIVertexRead, cfg)
	s.RegisterFunction("functions.graph.api.link.create", crud.LLAPILinkCreate, cfg)
	s.RegisterFunction("functions.graph.api.link.read", crud.LLAPILinkRead, cfg)

	// Debug export: all export operations go through one function type
	debugCfg := *statefun.NewFunctionTypeConfig().
		SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).
		SetMsgAckWaitMs(MAX_ACK_WAIT_MS)
	s.RegisterFunction("functions.graph.api.object.debug.print.graph", LLAPIPrintGraph, debugCfg)

	s.NoError(s.StartRuntime())
}

// buildGraph creates root → child1, root → child2.
func (s *ExportTestSuite) buildGraph() {
	create := func(id string) {
		p := easyjson.NewJSONObject()
		result, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.create", id, &p, nil)
		s.NoError(err)
		s.Equal("ok", result.GetByPath("status").AsStringDefault(""))
	}

	link := func(from, to, name, tp string) {
		p := easyjson.NewJSONObject()
		p.SetByPath("to", easyjson.NewJSON(s.SetThisDomainPreffix(to)))
		p.SetByPath("name", easyjson.NewJSON(name))
		p.SetByPath("type", easyjson.NewJSON(tp))
		result, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.create", from, &p, nil)
		s.NoError(err)
		s.Equal("ok", result.GetByPath("status").AsStringDefault(""))
	}

	create("root")
	create("child1")
	create("child2")
	link("root", "child1", "c1", "contains")
	link("root", "child2", "c2", "contains")
}

// printGraph requests a full graph export and returns the raw result JSON.
func (s *ExportTestSuite) printGraph(id, format, delivery string) *easyjson.JSON {
	p := easyjson.NewJSONObject()
	p.SetByPath("format", easyjson.NewJSON(format))
	p.SetByPath("delivery", easyjson.NewJSON(delivery))

	result, err := s.Request(sfPlugins.AutoRequestSelect,
		"functions.graph.api.object.debug.print.graph", id, &p, nil)
	s.NoError(err)
	return result
}

// assembleChunks fetches all chunks for a session and returns the assembled string.
func (s *ExportTestSuite) assembleChunks(sessionID, vertexID string, totalChunks int) string {
	var buf strings.Builder
	for i := 0; i < totalChunks; i++ {
		p := easyjson.NewJSONObject()
		p.SetByPath("export_action", easyjson.NewJSON("get_chunk"))
		p.SetByPath("session_id", easyjson.NewJSON(sessionID))
		p.SetByPath("chunk_index", easyjson.NewJSON(i))

		result, err := s.Request(sfPlugins.AutoRequestSelect,
			"functions.graph.api.object.debug.print.graph", vertexID, &p, nil)
		s.NoError(err)
		s.Equal("ok", result.GetByPath("status").AsStringDefault(""), "chunk %d status", i)

		buf.WriteString(result.GetByPath("data.data").AsStringDefault(""))

		isLast := result.GetByPath("data.last").AsBoolDefault(false)
		if i == totalChunks-1 {
			s.True(isLast, "last chunk must set last=true")
		} else {
			s.False(isLast, "chunk %d should not be last", i)
		}
	}
	return buf.String()
}

// ---- tests ------------------------------------------------------------------

// Test_ChunkedExport_AssemblesCorrectly verifies that chunked assembly produces
// a complete graph export matching total_bytes and containing expected content.
func (s *ExportTestSuite) Test_ChunkedExport_AssemblesCorrectly() {
	s.setupRuntime()
	s.buildGraph()

	chunkedResult := s.printGraph("root", "dot", "chunks")
	s.Equal("ok", chunkedResult.GetByPath("status").AsStringDefault(""))
	s.Equal("chunks", chunkedResult.GetByPath("data.delivery").AsStringDefault(""))

	sessionID := chunkedResult.GetByPath("data.session_id").AsStringDefault("")
	vertexID := chunkedResult.GetByPath("data.vertex_id").AsStringDefault("")
	totalChunks := int(chunkedResult.GetByPath("data.total_chunks").AsNumericDefault(0))
	totalBytes := int(chunkedResult.GetByPath("data.total_bytes").AsNumericDefault(0))
	s.NotEmpty(sessionID)
	s.NotEmpty(vertexID)
	s.Greater(totalChunks, 0)
	s.Greater(totalBytes, 0)

	assembled := s.assembleChunks(sessionID, vertexID, totalChunks)

	// Assembled length must match advertised total_bytes
	s.Equal(totalBytes, len(assembled))

	// Basic structural checks on DOT output
	s.Contains(assembled, "digraph")
	s.Contains(assembled, "hub/root")
	s.Contains(assembled, "hub/child1")
	s.Contains(assembled, "hub/child2")
	s.Contains(assembled, "c1")
	s.Contains(assembled, "c2")
}

// Test_ChunkedExport_FinishSession_ClearsSession verifies that FinishSession
// releases the in-memory session so subsequent chunk reads fail.
func (s *ExportTestSuite) Test_ChunkedExport_FinishSession_ClearsSession() {
	s.setupRuntime()
	s.buildGraph()

	chunkedResult := s.printGraph("root", "dot", "chunks")
	s.Equal("ok", chunkedResult.GetByPath("status").AsStringDefault(""))

	sessionID := chunkedResult.GetByPath("data.session_id").AsStringDefault("")
	vertexID := chunkedResult.GetByPath("data.vertex_id").AsStringDefault("")
	s.NotEmpty(sessionID)

	// Finish session explicitly
	fp := easyjson.NewJSONObject()
	fp.SetByPath("export_action", easyjson.NewJSON("finish_session"))
	fp.SetByPath("session_id", easyjson.NewJSON(sessionID))
	finishResult, err := s.Request(sfPlugins.AutoRequestSelect,
		"functions.graph.api.object.debug.print.graph", vertexID, &fp, nil)
	s.NoError(err)
	s.Equal("ok", finishResult.GetByPath("status").AsStringDefault(""))

	// Session must be gone — get_chunk should now fail
	cp := easyjson.NewJSONObject()
	cp.SetByPath("export_action", easyjson.NewJSON("get_chunk"))
	cp.SetByPath("session_id", easyjson.NewJSON(sessionID))
	cp.SetByPath("chunk_index", easyjson.NewJSON(0))
	chunkResult, err := s.Request(sfPlugins.AutoRequestSelect,
		"functions.graph.api.object.debug.print.graph", vertexID, &cp, nil)
	s.NoError(err)
	s.NotEqual("ok", chunkResult.GetByPath("status").AsStringDefault("ok"),
		"get_chunk after finish_session should not return ok")
}

// Test_ChunkedExport_ChunksAreIdempotent verifies that reading the same chunk
// index twice before FinishSession returns identical data.
func (s *ExportTestSuite) Test_ChunkedExport_ChunksAreIdempotent() {
	s.setupRuntime()
	s.buildGraph()

	result := s.printGraph("root", "dot", "chunks")
	s.Equal("ok", result.GetByPath("status").AsStringDefault(""))

	sessionID := result.GetByPath("data.session_id").AsStringDefault("")
	vertexID := result.GetByPath("data.vertex_id").AsStringDefault("")

	cp := easyjson.NewJSONObject()
	cp.SetByPath("export_action", easyjson.NewJSON("get_chunk"))
	cp.SetByPath("session_id", easyjson.NewJSON(sessionID))
	cp.SetByPath("chunk_index", easyjson.NewJSON(0))

	r1, _ := s.Request(sfPlugins.AutoRequestSelect,
		"functions.graph.api.object.debug.print.graph", vertexID, &cp, nil)
	r2, _ := s.Request(sfPlugins.AutoRequestSelect,
		"functions.graph.api.object.debug.print.graph", vertexID, &cp, nil)

	s.Equal(
		r1.GetByPath("data.data").AsStringDefault(""),
		r2.GetByPath("data.data").AsStringDefault(""),
	)
}

// Test_InlineDelivery_ReturnsFileField checks that a plain inline export still
// works (backward compatibility).
func (s *ExportTestSuite) Test_InlineDelivery_ReturnsFileField() {
	s.setupRuntime()
	s.buildGraph()

	result := s.printGraph("root", "dot", "inline")
	s.Equal("ok", result.GetByPath("status").AsStringDefault(""))
	s.Equal("inline", result.GetByPath("data.delivery").AsStringDefault(""))
	s.NotEmpty(result.GetByPath("data.file").AsStringDefault(""))
	// chunked fields must be absent
	s.Empty(result.GetByPath("data.session_id").AsStringDefault(""))
}

// Test_UnknownExportAction verifies that an unrecognised export_action returns failed
// instead of silently running a full graph export.
func (s *ExportTestSuite) Test_UnknownExportAction() {
	s.setupRuntime()

	p := easyjson.NewJSONObject()
	p.SetByPath("export_action", easyjson.NewJSON("get-chunk")) // typo: dash instead of underscore

	result, err := s.Request(sfPlugins.AutoRequestSelect,
		"functions.graph.api.object.debug.print.graph", "any-vertex", &p, nil)
	s.NoError(err)
	s.NotEqual("ok", result.GetByPath("status").AsStringDefault("ok"),
		"unknown export_action must not silently run a graph export")
}

// Test_GetChunk_MissingSessionID verifies that an absent session_id returns a failed status.
func (s *ExportTestSuite) Test_GetChunk_MissingSessionID() {
	s.setupRuntime()

	p := easyjson.NewJSONObject()
	p.SetByPath("export_action", easyjson.NewJSON("get_chunk"))
	p.SetByPath("chunk_index", easyjson.NewJSON(0)) // no session_id

	result, err := s.Request(sfPlugins.AutoRequestSelect,
		"functions.graph.api.object.debug.print.graph", "any-vertex", &p, nil)
	s.NoError(err)
	s.NotEqual("ok", result.GetByPath("status").AsStringDefault("ok"))
}

// Test_GetChunk_MissingChunkIndex verifies that an absent chunk_index returns a failed status.
func (s *ExportTestSuite) Test_GetChunk_MissingChunkIndex() {
	s.setupRuntime()

	p := easyjson.NewJSONObject()
	p.SetByPath("export_action", easyjson.NewJSON("get_chunk"))
	p.SetByPath("session_id", easyjson.NewJSON("some-session")) // no chunk_index

	result, err := s.Request(sfPlugins.AutoRequestSelect,
		"functions.graph.api.object.debug.print.graph", "any-vertex", &p, nil)
	s.NoError(err)
	s.NotEqual("ok", result.GetByPath("status").AsStringDefault("ok"))
}

// Test_AutoDelivery_SmallGraph_UsesInline verifies that delivery:"auto" selects
// inline for a graph that fits below the 1 MB threshold.
func (s *ExportTestSuite) Test_AutoDelivery_SmallGraph_UsesInline() {
	s.setupRuntime()
	s.buildGraph()

	p := easyjson.NewJSONObject()
	p.SetByPath("format", easyjson.NewJSON("dot"))
	p.SetByPath("delivery", easyjson.NewJSON("auto"))

	result, err := s.Request(sfPlugins.AutoRequestSelect,
		"functions.graph.api.object.debug.print.graph", "root", &p, nil)
	s.NoError(err)
	s.Equal("ok", result.GetByPath("status").AsStringDefault(""))
	s.Equal("inline", result.GetByPath("data.delivery").AsStringDefault(""),
		"small graph with delivery:auto must choose inline")
	s.NotEmpty(result.GetByPath("data.file").AsStringDefault(""))
	s.Empty(result.GetByPath("data.session_id").AsStringDefault(""))
}

// Test_ChunkedExport_GraphML verifies that graphml format works end-to-end
// with chunked delivery.
func (s *ExportTestSuite) Test_ChunkedExport_GraphML() {
	s.setupRuntime()
	s.buildGraph()

	result := s.printGraph("root", "graphml", "chunks")
	s.Equal("ok", result.GetByPath("status").AsStringDefault(""))
	s.Equal("chunks", result.GetByPath("data.delivery").AsStringDefault(""))

	sessionID := result.GetByPath("data.session_id").AsStringDefault("")
	vertexID := result.GetByPath("data.vertex_id").AsStringDefault("")
	totalChunks := int(result.GetByPath("data.total_chunks").AsNumericDefault(0))
	totalBytes := int(result.GetByPath("data.total_bytes").AsNumericDefault(0))
	s.NotEmpty(sessionID)
	s.Greater(totalChunks, 0)

	assembled := s.assembleChunks(sessionID, vertexID, totalChunks)

	s.Equal(totalBytes, len(assembled))
	s.Contains(assembled, "<graphml")
	s.Contains(assembled, "hub/root")
	s.Contains(assembled, "hub/child1")
	s.Contains(assembled, "hub/child2")
}

// Test_AutoDelivery_LargeGraph_UsesChunks verifies that delivery:"auto" switches
// to chunked mode when the export exceeds exportAutoThreshold.
// The threshold is temporarily lowered to 1 byte to avoid building a real 1 MB graph.
func (s *ExportTestSuite) Test_AutoDelivery_LargeGraph_UsesChunks() {
	old := exportAutoThreshold
	exportAutoThreshold = 1 // any non-empty export will exceed this
	defer func() { exportAutoThreshold = old }()

	s.setupRuntime()
	s.buildGraph()

	p := easyjson.NewJSONObject()
	p.SetByPath("format", easyjson.NewJSON("dot"))
	p.SetByPath("delivery", easyjson.NewJSON("auto"))

	result, err := s.Request(sfPlugins.AutoRequestSelect,
		"functions.graph.api.object.debug.print.graph", "root", &p, nil)
	s.NoError(err)
	s.Equal("ok", result.GetByPath("status").AsStringDefault(""))
	s.Equal("chunks", result.GetByPath("data.delivery").AsStringDefault(""),
		"export exceeding threshold must choose chunks")

	// Verify assembly works correctly in auto→chunks path
	sessionID := result.GetByPath("data.session_id").AsStringDefault("")
	vertexID := result.GetByPath("data.vertex_id").AsStringDefault("")
	totalChunks := int(result.GetByPath("data.total_chunks").AsNumericDefault(0))
	totalBytes := int(result.GetByPath("data.total_bytes").AsNumericDefault(0))
	s.Greater(totalChunks, 0)

	assembled := s.assembleChunks(sessionID, vertexID, totalChunks)
	s.Equal(totalBytes, len(assembled))
	s.Contains(assembled, "digraph")
}
