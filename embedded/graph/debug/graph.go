// Foliage graph store debug package.
// Provides debug stateful functions for the graph store

package debug

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/clients/go/db"
	"github.com/foliagecp/sdk/embedded/graph/crud"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"

	lg "github.com/foliagecp/sdk/statefun/logger"
)

// exportAutoThreshold is the file size above which delivery:"auto" switches to
// chunked mode. Declared as a variable so tests can lower it without building
// a multi-megabyte graph.
var exportAutoThreshold = 1 << 20 // 1 MB

type gNode struct {
	id    string
	depth int
}

type gEdge struct {
	from string        // parent vertex id
	name string        // link name
	to   string        // child vertex id
	tp   string        // link type
	tags []string      // link tags
	body easyjson.JSON // link's body
}

type exportConfig struct {
	excludeVertexFields []string
	excludeEdgeFields   []string
}

/*
LLAPIPrintGraph handles all graph export operations for a single vertex, dispatched
by the optional export_action field:

  - (absent)         — BFS graph traversal + export (default)
  - "get_chunk"      — fetch one chunk of a previously started chunked export
  - "finish_session" — release an in-memory export session

All three actions are intentionally handled by the same function type so that
the statefun single-instance lock guarantees they execute on the same process,
where the in-memory ExportSessionStore lives.

BFS payload:

	{
	    "depth":    int    // optional, default: -1 (unlimited)
	    "format":   string // "dot" | "graphml", default: "dot"
	    "delivery": string // "inline" | "auto" | "chunks", default: "inline"
	    "json2xml": bool   // graphml only, default: false
	    "exclude":  { "vertex": [...], "edge": [...] }  // optional
	}

get_chunk payload:

	{ "export_action": "get_chunk", "session_id": "...", "chunk_index": N }

finish_session payload:

	{ "export_action": "finish_session", "session_id": "..." }
*/
func LLAPIPrintGraph(executor sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	switch action := ctx.Payload.GetByPath("export_action").AsStringDefault(""); action {
	case "":
		exportPrintGraph(ctx)
	case "get_chunk":
		exportGetChunk(ctx)
	case "finish_session":
		exportFinishSession(ctx)
	default:
		om := sfMediators.NewOpMediator(ctx)
		om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("unknown export_action %q", action))).Reply()
	}
}

func exportPrintGraph(ctx *sfPlugins.StatefunContextProcessor) {
	self := ctx.Self
	payload := ctx.Payload

	maxDepth := int(payload.GetByPath("depth").AsNumericDefault(-1))

	var conf exportConfig
	if arr, ok := payload.GetByPath("exclude.vertex").AsArrayString(); ok {
		conf.excludeVertexFields = arr
	}
	if arr, ok := payload.GetByPath("exclude.edge").AsArrayString(); ok {
		conf.excludeEdgeFields = arr
	}

	nodes := map[string]*easyjson.JSON{}
	uniqueEdges := map[string]struct{}{}
	queue := []gNode{{self.ID, 0}}
	edges := []gEdge{}

	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]

		if _, exists := nodes[node.id]; exists {
			continue
		}

		b, e := getVertexBodyAndOutLinks(ctx, node.id)
		nodes[node.id] = b

		for _, edge := range e {
			if maxDepth < 0 || node.depth < maxDepth {
				if _, ok := nodes[edge.to]; !ok {
					queue = append(queue, gNode{edge.to, node.depth + 1}) // Forward link itrospection
				}
				if _, ok := nodes[edge.from]; !ok {
					queue = append(queue, gNode{edge.from, node.depth + 1}) // Inward link introspection
				}
			}
			if edge.from == node.id {
				if _, ok := uniqueEdges[edge.from+edge.name]; !ok {
					uniqueEdges[edge.from+edge.name] = struct{}{}
					edges = append(edges, edge)
				}
			}
		}
	}

	om := sfMediators.NewOpMediator(ctx)

	format := payload.GetByPath("format").AsStringDefault("dot")
	var fileData string
	switch format {
	case "graphml":
		fileData = createGraphML(ctx.Self.ID, ctx.Domain, nodes, edges, conf, payload.GetByPath("json2xml").AsBoolDefault(false))
	case "dot":
		fileData = createGraphViz(ctx.Self.ID, ctx.Domain, nodes, edges)
	default:
		om.AggregateOpMsg(sfMediators.OpMsgFailed(fmt.Sprintf("%s – unsopported format", format))).Reply()
		return
	}

	requestedDelivery := payload.GetByPath("delivery").AsStringDefault("inline")
	useChunked := requestedDelivery == "chunks" ||
		(requestedDelivery == "auto" && len(fileData) > exportAutoThreshold)

	if useChunked {
		sessionID, totalChunks, chunkSz := defaultStore.Put(fileData)
		reply := easyjson.NewJSONObject()
		reply.SetByPath("format", easyjson.NewJSON(format))
		reply.SetByPath("delivery", easyjson.NewJSON("chunks"))
		reply.SetByPath("session_id", easyjson.NewJSON(sessionID))
		reply.SetByPath("vertex_id", easyjson.NewJSON(ctx.Self.ID))
		reply.SetByPath("total_chunks", easyjson.NewJSON(totalChunks))
		reply.SetByPath("total_bytes", easyjson.NewJSON(int64(len(fileData))))
		reply.SetByPath("chunk_size", easyjson.NewJSON(chunkSz))
		om.AggregateOpMsg(sfMediators.OpMsgOk(reply)).Reply()
		return
	}

	reply := easyjson.NewJSONObjectWithKeyValue("file", easyjson.NewJSON(fileData))
	reply.SetByPath("format", easyjson.NewJSON(format))
	reply.SetByPath("delivery", easyjson.NewJSON("inline"))
	om.AggregateOpMsg(sfMediators.OpMsgOk(reply)).Reply()
}

func exportGetChunk(ctx *sfPlugins.StatefunContextProcessor) {
	om := sfMediators.NewOpMediator(ctx)
	payload := ctx.Payload

	sessionID := payload.GetByPath("session_id").AsStringDefault("")
	chunkIndex := int(payload.GetByPath("chunk_index").AsNumericDefault(-1))
	if sessionID == "" || chunkIndex < 0 {
		om.AggregateOpMsg(sfMediators.OpMsgFailed("session_id and chunk_index are required")).Reply()
		return
	}

	data, last, err := defaultStore.GetChunk(sessionID, chunkIndex)
	if err != nil {
		lg.Logf(lg.WarnLevel, "get_chunk: vertex=%s session=%s index=%d: %v", ctx.Self.ID, sessionID, chunkIndex, err)
		om.AggregateOpMsg(sfMediators.OpMsgFailed(err.Error())).Reply()
		return
	}

	reply := easyjson.NewJSONObject()
	reply.SetByPath("data", easyjson.NewJSON(data))
	reply.SetByPath("chunk_index", easyjson.NewJSON(chunkIndex))
	reply.SetByPath("last", easyjson.NewJSON(last))
	om.AggregateOpMsg(sfMediators.OpMsgOk(reply)).Reply()
}

func exportFinishSession(ctx *sfPlugins.StatefunContextProcessor) {
	om := sfMediators.NewOpMediator(ctx)
	sessionID := ctx.Payload.GetByPath("session_id").AsStringDefault("")
	if sessionID == "" {
		om.AggregateOpMsg(sfMediators.OpMsgFailed("session_id is required")).Reply()
		return
	}
	defaultStore.FinishSession(sessionID)
	om.AggregateOpMsg(sfMediators.OpMsgOk(easyjson.NewJSONObject())).Reply()
}

func getVertexBodyAndOutLinks(ctx *sfPlugins.StatefunContextProcessor, id string) (*easyjson.JSON, []gEdge) {
	var outLinkNames []string
	var inLinks *easyjson.JSON

	var vertexBody *easyjson.JSON
	payload := easyjson.NewJSONObjectWithKeyValue("details", easyjson.NewJSON(true))
	som := sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.read", id, &payload, nil))
	if som.Status == sfMediators.SYNC_OP_STATUS_OK {
		if arr, ok := som.Data.GetByPath("links.out.names").AsArrayString(); ok {
			outLinkNames = arr
		}
		inLinks = som.Data.GetByPath("links.in").GetPtr()
		vertexBody = som.Data.GetByPathPtr("body")
	}
	if outLinkNames == nil {
		outLinkNames = []string{}
	}
	if inLinks == nil {
		inLinks = easyjson.NewJSONArray().GetPtr()
	}

	edges := []gEdge{}

	for _, outLinkName := range outLinkNames {
		lt := ""
		to := ""
		var body easyjson.JSON
		var tags []string

		payload := easyjson.NewJSONObjectWithKeyValue("details", easyjson.NewJSON(true))
		payload.SetByPath("name", easyjson.NewJSON(outLinkName))
		som := sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.read", id, &payload, nil))
		if som.Status == sfMediators.SYNC_OP_STATUS_OK {
			lt = som.Data.GetByPath("type").AsStringDefault(lt)
			to = som.Data.GetByPath("to").AsStringDefault(to)
			body = som.Data.GetByPath("body")
			if arr, ok := som.Data.GetByPath("tags").AsArrayString(); ok {
				tags = arr
			}
		}
		if tags == nil {
			tags = []string{}
		}

		if len(id) > 0 && len(outLinkName) > 0 && len(to) > 0 && len(lt) > 0 {
			edges = append(edges, gEdge{
				from: id,
				name: outLinkName,
				to:   to,
				tp:   lt,
				tags: tags,
				body: body,
			})
		}
	}

	for i := 0; i < inLinks.ArraySize(); i++ {
		inLink := inLinks.ArrayElement(i)
		from := inLink.GetByPath("from").AsStringDefault("")
		linkName := inLink.GetByPath("name").AsStringDefault("")

		lt := ""
		var tags []string

		payload := easyjson.NewJSONObjectWithKeyValue("details", easyjson.NewJSON(true))
		payload.SetByPath("name", easyjson.NewJSON(linkName))
		// TODO: Do not read in link to get link type, but tags cannot be obtained by any other means
		som := sfMediators.OpMsgFromSfReply(ctx.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.read", from, &payload, nil))
		if som.Status == sfMediators.SYNC_OP_STATUS_OK {
			lt = som.Data.GetByPath("type").AsStringDefault(lt)
			if arr, ok := som.Data.GetByPath("tags").AsArrayString(); ok {
				tags = arr
			}
		}
		if tags == nil {
			tags = []string{}
		}

		if len(from) > 0 && len(linkName) > 0 && len(id) > 0 && len(lt) > 0 {
			edges = append(edges, gEdge{
				from: from,
				name: linkName,
				to:   id,
				tp:   lt,
				tags: tags,
			})
		}
	}

	return vertexBody, edges
}

/*
format: string // "graphml"
source: string // "file" | "payload"
data: string // "graph data" | "file path"

Example:
nats -s nats://nats:foliage@nats:4222 pub signal.hub.functions.graph.api.import.a "{\"payload\":{\"format\":\"graphml\",\"source\":\"file\",\"data\":\"./skala-xml.graphml\"}}"
*/
func LLAPIImportGraph(executor sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
	const workers = 10

	om := sfMediators.NewOpMediator(ctx)

	payload := ctx.Payload
	format := payload.GetByPath("format").AsStringDefault("dot")
	source := payload.GetByPath("source").AsStringDefault("payload")

	lg.Logln(lg.InfoLevel, "------------------ GRAPH <<< IMPORTING <<< DATA ------------------")

	switch format {
	case "graphml":
		lg.Logln(lg.InfoLevel, "Format: graphml")
		graph := RawGraph{}
		switch source {
		case "payload":
			lg.Logln(lg.InfoLevel, "Source: payload.data")
			reader := strings.NewReader(payload.GetByPath("data").AsStringDefault(""))
			g, err := ImportGraphML(reader)
			if err != nil {
				om.AggregateOpMsg(sfMediators.OpMsgFailed(err.Error())).Reply()
				lg.Logln(lg.ErrorLevel, "Termination due to an error: %s", err.Error())
				return
			}
			graph = g
		case "file":
			fileName := payload.GetByPath("data").AsStringDefault("")
			lg.Logln(lg.InfoLevel, "Source: file %s", fileName)
			f, err := os.Open(fileName)
			if err != nil {
				om.AggregateOpMsg(sfMediators.OpMsgFailed(err.Error())).Reply()
				lg.Logln(lg.ErrorLevel, "Termination due to an error: %s", err.Error())
				return
			}
			defer f.Close()
			g, err := ImportGraphML(f)
			if err != nil {
				om.AggregateOpMsg(sfMediators.OpMsgFailed(err.Error())).Reply()
				lg.Logln(lg.ErrorLevel, "Termination due to an error: %s", err.Error())
				return
			}
			graph = g
		}

		dbc, err := db.NewDBSyncClientFromRequestFunction(ctx.Request)
		if err != nil {
			lg.Logln(lg.ErrorLevel, "Termination due to an error: %s", err.Error())
			om.AggregateOpMsg(sfMediators.OpMsgFailed(err.Error())).Reply()
			return
		}

		t0 := time.Now()
		// ---------- PHASE 1: vertices ----------
		lg.Logln(lg.InfoLevel, "Importing vertices with %d workers...", workers)

		var wg sync.WaitGroup
		vertexIdx := make(chan int, workers*4)

		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := range vertexIdx {
					n := graph.Nodes[i]
					uuid := ctx.Domain.CreateObjectIDWithHubDomain(n.Id, true)

					body, _ := ExtractBodyAsJSON(n.Attributes)

					// Built-in skeleton (root/types/objects/nav and the built-in
					// types): MERGE the dumped body and KEEP the vertex with its
					// links. Deleting it would cascade away registrations the dump
					// cannot restore — a dump taken before a built-in type existed
					// carries no edge for it, so wiping `types` would silently
					// unregister it and CRUD would stop recognizing the type.
					//
					// Merge, not replace, because these bodies are not the dump's
					// to own: `root` carries the graph's protected-field policy,
					// the `types` and `objects` roots carry meta-trigger
					// registrations. A dump knows only what its source system had
					// at export time, and replacing would silently drop whatever
					// this graph has configured since — settings no import was
					// asked to touch. What the dump does carry still lands (merge
					// gives the incoming value the last word), so importing a
					// skeleton body remains meaningful.
					if crud.IsBuiltInSchemaID(ctx.Domain, uuid) {
						if err := dbc.Graph.VertexUpdate(uuid, body, false, true); err != nil {
							system.MsgOnErrorReturn(err)
						}
						continue
					}

					if err := dbc.Graph.VertexDelete(uuid); err != nil {
						system.MsgOnErrorReturn(err)
					}
					if err := dbc.Graph.VertexCreate(uuid, body); err != nil {
						system.MsgOnErrorReturn(err)
					}
				}
			}()
		}

		for i := range graph.Nodes {
			vertexIdx <- i
		}
		close(vertexIdx)
		wg.Wait()

		// ---------- PHASE 2: edges ----------
		lg.Logln(lg.InfoLevel, "Importing edges with %d workers...", workers)

		var wgE sync.WaitGroup
		edgeIdx := make(chan int, workers*4)

		for w := 0; w < workers; w++ {
			wgE.Add(1)
			go func() {
				defer wgE.Done()
				for i := range edgeIdx {
					e := graph.Edges[i]
					uuidFrom := ctx.Domain.CreateObjectIDWithHubDomain(e.Source, true)
					uuidTo := ctx.Domain.CreateObjectIDWithHubDomain(e.Target, true)

					tp, name, tags := ExtractEdgeTypeAndNameAndTags(e.Attributes)
					body, _ := ExtractBodyAsJSON(e.Attributes)

					if err := dbc.Graph.VerticesLinkCreate(uuidFrom, uuidTo, name, tp, tags, body); err != nil {
						system.MsgOnErrorReturn(err)
					}
				}
			}()
		}

		for i := range graph.Edges {
			edgeIdx <- i
		}
		close(edgeIdx)
		wgE.Wait()

		// The import rebuilt the graph from the file, so the built-in CMDB
		// skeleton is only as complete as the file was: pieces it never carried
		// are absent, and pieces it DID carry were deleted and recreated —
		// which cascaded away the links of anything the dump predates (a
		// pre-trash-can dump leaves `trash-can` unregistered under `types`).
		// Repair whatever is missing; correct pieces stay untouched.
		crud.EnsureBuiltInSchema(ctx.Request, ctx.Domain)

		lg.Logln(lg.InfoLevel, fmt.Sprintf("Import is done within %.2f sec", time.Since(t0).Seconds()))
		om.AggregateOpMsg(sfMediators.OpMsgOk(easyjson.NewJSONObject())).Reply()

	default:
		msg := fmt.Sprintf("%s – unsopported format", format)
		om.AggregateOpMsg(sfMediators.OpMsgFailed(msg)).Reply()
		lg.Logln(lg.ErrorLevel, "Termination due to an error: %s", msg)
		return
	}
}
