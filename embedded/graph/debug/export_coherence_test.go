package debug

// A full graph export, in both representations of the cache, taken twice: from
// the live graph and from a KV that has been drained and reloaded.
//
// This is the one read that touches every half of every edge at once. An
// object hangs off the `objects` vertex by one edge and off its type by
// another, each stored as two independent halves, and an export that walks the
// graph has to find all of them — a missing half is a vertex the export cannot
// reach, or an edge it cannot resolve.
//
// Taking the export a second time, after the WAL has been drained into KV and
// the cache reloaded from it, asks the other half of the question: that the
// WAL carried the whole graph out and the reload brought the whole graph back.
// That is what a backup is, and what a restart is.
//
// Both cache representations answer here, and they must answer identically:
// the graph held as records may not export less than the same graph held as a
// tree. Nothing else in the suite pins that — the mode is a process-wide
// setting, and every test outside statefun/cache runs in whichever one is the
// default.

import (
	"context"
	"encoding/xml"
	"fmt"
	"sort"
	"testing"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/clients/go/db"
	"github.com/foliagecp/sdk/embedded/graph/crud"
	"github.com/foliagecp/sdk/embedded/graph/jpgql"
	"github.com/foliagecp/sdk/statefun"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/test"
	"github.com/stretchr/testify/suite"
)

// objectsSeeded is deliberately past the bucket a record holds (32 links), so
// the `objects` hub splits its directory and is exported while holding more
// than one bucket — the shape a real hub always has.
const objectsSeeded = 40

type CoherentExportTestSuite struct {
	test.StatefunTestSuite
	dbc db.DBSyncClient
}

func TestCoherentExportTestSuite(t *testing.T) {
	suite.Run(t, new(CoherentExportTestSuite))
}

// One test, in whichever representation the run is using. Both are covered by
// running the suite twice (scripts/run-all-tests.sh --cache-mode) rather than
// by switching the mode here: it is a process-wide setting, and a test that
// flips it races the maintenance pass of every runtime still alive.
func (s *CoherentExportTestSuite) Test_CoherentExport() {
	s.checkCoherentExport()
}

// graphShape is an export reduced to what a graph IS: which vertices it holds
// and which links join them. The exporters emit vertices in map order, so two
// exports of the same graph are the same shape and not the same bytes.
type graphShape struct {
	vertices []string
	edges    []string // "<from> -<name>-> <to>"
}

func parseGraphML(s *CoherentExportTestSuite, doc string) graphShape {
	var parsed struct {
		Nodes []struct {
			ID string `xml:"id,attr"`
		} `xml:"graph>node"`
		Edges []struct {
			Source string `xml:"source,attr"`
			Target string `xml:"target,attr"`
			Data   []struct {
				Key   string `xml:"key,attr"`
				Value string `xml:",chardata"`
			} `xml:"data"`
		} `xml:"graph>edge"`
	}
	s.Require().NoError(xml.Unmarshal([]byte(doc), &parsed), "the export is not parseable GraphML")

	shape := graphShape{}
	for _, n := range parsed.Nodes {
		shape.vertices = append(shape.vertices, n.ID)
	}
	for _, e := range parsed.Edges {
		name := ""
		for _, d := range e.Data {
			if d.Key == LINK_NAME_STRING_ATTR_ID {
				name = d.Value
			}
		}
		shape.edges = append(shape.edges, fmt.Sprintf("%s -%s-> %s", e.Source, name, e.Target))
	}
	sort.Strings(shape.vertices)
	sort.Strings(shape.edges)
	return shape
}

func (s *CoherentExportTestSuite) bootstrap() {
	crud.RegisterAllFunctionTypes(s.Runtime())
	jpgql.RegisterAllFunctionTypes(s.Runtime())
	s.RegisterFunction("functions.graph.api.object.debug.print.graph", LLAPIPrintGraph,
		*statefun.NewFunctionTypeConfig().
			SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).
			SetMsgAckWaitMs(MAX_ACK_WAIT_MS))
	s.Require().NoError(s.StartRuntime())
	s.waitForVertex(crud.BUILT_IN_OBJECTS, 15*time.Second)

	dbc, err := db.NewDBSyncClientFromRequestFunction(s.Runtime().Request)
	s.Require().NoError(err)
	s.dbc = dbc
}

func (s *CoherentExportTestSuite) waitForVertex(id string, timeout time.Duration) {
	s.T().Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := s.CacheValue(id); err == nil {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	s.Require().Failf("vertex never appeared", "%s did not exist after %s", id, timeout)
}

// exportWholeGraph walks the graph from the `objects` vertex, which every
// object hangs from, and returns what the export contains.
func (s *CoherentExportTestSuite) exportWholeGraph() graphShape {
	s.T().Helper()
	p := easyjson.NewJSONObject()
	p.SetByPath("format", easyjson.NewJSON("graphml"))
	p.SetByPath("delivery", easyjson.NewJSON("inline"))

	result, err := s.Request(sfPlugins.AutoRequestSelect,
		"functions.graph.api.object.debug.print.graph", crud.BUILT_IN_OBJECTS, &p, nil)
	s.Require().NoError(err)
	s.Require().Equal("ok", result.GetByPath("status").AsStringDefault(""),
		"export failed: %s", result.ToString())

	doc := result.GetByPath("data.file").AsStringDefault("")
	s.Require().NotEmpty(doc, "the export produced nothing")
	return parseGraphML(s, doc)
}

func (s *CoherentExportTestSuite) checkCoherentExport() {
	s.bootstrap()

	const objType = "ce_thing"
	s.Require().NoError(s.dbc.CMDB.TypeCreate(objType))
	s.Require().NoError(s.dbc.CMDB.TypesLinkCreate(objType, objType, "ce_peer", nil))

	ids := make([]string, 0, objectsSeeded)
	for i := 0; i < objectsSeeded; i++ {
		id := fmt.Sprintf("ce-o%02d", i)
		body := easyjson.NewJSONObjectWithKeyValue("idx", easyjson.NewJSON(i))
		s.Require().NoError(s.dbc.CMDB.ObjectUpdate(id, body, false, objType))
		ids = append(ids, s.SetThisDomainPreffix(id))
	}
	// Links between the objects themselves, so the export carries more than the
	// skeleton every object is born with.
	for i := 0; i+1 < objectsSeeded; i += 2 {
		s.Require().NoError(s.dbc.CMDB.ObjectsLinkCreate(
			fmt.Sprintf("ce-o%02d", i), fmt.Sprintf("ce-o%02d", i+1),
			fmt.Sprintf("peer%02d", i), nil))
	}

	live := s.exportWholeGraph()
	s.assertHoldsTheSeededGraph(live, ids)
	s.assertObjectsAreReadable(ids)

	// The coherence barrier the backup tooling uses: everything written is in
	// KV. Then the cache is thrown away and rebuilt from KV alone.
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	s.Require().NoError(s.Runtime().Domain.WaitForKVCaughtUp(ctx, 60*time.Second))
	s.Require().NoError(s.Runtime().Domain.Cache().RehydrateFromKV(ctx))

	fromKV := s.exportWholeGraph()
	s.Require().Equalf(live.vertices, fromKV.vertices,
		"the graph reloaded from KV holds different vertices than the one that was written")
	s.Require().Equalf(live.edges, fromKV.edges,
		"the graph reloaded from KV holds different links than the one that was written")
	s.assertHoldsTheSeededGraph(fromKV, ids)
	s.assertObjectsAreReadable(ids)
}

// assertHoldsTheSeededGraph checks the export against what was seeded: every
// object is there, and so is the edge it hangs from. Losing the owner's half of
// that edge takes the object out of the export entirely.
func (s *CoherentExportTestSuite) assertHoldsTheSeededGraph(g graphShape, ids []string) {
	s.T().Helper()
	objects := s.SetThisDomainPreffix(crud.BUILT_IN_OBJECTS)
	s.Require().GreaterOrEqualf(len(g.vertices), len(ids),
		"the export holds %d vertices, fewer than the %d objects that were seeded", len(g.vertices), len(ids))
	for i, id := range ids {
		s.Require().Containsf(g.vertices, id, "object %s is missing from the export", id)
		s.Require().Containsf(g.edges, fmt.Sprintf("%s -ce-o%02d-> %s", objects, i, id),
			"the edge from objects to %s is missing from the export", id)
	}
}

// assertObjectsAreReadable asks the question production asks. An object is read
// through the mirror halves of its skeleton, not through the halves the export
// walks, so this catches what an export alone cannot: the missing in-key that
// makes a vertex stop being an object ("not connected to objects topology").
func (s *CoherentExportTestSuite) assertObjectsAreReadable(ids []string) {
	s.T().Helper()
	for _, id := range ids {
		data, err := s.dbc.CMDB.ObjectReadV2(id)
		s.Require().NoErrorf(err, "object %s is in the graph but no longer reads as an object", id)
		s.Require().Truef(data.PathExists("body.idx"), "object %s came back without its body", id)
	}
}
