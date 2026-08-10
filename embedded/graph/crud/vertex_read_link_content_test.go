package crud_test

// Contract tests for vertex.read `with_link_content` (details_v2 only): each
// links.out element may additionally carry the link's `body` and `tags`, read
// from the owner-vertex keys inside the same handler pass. Pinned here:
//
//   1. regression — details_v2 WITHOUT the flag stays byte-identical: no
//      body/tags fields appear on any links.out element;
//   2. with the flag — per-link presence/omission: a bare link carries
//      neither field, body-only carries just body, tags-only just tags,
//      body+tags carries both;
//   3. links.in NEVER carries content (an edge's content travels with its
//      source vertex only);
//   4. the flag without details_v2 is IGNORED (documented behavior);
//   5. the client surface: VertexReadDetailsV2(id, true) and
//      VertexRead(id, true, true) (details[1] switches to the v2 format);
//   6. the same reply comes through the functions.graph.api.batch executor —
//      snapshot readers call vertex.read exactly this way.

import (
	"testing"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/clients/go/db"
	"github.com/foliagecp/sdk/embedded/graph/batch"
	"github.com/foliagecp/sdk/embedded/graph/crud"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/test"
	"github.com/stretchr/testify/suite"
)

type VertexReadLinkContentTestSuite struct {
	test.StatefunTestSuite
	dbc db.DBSyncClient
}

func TestVertexReadLinkContentTestSuite(t *testing.T) {
	suite.Run(t, new(VertexReadLinkContentTestSuite))
}

func (s *VertexReadLinkContentTestSuite) bootstrap() {
	crud.RegisterAllFunctionTypes(s.Runtime())
	batch.RegisterAllFunctionTypes(s.Runtime())
	s.NoError(s.StartRuntime())
	deadline := time.Now().Add(15 * time.Second)
	for _, id := range []string{crud.BUILT_IN_TYPES, crud.BUILT_IN_OBJECTS} {
		for {
			if _, err := s.CacheValue(id); err == nil {
				break
			}
			if time.Now().After(deadline) {
				s.T().Fatalf("vertex %q did not appear in time", id)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	dbc, err := db.NewDBSyncClientFromRequestFunction(s.Runtime().Request)
	s.NoError(err)
	s.dbc = dbc
}

// seedLinks creates lc-src with four out-links covering every content shape.
func (s *VertexReadLinkContentTestSuite) seedLinks() {
	g := s.dbc.Graph
	for _, id := range []string{"lc-src", "lc-plain", "lc-body", "lc-tags", "lc-tagsonly"} {
		s.NoError(g.VertexCreate(id))
	}
	s.NoError(g.VerticesLinkCreate("lc-src", "lc-plain", "ln-plain", "lt", nil))
	s.NoError(g.VerticesLinkCreate("lc-src", "lc-body", "ln-body", "lt", nil,
		easyjson.NewJSONObjectWithKeyValue("k", easyjson.NewJSON(1))))
	s.NoError(g.VerticesLinkCreate("lc-src", "lc-tags", "ln-tags", "lt", []string{"t1", "t2"},
		easyjson.NewJSONObjectWithKeyValue("z", easyjson.NewJSON(9))))
	s.NoError(g.VerticesLinkCreate("lc-src", "lc-tagsonly", "ln-tagsonly", "lt", []string{"only"}))
}

func outLinksByName(data easyjson.JSON) map[string]easyjson.JSON {
	m := map[string]easyjson.JSON{}
	out := data.GetByPath("links.out")
	for i := 0; i < out.ArraySize(); i++ {
		l := out.ArrayElement(i)
		m[l.GetByPath("name").AsStringDefault("")] = l
	}
	return m
}

func tagsOf(l easyjson.JSON) []string {
	tags := []string{}
	arr := l.GetByPath("tags")
	for i := 0; i < arr.ArraySize(); i++ {
		tags = append(tags, arr.ArrayElement(i).AsStringDefault(""))
	}
	return tags
}

func (s *VertexReadLinkContentTestSuite) assertContentShape(links map[string]easyjson.JSON) {
	s.Len(links, 4)

	s.False(links["ln-plain"].PathExists("body"), "bare link must omit body")
	s.False(links["ln-plain"].PathExists("tags"), "bare link must omit tags")

	s.EqualValues(1, links["ln-body"].GetByPath("body.k").AsNumericDefault(0), "body-only link must carry its body")
	s.False(links["ln-body"].PathExists("tags"), "body-only link must omit tags")

	s.EqualValues(9, links["ln-tags"].GetByPath("body.z").AsNumericDefault(0))
	s.ElementsMatch([]string{"t1", "t2"}, tagsOf(links["ln-tags"]))

	s.False(links["ln-tagsonly"].PathExists("body"), "tags-only link must omit body (empty body is omitted)")
	s.ElementsMatch([]string{"only"}, tagsOf(links["ln-tagsonly"]))
}

func (s *VertexReadLinkContentTestSuite) Test_WithLinkContent_Contract() {
	s.bootstrap()
	s.seedLinks()

	// 1. Regression: details_v2 WITHOUT the flag — byte-identical shape,
	// no body/tags on any links.out element.
	data, err := s.dbc.Graph.VertexReadDetailsV2("lc-src")
	s.NoError(err)
	bare := outLinksByName(data)
	s.Len(bare, 4)
	for name, l := range bare {
		s.Falsef(l.PathExists("body"), "without with_link_content %q must carry no body", name)
		s.Falsef(l.PathExists("tags"), "without with_link_content %q must carry no tags", name)
	}

	// 2. With the flag: per-link presence/omission.
	data, err = s.dbc.Graph.VertexReadDetailsV2Full("lc-src", true)
	s.NoError(err)
	s.assertContentShape(outLinksByName(data))

	// VertexReadDetailsV2Full without arguments behaves exactly like
	// VertexReadDetailsV2 (no content fields).
	data, err = s.dbc.Graph.VertexReadDetailsV2Full("lc-src")
	s.NoError(err)
	for name, l := range outLinksByName(data) {
		s.Falsef(l.PathExists("body"), "Full without linkContent: %q must carry no body", name)
		s.Falsef(l.PathExists("tags"), "Full without linkContent: %q must carry no tags", name)
	}

	// 3. links.in never carries content — the edge's content travels with
	// its source vertex only.
	data, err = s.dbc.Graph.VertexReadDetailsV2Full("lc-body", true)
	s.NoError(err)
	in := data.GetByPath("links.in")
	s.True(in.IsArray())
	for i := 0; i < in.ArraySize(); i++ {
		l := in.ArrayElement(i)
		s.False(l.PathExists("body"), "links.in must not carry body")
		s.False(l.PathExists("tags"), "links.in must not carry tags")
	}

	// 4. The flag without details_v2 is ignored: a plain read, no links at all.
	payload := easyjson.NewJSONObjectWithKeyValue("with_link_content", easyjson.NewJSON(true))
	reply, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.read",
		s.SetThisDomainPreffix("lc-src"), &payload, nil)
	om := sfMediators.OpMsgFromSfReply(reply, err)
	s.Equal(sfMediators.SYNC_OP_STATUS_OK, om.Status)
	s.True(om.Data.PathExists("body"))
	s.False(om.Data.PathExists("links"), "with_link_content without details_v2 must not add links")

	// 5. Client surface: VertexRead(id, true, true) switches to the v2 format
	// with content; VertexRead(id, true) keeps the legacy parallel arrays.
	data, err = s.dbc.Graph.VertexRead("lc-src", true, true)
	s.NoError(err)
	s.assertContentShape(outLinksByName(data))
	data, err = s.dbc.Graph.VertexRead("lc-src", true)
	s.NoError(err)
	s.True(data.PathExists("links.out.names"), "VertexRead(id, true) must keep the legacy shape")
}

func (s *VertexReadLinkContentTestSuite) Test_WithLinkContent_ThroughBatch() {
	s.bootstrap()
	s.seedLinks()

	p := easyjson.NewJSONObject()
	p.SetByPath("details_v2", easyjson.NewJSON(true))
	p.SetByPath("with_link_content", easyjson.NewJSON(true))

	results, err := s.dbc.BatchCreate("lc_batch").
		Operation("functions.graph.api.vertex.read", "lc-src", p).
		Commit()
	s.NoError(err)
	s.Len(results, 1)
	s.Truef(results[0].OK(), "batched vertex.read must succeed; status=%s", results[0].Status)
	s.assertContentShape(outLinksByName(results[0].Data))
}
