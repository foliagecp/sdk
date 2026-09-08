package crud_test

// Contract tests for object.read `with_link_content` (details_v2 only).
//
// The flag has the same meaning it has in vertex.read — each links.out
// element may additionally carry the link's `body` and `tags` — and it has to,
// because object.read is the only correct way to read an object once the trash
// can exists: vertex.read hands back parked vertices as if they were alive.
// Without the flag the alternative is one link.read per edge.
//
// Pinned here:
//
//  1. with the flag — per-link presence/omission, exactly as the low-level
//     read defines it: a bare link carries neither field, body-only carries
//     just body, tags-only just tags, body+tags carries both;
//  2. regression — details_v2 WITHOUT the flag stays as it was: no body and no
//     tags on any element;
//  3. the flag without details_v2 is ignored, and the legacy shape is
//     unchanged;
//  4. links.in never carries content;
//  5. an object in the trash can answers "not found" whether the flag is set
//     or not — the flag must not open a way to read what was deleted;
//  6. the client surface: ObjectReadV2Full(id, true) carries content and
//     ObjectReadV2Full(id) is ObjectReadV2.

import (
	"testing"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/clients/go/db"
	"github.com/foliagecp/sdk/embedded/graph/crud"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/test"
	"github.com/stretchr/testify/suite"
)

type ObjectReadLinkContentTestSuite struct {
	test.StatefunTestSuite
	dbc db.DBSyncClient
}

func TestObjectReadLinkContentTestSuite(t *testing.T) {
	suite.Run(t, new(ObjectReadLinkContentTestSuite))
}

func (s *ObjectReadLinkContentTestSuite) bootstrap() {
	crud.RegisterAllFunctionTypes(s.Runtime())
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

// seedObjects builds olc-src with one out-link of every content shape, so a
// single read answers the whole presence/omission question.
func (s *ObjectReadLinkContentTestSuite) seedObjects() {
	cmdb := s.dbc.CMDB
	const t1, t2 = "OlcType", "OlcPeer"
	s.Require().NoError(cmdb.TypeCreate(t1))
	s.Require().NoError(cmdb.TypeCreate(t2))
	s.Require().NoError(cmdb.TypesLinkCreate(t1, t2, "olc-rel", nil))

	s.Require().NoError(cmdb.ObjectCreate("olc-src", t1))
	for _, id := range []string{"olc-plain", "olc-body", "olc-both", "olc-tagsonly"} {
		s.Require().NoError(cmdb.ObjectCreate(id, t2))
	}
	s.Require().NoError(cmdb.ObjectsLinkCreate("olc-src", "olc-plain", "ln-plain", nil))
	s.Require().NoError(cmdb.ObjectsLinkCreate("olc-src", "olc-body", "ln-body", nil,
		easyjson.NewJSONObjectWithKeyValue("k", easyjson.NewJSON(1))))
	s.Require().NoError(cmdb.ObjectsLinkCreate("olc-src", "olc-both", "ln-both", []string{"t1", "t2"},
		easyjson.NewJSONObjectWithKeyValue("z", easyjson.NewJSON(9))))
	s.Require().NoError(cmdb.ObjectsLinkCreate("olc-src", "olc-tagsonly", "ln-tagsonly", []string{"only"}))
}

// readObject calls the endpoint directly, so a payload without details_v2 can
// be sent — the client never sends one.
func (s *ObjectReadLinkContentTestSuite) readObject(id string, detailsV2, withLinkContent bool) sfMediators.OpMsg {
	payload := easyjson.NewJSONObject()
	if detailsV2 {
		payload.SetByPath("details_v2", easyjson.NewJSON(true))
	}
	if withLinkContent {
		payload.SetByPath("with_link_content", easyjson.NewJSON(true))
	}
	reply, err := s.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.object.read", id, &payload, nil)
	s.Require().NoError(err)
	return sfMediators.OpMsgFromSfReply(reply, nil)
}

func (s *ObjectReadLinkContentTestSuite) Test_WithLinkContent_CarriesBodyAndTags() {
	s.bootstrap()
	s.seedObjects()

	m := s.readObject("olc-src", true, true)
	s.Require().Equal(sfMediators.SYNC_OP_STATUS_OK, m.Status, "details: %s", m.Details)

	links := outLinksByName(m.Data)
	s.Require().Contains(links, "ln-plain")

	s.False(links["ln-plain"].PathExists("body"), "bare link must omit body")
	s.False(links["ln-plain"].PathExists("tags"), "bare link must omit tags")

	s.EqualValues(1, links["ln-body"].GetByPath("body.k").AsNumericDefault(0), "body-only link must carry its body")
	s.False(links["ln-body"].PathExists("tags"), "body-only link must omit tags")

	s.EqualValues(9, links["ln-both"].GetByPath("body.z").AsNumericDefault(0))
	s.ElementsMatch([]string{"t1", "t2"}, tagsOf(links["ln-both"]))

	s.False(links["ln-tagsonly"].PathExists("body"), "tags-only link must omit body")
	s.ElementsMatch([]string{"only"}, tagsOf(links["ln-tagsonly"]))

	// An edge's content travels with its source vertex only.
	in := m.Data.GetByPath("links.in")
	for i := 0; i < in.ArraySize(); i++ {
		s.False(in.ArrayElement(i).PathExists("body"), "links.in must never carry a body")
		s.False(in.ArrayElement(i).PathExists("tags"), "links.in must never carry tags")
	}
}

func (s *ObjectReadLinkContentTestSuite) Test_WithoutTheFlag_ShapeIsUnchanged() {
	s.bootstrap()
	s.seedObjects()

	m := s.readObject("olc-src", true, false)
	s.Require().Equal(sfMediators.SYNC_OP_STATUS_OK, m.Status, "details: %s", m.Details)

	for name, l := range outLinksByName(m.Data) {
		s.Falsef(l.PathExists("body"), "link %q carries a body without the flag", name)
		s.Falsef(l.PathExists("tags"), "link %q carries tags without the flag", name)
	}
}

func (s *ObjectReadLinkContentTestSuite) Test_FlagWithoutDetailsV2_IsIgnored() {
	s.bootstrap()
	s.seedObjects()

	m := s.readObject("olc-src", false, true)
	s.Require().Equal(sfMediators.SYNC_OP_STATUS_OK, m.Status, "details: %s", m.Details)

	// The legacy shape: parallel arrays, and nothing resembling content.
	s.True(m.Data.PathExists("links.out.names"), "without details_v2 the legacy links shape must stay")
	s.False(m.Data.GetByPath("links.out").IsArray(), "the flag must not switch the shape on its own")
}

// A parked object is outside the observable model, and asking for link content
// must not become a way back in.
func (s *ObjectReadLinkContentTestSuite) Test_ParkedObject_IsNotFoundWithOrWithoutTheFlag() {
	s.bootstrap()
	s.seedObjects()
	s.Require().NoError(s.dbc.CMDB.ObjectDelete("olc-body"))

	for _, withContent := range []bool{false, true} {
		m := s.readObject("olc-body", true, withContent)
		s.Equalf(sfMediators.SYNC_OP_STATUS_IDLE, m.Status,
			"a parked object must read as missing (with_link_content=%v), got %s: %s", withContent, m.Status, m.Details)
	}
}

func (s *ObjectReadLinkContentTestSuite) Test_ClientSurface() {
	s.bootstrap()
	s.seedObjects()

	full, err := s.dbc.CMDB.ObjectReadV2Full("olc-src", true)
	s.Require().NoError(err)
	fullLinks := outLinksByName(full)
	s.Require().Contains(fullLinks, "ln-both")
	s.EqualValues(9, fullLinks["ln-both"].GetByPath("body.z").AsNumericDefault(0),
		"ObjectReadV2Full(id, true) must carry link bodies")

	plain, err := s.dbc.CMDB.ObjectReadV2Full("olc-src")
	s.Require().NoError(err)
	for name, l := range outLinksByName(plain) {
		s.Falsef(l.PathExists("body"), "ObjectReadV2Full(id) must behave like ObjectReadV2, link %q carries a body", name)
	}

	v2, err := s.dbc.CMDB.ObjectReadV2("olc-src")
	s.Require().NoError(err)
	s.Equal(v2.ToString(), plain.ToString(), "ObjectReadV2Full(id) and ObjectReadV2(id) must answer identically")
}
