package crud_test

// Bug hunt: link lifecycle index consistency at the KV level.
//
// Unlike the regression suites, these are adversarial: they read the raw cache
// keys and assert there is nothing dangling and nothing missing after delete /
// replace-update. A failure here is a real defect (orphaned index, leaked tag).
//
// Methods on CMDBClientContractTestSuite to reuse its bootstrap.

import (
	"fmt"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/embedded/graph/crud"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
)

func (s *CMDBClientContractTestSuite) llVertex(id string) {
	res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.create", id, easyjson.NewJSONObject().GetPtr(), nil)
	s.NoError(err)
	s.Equal("ok", res.GetByPath("status").AsStringDefault(""))
}

func (s *CMDBClientContractTestSuite) llLink(from, to, name, ltype string, tags []string) {
	lp := easyjson.NewJSONObject()
	lp.SetByPath("to", easyjson.NewJSON(to))
	lp.SetByPath("name", easyjson.NewJSON(name))
	lp.SetByPath("type", easyjson.NewJSON(ltype))
	if tags != nil {
		lp.SetByPath("tags", easyjson.NewJSON(tags))
	}
	lp.SetByPath("body.x", easyjson.NewJSON(1))
	res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.create", from, &lp, nil)
	s.NoError(err)
	s.Equal("ok", res.GetByPath("status").AsStringDefault(""))
}

// After link.delete, NO key of the link may remain (out.to, out.body, the whole
// out.index subtree, and the ltype index).
func (s *CMDBClientContractTestSuite) Test_Hunt_LinkDelete_NoDanglingKeys() {
	s.bootstrap()
	s.llVertex("ha")
	s.llVertex("hb")
	s.llLink("ha", "hb", "h-edge", "rel", []string{"t1", "t2"})

	c := s.Runtime().Domain.Cache()
	aID := s.SetThisDomainPreffix("ha")
	toKey := fmt.Sprintf(crud.OutLinkTargetKeyPrefPattern+"%s", aID, "h-edge")
	bodyKey := fmt.Sprintf(crud.OutLinkBodyKeyPrefPattern+"%s", aID, "h-edge")
	indexPat := fmt.Sprintf(crud.OutLinkIndexPrefPattern+"%s.>", aID, "h-edge")

	s.NotEmpty(c.GetKeysByPattern(toKey), "out.to must exist after create")
	s.NotEmpty(c.GetKeysByPattern(bodyKey), "out.body must exist after create")
	s.NotEmpty(c.GetKeysByPattern(indexPat), "out.index must exist after create")
	s.True(s.hasOutLinkOfType("ha", "rel"), "ltype index must exist after create")

	dp := easyjson.NewJSONObjectWithKeyValue("name", easyjson.NewJSON("h-edge"))
	res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.delete", "ha", &dp, nil)
	s.NoError(err)
	s.Contains([]string{"ok", "idle"}, res.GetByPath("status").AsStringDefault(""))

	s.Emptyf(c.GetKeysByPattern(toKey), "out.to dangling after delete: %v", c.GetKeysByPattern(toKey))
	s.Emptyf(c.GetKeysByPattern(bodyKey), "out.body dangling after delete: %v", c.GetKeysByPattern(bodyKey))
	s.Emptyf(c.GetKeysByPattern(indexPat), "out.index keys dangling after delete: %v", c.GetKeysByPattern(indexPat))
	s.Falsef(s.hasOutLinkOfType("ha", "rel"), "ltype index dangling after delete")
}

// A replace-update that drops a tag must sweep that tag's index key.
func (s *CMDBClientContractTestSuite) Test_Hunt_LinkReplaceUpdate_SweepsDroppedTag() {
	s.bootstrap()
	s.llVertex("ra")
	s.llVertex("rb")
	s.llLink("ra", "rb", "r-edge", "rel", []string{"t1", "t2"})

	c := s.Runtime().Domain.Cache()
	aID := s.SetThisDomainPreffix("ra")
	tagKey := func(tag string) string {
		return fmt.Sprintf(crud.OutLinkIndexPrefPattern+"%s.tag.%s", aID, "r-edge", tag)
	}
	s.NotEmpty(c.GetKeysByPattern(tagKey("t1")), "t1 tag index must exist after create")
	s.NotEmpty(c.GetKeysByPattern(tagKey("t2")), "t2 tag index must exist after create")

	lp := easyjson.NewJSONObject()
	lp.SetByPath("to", easyjson.NewJSON("rb"))
	lp.SetByPath("name", easyjson.NewJSON("r-edge"))
	lp.SetByPath("type", easyjson.NewJSON("rel"))
	lp.SetByPath("tags", easyjson.NewJSON([]string{"t1"}))
	lp.SetByPath("replace", easyjson.NewJSON(true))
	lp.SetByPath("body.x", easyjson.NewJSON(2))
	res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.update", "ra", &lp, nil)
	s.NoError(err)
	s.Equal("ok", res.GetByPath("status").AsStringDefault(""))

	s.NotEmpty(c.GetKeysByPattern(tagKey("t1")), "t1 tag index must remain after replace-update")
	s.Emptyf(c.GetKeysByPattern(tagKey("t2")), "dropped tag t2 index must be swept on replace-update; dangling: %v", c.GetKeysByPattern(tagKey("t2")))
}
