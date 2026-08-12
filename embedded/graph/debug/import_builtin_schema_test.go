package debug_test

// A GraphML import must not destroy the built-in CMDB skeleton.
//
// The dump is produced by walking the graph from `root`, so it always contains
// `root`, `types`, `objects` and the built-in types the source system had. The
// import used to delete and recreate every dumped vertex — and deleting `types`
// cascades away its registrations, of which only the ones present in the dump
// come back. A dump taken before a built-in type existed (here: `trash-can`)
// therefore left that type unregistered, and CRUD stopped recognizing it.

import (
	"fmt"
	"testing"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/clients/go/db"
	"github.com/foliagecp/sdk/embedded/graph/crud"
	"github.com/foliagecp/sdk/embedded/graph/debug"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/test"
	"github.com/stretchr/testify/suite"
)

type ImportBuiltInSchemaTestSuite struct {
	test.StatefunTestSuite
	cmdb db.CMDBSyncClient
}

func TestImportBuiltInSchemaTestSuite(t *testing.T) {
	suite.Run(t, new(ImportBuiltInSchemaTestSuite))
}

func (s *ImportBuiltInSchemaTestSuite) boot() {
	crud.RegisterAllFunctionTypes(s.Runtime())
	debug.RegisterAllFunctionTypes(s.Runtime())
	s.NoError(s.StartRuntime())
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := s.CacheValue(crud.BUILT_IN_TRASH_CAN); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	dbc, err := db.NewDBSyncClientFromRequestFunction(s.Runtime().Request)
	s.NoError(err)
	s.cmdb = dbc.CMDB
}

func (s *ImportBuiltInSchemaTestSuite) vertexExists(id string) bool {
	return s.Runtime().Domain.Cache().ExistsJson(s.SetThisDomainPreffix(id))
}

// typeRegistered reports whether `types` holds the __type entry for typeName.
func (s *ImportBuiltInSchemaTestSuite) typeRegistered(typeName string) bool {
	key := fmt.Sprintf(crud.OutLinkTypeKeyPrefPattern+crud.KeySuff2Pattern,
		s.SetThisDomainPreffix(crud.BUILT_IN_TYPES), crud.TO_TYPELINK, s.SetThisDomainPreffix(typeName))
	return s.Runtime().Domain.Cache().Exists(key)
}

// legacyDump mimics an export taken by an older system: it carries the skeleton
// and one user type, and knows nothing about `trash-can`.
const legacyDump = `<?xml version="1.0" encoding="UTF-8"?>
<graphml>
  <graph id="G" edgedefault="directed">
    <node id="root"/>
    <node id="types"/>
    <node id="objects"/>
    <node id="group"/>
    <node id="legacy_t"/>
    <edge source="root" target="types"><data key="tps">__types</data><data key="nms">types</data></edge>
    <edge source="root" target="objects"><data key="tps">__objects</data><data key="nms">objects</data></edge>
    <edge source="types" target="group"><data key="tps">__type</data><data key="nms">group</data></edge>
    <edge source="types" target="legacy_t"><data key="tps">__type</data><data key="nms">legacy_t</data></edge>
  </graph>
</graphml>`

func (s *ImportBuiltInSchemaTestSuite) importDump(dump string) {
	p := easyjson.NewJSONObjectWithKeyValue("format", easyjson.NewJSON("graphml"))
	p.SetByPath("source", easyjson.NewJSON("payload"))
	p.SetByPath("data", easyjson.NewJSON(dump))
	res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.import", crud.BUILT_IN_ROOT, &p, nil)
	s.Require().NoError(err)
	s.Require().Equal("ok", res.GetByPath("status").AsStringDefault(""), "import must succeed: %s", res.GetByPath("details").AsStringDefault(""))
}

// The regression: a built-in type the dump never heard of must still be a
// registered type after the import.
func (s *ImportBuiltInSchemaTestSuite) Test_ImportOfLegacyDump_KeepsBuiltInTypes() {
	s.boot()
	s.Require().True(s.typeRegistered(crud.BUILT_IN_TRASH_CAN), "sanity: registered before the import")

	s.importDump(legacyDump)

	s.True(s.vertexExists(crud.BUILT_IN_TRASH_CAN), "the built-in type vertex must survive the import")
	s.True(s.typeRegistered(crud.BUILT_IN_TRASH_CAN), "and must still be registered under types")
	s.True(s.typeRegistered(crud.BUILT_IN_TYPE_GROUP), "group as well")
	s.True(s.vertexExists(crud.BUILT_IN_OBJECT_NAV), "the nav object must survive too")
}

// The import must still do its job: what the dump carries lands in the graph.
func (s *ImportBuiltInSchemaTestSuite) Test_ImportStillLoadsDumpContent() {
	s.boot()
	s.importDump(legacyDump)

	s.True(s.vertexExists("legacy_t"), "a vertex from the dump must be imported")
	s.True(s.typeRegistered("legacy_t"), "and its registration from the dump must be applied")
}

// CRUD must actually work on the built-in types after an import — the trash can
// is the strictest witness: deleting an object parks it under `trash-can`.
func (s *ImportBuiltInSchemaTestSuite) Test_AfterImport_TrashCanStillFunctional() {
	s.boot()
	s.importDump(legacyDump)

	s.NoError(s.cmdb.TypeCreate("imp_t"))
	s.NoError(s.cmdb.ObjectCreate("imp1", "imp_t", easyjson.NewJSONObject()))
	s.NoError(s.cmdb.ObjectDelete("imp1"))

	s.True(s.vertexExists("imp1"), "the deleted object must be parked, not erased")
	key := fmt.Sprintf(crud.OutLinkTypeKeyPrefPattern+crud.KeySuff2Pattern,
		s.SetThisDomainPreffix(crud.BUILT_IN_TRASH_CAN), crud.OBJECT_TYPELINK, s.SetThisDomainPreffix("imp1"))
	s.True(s.Runtime().Domain.Cache().Exists(key), "the trash-can type must hold the parked object")
}
