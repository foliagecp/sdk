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
	cmdb  db.CMDBSyncClient
	graph db.GraphSyncClient
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
	s.cmdb, s.graph = dbc.CMDB, dbc.Graph
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

// dumpWithSkeletonBodies carries bodies ON the skeleton vertices — an export
// from a system that kept something of its own on `root` and `objects`.
const dumpWithSkeletonBodies = `<?xml version="1.0" encoding="UTF-8"?>
<graphml>
  <graph id="G" edgedefault="directed">
    <node id="root"><data key="bdj">{"from_dump":"yes"}</data></node>
    <node id="types"/>
    <node id="objects"><data key="bdj">{"obj_note":"from dump"}</data></node>
    <node id="group"/>
    <edge source="root" target="types"><data key="tps">__types</data><data key="nms">types</data></edge>
    <edge source="root" target="objects"><data key="tps">__objects</data><data key="nms">objects</data></edge>
    <edge source="types" target="group"><data key="tps">__type</data><data key="nms">group</data></edge>
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

// bodyOf reads a vertex body through the graph API.
func (s *ImportBuiltInSchemaTestSuite) bodyOf(id string) easyjson.JSON {
	data, err := s.graph.VertexRead(id)
	s.Require().NoError(err)
	return data.GetByPath("body")
}

// An import must not wipe what the skeleton vertices carry. Those bodies hold
// configuration of THIS graph — the protected-field policy on `root`, the
// meta-trigger registrations on the `types` and `objects` roots — which no dump
// is authoritative about: it knows only what its source system had at export
// time. The dumped body is merged in, not swapped for.
func (s *ImportBuiltInSchemaTestSuite) Test_ImportMergesSkeletonBodies_KeepsLocalSettings() {
	s.boot()

	// What this graph has configured, of which the dump knows nothing.
	p := easyjson.NewJSONObjectWithKeyValue("replace", easyjson.NewJSON(false))
	p.SetByPath("body."+crud.ProtectedBodyFieldsBodyPath, easyjson.NewJSON([]string{"usr"}))
	p.SetByPath("body.local_setting", easyjson.NewJSON("keep me"))
	_, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.update", crud.BUILT_IN_ROOT, &p, nil)
	s.Require().NoError(err)

	s.importDump(dumpWithSkeletonBodies)

	root := s.bodyOf(crud.BUILT_IN_ROOT)
	list, _ := root.GetByPath(crud.ProtectedBodyFieldsBodyPath).AsArrayString()
	s.Equal([]string{"usr"}, list, "the protected-field policy must survive an import")
	s.Equal("keep me", root.GetByPath("local_setting").AsStringDefault(""),
		"anything else living on root must survive too")
	s.Equal("yes", root.GetByPath("from_dump").AsStringDefault(""),
		"and what the dump carries must still land")

	s.Equal("from dump", s.bodyOf(crud.BUILT_IN_OBJECTS).GetByPath("obj_note").AsStringDefault(""),
		"the same for the other skeleton vertices")
}

// The protected-field list is not just stored — it must still be IN FORCE after
// an import, which is what a runtime reads it for.
func (s *ImportBuiltInSchemaTestSuite) Test_AfterImport_ProtectedFieldsStillEnforced() {
	s.boot()
	crud.EnsureBuiltInSchema(s.Runtime().Request, s.Runtime().Domain, "usr")

	s.importDump(dumpWithSkeletonBodies)
	crud.LoadProtectedBodyFieldsFromGraph(s.Runtime().Request, s.Runtime().Domain)
	s.Equal([]string{"usr"}, s.Runtime().Domain.ProtectedBodyFields())

	s.NoError(s.cmdb.TypeCreate("imp_pf_t"))
	body := easyjson.NewJSONObjectWithKeyValue("hostname", easyjson.NewJSON("srv"))
	body.SetByPath("usr.attrs.responsible", easyjson.NewJSON("alice"))
	s.NoError(s.cmdb.ObjectCreate("imp_pf1", "imp_pf_t", body))

	// An inventory rebuild: whole body, replace=true, no usr.
	s.NoError(s.cmdb.ObjectUpdate("imp_pf1", easyjson.NewJSONObjectWithKeyValue("hostname", easyjson.NewJSON("srv-2")), true))

	read, err := s.cmdb.ObjectRead("imp_pf1")
	s.Require().NoError(err)
	s.Equal("alice", read.GetByPath("body.usr.attrs.responsible").AsStringDefault(""),
		"protection declared before the import must still hold after it")
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
