package crud_test

// The built-in CMDB skeleton must survive a bulk graph load. A GraphML import
// rewrites the graph from a file: pieces the file never carried are simply
// absent afterwards, and pieces it DID carry are deleted and recreated — which
// cascades away the links of everything the dump predates (a pre-trash-can dump
// leaves `trash-can` unregistered under `types`). These tests pin that
// EnsureBuiltInSchema repairs each of those leftovers.

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/clients/go/db"
	"github.com/foliagecp/sdk/embedded/graph/crud"
	"github.com/foliagecp/sdk/statefun"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/test"
	"github.com/stretchr/testify/suite"
)

type BuiltInSchemaTestSuite struct {
	test.StatefunTestSuite
	cmdb db.CMDBSyncClient
}

func TestBuiltInSchemaTestSuite(t *testing.T) { suite.Run(t, new(BuiltInSchemaTestSuite)) }

func (s *BuiltInSchemaTestSuite) boot() {
	crud.RegisterAllFunctionTypes(s.Runtime())
	// The built-in schema is prepared by an after-start hook. Hooks run in
	// registration order, so a hook registered AFTER crud's fires once that one
	// is done — a deterministic signal, unlike polling for one of the vertices
	// the hook creates half-way through its work (which under -race let the
	// assertions run against a schema that was still being built).
	schemaReady := make(chan struct{})
	s.Runtime().RegisterOnAfterStartFunction(func(context.Context, *statefun.Runtime) error {
		close(schemaReady)
		return nil
	}, false)
	s.NoError(s.StartRuntime())
	select {
	case <-schemaReady:
	case <-time.After(20 * time.Second):
		s.T().Fatal("built-in schema was not prepared in time")
	}
	dbc, err := db.NewDBSyncClientFromRequestFunction(s.Runtime().Request)
	s.NoError(err)
	s.cmdb = dbc.CMDB
}

func (s *BuiltInSchemaTestSuite) vertexExists(id string) bool {
	return s.Runtime().Domain.Cache().ExistsJson(s.SetThisDomainPreffix(id))
}

// typeRegistered reports whether `types` holds the __type entry for typeName —
// the link CRUD needs to recognize the type at all.
func (s *BuiltInSchemaTestSuite) typeRegistered(typeName string) bool {
	key := fmt.Sprintf(crud.OutLinkTypeKeyPrefPattern+crud.KeySuff2Pattern,
		s.SetThisDomainPreffix(crud.BUILT_IN_TYPES), crud.TO_TYPELINK, s.SetThisDomainPreffix(typeName))
	return s.Runtime().Domain.Cache().Exists(key)
}

func (s *BuiltInSchemaTestSuite) ensure() {
	crud.EnsureBuiltInSchema(s.Runtime().Request, s.Runtime().Domain)
}

// Sanity: a freshly started runtime has the whole skeleton, and running the
// repair over a healthy graph changes nothing.
func (s *BuiltInSchemaTestSuite) Test_HealthySchema_EnsureIsIdempotent() {
	s.boot()
	for _, v := range []string{crud.BUILT_IN_ROOT, crud.BUILT_IN_TYPES, crud.BUILT_IN_OBJECTS, crud.BUILT_IN_TYPE_GROUP, crud.BUILT_IN_TRASH_CAN, crud.BUILT_IN_OBJECT_NAV} {
		s.Truef(s.vertexExists(v), "%s must exist after startup", v)
	}
	s.True(s.typeRegistered(crud.BUILT_IN_TYPE_GROUP))
	s.True(s.typeRegistered(crud.BUILT_IN_TRASH_CAN))

	s.ensure()

	s.True(s.typeRegistered(crud.BUILT_IN_TYPE_GROUP), "repair must not break a healthy schema")
	s.True(s.typeRegistered(crud.BUILT_IN_TRASH_CAN))
	s.True(s.vertexExists(crud.BUILT_IN_OBJECT_NAV))
}

// The import case: `types` came from the dump, so it was deleted and recreated
// — every built-in type lost its registration while its vertex survived. This
// is exactly the state functions.cmdb.api.type.create alone cannot repair (it
// bails on the existing vertex).
func (s *BuiltInSchemaTestSuite) Test_TypesVertexRebuilt_RegistrationsRestored() {
	s.boot()

	// Simulate the import: drop and recreate the `types` vertex.
	p := easyjson.NewJSONObject()
	_, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.delete", crud.BUILT_IN_TYPES, &p, nil)
	s.NoError(err)
	body := easyjson.NewJSONObjectWithKeyValue("body", easyjson.NewJSONObject())
	_, err = s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.create", crud.BUILT_IN_TYPES, &body, nil)
	s.NoError(err)

	s.False(s.typeRegistered(crud.BUILT_IN_TRASH_CAN), "sanity: the cascade unregistered the built-in types")
	s.True(s.vertexExists(crud.BUILT_IN_TRASH_CAN), "sanity: the type vertex itself survived")

	s.ensure()

	s.True(s.typeRegistered(crud.BUILT_IN_TRASH_CAN), "trash-can must be registered under types again")
	s.True(s.typeRegistered(crud.BUILT_IN_TYPE_GROUP), "group must be registered under types again")
}

// A dump that never carried a built-in type at all: the vertex is missing
// entirely and must be recreated together with its registration.
func (s *BuiltInSchemaTestSuite) Test_MissingBuiltInType_Recreated() {
	s.boot()

	p := easyjson.NewJSONObject()
	_, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.delete", crud.BUILT_IN_TRASH_CAN, &p, nil)
	s.NoError(err)
	s.False(s.vertexExists(crud.BUILT_IN_TRASH_CAN), "sanity: the type is gone")

	s.ensure()

	s.True(s.vertexExists(crud.BUILT_IN_TRASH_CAN), "the missing built-in type must be recreated")
	s.True(s.typeRegistered(crud.BUILT_IN_TRASH_CAN), "and registered under types")
}

// The repaired skeleton must actually WORK afterwards: the trash can is the
// strictest witness — deleting an object parks it under the restored type.
func (s *BuiltInSchemaTestSuite) Test_RepairedSchema_TrashCanStillWorks() {
	s.boot()

	p := easyjson.NewJSONObject()
	_, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.delete", crud.BUILT_IN_TYPES, &p, nil)
	s.NoError(err)
	body := easyjson.NewJSONObjectWithKeyValue("body", easyjson.NewJSONObject())
	_, err = s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.create", crud.BUILT_IN_TYPES, &body, nil)
	s.NoError(err)

	s.ensure()

	s.NoError(s.cmdb.TypeCreate("bs_t"))
	s.NoError(s.cmdb.ObjectCreate("bs1", "bs_t", easyjson.NewJSONObject()))
	s.NoError(s.cmdb.ObjectDelete("bs1"))

	s.True(s.vertexExists("bs1"), "the deleted object must be parked, not erased")
	key := fmt.Sprintf(crud.OutLinkTypeKeyPrefPattern+crud.KeySuff2Pattern,
		s.SetThisDomainPreffix(crud.BUILT_IN_TRASH_CAN), crud.OBJECT_TYPELINK, s.SetThisDomainPreffix("bs1"))
	s.True(s.Runtime().Domain.Cache().Exists(key), "the restored trash-can type must hold the parked object")
}

// The statefun wrapper is what out-of-process importers (snapshot restore in
// pregel-backend, tooling) call — it must repair the same way.
func (s *BuiltInSchemaTestSuite) Test_EnsureViaStatefun() {
	s.boot()

	p := easyjson.NewJSONObject()
	_, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.delete", crud.BUILT_IN_TRASH_CAN, &p, nil)
	s.NoError(err)

	res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.schema.ensure", crud.BUILT_IN_ROOT, easyjson.NewJSONObject().GetPtr(), nil)
	s.NoError(err)
	s.Equal("ok", res.GetByPath("status").AsStringDefault(""))

	s.True(s.vertexExists(crud.BUILT_IN_TRASH_CAN))
	s.True(s.typeRegistered(crud.BUILT_IN_TRASH_CAN))
}
