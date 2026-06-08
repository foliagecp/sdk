package crud_test

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/foliagecp/sdk/clients/go/db"
	"github.com/foliagecp/sdk/embedded/graph/crud"
	"github.com/foliagecp/sdk/statefun/test"
	"github.com/stretchr/testify/suite"
)

// TriggerSchemaConsistencyTestSuite verifies that installing triggers on types
// and on type-to-type relations is consistent, whether the application schema
// (the types and their relation) is materialised BY the trigger-set calls
// themselves (upsert) or created explicitly first and decorated afterwards.
//
// "Before schema init" = no explicit TypeCreate/TypesLinkCreate; the triggers
// upsert the types and the relation into existence. "After" = create the
// types and relation first, then attach triggers. Both must end in the same
// consistent state: every trigger present (none overwritten), each type linked
// to the types root exactly once, exactly one ta->tb schema link, and no extra
// vertices or type-to-type links.
//
// The built-in roots (root/types/objects) are always initialised at startup;
// this exercises the application schema layered on top.
type TriggerSchemaConsistencyTestSuite struct {
	test.StatefunTestSuite
	dbc db.DBSyncClient
}

func TestTriggerSchemaConsistencyTestSuite(t *testing.T) {
	suite.Run(t, new(TriggerSchemaConsistencyTestSuite))
}

func (s *TriggerSchemaConsistencyTestSuite) bootstrap() {
	crud.RegisterAllFunctionTypes(s.Runtime())
	s.NoError(s.StartRuntime())
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := s.CacheValue(crud.BUILT_IN_TYPES); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	dbc, err := db.NewDBSyncClientFromRequestFunction(s.Runtime().Request)
	s.NoError(err)
	s.dbc = dbc
}

// schemaLinkKeysFrom returns every __type (schema) out-link key of a type — one
// per distinct relation. A duplicated relation would surface as extra keys.
func (s *TriggerSchemaConsistencyTestSuite) schemaLinkKeysFrom(typeName string) []string {
	domID := s.SetThisDomainPreffix(typeName)
	pattern := fmt.Sprintf(crud.OutLinkTypeKeyPrefPattern+"%s.>", domID, crud.TO_TYPELINK)
	return s.Runtime().Domain.Cache().GetKeysByPattern(pattern)
}

// rootLinkedOnce reports whether the types root links to typeName exactly once.
func (s *TriggerSchemaConsistencyTestSuite) rootLinkedOnce(typeName string) bool {
	domTypes := s.SetThisDomainPreffix(crud.BUILT_IN_TYPES)
	pattern := fmt.Sprintf(crud.OutLinkTypeKeyPrefPattern+"%s.>", domTypes, crud.TO_TYPELINK)
	target := s.SetThisDomainPreffix(typeName)
	n := 0
	for _, k := range s.Runtime().Domain.Cache().GetKeysByPattern(pattern) {
		if strings.HasSuffix(k, "."+target) {
			n++
		}
	}
	return n == 1
}

func (s *TriggerSchemaConsistencyTestSuite) objectTrigger(typeName, triggerType string) []string {
	data, err := s.dbc.CMDB.TypeRead(typeName)
	s.NoError(err)
	arr, _ := data.GetByPath("body.triggers." + triggerType).AsArrayString()
	return arr
}

func (s *TriggerSchemaConsistencyTestSuite) linkTrigger(fromType, toType, triggerType string) []string {
	data, err := s.dbc.CMDB.TypesLinkRead(fromType, toType)
	s.NoError(err)
	arr, _ := data.GetByPath("body.triggers." + triggerType).AsArrayString()
	return arr
}

// setTriggers attaches the same set of object + link triggers used by both
// scenarios: two object triggers on ta (create+update), one on tb, and two
// link triggers on ta->tb (create+update).
func (s *TriggerSchemaConsistencyTestSuite) setTriggers() {
	s.NoError(s.dbc.CMDB.TriggerObjectSet("ta", db.CreateTrigger, "fn.obj.create"))
	s.NoError(s.dbc.CMDB.TriggerObjectSet("ta", db.UpdateTrigger, "fn.obj.update"))
	s.NoError(s.dbc.CMDB.TriggerObjectSet("tb", db.CreateTrigger, "fn.obj.create.b"))
	s.NoError(s.dbc.CMDB.TriggerLinkSet("ta", "tb", db.CreateTrigger, "fn.link.create"))
	s.NoError(s.dbc.CMDB.TriggerLinkSet("ta", "tb", db.UpdateTrigger, "fn.link.update"))
}

// assertConsistent checks the final invariant state shared by both scenarios.
func (s *TriggerSchemaConsistencyTestSuite) assertConsistent() {
	// Both types exist and are each linked to the types root exactly once.
	_, err := s.dbc.CMDB.TypeRead("ta")
	s.NoError(err, "type ta must exist")
	_, err = s.dbc.CMDB.TypeRead("tb")
	s.NoError(err, "type tb must exist")
	s.True(s.rootLinkedOnce("ta"), "ta must be linked to the types root exactly once")
	s.True(s.rootLinkedOnce("tb"), "tb must be linked to the types root exactly once")

	// Exactly one ta->tb schema link, and no extra schema links anywhere.
	taLinks := s.schemaLinkKeysFrom("ta")
	tbSuffix := "." + s.SetThisDomainPreffix("tb")
	tbCount := 0
	for _, k := range taLinks {
		if strings.HasSuffix(k, tbSuffix) {
			tbCount++
		}
	}
	s.Equal(1, tbCount, "exactly one ta->tb schema link (no duplicate type-to-type link)")
	s.Len(taLinks, 1, "ta must have no extra outgoing schema links")
	s.Len(s.schemaLinkKeysFrom("tb"), 0, "tb must have no outgoing schema links")

	// Every trigger is present and none was overwritten by a later set.
	s.Equal([]string{"fn.obj.create"}, s.objectTrigger("ta", db.CreateTrigger), "ta object create trigger")
	s.Equal([]string{"fn.obj.update"}, s.objectTrigger("ta", db.UpdateTrigger), "ta object update trigger (create not overwritten)")
	s.Equal([]string{"fn.obj.create.b"}, s.objectTrigger("tb", db.CreateTrigger), "tb object create trigger")
	s.Equal([]string{"fn.link.create"}, s.linkTrigger("ta", "tb", db.CreateTrigger), "ta->tb link create trigger")
	s.Equal([]string{"fn.link.update"}, s.linkTrigger("ta", "tb", db.UpdateTrigger), "ta->tb link update trigger (create not overwritten)")
}

// Triggers materialise the schema: no explicit TypeCreate / TypesLinkCreate.
func (s *TriggerSchemaConsistencyTestSuite) Test_TriggersBeforeSchemaInit_Consistent() {
	s.bootstrap()
	s.setTriggers()
	s.assertConsistent()
}

// Schema created explicitly first, then decorated with triggers.
func (s *TriggerSchemaConsistencyTestSuite) Test_TriggersAfterSchemaInit_Consistent() {
	s.bootstrap()
	s.NoError(s.dbc.CMDB.TypeCreate("ta"))
	s.NoError(s.dbc.CMDB.TypeCreate("tb"))
	s.NoError(s.dbc.CMDB.TypesLinkCreate("ta", "tb", "rel", nil))
	s.setTriggers()
	s.assertConsistent()
}
