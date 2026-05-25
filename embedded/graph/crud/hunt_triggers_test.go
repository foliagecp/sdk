package crud_test

// Bug hunt H10: CMDB triggers fire end-to-end.
//
// Register a statefun and wire it as an object create-trigger on a type; a
// create of an object of that type must fire the trigger exactly once. Also
// verify a type WITHOUT a trigger fires nothing.
//
// Own suite (not the contract one) because the trigger statefun must be
// registered before StartRuntime.

import (
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

type TriggersHuntTestSuite struct {
	test.StatefunTestSuite
	dbc   db.DBSyncClient
	fired chan string
}

func TestTriggersHuntTestSuite(t *testing.T) {
	suite.Run(t, new(TriggersHuntTestSuite))
}

func (s *TriggersHuntTestSuite) bootstrap() {
	s.fired = make(chan string, 32)
	crud.RegisterAllFunctionTypes(s.Runtime())
	jpgql.RegisterAllFunctionTypes(s.Runtime())
	cfg := *statefun.NewFunctionTypeConfig().
		SetAllowedSignalProviders(sfPlugins.AutoSignalSelect).
		SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).
		SetMaxIdHandlers(-1)
	s.RegisterFunction("test.trigger.rec", func(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
		select {
		case s.fired <- ctx.Self.ID:
		default:
		}
	}, cfg)

	s.NoError(s.StartRuntime())
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := s.CacheValue(crud.BUILT_IN_OBJECTS); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	dbc, err := db.NewDBSyncClientFromRequestFunction(s.Runtime().Request)
	s.NoError(err)
	s.dbc = dbc
}

func (s *TriggersHuntTestSuite) Test_Hunt_TriggerFiresOnObjectCreate() {
	s.bootstrap()
	s.NoError(s.dbc.CMDB.TypeCreate("TrType"))
	s.NoError(s.dbc.CMDB.TriggerObjectSet("TrType", db.CreateTrigger, "test.trigger.rec"))

	s.NoError(s.dbc.CMDB.ObjectUpdate("tr-1", easyjson.NewJSONObject(), false, "TrType"))

	select {
	case id := <-s.fired:
		s.T().Logf("create trigger fired on id=%q", id)
		s.Containsf(id, "tr-1", "create trigger must fire for the created object; fired on %q", id)
	case <-time.After(10 * time.Second):
		s.T().Fatal("create trigger did not fire within 10s")
	}
}

func (s *TriggersHuntTestSuite) Test_Hunt_NoTriggerWhenNotConfigured() {
	s.bootstrap()
	s.NoError(s.dbc.CMDB.TypeCreate("TrNoneType"))
	// No TriggerObjectSet for this type.
	s.NoError(s.dbc.CMDB.ObjectUpdate("trn-1", easyjson.NewJSONObject(), false, "TrNoneType"))

	select {
	case id := <-s.fired:
		s.T().Fatalf("trigger fired (%q) for a type with no trigger configured", id)
	case <-time.After(2 * time.Second):
		// expected: nothing fired
	}
}
