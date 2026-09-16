package crud_test

// Removing a trigger.
//
// A trigger is a statefun name in the type's body, under triggers.<kind>, and
// the client offers three ways to take it out: delete one name, drop a kind,
// and the same two for the triggers a types-link carries. Each of them must
// remove exactly what it names — the other names of the kind stay, the other
// kinds stay, the rest of the body stays — and the removed function must stop
// firing.
//
// Checked both ways: what the type body says afterwards, and what actually
// fires when an object of the type is written.

import (
	"sort"
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

type TriggerDeleteTestSuite struct {
	test.StatefunTestSuite
	dbc   db.DBSyncClient
	fired chan string // "<statefun>@<object id>"
}

func TestTriggerDeleteTestSuite(t *testing.T) { suite.Run(t, new(TriggerDeleteTestSuite)) }

const (
	trigA = "test.trigger.del.a"
	trigB = "test.trigger.del.b"
)

func (s *TriggerDeleteTestSuite) bootstrap() {
	s.fired = make(chan string, 64)
	crud.RegisterAllFunctionTypes(s.Runtime())
	cfg := *statefun.NewFunctionTypeConfig().
		SetAllowedSignalProviders(sfPlugins.AutoSignalSelect).
		SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).
		SetMaxIdHandlers(-1)
	for _, name := range []string{trigA, trigB} {
		name := name
		s.RegisterFunction(name, func(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
			select {
			case s.fired <- name + "@" + ctx.Self.ID:
			default:
			}
		}, cfg)
	}
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

// typeTriggers reads the names registered under triggers.<kind> of a type.
func (s *TriggerDeleteTestSuite) typeTriggers(typeName, kind string) []string {
	s.T().Helper()
	data, err := s.dbc.CMDB.TypeRead(typeName)
	s.Require().NoError(err)
	arr, _ := data.GetByPath("body.triggers." + kind).AsArrayString()
	sort.Strings(arr)
	return arr
}

// linkTriggers reads the names registered under triggers.<kind> of a types-link.
func (s *TriggerDeleteTestSuite) linkTriggers(from, to, kind string) []string {
	s.T().Helper()
	data, err := s.dbc.CMDB.TypesLinkRead(from, to)
	s.Require().NoError(err)
	arr, _ := data.GetByPath("body.triggers." + kind).AsArrayString()
	sort.Strings(arr)
	return arr
}

// firedWithin collects what fires in the window, as sorted "<statefun>@<id>".
func (s *TriggerDeleteTestSuite) firedWithin(d time.Duration) []string {
	var got []string
	deadline := time.After(d)
	for {
		select {
		case f := <-s.fired:
			got = append(got, f)
		case <-deadline:
			sort.Strings(got)
			return got
		}
	}
}

func (s *TriggerDeleteTestSuite) Test_DeleteOneObjectTriggerKeepsTheOthers() {
	s.bootstrap()
	s.Require().NoError(s.dbc.CMDB.TypeCreate("TdOne", easyjson.NewJSONObjectWithKeyValue("kept", easyjson.NewJSON("field"))))
	s.Require().NoError(s.dbc.CMDB.TriggerObjectSet("TdOne", db.CreateTrigger, trigA, trigB))
	s.Require().NoError(s.dbc.CMDB.TriggerObjectSet("TdOne", db.UpdateTrigger, trigA))
	s.Require().Equal([]string{trigA, trigB}, s.typeTriggers("TdOne", "create"), "sanity: both create triggers registered")

	s.Require().NoError(s.dbc.CMDB.TriggerObjectDelete("TdOne", db.CreateTrigger, trigA))

	s.Equal([]string{trigB}, s.typeTriggers("TdOne", "create"), "deleting one create trigger must leave the other")
	s.Equal([]string{trigA}, s.typeTriggers("TdOne", "update"), "and must not touch the update triggers")
	data, err := s.dbc.CMDB.TypeRead("TdOne")
	s.Require().NoError(err)
	s.Equal("field", data.GetByPath("body.kept").AsStringDefault(""), "and must not touch the rest of the type body")

	// What fires: a create of an object of the type reaches B and not A.
	s.Require().NoError(s.dbc.CMDB.ObjectCreate("td-one-1", "TdOne", easyjson.NewJSONObject()))
	got := s.firedWithin(2 * time.Second)
	s.Equal([]string{trigB + "@" + s.SetThisDomainPreffix("td-one-1")}, got,
		"after deleting A from the create triggers, a create must fire B alone")
}

func (s *TriggerDeleteTestSuite) Test_DropObjectTriggerKindClearsOnlyThatKind() {
	s.bootstrap()
	s.Require().NoError(s.dbc.CMDB.TypeCreate("TdDrop"))
	s.Require().NoError(s.dbc.CMDB.TriggerObjectSet("TdDrop", db.CreateTrigger, trigA))
	s.Require().NoError(s.dbc.CMDB.TriggerObjectSet("TdDrop", db.UpdateTrigger, trigB))

	s.Require().NoError(s.dbc.CMDB.TriggerObjectDrop("TdDrop", db.CreateTrigger))

	s.Empty(s.typeTriggers("TdDrop", "create"), "dropping the create kind must clear it")
	s.Equal([]string{trigB}, s.typeTriggers("TdDrop", "update"), "and leave the update kind alone")

	s.Require().NoError(s.dbc.CMDB.ObjectCreate("td-drop-1", "TdDrop", easyjson.NewJSONObject()))
	s.Empty(s.firedWithin(2*time.Second), "nothing may fire on create once the create kind is dropped")
	s.Require().NoError(s.dbc.CMDB.ObjectUpdate("td-drop-1", easyjson.NewJSONObjectWithKeyValue("v", easyjson.NewJSON(1)), false))
	s.Equal([]string{trigB + "@" + s.SetThisDomainPreffix("td-drop-1")}, s.firedWithin(2*time.Second),
		"the update trigger must still fire")
}

func (s *TriggerDeleteTestSuite) Test_DeleteAndDropLinkTriggers() {
	s.bootstrap()
	s.Require().NoError(s.dbc.CMDB.TypeCreate("TdLFrom"))
	s.Require().NoError(s.dbc.CMDB.TypeCreate("TdLTo"))
	s.Require().NoError(s.dbc.CMDB.TypesLinkCreate("TdLFrom", "TdLTo", "tdl_rel", []string{"keep"}))
	// A field of the link body besides the type and the triggers, put there the
	// way the create path does not (it writes the type alone).
	s.Require().NoError(s.dbc.CMDB.TypesLinkUpdate("TdLFrom", "TdLTo", nil,
		easyjson.NewJSONObjectWithKeyValue("kept", easyjson.NewJSON("field")), false))
	s.Require().NoError(s.dbc.CMDB.TriggerLinkSet("TdLFrom", "TdLTo", db.CreateTrigger, trigA, trigB))
	s.Require().NoError(s.dbc.CMDB.TriggerLinkSet("TdLFrom", "TdLTo", db.DeleteTrigger, trigA))
	s.Require().Equal([]string{trigA, trigB}, s.linkTriggers("TdLFrom", "TdLTo", "create"), "sanity: both create triggers registered")

	s.Require().NoError(s.dbc.CMDB.TriggerLinkRemove("TdLFrom", "TdLTo", db.CreateTrigger, trigA))
	s.Equal([]string{trigB}, s.linkTriggers("TdLFrom", "TdLTo", "create"), "removing one create trigger must leave the other")
	s.Equal([]string{trigA}, s.linkTriggers("TdLFrom", "TdLTo", "delete"), "and must not touch the delete triggers")

	data, err := s.dbc.CMDB.TypesLinkRead("TdLFrom", "TdLTo")
	s.Require().NoError(err)
	s.Equal("field", data.GetByPath("body.kept").AsStringDefault(""), "the rest of the link body must survive")
	s.Equal("tdl_rel", data.GetByPath("body.type").AsStringDefault(""), "and so must the object link type the schema declares")
	tags, _ := data.GetByPath("tags").AsArrayString()
	s.Equal([]string{"keep"}, tags, "and the tags")

	s.Require().NoError(s.dbc.CMDB.TriggerLinkDrop("TdLFrom", "TdLTo", db.CreateTrigger))
	s.Empty(s.linkTriggers("TdLFrom", "TdLTo", "create"), "dropping the create kind must clear it")
	s.Equal([]string{trigA}, s.linkTriggers("TdLFrom", "TdLTo", "delete"), "and leave the delete kind alone")

	// What fires: a link create between objects of the two types reaches B alone
	// before the drop — checked after the remove, before the drop, in a second
	// pair so the drop above is not undone.
	s.Require().NoError(s.dbc.CMDB.TriggerLinkSet("TdLFrom", "TdLTo", db.CreateTrigger, trigA, trigB))
	s.Require().NoError(s.dbc.CMDB.TriggerLinkRemove("TdLFrom", "TdLTo", db.CreateTrigger, trigA))
	s.Require().NoError(s.dbc.CMDB.ObjectCreate("tdl-a", "TdLFrom", easyjson.NewJSONObject()))
	s.Require().NoError(s.dbc.CMDB.ObjectCreate("tdl-b", "TdLTo", easyjson.NewJSONObject()))
	s.firedWithin(500 * time.Millisecond) // object creates carry no object triggers here; drain anyway
	s.Require().NoError(s.dbc.CMDB.ObjectsLinkCreate("tdl-a", "tdl-b", "tdl-b", nil, easyjson.NewJSONObject()))
	got := s.firedWithin(2 * time.Second)
	s.Len(got, 1, "exactly one link create trigger must fire: %v", got)
	if len(got) == 1 {
		s.Contains(got[0], trigB+"@", "and it must be B, the one that was not removed: %v", got)
	}
}
