package crud_test

import (
	"fmt"
	"strings"
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

// MetaTriggersTestSuite pins the meta lifecycle triggers: user statefuns that
// fire on create/update/delete/read of TYPES and type-to-type links (registered
// in the `types` root) and — globally — of ALL objects and object-to-object
// links (registered in the `objects` root). It also pins that they coexist with
// the existing per-type triggers (no double-fire) and that the no-subscriber
// path stays silent.
type MetaTriggersTestSuite struct {
	test.StatefunTestSuite
	dbc   db.DBSyncClient
	fired chan string
}

func TestMetaTriggersTestSuite(t *testing.T) {
	suite.Run(t, new(MetaTriggersTestSuite))
}

var metaKinds = []string{"type", "types_link", "object_meta", "object_link"}
var metaEvents = []string{"create", "update", "delete", "read"}

func (s *MetaTriggersTestSuite) bootstrap() {
	s.fired = make(chan string, 512)
	crud.RegisterAllFunctionTypes(s.Runtime())
	cfg := *statefun.NewFunctionTypeConfig().
		SetAllowedSignalProviders(sfPlugins.AutoSignalSelect).
		SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).
		SetMaxIdHandlers(-1)

	// Records which meta event fired (from the payload path) on which subject.
	s.RegisterFunction("test.meta.rec", func(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
		for _, k := range metaKinds {
			for _, e := range metaEvents {
				if ctx.Payload.PathExists(fmt.Sprintf("trigger.%s.%s", k, e)) {
					s.record(fmt.Sprintf("%s.%s", k, e), ctx.Self.ID)
				}
			}
		}
	}, cfg)

	// Records the EXISTING per-type object triggers (trigger.object.*), to prove
	// coexistence with the new global object meta path.
	s.RegisterFunction("test.pertype.rec", func(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
		for _, e := range metaEvents {
			if ctx.Payload.PathExists(fmt.Sprintf("trigger.object.%s", e)) {
				s.record(fmt.Sprintf("pertype.object.%s", e), ctx.Self.ID)
			}
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

func (s *MetaTriggersTestSuite) record(event, id string) {
	select {
	case s.fired <- event + "|" + id:
	default:
	}
}

// collect drains fired events until ~400ms of quiet (or a 4s ceiling). Meta
// triggers are delivered via JetStream signals, so this lets them propagate.
func (s *MetaTriggersTestSuite) collect() []string {
	out := []string{}
	for {
		select {
		case f := <-s.fired:
			out = append(out, f)
		case <-time.After(400 * time.Millisecond):
			return out
		}
	}
}

func countPrefix(events []string, prefix string) int {
	n := 0
	for _, e := range events {
		if strings.HasPrefix(e, prefix+"|") {
			n++
		}
	}
	return n
}

// drain clears any pending fired events before an action under test.
func (s *MetaTriggersTestSuite) drain() {
	for {
		select {
		case <-s.fired:
		case <-time.After(150 * time.Millisecond):
			return
		}
	}
}

func nonEmptyBody(k, v string) easyjson.JSON {
	return easyjson.NewJSONObjectWithKeyValue(k, easyjson.NewJSON(v))
}

// --- TYPE lifecycle ---

func (s *MetaTriggersTestSuite) Test_TypeLifecycle() {
	s.bootstrap()
	for _, e := range metaEvents {
		s.NoError(s.dbc.CMDB.MetaTriggerTypeSet(e, "test.meta.rec"))
	}

	s.drain()
	s.NoError(s.dbc.CMDB.TypeCreate("mt-a"))
	s.Equal(1, countPrefix(s.collect(), "type.create"), "type create fires once")

	s.drain()
	s.NoError(s.dbc.CMDB.TypeUpdate("mt-a", nonEmptyBody("k", "v1"), false))
	s.Equal(1, countPrefix(s.collect(), "type.update"), "type update fires once")

	s.drain()
	_, err := s.dbc.CMDB.TypeRead("mt-a")
	s.NoError(err)
	s.Equal(1, countPrefix(s.collect(), "type.read"), "type read fires once")

	s.drain()
	s.NoError(s.dbc.CMDB.TypeDelete("mt-a"))
	s.Equal(1, countPrefix(s.collect(), "type.delete"), "type delete fires once")
}

// --- TYPE-TO-TYPE link lifecycle ---

func (s *MetaTriggersTestSuite) Test_TypesLinkLifecycle() {
	s.bootstrap()
	s.NoError(s.dbc.CMDB.TypeCreate("mt-la"))
	s.NoError(s.dbc.CMDB.TypeCreate("mt-lb"))
	for _, e := range metaEvents {
		s.NoError(s.dbc.CMDB.MetaTriggerTypesLinkSet(e, "test.meta.rec"))
	}

	s.drain()
	s.NoError(s.dbc.CMDB.TypesLinkCreate("mt-la", "mt-lb", "rel", nil))
	s.Equal(1, countPrefix(s.collect(), "types_link.create"), "types-link create fires once")

	s.drain()
	s.NoError(s.dbc.CMDB.TypesLinkUpdate("mt-la", "mt-lb", nil, nonEmptyBody("k", "v1"), false))
	s.Equal(1, countPrefix(s.collect(), "types_link.update"), "types-link update fires once")

	s.drain()
	_, err := s.dbc.CMDB.TypesLinkRead("mt-la", "mt-lb")
	s.NoError(err)
	s.Equal(1, countPrefix(s.collect(), "types_link.read"), "types-link read fires once")

	s.drain()
	s.NoError(s.dbc.CMDB.TypesLinkDelete("mt-la", "mt-lb"))
	s.Equal(1, countPrefix(s.collect(), "types_link.delete"), "types-link delete fires once")
}

// --- GLOBAL object lifecycle ---

func (s *MetaTriggersTestSuite) Test_ObjectLifecycle() {
	s.bootstrap()
	s.NoError(s.dbc.CMDB.TypeCreate("mt-ot"))
	for _, e := range metaEvents {
		s.NoError(s.dbc.CMDB.MetaTriggerObjectSet(e, "test.meta.rec"))
	}

	s.drain()
	s.NoError(s.dbc.CMDB.ObjectCreate("mt-o1", "mt-ot"))
	s.Equal(1, countPrefix(s.collect(), "object_meta.create"), "object create fires once")

	s.drain()
	s.NoError(s.dbc.CMDB.ObjectUpdate("mt-o1", nonEmptyBody("k", "v1"), false))
	s.Equal(1, countPrefix(s.collect(), "object_meta.update"), "object update fires once")

	s.drain()
	_, err := s.dbc.CMDB.ObjectRead("mt-o1")
	s.NoError(err)
	s.Equal(1, countPrefix(s.collect(), "object_meta.read"), "object read fires once")

	s.drain()
	s.NoError(s.dbc.CMDB.ObjectDelete("mt-o1"))
	s.Equal(1, countPrefix(s.collect(), "object_meta.delete"), "object delete fires once")
}

// --- GLOBAL object-to-object link lifecycle ---

func (s *MetaTriggersTestSuite) Test_ObjectLinkLifecycle() {
	s.bootstrap()
	s.NoError(s.dbc.CMDB.TypeCreate("mt-na"))
	s.NoError(s.dbc.CMDB.TypeCreate("mt-nb"))
	s.NoError(s.dbc.CMDB.TypesLinkCreate("mt-na", "mt-nb", "nrel", nil))
	s.NoError(s.dbc.CMDB.ObjectCreate("mt-n1", "mt-na"))
	s.NoError(s.dbc.CMDB.ObjectCreate("mt-n2", "mt-nb"))
	for _, e := range metaEvents {
		s.NoError(s.dbc.CMDB.MetaTriggerObjectLinkSet(e, "test.meta.rec"))
	}

	s.drain()
	s.NoError(s.dbc.CMDB.ObjectsLinkCreate("mt-n1", "mt-n2", "edge", nil, easyjson.NewJSONObject()))
	s.Equal(1, countPrefix(s.collect(), "object_link.create"), "object-link create fires once")

	s.drain()
	s.NoError(s.dbc.CMDB.ObjectsLinkUpdate("mt-n1", "mt-n2", nil, nonEmptyBody("k", "v1"), false))
	s.Equal(1, countPrefix(s.collect(), "object_link.update"), "object-link update fires once")

	s.drain()
	_, err := s.dbc.CMDB.ObjectsLinkRead("mt-n1", "mt-n2")
	s.NoError(err)
	s.Equal(1, countPrefix(s.collect(), "object_link.read"), "object-link read fires once")

	s.drain()
	s.NoError(s.dbc.CMDB.ObjectsLinkDelete("mt-n1", "mt-n2"))
	s.Equal(1, countPrefix(s.collect(), "object_link.delete"), "object-link delete fires once")
}

// --- No subscriber => silent (the no-work fast path) ---

func (s *MetaTriggersTestSuite) Test_NoSubscriber_Silent() {
	s.bootstrap()
	s.NoError(s.dbc.CMDB.TypeCreate("mt-q"))

	s.drain()
	s.NoError(s.dbc.CMDB.ObjectCreate("mt-q1", "mt-q"))
	_, err := s.dbc.CMDB.ObjectRead("mt-q1")
	s.NoError(err)
	_, err = s.dbc.CMDB.TypeRead("mt-q")
	s.NoError(err)

	s.Empty(s.collect(), "no meta triggers registered => nothing fires")
}

// --- Coexistence with the existing per-type trigger; no double-fire ---

func (s *MetaTriggersTestSuite) Test_CoexistsWithPerTypeTrigger() {
	s.bootstrap()
	s.NoError(s.dbc.CMDB.TypeCreate("mt-ct"))
	// Existing per-type object create trigger on the type itself.
	s.NoError(s.dbc.CMDB.TriggerObjectSet("mt-ct", db.CreateTrigger, "test.pertype.rec"))
	// New GLOBAL object create meta trigger.
	s.NoError(s.dbc.CMDB.MetaTriggerObjectSet(db.CreateTrigger, "test.meta.rec"))

	s.drain()
	s.NoError(s.dbc.CMDB.ObjectCreate("mt-c1", "mt-ct"))
	got := s.collect()
	s.Equal(1, countPrefix(got, "pertype.object.create"), "per-type object trigger still fires exactly once")
	s.Equal(1, countPrefix(got, "object_meta.create"), "global object meta trigger fires exactly once")
}

// --- DeleteType cascade fires a global object delete per cascaded object ---

func (s *MetaTriggersTestSuite) Test_CascadeDelete_FiresPerObject() {
	s.bootstrap()
	s.NoError(s.dbc.CMDB.TypeCreate("mt-ct2"))
	s.NoError(s.dbc.CMDB.ObjectCreate("mt-x1", "mt-ct2"))
	s.NoError(s.dbc.CMDB.ObjectCreate("mt-x2", "mt-ct2"))
	s.NoError(s.dbc.CMDB.ObjectCreate("mt-x3", "mt-ct2"))
	s.NoError(s.dbc.CMDB.MetaTriggerObjectSet(db.DeleteTrigger, "test.meta.rec"))
	s.NoError(s.dbc.CMDB.MetaTriggerTypeSet(db.DeleteTrigger, "test.meta.rec"))

	s.drain()
	s.NoError(s.dbc.CMDB.TypeDelete("mt-ct2"))
	got := s.collect()
	s.Equal(3, countPrefix(got, "object_meta.delete"), "object meta delete fires once per cascaded object")
	s.Equal(1, countPrefix(got, "type.delete"), "type meta delete fires once for the type")
}

// --- Removal via Delete: a removed trigger stops firing (all four kinds) ---

func (s *MetaTriggersTestSuite) Test_Removal_Delete_StopsFiring() {
	s.bootstrap()

	// object: a SELECTIVE delete of an unrelated fn must keep ours firing; a
	// delete of ours must stop it.
	s.NoError(s.dbc.CMDB.TypeCreate("rm-ot"))
	s.NoError(s.dbc.CMDB.MetaTriggerObjectSet(db.CreateTrigger, "noop.fn", "test.meta.rec"))
	s.drain()
	s.NoError(s.dbc.CMDB.ObjectCreate("rm-o1", "rm-ot"))
	s.Equal(1, countPrefix(s.collect(), "object_meta.create"), "object fires while registered")

	s.NoError(s.dbc.CMDB.MetaTriggerObjectDelete(db.CreateTrigger, "noop.fn"))
	s.drain()
	s.NoError(s.dbc.CMDB.ObjectCreate("rm-o2", "rm-ot"))
	s.Equal(1, countPrefix(s.collect(), "object_meta.create"), "deleting another fn keeps ours firing")

	s.NoError(s.dbc.CMDB.MetaTriggerObjectDelete(db.CreateTrigger, "test.meta.rec"))
	s.drain()
	s.NoError(s.dbc.CMDB.ObjectCreate("rm-o3", "rm-ot"))
	s.Equal(0, countPrefix(s.collect(), "object_meta.create"), "object silent after deleting ours")

	// type
	s.NoError(s.dbc.CMDB.MetaTriggerTypeSet(db.CreateTrigger, "test.meta.rec"))
	s.drain()
	s.NoError(s.dbc.CMDB.TypeCreate("rm-t1"))
	s.Equal(1, countPrefix(s.collect(), "type.create"), "type fires while registered")
	s.NoError(s.dbc.CMDB.MetaTriggerTypeDelete(db.CreateTrigger, "test.meta.rec"))
	s.drain()
	s.NoError(s.dbc.CMDB.TypeCreate("rm-t2"))
	s.Equal(0, countPrefix(s.collect(), "type.create"), "type silent after delete")

	// types_link
	s.NoError(s.dbc.CMDB.TypeCreate("rm-la"))
	s.NoError(s.dbc.CMDB.TypeCreate("rm-lb"))
	s.NoError(s.dbc.CMDB.TypeCreate("rm-lc"))
	s.NoError(s.dbc.CMDB.MetaTriggerTypesLinkSet(db.CreateTrigger, "test.meta.rec"))
	s.drain()
	s.NoError(s.dbc.CMDB.TypesLinkCreate("rm-la", "rm-lb", "r1", nil))
	s.Equal(1, countPrefix(s.collect(), "types_link.create"), "types-link fires while registered")
	s.NoError(s.dbc.CMDB.MetaTriggerTypesLinkDelete(db.CreateTrigger, "test.meta.rec"))
	s.drain()
	s.NoError(s.dbc.CMDB.TypesLinkCreate("rm-la", "rm-lc", "r2", nil))
	s.Equal(0, countPrefix(s.collect(), "types_link.create"), "types-link silent after delete")

	// object_link
	s.NoError(s.dbc.CMDB.TypeCreate("rm-na"))
	s.NoError(s.dbc.CMDB.TypeCreate("rm-nb"))
	s.NoError(s.dbc.CMDB.TypesLinkCreate("rm-na", "rm-nb", "nrel", nil))
	s.NoError(s.dbc.CMDB.ObjectCreate("rm-n1", "rm-na"))
	s.NoError(s.dbc.CMDB.ObjectCreate("rm-n2", "rm-nb"))
	s.NoError(s.dbc.CMDB.ObjectCreate("rm-n3", "rm-nb"))
	s.NoError(s.dbc.CMDB.MetaTriggerObjectLinkSet(db.CreateTrigger, "test.meta.rec"))
	s.drain()
	s.NoError(s.dbc.CMDB.ObjectsLinkCreate("rm-n1", "rm-n2", "e1", nil))
	s.Equal(1, countPrefix(s.collect(), "object_link.create"), "object-link fires while registered")
	s.NoError(s.dbc.CMDB.MetaTriggerObjectLinkDelete(db.CreateTrigger, "test.meta.rec"))
	s.drain()
	s.NoError(s.dbc.CMDB.ObjectsLinkCreate("rm-n1", "rm-n3", "e2", nil))
	s.Equal(0, countPrefix(s.collect(), "object_link.create"), "object-link silent after delete")
}

// --- Removal via Drop: clears the whole event list ---

func (s *MetaTriggersTestSuite) Test_Removal_Drop_StopsFiring() {
	s.bootstrap()

	s.NoError(s.dbc.CMDB.TypeCreate("dp-ot"))
	s.NoError(s.dbc.CMDB.MetaTriggerObjectSet(db.CreateTrigger, "fn.a", "test.meta.rec"))
	s.drain()
	s.NoError(s.dbc.CMDB.ObjectCreate("dp-o1", "dp-ot"))
	s.Equal(1, countPrefix(s.collect(), "object_meta.create"), "object fires before drop")
	s.NoError(s.dbc.CMDB.MetaTriggerObjectDrop(db.CreateTrigger))
	s.drain()
	s.NoError(s.dbc.CMDB.ObjectCreate("dp-o2", "dp-ot"))
	s.Equal(0, countPrefix(s.collect(), "object_meta.create"), "object silent after drop")

	s.NoError(s.dbc.CMDB.MetaTriggerTypeSet(db.CreateTrigger, "test.meta.rec"))
	s.drain()
	s.NoError(s.dbc.CMDB.TypeCreate("dp-t1"))
	s.Equal(1, countPrefix(s.collect(), "type.create"), "type fires before drop")
	s.NoError(s.dbc.CMDB.MetaTriggerTypeDrop(db.CreateTrigger))
	s.drain()
	s.NoError(s.dbc.CMDB.TypeCreate("dp-t2"))
	s.Equal(0, countPrefix(s.collect(), "type.create"), "type silent after drop")
}
