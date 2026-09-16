package crud_test

// Every trigger the SDK has, in one table.
//
// Six families hold triggers: a type (for its objects), a types-link (for the
// object links it declares), and the four meta sections of the roots — type,
// types_link, object, object_link. Each fires on four kinds of event: create,
// update, delete, read. Each is registered with Set and removed with Delete
// (one name) or Drop (the whole kind).
//
// For every family and every kind the same five steps run — set A; set B
// alongside; set A again; delete A; drop — and after every step two things are
// checked against a model kept beside the graph: what the body holds for ALL
// four kinds of the family (so a step on one kind is seen touching another),
// and what actually fires when the subject operation of that kind is
// performed — exactly the modelled functions, once each, on the right subject,
// under the right payload path. The rest of the body is compared before and
// after as well: a types-link keeps the object link type it declares and its
// tags, a root keeps its version marker.
//
// Bodies are read straight from the cache, not through the read APIs, because
// a read is itself an event some of these triggers fire on.
//
// The per-type read triggers fire only when the caller asks for an op-stack —
// the plain client reads do not — so that kind is exercised with the option set.

import (
	"fmt"
	"sort"
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

type TriggerMatrixTestSuite struct {
	test.StatefunTestSuite
	dbc   db.DBSyncClient
	fired chan string // "<fn>|<payload path>|<subject id>"
}

func TestTriggerMatrixTestSuite(t *testing.T) { suite.Run(t, new(TriggerMatrixTestSuite)) }

const (
	mxA = "test.mx.a"
	mxB = "test.mx.b"
	mxS = "test.mx.s" // the sentinel: on every kind from the start, so a step on one kind is seen touching another
)

var mxKinds = []string{db.CreateTrigger, db.UpdateTrigger, db.DeleteTrigger, db.ReadTrigger}

// triggerFamily is one holder of triggers: how to register and remove them,
// where they are stored, what the rest of its body is, and how to make each
// kind of event happen.
type triggerFamily struct {
	name string
	path string // payload path prefix the recorder sees: "trigger.object", "trigger.link", "trigger.type", ...

	set    func(kind string, fns ...string) error
	remove func(kind string, fns ...string) error
	drop   func(kind string) error

	stored func(kind string) []string // the names registered for the kind, sorted
	rest   func() string              // the body apart from the triggers, canonical

	act func(kind string) string // performs the subject operation of the kind; returns the id the trigger fires on
}

func (s *TriggerMatrixTestSuite) bootstrap() {
	s.fired = make(chan string, 1024)
	crud.RegisterAllFunctionTypes(s.Runtime())
	cfg := *statefun.NewFunctionTypeConfig().
		SetAllowedSignalProviders(sfPlugins.AutoSignalSelect).
		SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).
		SetMaxIdHandlers(-1)
	for _, name := range []string{mxA, mxB, mxS} {
		name := name
		s.RegisterFunction(name, func(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
			// The payload is {"trigger": {"<family>": {"<kind>": {...}}}}.
			trigger := ctx.Payload.GetByPath("trigger")
			for _, fam := range trigger.ObjectKeys() {
				for _, kind := range trigger.GetByPath(fam).ObjectKeys() {
					select {
					case s.fired <- name + "|trigger." + fam + "." + kind + "|" + ctx.Self.ID:
					default:
					}
				}
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

// collect gathers what fires. Triggers travel as JetStream signals, so when
// `expected` of them are due it waits for that many (up to four seconds) and
// then for a short quiet spell, so anything extra is caught too; when none are
// due it simply listens through the quiet spell. Sorted.
func (s *TriggerMatrixTestSuite) collect(expected int) []string {
	out := []string{}
	deadline := time.After(4 * time.Second)
	for len(out) < expected {
		select {
		case f := <-s.fired:
			out = append(out, f)
		case <-deadline:
			sort.Strings(out)
			return out
		}
	}
	quiet := 250 * time.Millisecond
	if expected == 0 {
		quiet = 400 * time.Millisecond
	}
	for {
		select {
		case f := <-s.fired:
			out = append(out, f)
		case <-time.After(quiet):
			sort.Strings(out)
			return out
		}
	}
}

// drain discards whatever a setup step fired.
func (s *TriggerMatrixTestSuite) drain() {
	for {
		select {
		case <-s.fired:
		case <-time.After(150 * time.Millisecond):
			return
		}
	}
}

// vertexBody reads a vertex body from the cache, without a read event.
func (s *TriggerMatrixTestSuite) vertexBody(id string) easyjson.JSON {
	s.T().Helper()
	b, err := s.CacheValue(s.SetThisDomainPreffix(id))
	s.Require().NoError(err, "vertex %s must exist", id)
	return *b
}

// linkBody reads a link body from the cache, without a read event. The name
// is stored without a domain, whatever the caller passed.
func (s *TriggerMatrixTestSuite) linkBody(from, name string) easyjson.JSON {
	s.T().Helper()
	b, err := s.Runtime().Domain.Cache().GetValueJSON(fmt.Sprintf(crud.OutLinkBodyKeyPrefPattern+crud.KeySuff1Pattern,
		s.SetThisDomainPreffix(from), name))
	s.Require().NoError(err, "link %s -> %s must exist", from, name)
	return *b
}

// linkTags reads a link's tags from its index, sorted.
func (s *TriggerMatrixTestSuite) linkTags(from, name string) []string {
	keys := s.Runtime().Domain.Cache().GetKeysByPattern(fmt.Sprintf(crud.OutLinkIndexPrefPattern+crud.KeySuff3Pattern,
		s.SetThisDomainPreffix(from), name, "tag", ">"))
	tags := make([]string, 0, len(keys))
	for _, k := range keys {
		tags = append(tags, k[strings.LastIndex(k, ".")+1:])
	}
	sort.Strings(tags)
	return tags
}

func namesAt(body easyjson.JSON, path string) []string {
	arr, _ := body.GetByPath(path).AsArrayString()
	sort.Strings(arr)
	return arr
}

func without(body easyjson.JSON, path string) string {
	c := body.Clone()
	c.RemoveByPath(path)
	return c.ToString()
}

func sorted(ss ...string) []string {
	out := append([]string{}, ss...)
	sort.Strings(out)
	return out
}

// runMatrix drives the five steps for every kind of one family against the
// model, checking storage, the rest of the body and what fires after each.
func (s *TriggerMatrixTestSuite) runMatrix(f triggerFamily) {
	model := map[string][]string{}
	// The body apart from the triggers, as of the last subject operation: an
	// operation may change it (deleting a type stamps the types root with a
	// version), a trigger step may not.
	rest := f.rest()
	for _, k := range mxKinds {
		s.Require().NoError(f.set(k, mxS), "%s: sentinel on %s", f.name, k)
		model[k] = []string{mxS}
	}
	s.drain() // whatever registering caused

	// expectStored compares all four kinds with the model after a step.
	expectStored := func(step string) {
		s.T().Helper()
		for _, k := range mxKinds {
			s.Equalf(sorted(model[k]...), f.stored(k), "%s: after %s, the %s triggers differ from the model", f.name, step, k)
		}
		s.Equalf(rest, f.rest(), "%s: after %s, the body apart from the triggers changed", f.name, step)
	}
	// expectFired performs the subject operation of the kind and compares what
	// fired with the model: each modelled function once, on the subject, under
	// the family's path for that kind — and nothing else.
	expectFired := func(step, kind string) {
		s.T().Helper()
		s.drain()
		subject := f.act(kind)
		want := []string{}
		for _, fn := range model[kind] {
			want = append(want, fn+"|"+f.path+"."+kind+"|"+subject)
		}
		s.Equalf(sorted(want...), s.collect(len(want)), "%s: after %s, a %s event fired something other than the model", f.name, step, kind)
		rest = f.rest()
	}

	for _, kind := range mxKinds {
		step := func(what string) string { return what + " on " + kind }

		s.Require().NoError(f.set(kind, mxA))
		model[kind] = append(model[kind], mxA)
		expectStored(step("set A"))
		expectFired(step("set A"), kind)

		s.Require().NoError(f.set(kind, mxB))
		model[kind] = append(model[kind], mxB)
		expectStored(step("set B alongside"))
		expectFired(step("set B alongside"), kind)

		s.Require().NoError(f.set(kind, mxA))
		expectStored(step("set A again")) // the model did not change: no duplicate may appear
		expectFired(step("set A again"), kind)

		s.Require().NoError(f.remove(kind, mxA))
		model[kind] = sorted(mxB, mxS)
		expectStored(step("delete A"))
		expectFired(step("delete A"), kind)

		s.Require().NoError(f.drop(kind))
		model[kind] = []string{}
		expectStored(step("drop"))
		expectFired(step("drop"), kind)
	}
}

// --- the six families ---

func (s *TriggerMatrixTestSuite) Test_TypeObjectTriggers() {
	s.bootstrap()
	const T = "mx_ot"
	s.Require().NoError(s.dbc.CMDB.TypeCreate(T, easyjson.NewJSONObjectWithKeyValue("kept", easyjson.NewJSON("field"))))
	s.Require().NoError(s.dbc.CMDB.ObjectCreate("mx_ot_subject", T, easyjson.NewJSONObject()))
	n := 0

	s.runMatrix(triggerFamily{
		name:   "type object triggers",
		path:   "trigger.object",
		set:    func(k string, fns ...string) error { return s.dbc.CMDB.TriggerObjectSet(T, k, fns...) },
		remove: func(k string, fns ...string) error { return s.dbc.CMDB.TriggerObjectDelete(T, k, fns...) },
		drop:   func(k string) error { return s.dbc.CMDB.TriggerObjectDrop(T, k) },
		stored: func(k string) []string { return namesAt(s.vertexBody(T), "triggers."+k) },
		rest:   func() string { return without(s.vertexBody(T), "triggers") },
		act: func(k string) string {
			n++
			switch k {
			case db.CreateTrigger:
				id := fmt.Sprintf("mx_ot_new%d", n)
				s.Require().NoError(s.dbc.CMDB.ObjectCreate(id, T, easyjson.NewJSONObject()))
				return s.SetThisDomainPreffix(id)
			case db.UpdateTrigger:
				s.Require().NoError(s.dbc.CMDB.ObjectUpdate("mx_ot_subject", easyjson.NewJSONObjectWithKeyValue("n", easyjson.NewJSON(n)), false))
				return s.SetThisDomainPreffix("mx_ot_subject")
			case db.DeleteTrigger:
				id := fmt.Sprintf("mx_ot_del%d", n)
				s.Require().NoError(s.dbc.CMDB.ObjectCreate(id, T, easyjson.NewJSONObject()))
				s.drain()
				s.Require().NoError(s.dbc.CMDB.ObjectDelete(id))
				return s.SetThisDomainPreffix(id)
			default: // read, with the op-stack the per-type read triggers need
				opts := easyjson.NewJSONObjectWithKeyValue("op_stack", easyjson.NewJSON(true))
				res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.object.read", "mx_ot_subject", nil, &opts)
				s.Require().NoError(err)
				s.Require().Equal("ok", res.GetByPath("status").AsStringDefault(""), res.ToString())
				return s.SetThisDomainPreffix("mx_ot_subject")
			}
		},
	})
}

func (s *TriggerMatrixTestSuite) Test_TypesLinkTriggers() {
	s.bootstrap()
	const F, T = "mx_lf", "mx_lt"
	s.Require().NoError(s.dbc.CMDB.TypeCreate(F))
	s.Require().NoError(s.dbc.CMDB.TypeCreate(T))
	s.Require().NoError(s.dbc.CMDB.TypesLinkCreate(F, T, "mx_rel", []string{"keep"}))
	s.Require().NoError(s.dbc.CMDB.TypesLinkUpdate(F, T, nil, easyjson.NewJSONObjectWithKeyValue("kept", easyjson.NewJSON("field")), false))
	s.Require().NoError(s.dbc.CMDB.ObjectCreate("mx_l_a", F, easyjson.NewJSONObject()))
	s.Require().NoError(s.dbc.CMDB.ObjectCreate("mx_l_b", T, easyjson.NewJSONObject()))
	s.Require().NoError(s.dbc.CMDB.ObjectsLinkCreate("mx_l_a", "mx_l_b", "mx_l_b", nil, easyjson.NewJSONObject()))
	n := 0

	s.runMatrix(triggerFamily{
		name:   "types-link triggers",
		path:   "trigger.link",
		set:    func(k string, fns ...string) error { return s.dbc.CMDB.TriggerLinkSet(F, T, k, fns...) },
		remove: func(k string, fns ...string) error { return s.dbc.CMDB.TriggerLinkRemove(F, T, k, fns...) },
		drop:   func(k string) error { return s.dbc.CMDB.TriggerLinkDrop(F, T, k) },
		stored: func(k string) []string { return namesAt(s.linkBody(F, T), "triggers."+k) },
		rest: func() string {
			return without(s.linkBody(F, T), "triggers") + " tags=" + strings.Join(s.linkTags(F, T), ",")
		},
		act: func(k string) string {
			n++
			switch k {
			case db.CreateTrigger:
				to := fmt.Sprintf("mx_l_new%d", n)
				s.Require().NoError(s.dbc.CMDB.ObjectCreate(to, T, easyjson.NewJSONObject()))
				s.drain()
				s.Require().NoError(s.dbc.CMDB.ObjectsLinkCreate("mx_l_a", to, to, nil, easyjson.NewJSONObject()))
			case db.UpdateTrigger:
				s.Require().NoError(s.dbc.CMDB.ObjectsLinkUpdate("mx_l_a", "mx_l_b", nil, easyjson.NewJSONObjectWithKeyValue("n", easyjson.NewJSON(n)), false))
			case db.DeleteTrigger:
				to := fmt.Sprintf("mx_l_del%d", n)
				s.Require().NoError(s.dbc.CMDB.ObjectCreate(to, T, easyjson.NewJSONObject()))
				s.Require().NoError(s.dbc.CMDB.ObjectsLinkCreate("mx_l_a", to, to, nil, easyjson.NewJSONObject()))
				s.drain()
				s.Require().NoError(s.dbc.CMDB.ObjectsLinkDelete("mx_l_a", to))
			default: // read, with the op-stack the per-type read triggers need
				p := easyjson.NewJSONObjectWithKeyValue("to", easyjson.NewJSON("mx_l_b"))
				opts := easyjson.NewJSONObjectWithKeyValue("op_stack", easyjson.NewJSON(true))
				res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.objects.link.read", "mx_l_a", &p, &opts)
				s.Require().NoError(err)
				s.Require().Equal("ok", res.GetByPath("status").AsStringDefault(""), res.ToString())
			}
			return s.SetThisDomainPreffix("mx_l_a")
		},
	})
}

func (s *TriggerMatrixTestSuite) Test_MetaTypeTriggers() {
	s.bootstrap()
	s.Require().NoError(s.dbc.CMDB.TypeCreate("mx_mt_subject"))
	n := 0

	s.runMatrix(triggerFamily{
		name:   "meta type triggers",
		path:   "trigger.type",
		set:    func(k string, fns ...string) error { return s.dbc.CMDB.MetaTriggerTypeSet(k, fns...) },
		remove: func(k string, fns ...string) error { return s.dbc.CMDB.MetaTriggerTypeDelete(k, fns...) },
		drop:   func(k string) error { return s.dbc.CMDB.MetaTriggerTypeDrop(k) },
		stored: func(k string) []string { return namesAt(s.vertexBody(crud.BUILT_IN_TYPES), "meta_triggers.type."+k) },
		rest:   func() string { return without(s.vertexBody(crud.BUILT_IN_TYPES), "meta_triggers") },
		act: func(k string) string {
			n++
			switch k {
			case db.CreateTrigger:
				id := fmt.Sprintf("mx_mt_new%d", n)
				s.Require().NoError(s.dbc.CMDB.TypeCreate(id))
				return s.SetThisDomainPreffix(id)
			case db.UpdateTrigger:
				s.Require().NoError(s.dbc.CMDB.TypeUpdate("mx_mt_subject", easyjson.NewJSONObjectWithKeyValue("n", easyjson.NewJSON(n)), false))
				return s.SetThisDomainPreffix("mx_mt_subject")
			case db.DeleteTrigger:
				id := fmt.Sprintf("mx_mt_del%d", n)
				s.Require().NoError(s.dbc.CMDB.TypeCreate(id))
				s.drain()
				s.Require().NoError(s.dbc.CMDB.TypeDelete(id))
				return s.SetThisDomainPreffix(id)
			default:
				_, err := s.dbc.CMDB.TypeRead("mx_mt_subject")
				s.Require().NoError(err)
				return s.SetThisDomainPreffix("mx_mt_subject")
			}
		},
	})
}

func (s *TriggerMatrixTestSuite) Test_MetaTypesLinkTriggers() {
	s.bootstrap()
	const F, T = "mx_mlf", "mx_mlt"
	s.Require().NoError(s.dbc.CMDB.TypeCreate(F))
	s.Require().NoError(s.dbc.CMDB.TypeCreate(T))
	s.Require().NoError(s.dbc.CMDB.TypesLinkCreate(F, T, "mx_mrel", nil))
	n := 0

	s.runMatrix(triggerFamily{
		name:   "meta types-link triggers",
		path:   "trigger.types_link",
		set:    func(k string, fns ...string) error { return s.dbc.CMDB.MetaTriggerTypesLinkSet(k, fns...) },
		remove: func(k string, fns ...string) error { return s.dbc.CMDB.MetaTriggerTypesLinkDelete(k, fns...) },
		drop:   func(k string) error { return s.dbc.CMDB.MetaTriggerTypesLinkDrop(k) },
		stored: func(k string) []string {
			return namesAt(s.vertexBody(crud.BUILT_IN_TYPES), "meta_triggers.types_link."+k)
		},
		rest: func() string { return without(s.vertexBody(crud.BUILT_IN_TYPES), "meta_triggers") },
		act: func(k string) string {
			n++
			switch k {
			case db.CreateTrigger:
				to := fmt.Sprintf("mx_mlt_new%d", n)
				s.Require().NoError(s.dbc.CMDB.TypeCreate(to))
				s.drain()
				s.Require().NoError(s.dbc.CMDB.TypesLinkCreate(F, to, "mx_mrel", nil))
			case db.UpdateTrigger:
				s.Require().NoError(s.dbc.CMDB.TypesLinkUpdate(F, T, nil, easyjson.NewJSONObjectWithKeyValue("n", easyjson.NewJSON(n)), false))
			case db.DeleteTrigger:
				to := fmt.Sprintf("mx_mlt_del%d", n)
				s.Require().NoError(s.dbc.CMDB.TypeCreate(to))
				s.Require().NoError(s.dbc.CMDB.TypesLinkCreate(F, to, "mx_mrel", nil))
				s.drain()
				s.Require().NoError(s.dbc.CMDB.TypesLinkDelete(F, to))
			default:
				_, err := s.dbc.CMDB.TypesLinkRead(F, T)
				s.Require().NoError(err)
			}
			return s.SetThisDomainPreffix(F)
		},
	})
}

func (s *TriggerMatrixTestSuite) Test_MetaObjectTriggers() {
	s.bootstrap()
	const T = "mx_mo_t"
	s.Require().NoError(s.dbc.CMDB.TypeCreate(T))
	s.Require().NoError(s.dbc.CMDB.ObjectCreate("mx_mo_subject", T, easyjson.NewJSONObject()))
	n := 0

	s.runMatrix(triggerFamily{
		name:   "meta object triggers",
		path:   "trigger.object_meta",
		set:    func(k string, fns ...string) error { return s.dbc.CMDB.MetaTriggerObjectSet(k, fns...) },
		remove: func(k string, fns ...string) error { return s.dbc.CMDB.MetaTriggerObjectDelete(k, fns...) },
		drop:   func(k string) error { return s.dbc.CMDB.MetaTriggerObjectDrop(k) },
		stored: func(k string) []string {
			return namesAt(s.vertexBody(crud.BUILT_IN_OBJECTS), "meta_triggers.object."+k)
		},
		rest: func() string { return without(s.vertexBody(crud.BUILT_IN_OBJECTS), "meta_triggers") },
		act: func(k string) string {
			n++
			switch k {
			case db.CreateTrigger:
				id := fmt.Sprintf("mx_mo_new%d", n)
				s.Require().NoError(s.dbc.CMDB.ObjectCreate(id, T, easyjson.NewJSONObject()))
				return s.SetThisDomainPreffix(id)
			case db.UpdateTrigger:
				s.Require().NoError(s.dbc.CMDB.ObjectUpdate("mx_mo_subject", easyjson.NewJSONObjectWithKeyValue("n", easyjson.NewJSON(n)), false))
				return s.SetThisDomainPreffix("mx_mo_subject")
			case db.DeleteTrigger:
				id := fmt.Sprintf("mx_mo_del%d", n)
				s.Require().NoError(s.dbc.CMDB.ObjectCreate(id, T, easyjson.NewJSONObject()))
				s.drain()
				s.Require().NoError(s.dbc.CMDB.ObjectDelete(id))
				return s.SetThisDomainPreffix(id)
			default:
				_, err := s.dbc.CMDB.ObjectRead("mx_mo_subject")
				s.Require().NoError(err)
				return s.SetThisDomainPreffix("mx_mo_subject")
			}
		},
	})
}

func (s *TriggerMatrixTestSuite) Test_MetaObjectLinkTriggers() {
	s.bootstrap()
	const F, T = "mx_molf", "mx_molt"
	s.Require().NoError(s.dbc.CMDB.TypeCreate(F))
	s.Require().NoError(s.dbc.CMDB.TypeCreate(T))
	s.Require().NoError(s.dbc.CMDB.TypesLinkCreate(F, T, "mx_molrel", nil))
	s.Require().NoError(s.dbc.CMDB.ObjectCreate("mx_mol_a", F, easyjson.NewJSONObject()))
	s.Require().NoError(s.dbc.CMDB.ObjectCreate("mx_mol_b", T, easyjson.NewJSONObject()))
	s.Require().NoError(s.dbc.CMDB.ObjectsLinkCreate("mx_mol_a", "mx_mol_b", "mx_mol_b", nil, easyjson.NewJSONObject()))
	n := 0

	s.runMatrix(triggerFamily{
		name:   "meta object-link triggers",
		path:   "trigger.object_link",
		set:    func(k string, fns ...string) error { return s.dbc.CMDB.MetaTriggerObjectLinkSet(k, fns...) },
		remove: func(k string, fns ...string) error { return s.dbc.CMDB.MetaTriggerObjectLinkDelete(k, fns...) },
		drop:   func(k string) error { return s.dbc.CMDB.MetaTriggerObjectLinkDrop(k) },
		stored: func(k string) []string {
			return namesAt(s.vertexBody(crud.BUILT_IN_OBJECTS), "meta_triggers.object_link."+k)
		},
		rest: func() string { return without(s.vertexBody(crud.BUILT_IN_OBJECTS), "meta_triggers") },
		act: func(k string) string {
			n++
			switch k {
			case db.CreateTrigger:
				to := fmt.Sprintf("mx_mol_new%d", n)
				s.Require().NoError(s.dbc.CMDB.ObjectCreate(to, T, easyjson.NewJSONObject()))
				s.drain()
				s.Require().NoError(s.dbc.CMDB.ObjectsLinkCreate("mx_mol_a", to, to, nil, easyjson.NewJSONObject()))
			case db.UpdateTrigger:
				s.Require().NoError(s.dbc.CMDB.ObjectsLinkUpdate("mx_mol_a", "mx_mol_b", nil, easyjson.NewJSONObjectWithKeyValue("n", easyjson.NewJSON(n)), false))
			case db.DeleteTrigger:
				to := fmt.Sprintf("mx_mol_del%d", n)
				s.Require().NoError(s.dbc.CMDB.ObjectCreate(to, T, easyjson.NewJSONObject()))
				s.Require().NoError(s.dbc.CMDB.ObjectsLinkCreate("mx_mol_a", to, to, nil, easyjson.NewJSONObject()))
				s.drain()
				s.Require().NoError(s.dbc.CMDB.ObjectsLinkDelete("mx_mol_a", to))
			default:
				_, err := s.dbc.CMDB.ObjectsLinkRead("mx_mol_a", "mx_mol_b")
				s.Require().NoError(err)
			}
			return s.SetThisDomainPreffix("mx_mol_a")
		},
	})
}
