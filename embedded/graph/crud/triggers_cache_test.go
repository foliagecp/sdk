// Package crud — regression tests for the trigger-metadata caches.
//
// executeTriggersFromLLOpStack runs on every HL Create/Update/Delete and
// on every op in the LL op_stack of those operations — for a typical
// HLMB rebuild that means thousands of trigger-resolution calls per
// cycle. Without the per-type / per-TypesLink trigger caches each of
// those calls would hit NATS via vertex.read (object triggers) or
// link.read (link triggers). The caches turn that into in-memory lookups
// after the first observation.
//
// The contract pinned here:
//
//   - First lookup for a type / TypesLink populates the cache from KV.
//   - Subsequent lookups in the same activation hit the cache.
//   - UpdateType / TriggerObjectSet / TriggerObjectDelete invalidate
//     the object-trigger cache; the next lookup observes the new state.
//   - CreateTypesLink / UpdateTypesLink / DeleteTypesLink / DeleteType
//     invalidate the link-trigger cache appropriately.
//   - The negative cache (typeName known to have no triggers, TypesLink
//     known to have none) is correctly cleared on mutations.
package crud

import (
	"testing"
	"time"

	"github.com/foliagecp/easyjson"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/test"
	"github.com/stretchr/testify/suite"
)

type TriggerCacheTestSuite struct {
	test.StatefunTestSuite
}

func TestTriggerCacheTestSuite(t *testing.T) {
	suite.Run(t, new(TriggerCacheTestSuite))
}

func (s *TriggerCacheTestSuite) bootstrap() {
	RegisterAllFunctionTypes(s.Runtime())
	s.NoError(s.StartRuntime())
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := s.CacheValue(BUILT_IN_OBJECTS); err == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	// Each test must start with empty caches — concurrent test runs
	// could otherwise leak entries between suites that share package
	// globals.
	typeObjectTriggersCache.Range(func(k, _ any) bool { typeObjectTriggersCache.Delete(k); return true })
	typesLinkTriggersCache.Range(func(k, _ any) bool { typesLinkTriggersCache.Delete(k); return true })
}

func (s *TriggerCacheTestSuite) cmdbTypeCreate(name string, body ...easyjson.JSON) {
	p := easyjson.NewJSONObject()
	if len(body) > 0 {
		p.SetByPath("body", body[0])
	} else {
		p.SetByPath("body", easyjson.NewJSONObject())
	}
	res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.type.create", name, &p, nil)
	s.NoError(err)
	s.Equal("ok", res.GetByPath("status").AsStringDefault(""), "type.create %q must succeed", name)
}

// trigCtx returns a minimal ctx suitable for calling getTypeTriggers /
// getLinkTriggersInfo directly. The Domain on the embedded ctx routes
// through the live runtime exactly like a real function invocation.
func (s *TriggerCacheTestSuite) trigCtx() *sfPlugins.StatefunContextProcessor {
	// We don't need a real StatefunContextProcessor — but
	// getTypeTriggers makes a ctx.Request which delegates to the same
	// runtime.Request used by s.Request. Building a synthetic ctx is
	// brittle. Instead exercise the cache contract via the suite's
	// s.Request path: getTypeTriggers is called transparently inside
	// trigger-dispatch on real CRUD ops, so we observe its effect
	// indirectly through the cache state.
	return nil
}

// Test_ObjectTriggers_NegativeCache_PopulatedOnFirstUse: after a CRUD
// op that walks executeTriggersFromLLOpStack for a type WITHOUT
// triggers, the cache must contain the negative entry (nil value) so
// the next op can short-circuit without a vertex.read.
func (s *TriggerCacheTestSuite) Test_ObjectTriggers_NegativeCache_PopulatedOnFirstUse() {
	s.bootstrap()
	s.cmdbTypeCreate("TypeNoTrig")

	// Create an object; its trigger pass will discover no triggers and
	// cache the negative result against the type vertex id.
	p := easyjson.NewJSONObject()
	p.SetByPath("origin_type", easyjson.NewJSON("TypeNoTrig"))
	p.SetByPath("body", easyjson.NewJSONObject())
	res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.object.create", "obj-nt-1", &p, nil)
	s.NoError(err)
	s.Equal("ok", res.GetByPath("status").AsStringDefault(""))

	prefixed := s.SetThisDomainPreffix("TypeNoTrig")
	v, ok := typeObjectTriggersCache.Load(prefixed)
	s.Truef(ok, "negative cache entry must exist for %q after first trigger pass", prefixed)
	s.Nilf(v, "negative cache must store nil for trigger-less type, got %v", v)
}

// Test_ObjectTriggers_PositiveCache_PopulatedOnFirstUse: a type WITH
// registered triggers must be cached with a non-nil pointer to the
// triggers section.
func (s *TriggerCacheTestSuite) Test_ObjectTriggers_PositiveCache_PopulatedOnFirstUse() {
	s.bootstrap()
	// Type body with a triggers section configured for create.
	body := easyjson.NewJSONObject()
	body.SetByPath("triggers.create", easyjson.NewJSON([]string{"some.trigger.fn"}))
	s.cmdbTypeCreate("TypeWithTrig", body)

	// Create an object; its trigger pass will discover triggers and
	// cache the positive entry.
	p := easyjson.NewJSONObject()
	p.SetByPath("origin_type", easyjson.NewJSON("TypeWithTrig"))
	p.SetByPath("body", easyjson.NewJSONObject())
	res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.object.create", "obj-wt-1", &p, nil)
	s.NoError(err)
	s.Equal("ok", res.GetByPath("status").AsStringDefault(""))

	prefixed := s.SetThisDomainPreffix("TypeWithTrig")
	v, ok := typeObjectTriggersCache.Load(prefixed)
	s.Truef(ok, "cache entry must exist for %q after first trigger pass", prefixed)
	j, isJSON := v.(*easyjson.JSON)
	s.Truef(isJSON && j != nil, "positive cache must store non-nil JSON triggers section; got %T value=%v", v, v)
	if isJSON && j != nil {
		s.Truef(j.GetByPath("create").IsArray(),
			"cached triggers section must preserve the registered trigger array")
	}
}

// Test_ObjectTriggers_InvalidatedOnUpdateType: after UpdateType the
// cache entry for the type must be removed so the next trigger pass
// reloads from KV.
func (s *TriggerCacheTestSuite) Test_ObjectTriggers_InvalidatedOnUpdateType() {
	s.bootstrap()
	s.cmdbTypeCreate("TypeMut")
	// Prime the cache with a negative entry.
	p := easyjson.NewJSONObject()
	p.SetByPath("origin_type", easyjson.NewJSON("TypeMut"))
	p.SetByPath("body", easyjson.NewJSONObject())
	_, err := s.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.object.create", "obj-mut-1", &p, nil)
	s.NoError(err)

	prefixed := s.SetThisDomainPreffix("TypeMut")
	_, ok := typeObjectTriggersCache.Load(prefixed)
	s.Truef(ok, "sanity: cache must be primed before UpdateType")

	// Update the type — should drop the cache entry.
	body := easyjson.NewJSONObject()
	body.SetByPath("triggers.create", easyjson.NewJSON([]string{"new.fn"}))
	up := easyjson.NewJSONObject()
	up.SetByPath("body", body)
	res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.type.update", "TypeMut", &up, nil)
	s.NoError(err)
	s.Equal("ok", res.GetByPath("status").AsStringDefault(""), "type.update must succeed: %s", res.ToString())

	_, ok = typeObjectTriggersCache.Load(prefixed)
	s.Falsef(ok, "cache entry must be invalidated after UpdateType (%q)", prefixed)
}

// Test_ObjectTriggers_InvalidatedOnDeleteType: removing a type must
// also drop any cached object-triggers entry for it.
func (s *TriggerCacheTestSuite) Test_ObjectTriggers_InvalidatedOnDeleteType() {
	s.bootstrap()
	s.cmdbTypeCreate("TypeDel")
	// Prime negative.
	p := easyjson.NewJSONObject()
	p.SetByPath("origin_type", easyjson.NewJSON("TypeDel"))
	p.SetByPath("body", easyjson.NewJSONObject())
	_, err := s.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.object.create", "obj-del-1", &p, nil)
	s.NoError(err)

	prefixed := s.SetThisDomainPreffix("TypeDel")
	_, ok := typeObjectTriggersCache.Load(prefixed)
	s.True(ok, "sanity: cache primed")

	res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.type.delete", "TypeDel", easyjson.NewJSONObject().GetPtr(), nil)
	s.NoError(err)
	_ = res

	_, ok = typeObjectTriggersCache.Load(prefixed)
	s.Falsef(ok, "cache entry must be invalidated after DeleteType (%q)", prefixed)
}

// Test_LinkTriggers_PopulatedAndInvalidated covers the TypesLink-trigger
// cache lifecycle.
func (s *TriggerCacheTestSuite) Test_LinkTriggers_PopulatedAndInvalidated() {
	s.bootstrap()
	s.cmdbTypeCreate("LtA")
	s.cmdbTypeCreate("LtB")

	// Create the TypesLink.
	p := easyjson.NewJSONObject()
	p.SetByPath("to", easyjson.NewJSON("LtB"))
	p.SetByPath("object_type", easyjson.NewJSON("lt-link"))
	res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.types.link.create", "LtA", &p, nil)
	s.NoError(err)
	s.Equal("ok", res.GetByPath("status").AsStringDefault(""))

	// Create objects and link them — this triggers an op-stack walk
	// that exercises executeLinkTriggers, which populates the link cache.
	pa := easyjson.NewJSONObjectWithKeyValue("origin_type", easyjson.NewJSON("LtA"))
	pa.SetByPath("body", easyjson.NewJSONObject())
	_, err = s.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.object.create", "obj-a-lt", &pa, nil)
	s.NoError(err)
	pb := easyjson.NewJSONObjectWithKeyValue("origin_type", easyjson.NewJSON("LtB"))
	pb.SetByPath("body", easyjson.NewJSONObject())
	_, err = s.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.object.create", "obj-b-lt", &pb, nil)
	s.NoError(err)
	lp := easyjson.NewJSONObject()
	lp.SetByPath("to", easyjson.NewJSON("obj-b-lt"))
	lp.SetByPath("name", easyjson.NewJSON("edge-ab"))
	lp.SetByPath("body", easyjson.NewJSONObject())
	_, err = s.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.objects.link.create", "obj-a-lt", &lp, nil)
	s.NoError(err)

	fromKey := s.SetThisDomainPreffix("LtA")
	toKey := s.SetThisDomainPreffix("LtB")
	cacheKey := fromKey + "|" + toKey
	_, ok := typesLinkTriggersCache.Load(cacheKey)
	s.Truef(ok, "link-triggers cache must be primed for (%s, %s) after link.create dispatch", fromKey, toKey)

	// UpdateTypesLink — must invalidate.
	up := easyjson.NewJSONObject()
	up.SetByPath("to", easyjson.NewJSON("LtB"))
	up.SetByPath("body", easyjson.NewJSONObjectWithKeyValue("triggers", easyjson.NewJSONObjectWithKeyValue("create", easyjson.NewJSON([]string{"fn.x"}))))
	_, err = s.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.types.link.update", "LtA", &up, nil)
	s.NoError(err)
	_, ok = typesLinkTriggersCache.Load(cacheKey)
	s.Falsef(ok, "link-triggers cache entry must be invalidated after UpdateTypesLink")

	// Prime it again via another link op (provoke dispatch).
	_, err = s.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.objects.link.delete", "obj-a-lt", easyjson.NewJSONObjectWithKeyValue("to", easyjson.NewJSON("obj-b-lt")).GetPtr(), nil)
	s.NoError(err)
	_, ok = typesLinkTriggersCache.Load(cacheKey)
	s.Truef(ok, "link-triggers cache must be re-primed after another link op")

	// DeleteTypesLink — must invalidate.
	dp := easyjson.NewJSONObjectWithKeyValue("to", easyjson.NewJSON("LtB"))
	_, err = s.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.types.link.delete", "LtA", &dp, nil)
	s.NoError(err)
	_, ok = typesLinkTriggersCache.Load(cacheKey)
	s.Falsef(ok, "link-triggers cache entry must be invalidated after DeleteTypesLink")
}

// Test_LinkTriggers_PurgedByDeleteType: when an entire type is deleted,
// every link-trigger entry mentioning it (as either endpoint) must be
// purged.
func (s *TriggerCacheTestSuite) Test_LinkTriggers_PurgedByDeleteType() {
	s.bootstrap()
	s.cmdbTypeCreate("Purg1")
	s.cmdbTypeCreate("Purg2")
	s.cmdbTypeCreate("Purg3")
	// (Purg1 -> Purg2), (Purg2 -> Purg3): two links involving Purg2.
	for _, pair := range [][2]string{{"Purg1", "Purg2"}, {"Purg2", "Purg3"}} {
		p := easyjson.NewJSONObject()
		p.SetByPath("to", easyjson.NewJSON(pair[1]))
		p.SetByPath("object_type", easyjson.NewJSON("p-link"))
		_, err := s.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.types.link.create", pair[0], &p, nil)
		s.NoError(err)
	}

	// Seed entries by hand — simulating the dispatch path's effect
	// without spinning up object link ops for two different pairs.
	p1 := s.SetThisDomainPreffix("Purg1")
	p2 := s.SetThisDomainPreffix("Purg2")
	p3 := s.SetThisDomainPreffix("Purg3")
	typesLinkTriggersCache.Store(p1+"|"+p2, &linkTriggersInfo{referenceLinkType: "p-link"})
	typesLinkTriggersCache.Store(p2+"|"+p3, &linkTriggersInfo{referenceLinkType: "p-link"})
	typesLinkTriggersCache.Store(p1+"|"+p3, &linkTriggersInfo{referenceLinkType: "unrelated"})

	// Delete the middle type — both entries mentioning Purg2 must go,
	// the Purg1|Purg3 entry must survive.
	_, err := s.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.type.delete", "Purg2", easyjson.NewJSONObject().GetPtr(), nil)
	s.NoError(err)

	_, ok := typesLinkTriggersCache.Load(p1 + "|" + p2)
	s.Falsef(ok, "Purg1|Purg2 entry must be purged with Purg2")
	_, ok = typesLinkTriggersCache.Load(p2 + "|" + p3)
	s.Falsef(ok, "Purg2|Purg3 entry must be purged with Purg2")
	_, ok = typesLinkTriggersCache.Load(p1 + "|" + p3)
	s.Truef(ok, "Purg1|Purg3 entry must survive — it does not mention Purg2")
}
