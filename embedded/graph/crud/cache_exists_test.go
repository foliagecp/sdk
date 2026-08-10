// Package crud — integration tests for cache.Exists / cache.ExistsJson.
//
// These methods are existence-only probes that avoid the Clone (for JSON
// values) or ToBytes (for byte values) overhead of GetValueJSON/GetValue
// when the caller only needs to know "is there something at this key?".
// They are used pervasively in HL/LL CRUD probe sites (LLAPIVertexCreate,
// LLAPIVertexUpdate, LLAPIVertexDelete, LLAPILinkCreate, DeleteObject /
// UpdateObject orphan probes) and the contract pinned here is what those
// call sites rely on.
//
// We exercise the methods against a real cache populated through the
// CRUD pipeline rather than a synthetic store: this catches any future
// regression where the cache's value-type tagging diverges from how
// CRUD writers tag values.
package crud

import (
	"testing"
	"time"

	"github.com/foliagecp/easyjson"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/test"
	"github.com/stretchr/testify/suite"
)

type CacheExistsTestSuite struct {
	test.StatefunTestSuite
}

func TestCacheExistsTestSuite(t *testing.T) {
	suite.Run(t, new(CacheExistsTestSuite))
}

func (s *CacheExistsTestSuite) bootstrap() {
	RegisterAllFunctionTypes(s.Runtime())
	s.NoError(s.StartRuntime())
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := s.CacheValue(BUILT_IN_OBJECTS); err == nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	s.T().Fatalf("built-in vertices did not appear within 5s")
}

// Test_ExistsJson_TrueForVertexBody — happy path on JSON-typed value.
// CMDB object creation writes the vertex body via SetValueJSON, so
// ExistsJson must return true for the domain-prefixed key.
func (s *CacheExistsTestSuite) Test_ExistsJson_TrueForVertexBody() {
	s.bootstrap()
	payload := easyjson.NewJSONObjectWithKeyValue("body", easyjson.NewJSONObject())
	_, err := s.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.type.create", "TypeXJ", &payload, nil)
	s.NoError(err)
	payload = easyjson.NewJSONObject()
	payload.SetByPath("origin_type", easyjson.NewJSON("TypeXJ"))
	payload.SetByPath("body", easyjson.NewJSONObject())
	_, err = s.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.object.create", "obj-xj", &payload, nil)
	s.NoError(err)

	prefixed := s.SetThisDomainPreffix("obj-xj")
	s.Truef(s.Runtime().Domain.Cache().ExistsJson(prefixed),
		"ExistsJson must return true for an existing vertex body (key=%s)", prefixed)
}

// Test_ExistsJson_FalseForMissingKey — missing key must not be a positive
// existence report. This pins down the contract that LL/HL probes rely
// on: "vertex does not exist" must be observable.
func (s *CacheExistsTestSuite) Test_ExistsJson_FalseForMissingKey() {
	s.bootstrap()
	s.Falsef(s.Runtime().Domain.Cache().ExistsJson(s.SetThisDomainPreffix("nope")),
		"ExistsJson must return false for a missing key")
}

// Test_Exists_TrueForByteValue — happy path on byte-typed value.
// CMDB link creation writes link-target metadata (e.g. OutLinkTargetKey)
// as []byte via SetValue, so Exists must return true for those keys.
func (s *CacheExistsTestSuite) Test_Exists_TrueForByteValue() {
	s.bootstrap()
	// Set up a type so we can create an object whose CMDB structural
	// links populate byte-typed KV entries.
	p := easyjson.NewJSONObjectWithKeyValue("body", easyjson.NewJSONObject())
	_, err := s.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.type.create", "TypeXB", &p, nil)
	s.NoError(err)
	p = easyjson.NewJSONObject()
	p.SetByPath("origin_type", easyjson.NewJSON("TypeXB"))
	_, err = s.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.object.create", "obj-xb", &p, nil)
	s.NoError(err)

	// The __type out-link is stored as "<id>.out.to.type" with value
	// "__type.<typeID>" — a []byte value. Use a manually-built key to
	// probe.
	prefixed := s.SetThisDomainPreffix("obj-xb")
	key := prefixed + ".out.to.type"
	s.Truef(s.Runtime().Domain.Cache().Exists(key),
		"Exists must return true for a byte-typed value (key=%s)", key)
}

// Test_Exists_FalseForMissingKey — same as above but for byte API.
func (s *CacheExistsTestSuite) Test_Exists_FalseForMissingKey() {
	s.bootstrap()
	s.Falsef(s.Runtime().Domain.Cache().Exists("absent.key.nothing"),
		"Exists must return false for a missing key")
}

// Test_ExistsJson_OnByteValue_StillReportsExistence pins down the
// type-affinity contract: ExistsJson on a byte-typed value still
// reports true (the entry does exist), it just logs a WarnLevel pointer
// to the right API. The log assertion is omitted — capturing logger
// output requires hooks we don't want to introduce — but the boolean
// behavior is what callers rely on.
func (s *CacheExistsTestSuite) Test_ExistsJson_OnByteValue_StillReportsExistence() {
	s.bootstrap()
	p := easyjson.NewJSONObjectWithKeyValue("body", easyjson.NewJSONObject())
	_, err := s.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.type.create", "TypeXM", &p, nil)
	s.NoError(err)
	p = easyjson.NewJSONObject()
	p.SetByPath("origin_type", easyjson.NewJSON("TypeXM"))
	_, err = s.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.object.create", "obj-xm", &p, nil)
	s.NoError(err)

	prefixed := s.SetThisDomainPreffix("obj-xm")
	byteKey := prefixed + ".out.to.type" // byte-typed
	s.Truef(s.Runtime().Domain.Cache().ExistsJson(byteKey),
		"ExistsJson on a byte-typed value must still return true; got false for key=%s", byteKey)
}

// Test_Exists_OnJsonValue_StillReportsExistence — symmetric to the
// previous case.
func (s *CacheExistsTestSuite) Test_Exists_OnJsonValue_StillReportsExistence() {
	s.bootstrap()
	p := easyjson.NewJSONObjectWithKeyValue("body", easyjson.NewJSONObject())
	_, err := s.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.type.create", "TypeXR", &p, nil)
	s.NoError(err)
	p = easyjson.NewJSONObject()
	p.SetByPath("origin_type", easyjson.NewJSON("TypeXR"))
	_, err = s.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.object.create", "obj-xr", &p, nil)
	s.NoError(err)

	prefixed := s.SetThisDomainPreffix("obj-xr") // JSON-typed body
	s.Truef(s.Runtime().Domain.Cache().Exists(prefixed),
		"Exists on a JSON-typed value must still return true; got false for key=%s", prefixed)
}

// Test_ExistsJson_FalseAfterDelete — after the vertex is removed via
// LL vertex.delete, ExistsJson must transition to false. This is the
// invariant the LLAPIVertexDelete idempotency check relies on.
func (s *CacheExistsTestSuite) Test_ExistsJson_FalseAfterDelete() {
	s.bootstrap()
	p := easyjson.NewJSONObjectWithKeyValue("body", easyjson.NewJSONObject())
	_, err := s.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.type.create", "TypeXD", &p, nil)
	s.NoError(err)
	p = easyjson.NewJSONObject()
	p.SetByPath("origin_type", easyjson.NewJSON("TypeXD"))
	_, err = s.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.object.create", "obj-xd", &p, nil)
	s.NoError(err)

	prefixed := s.SetThisDomainPreffix("obj-xd")
	s.True(s.Runtime().Domain.Cache().ExistsJson(prefixed), "sanity: must exist before delete")

	// Trash-can contract: the FIRST delete of a live object PARKS it (body
	// kept), the SECOND delete of the parked object erases it physically.
	_, err = s.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.object.delete", "obj-xd", easyjson.NewJSONObject().GetPtr(), nil)
	s.NoError(err)
	s.Truef(s.Runtime().Domain.Cache().ExistsJson(prefixed),
		"ExistsJson must still be true after the first delete — the object is parked in the trash can (key=%s)", prefixed)

	_, err = s.Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.object.delete", "obj-xd", easyjson.NewJSONObject().GetPtr(), nil)
	s.NoError(err)
	s.Falsef(s.Runtime().Domain.Cache().ExistsJson(prefixed),
		"ExistsJson must return false after the parked object is deleted physically (key=%s)", prefixed)
}
