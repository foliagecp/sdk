package crud_test

// Regression: UpdateTypesLink invoked on a SALTED id (`<type>===<hash>`, the
// sequence-free parallelization suffix) passed the raw salted ctx.Self.ID into
// getObjectsLinkTypeFromTypesLink. The lookup still succeeds (getLinkBody
// routes through link.read, which strips the salt) — but on success the
// helper caches the result via cacheSetTypeEdge under the SALTED fromType.
// cachePurgeTypeEdgesForType/cacheDeleteTypeEdge only ever address the clean
// type name, so every uniquely-salted upsert-update planted one PERMANENT
// entry in the process-global type-edge cache. A salted invocation must
// behave exactly like a clean one: same result, no new cache entries.

import (
	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/embedded/graph/crud"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
)

func (s *CMDBClientContractTestSuite) Test_TypesLinkUpdate_SaltedID_PreservesObjectLinkType() {
	s.bootstrap()
	s.NoError(s.dbc.CMDB.TypeCreate("SltA"))
	s.NoError(s.dbc.CMDB.TypeCreate("SltB"))
	s.NoError(s.dbc.CMDB.TypesLinkCreate("SltA", "SltB", "rel", nil))

	before, err := s.dbc.CMDB.TypesLinkRead("SltA", "SltB")
	s.NoError(err)
	objectLinkType := before.GetByPath("body.type").AsStringDefault("")
	s.NotEmpty(objectLinkType, "sanity: the created types link must carry its objects-link type")

	edgeCacheBefore := crud.TypeEdgeCacheSizeForTest()

	// Upsert-update through a SALTED self id — exactly the shape a
	// sequence-free parent-based dispatch produces.
	payload := easyjson.NewJSONObject()
	payload.SetByPath("to", easyjson.NewJSON("SltB"))
	payload.SetByPath("upsert", easyjson.NewJSON(true))
	payload.SetByPath("body.weight", easyjson.NewJSON(1))
	reply, err := s.Runtime().Request(sfPlugins.AutoRequestSelect, "functions.cmdb.api.types.link.update", "SltA===aaaa1111", &payload, nil)
	om := sfMediators.OpMsgFromSfReply(reply, err)
	s.Equalf(sfMediators.SYNC_OP_STATUS_OK, om.Status, "salted types.link.update failed: %s", om.Details)

	after, err := s.dbc.CMDB.TypesLinkRead("SltA", "SltB")
	s.NoError(err)
	s.Equalf(objectLinkType, after.GetByPath("body.type").AsStringDefault(""),
		"salted upsert must preserve the existing objects-link type, not overwrite it with the target type name (body: %s)", after.ToString())
	s.Equal(float64(1), after.GetByPath("body.weight").AsNumericDefault(0), "the update itself must still apply")

	// The leak pin: a salted invocation must not plant a new outer entry in
	// the process-global type-edge cache — the clean-name purge paths can
	// never remove a salted key.
	s.Equalf(edgeCacheBefore, crud.TypeEdgeCacheSizeForTest(),
		"salted types.link.update planted a permanent type-edge cache entry under the salted fromType")
}
