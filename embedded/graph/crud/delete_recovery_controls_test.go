package crud_test

import (
	"fmt"
	"strings"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/embedded/graph/crud"
	"github.com/foliagecp/sdk/statefun"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
)

func (s *TrashCanTestSuite) Test_VertexDelete_StaleIncomingReferencePreservesNewTarget() {
	s.boot()
	for _, id := range []string{"edge-owner", "old-target", "new-target"} {
		_, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.create", id, easyjson.NewJSONObject().GetPtr(), nil)
		s.Require().NoError(err)
	}
	owner, oldTarget, newTarget := s.SetThisDomainPreffix("edge-owner"), s.SetThisDomainPreffix("old-target"), s.SetThisDomainPreffix("new-target")
	payload := easyjson.NewJSONObjectWithKeyValue("name", easyjson.NewJSON("reused-name"))
	payload.SetByPath("type", easyjson.NewJSON("relation"))
	payload.SetByPath("to", easyjson.NewJSON(newTarget))
	_, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.link.create", "edge-owner", &payload, nil)
	s.Require().NoError(err)
	c := s.Runtime().Domain.Cache()
	now := system.GetCurrentTimeNs()
	c.SetValue(fmt.Sprintf(crud.InLinkKeyPrefPattern+crud.KeySuff2Pattern, oldTarget, owner, "reused-name"), []byte("relation"), true, now)
	c.SetValue(fmt.Sprintf(crud.OutLinkTypeKeyPrefPattern+crud.KeySuff2Pattern, owner, "relation", oldTarget), []byte("reused-name"), true, now)
	response, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api.vertex.delete", "old-target", easyjson.NewJSONObject().GetPtr(), nil)
	s.Require().NoError(err)
	s.Equal("ok", response.GetByPath("status").AsStringDefault(""))
	s.False(c.ExistsJson(oldTarget))
	for _, key := range []string{
		fmt.Sprintf(crud.OutLinkTargetKeyPrefPattern+crud.KeySuff1Pattern, owner, "reused-name"),
		fmt.Sprintf(crud.OutLinkBodyKeyPrefPattern+crud.KeySuff1Pattern, owner, "reused-name"),
		fmt.Sprintf(crud.OutLinkTypeKeyPrefPattern+crud.KeySuff2Pattern, owner, "relation", newTarget),
		fmt.Sprintf(crud.InLinkKeyPrefPattern+crud.KeySuff2Pattern, newTarget, owner, "reused-name"),
	} {
		s.True(c.Exists(key), "the valid replacement edge must survive: %s", key)
	}
	s.False(c.Exists(fmt.Sprintf(crud.OutLinkTypeKeyPrefPattern+crud.KeySuff2Pattern, owner, "relation", oldTarget)))
}

// Inject one failed remote unlink after the local out-link cascade. The body
// is retained deliberately; a later explicit delete is the existing retry path.
func (s *TrashCanTestSuite) Test_VertexDelete_FailedUnlinkRetainsBodyAndCanBeRetried() {
	statefun.NewFunctionType(s.Runtime(), "test.vertex.delete.failure", func(executor sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
		copyContext := *ctx
		original := ctx.Request
		copyContext.Request = func(provider sfPlugins.RequestProvider, function, id string, payload, options *easyjson.JSON, timeout ...time.Duration) (*easyjson.JSON, error) {
			if function == "functions.graph.api.link.delete" && strings.HasPrefix(id, "remote/") {
				return sfMediators.OpMsgFailed("injected remote unlink failure").ToJson(), nil
			}
			return original(provider, function, id, payload, options, timeout...)
		}
		crud.LLAPIVertexDelete(executor, &copyContext)
	}, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect))
	s.boot()
	s.Require().NoError(s.cmdb.TypeCreate("interrupted-delete"))
	s.Require().NoError(s.cmdb.ObjectCreate("interrupted-object", "interrupted-delete", easyjson.NewJSONObject()))
	c := s.Runtime().Domain.Cache()
	id := s.SetThisDomainPreffix("interrupted-object")
	remoteKey := fmt.Sprintf(crud.InLinkKeyPrefPattern+crud.KeySuff2Pattern, id, "remote/source", "remote-edge")
	c.SetValue(remoteKey, []byte("relation"), true, system.GetCurrentTimeNs())
	response, err := s.Request(sfPlugins.AutoRequestSelect, "test.vertex.delete.failure", "interrupted-object", easyjson.NewJSONObject().GetPtr(), nil)
	s.Require().NoError(err)
	s.Equal("failed", response.GetByPath("status").AsStringDefault(""))
	s.True(c.ExistsJson(id), "failed unlinking must not erase the remaining body")
	s.False(c.Exists(fmt.Sprintf(crud.OutLinkTargetKeyPrefPattern+crud.KeySuff1Pattern, id, "type")), "the local cascade was reached before the injected failure")
	// Model the remote reference being resolved before retrying the operation.
	c.DeleteValue(remoteKey, true, system.GetCurrentTimeNs())
	s.Equal("ok", s.objectOp("object.delete", "interrupted-object", easyjson.NewJSONObject()))
	s.False(c.ExistsJson(id), "an explicit retry must clean the typeless body")
}

func (s *TrashCanTestSuite) Test_LinkDelete_ExpectedOldTargetPreservesReplacement() {
	s.boot()
	for _, id := range []string{"expected-owner", "expected-old", "expected-new"} {
		s.Equal("ok", s.llOp("vertex.create", id, easyjson.NewJSONObject()))
	}
	owner, oldTarget, newTarget := s.SetThisDomainPreffix("expected-owner"), s.SetThisDomainPreffix("expected-old"), s.SetThisDomainPreffix("expected-new")
	payload := easyjson.NewJSONObjectWithKeyValue("to", easyjson.NewJSON(newTarget))
	payload.SetByPath("type", easyjson.NewJSON("relation"))
	payload.SetByPath("name", easyjson.NewJSON("reused"))
	s.Equal("ok", s.llOp("link.create", "expected-owner", payload))
	c := s.Runtime().Domain.Cache()
	stalePair := fmt.Sprintf(crud.OutLinkTypeKeyPrefPattern+crud.KeySuff2Pattern, owner, "relation", oldTarget)
	c.SetValue(stalePair, []byte("reused"), true, system.GetCurrentTimeNs())
	request := easyjson.NewJSONObjectWithKeyValue("name", easyjson.NewJSON("reused"))
	request.SetByPath("_delete_expected_to", easyjson.NewJSON(oldTarget))
	request.SetByPath("_delete_expected_kind", easyjson.NewJSON("relation"))
	s.Equal("idle", s.llOp("link.delete", "expected-owner", request))
	s.False(c.Exists(stalePair))
	for _, key := range linkKeys(owner, newTarget, "reused", "relation") {
		s.True(c.Exists(key), "the replacement edge must remain intact: %s", key)
	}
}

func (s *TrashCanTestSuite) Test_LinkDelete_RemoteFailurePreservesOwnerForRetry() {
	statefun.NewFunctionType(s.Runtime(), "test.link.delete.failure", func(executor sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
		copyContext := *ctx
		copyContext.Request = func(_ sfPlugins.RequestProvider, _, _ string, _, _ *easyjson.JSON, _ ...time.Duration) (*easyjson.JSON, error) {
			return sfMediators.OpMsgFailed("injected remote unlink failure").ToJson(), nil
		}
		crud.LLAPILinkDelete(executor, &copyContext)
	}, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect))
	s.boot()
	s.Equal("ok", s.llOp("vertex.create", "remote-owner", easyjson.NewJSONObject()))
	owner := s.SetThisDomainPreffix("remote-owner")
	s.seedTypeMember(owner, "remote/target", "remote-link")
	c := s.Runtime().Domain.Cache()
	keys := c.GetKeysByPattern(owner + ".>")
	response, err := s.Request(sfPlugins.AutoRequestSelect, "test.link.delete.failure", "remote-owner", easyjson.NewJSONObjectWithKeyValue("name", easyjson.NewJSON("remote-link")).GetPtr(), nil)
	s.Require().NoError(err)
	s.Equal("failed", response.GetByPath("status").AsStringDefault(""))
	for _, key := range keys {
		s.True(c.Exists(key), "a remote refusal must retain the owner's endpoint information: %s", key)
	}
}
