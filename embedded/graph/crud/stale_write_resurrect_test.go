package crud_test

// A late write must not bring back what a newer delete removed — before a
// reload of the cache from KV, and after it.
//
// The last-writer-wins guard in the cache refuses a write older than the
// key's last change, and a deleted key keeps its delete time as a tombstone
// for exactly this reason. The tombstone lives only in memory: KV keeps no
// times and its delete markers are purged, so a load from KV — a restart, or
// the passive→active rehydrate — knows nothing of the delete, and the late
// write it should have refused is applied.

import (
	"fmt"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/embedded/graph/crud"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/system"
)

func (s *TrashCanTestSuite) llOp(fn, id string, payload easyjson.JSON) string {
	res, err := s.Request(sfPlugins.AutoRequestSelect, "functions.graph.api."+fn, id, &payload, nil)
	s.Require().NoError(err)
	return res.GetByPath("status").AsStringDefault("")
}

func withOpTime(payload easyjson.JSON, opTime int64) easyjson.JSON {
	payload.SetByPath("op_time", easyjson.NewJSON(opTime))
	return payload
}

// linkKeys lists the keys one link from→to is made of.
func linkKeys(from, to, name, linkType string) []string {
	return []string{
		fmt.Sprintf(crud.OutLinkTargetKeyPrefPattern+crud.KeySuff1Pattern, from, name),
		fmt.Sprintf(crud.OutLinkBodyKeyPrefPattern+crud.KeySuff1Pattern, from, name),
		fmt.Sprintf(crud.OutLinkTypeKeyPrefPattern+crud.KeySuff2Pattern, from, linkType, to),
		fmt.Sprintf(crud.InLinkKeyPrefPattern+crud.KeySuff2Pattern, to, from, name),
	}
}

func (s *TrashCanTestSuite) liveKeys(keys []string) []string {
	var live []string
	for _, k := range keys {
		if s.Runtime().Domain.Cache().GetValueUpdateTime(k) > 0 {
			live = append(live, k)
		}
	}
	return live
}

// staleLinkCreateAfterDelete deletes a link at tDel and then replays its
// create stamped tDel-1, reloading the cache from KV in between if asked.
// It returns the keys of the link that are live afterwards.
func (s *TrashCanTestSuite) staleLinkCreateAfterDelete(prefix string, reload bool) []string {
	s.boot()
	a, b := prefix+"-a", prefix+"-b"
	s.Equal("ok", s.llOp("vertex.create", a, easyjson.NewJSONObject()))
	s.Equal("ok", s.llOp("vertex.create", b, easyjson.NewJSONObject()))
	aID, bID := s.SetThisDomainPreffix(a), s.SetThisDomainPreffix(b)

	create := easyjson.NewJSONObject()
	create.SetByPath("to", easyjson.NewJSON(bID))
	create.SetByPath("name", easyjson.NewJSON("rz-edge"))
	create.SetByPath("type", easyjson.NewJSON("rz-type"))
	s.Equal("ok", s.llOp("link.create", a, create))
	keys := linkKeys(aID, bID, "rz-edge", "rz-type")
	s.Require().Len(s.liveKeys(keys), len(keys), "the link must be there before the delete")

	tDel := system.GetCurrentTimeNs()
	del := easyjson.NewJSONObjectWithKeyValue("name", easyjson.NewJSON("rz-edge"))
	s.Equal("ok", s.llOp("link.delete", a, withOpTime(del, tDel)))
	s.Require().Empty(s.liveKeys(keys), "the delete must remove the link")

	if reload {
		s.reloadFromKV()
	}

	status := s.llOp("link.create", a, withOpTime(create.Clone(), tDel-1))
	live := s.liveKeys(keys)
	s.T().Logf("reload=%v: stale link.create status=%s, live keys=%d: %v", reload, status, len(live), live)
	return live
}

// Control: with the tombstones in memory the guard refuses the stale create.
func (s *TrashCanTestSuite) Test_StaleWrite_LinkNotResurrectedWithoutReload() {
	s.Empty(s.staleLinkCreateAfterDelete("rz1", false),
		"a create older than the delete must not bring the link back")
}

// The same stale create after the cache was reloaded from KV.
func (s *TrashCanTestSuite) Test_StaleWrite_LinkNotResurrectedAfterReload() {
	s.Empty(s.staleLinkCreateAfterDelete("rz2", true),
		"after a reload from KV a create older than the delete brought the link back")
}

// The body: a vertex deleted at tDel, a create stamped tDel-1 after a reload.
func (s *TrashCanTestSuite) Test_StaleWrite_VertexNotResurrectedAfterReload() {
	for _, reload := range []bool{false, true} {
		s.Run(fmt.Sprintf("reload=%v", reload), func() {
			if !reload {
				s.boot()
			}
			id := fmt.Sprintf("rz-v-%v", reload)
			body := easyjson.NewJSONObjectWithKeyValue("v", easyjson.NewJSON(1))
			s.Equal("ok", s.llOp("vertex.create", id, easyjson.NewJSONObjectWithKeyValue("body", body)))
			fullID := s.SetThisDomainPreffix(id)

			tDel := system.GetCurrentTimeNs()
			s.Equal("ok", s.llOp("vertex.delete", id, withOpTime(easyjson.NewJSONObject(), tDel)))
			s.Require().False(s.Runtime().Domain.Cache().ExistsJson(fullID), "the delete must remove the body")

			if reload {
				s.reloadFromKV()
			}

			status := s.llOp("vertex.create", id,
				withOpTime(easyjson.NewJSONObjectWithKeyValue("body", body), tDel-1))
			back := s.Runtime().Domain.Cache().ExistsJson(fullID)
			s.T().Logf("reload=%v: stale vertex.create status=%s, body back=%v", reload, status, back)
			s.False(back, "a create older than the delete must not bring the body back")
		})
	}
}
