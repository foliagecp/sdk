package crud_test

// What a link-delete trigger carries.
//
// A consumer of link-delete triggers learns from old_body what the link held
// at the moment it went: a membership tracker reads it to record which member
// left and with which data. The body must be there however
// the link is removed — by deleting the link itself, by deleting the object it
// leaves from, and by deleting the object it points to. The last one runs the
// incoming-link cascade, which once dropped the body before the op-stack entry
// was written, so the trigger fired with no old_body and the consumer threw the
// event away as malformed.
//
// Checked for both trigger families: the per-type trigger on the types-link
// (trigger.link.delete) and the global meta trigger (trigger.object_link.delete).

import (
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

type linkDeleteEvent struct {
	path string // trigger.link.delete | trigger.object_link.delete
	from string
	data easyjson.JSON
}

type TriggerLinkDeleteBodyTestSuite struct {
	test.StatefunTestSuite
	dbc   db.DBSyncClient
	fired chan linkDeleteEvent
}

func TestTriggerLinkDeleteBodyTestSuite(t *testing.T) {
	suite.Run(t, new(TriggerLinkDeleteBodyTestSuite))
}

const (
	ldPerType   = "test.linkdel.pertype"
	ldMeta      = "test.linkdel.meta"
	ldFromType  = "ld-from"
	ldToType    = "ld-to"
	ldLinkType  = "ld-rel"
	ldPathType  = "trigger.link.delete"
	ldPathMeta  = "trigger.object_link.delete"
	ldBodyKey   = "member"
	ldBodyValue = "kept"
)

func (s *TriggerLinkDeleteBodyTestSuite) bootstrap() {
	s.fired = make(chan linkDeleteEvent, 64)
	crud.RegisterAllFunctionTypes(s.Runtime())
	cfg := *statefun.NewFunctionTypeConfig().
		SetAllowedSignalProviders(sfPlugins.AutoSignalSelect).
		SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).
		SetMaxIdHandlers(-1)
	for name, path := range map[string]string{ldPerType: ldPathType, ldMeta: ldPathMeta} {
		path := path
		s.RegisterFunction(name, func(_ sfPlugins.StatefunExecutor, ctx *sfPlugins.StatefunContextProcessor) {
			if !ctx.Payload.PathExists(path) {
				return
			}
			select {
			case s.fired <- linkDeleteEvent{path: path, from: ctx.Self.ID, data: ctx.Payload.GetByPath(path).Clone()}:
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

	s.NoError(s.dbc.CMDB.TypeCreate(ldFromType))
	s.NoError(s.dbc.CMDB.TypeCreate(ldToType))
	s.NoError(s.dbc.CMDB.TypesLinkCreate(ldFromType, ldToType, ldLinkType, nil))
	s.NoError(s.dbc.CMDB.TriggerLinkSet(ldFromType, ldToType, db.DeleteTrigger, ldPerType))
	s.NoError(s.dbc.CMDB.MetaTriggerObjectLinkSet(db.DeleteTrigger, ldMeta))
}

// link creates from -> to with a body the delete triggers must hand back.
func (s *TriggerLinkDeleteBodyTestSuite) link(from, to string) {
	s.NoError(s.dbc.CMDB.ObjectCreate(from, ldFromType))
	s.NoError(s.dbc.CMDB.ObjectCreate(to, ldToType))
	s.NoError(s.dbc.CMDB.ObjectsLinkCreate(from, to, "edge", nil, easyjson.NewJSONObjectWithKeyValue(ldBodyKey, easyjson.NewJSON(ldBodyValue))))
	s.drain()
}

func (s *TriggerLinkDeleteBodyTestSuite) drain() {
	for {
		select {
		case <-s.fired:
		case <-time.After(150 * time.Millisecond):
			return
		}
	}
}

// expectBody waits for one delete event of each family on the from -> to link
// and checks that both carry the body the link held.
func (s *TriggerLinkDeleteBodyTestSuite) expectBody(from, to string) {
	want := map[string]bool{ldPathType: true, ldPathMeta: true}
	deadline := time.After(5 * time.Second)
	for len(want) > 0 {
		select {
		case e := <-s.fired:
			if e.data.GetByPath("type").AsStringDefault("") != ldLinkType || !want[e.path] {
				continue
			}
			s.Contains(e.from, from, "%s: fired on the link owner", e.path)
			s.Contains(e.data.GetByPath("to").AsStringDefault(""), to, "%s: names the target", e.path)
			s.Truef(e.data.GetByPath("old_body").IsObject(), "%s: old_body is missing: %s", e.path, e.data.ToString())
			s.Equal(ldBodyValue, e.data.GetByPath("old_body."+ldBodyKey).AsStringDefault(""), "%s: old_body is the link's body", e.path)
			delete(want, e.path)
		case <-deadline:
			s.Failf("link-delete triggers did not fire", "still waiting for %v on %s -> %s", want, from, to)
			return
		}
	}
}

func (s *TriggerLinkDeleteBodyTestSuite) Test_LinkDeleted() {
	s.bootstrap()
	s.link("ld-a1", "ld-b1")
	s.NoError(s.dbc.CMDB.ObjectsLinkDelete("ld-a1", "ld-b1"))
	s.expectBody("ld-a1", "ld-b1")
}

func (s *TriggerLinkDeleteBodyTestSuite) Test_LinkOwnerDeleted() {
	s.bootstrap()
	s.link("ld-a2", "ld-b2")
	s.NoError(s.dbc.CMDB.ObjectDelete("ld-a2"))
	s.expectBody("ld-a2", "ld-b2")
}

func (s *TriggerLinkDeleteBodyTestSuite) Test_LinkTargetDeleted() {
	s.bootstrap()
	s.link("ld-a3", "ld-b3")
	s.NoError(s.dbc.CMDB.ObjectDelete("ld-b3"))
	s.expectBody("ld-a3", "ld-b3")
}
