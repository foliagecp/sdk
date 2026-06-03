package crud_test

import (
	"testing"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/clients/go/db"
	"github.com/foliagecp/sdk/embedded/graph/crud"
	"github.com/foliagecp/sdk/statefun/test"
	"github.com/stretchr/testify/suite"
)

// ReadObjectNameTestSuite pins the human-readable name contract of
// functions.cmdb.api.object.read (ReadObject): the returned "name" is the
// value at the body path the object's type declares
// (body.human_readable_name_field), or a stable auto-generated fallback
// "<last token of the type name>_<first 6 chars of the object uuid>_auto".
type ReadObjectNameTestSuite struct {
	test.StatefunTestSuite
	dbc db.DBSyncClient
}

func TestReadObjectNameTestSuite(t *testing.T) {
	suite.Run(t, new(ReadObjectNameTestSuite))
}

func (s *ReadObjectNameTestSuite) bootstrap() {
	crud.RegisterAllFunctionTypes(s.Runtime())
	s.NoError(s.StartRuntime())
	s.waitForVertex(crud.BUILT_IN_TYPES, 15*time.Second)
	s.waitForVertex(crud.BUILT_IN_OBJECTS, 15*time.Second)

	dbc, err := db.NewDBSyncClientFromRequestFunction(s.Runtime().Request)
	s.NoError(err)
	s.dbc = dbc
}

func (s *ReadObjectNameTestSuite) waitForVertex(id string, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := s.CacheValue(id); err == nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	s.T().Fatalf("vertex %q did not appear within %s", id, timeout)
}

func (s *ReadObjectNameTestSuite) readName(objectID string) string {
	data, err := s.dbc.CMDB.ObjectRead(objectID)
	s.NoError(err)
	return data.GetByPath("name").AsStringDefault("")
}

func typeBodyWithNameField(path string) easyjson.JSON {
	return easyjson.NewJSONObjectWithKeyValue("human_readable_name_field", easyjson.NewJSON(path))
}

// The name is taken from the object's body at the path the type declares.
func (s *ReadObjectNameTestSuite) Test_NameFromTypeDeclaredField() {
	s.bootstrap()
	s.NoError(s.dbc.CMDB.TypeCreate("srv", typeBodyWithNameField("display_name")))

	body := easyjson.NewJSONObjectWithKeyValue("display_name", easyjson.NewJSON("WebServer-01"))
	s.NoError(s.dbc.CMDB.ObjectCreate("srvinstance", "srv", body))

	s.Equal("WebServer-01", s.readName("srvinstance"))
}

// The declared path may be nested inside the body.
func (s *ReadObjectNameTestSuite) Test_NameFromNestedPath() {
	s.bootstrap()
	s.NoError(s.dbc.CMDB.TypeCreate("doc", typeBodyWithNameField("meta.title")))

	body := easyjson.NewJSONObject()
	body.SetByPath("meta.title", easyjson.NewJSON("My Document"))
	s.NoError(s.dbc.CMDB.ObjectCreate("docinstance", "doc", body))

	s.Equal("My Document", s.readName("docinstance"))
}

// Type declares a path, but the object's body has nothing there -> fallback.
func (s *ReadObjectNameTestSuite) Test_AutoName_WhenBodyMissingValueAtPath() {
	s.bootstrap()
	s.NoError(s.dbc.CMDB.TypeCreate("srv", typeBodyWithNameField("display_name")))
	s.NoError(s.dbc.CMDB.ObjectCreate("abcdef123456", "srv", easyjson.NewJSONObject()))

	// last token of "srv" is "srv"; first 6 of "abcdef123456" is "abcdef".
	s.Equal("srv_abcdef_auto", s.readName("abcdef123456"))
}

// Type declares no human_readable_name_field at all -> fallback.
func (s *ReadObjectNameTestSuite) Test_AutoName_WhenTypeHasNoNameField() {
	s.bootstrap()
	s.NoError(s.dbc.CMDB.TypeCreate("rack")) // no body
	s.NoError(s.dbc.CMDB.ObjectCreate("zxcvbn000000", "rack", easyjson.NewJSONObject()))

	s.Equal("rack_zxcvbn_auto", s.readName("zxcvbn000000"))
}

// The type name's last token is taken splitting on both '_' and '-'.
func (s *ReadObjectNameTestSuite) Test_AutoName_TypeNameTokenization() {
	s.bootstrap()
	s.NoError(s.dbc.CMDB.TypeCreate("network-device_router"))
	s.NoError(s.dbc.CMDB.ObjectCreate("qwerty999999", "network-device_router", easyjson.NewJSONObject()))

	// last token of "network-device_router" (split on '_','-') is "router".
	s.Equal("router_qwerty_auto", s.readName("qwerty999999"))
}

// A non-string value at the declared path is not a valid name -> fallback.
func (s *ReadObjectNameTestSuite) Test_AutoName_WhenDeclaredValueNotString() {
	s.bootstrap()
	s.NoError(s.dbc.CMDB.TypeCreate("srv", typeBodyWithNameField("display_name")))

	body := easyjson.NewJSONObjectWithKeyValue("display_name", easyjson.NewJSON(42)) // number, not a string
	s.NoError(s.dbc.CMDB.ObjectCreate("nnnnnn111111", "srv", body))

	s.Equal("srv_nnnnnn_auto", s.readName("nnnnnn111111"))
}

// Updating the type's declared field must be observed on the next read
// (the per-type cache is invalidated by UpdateType).
func (s *ReadObjectNameTestSuite) Test_CacheInvalidatedOnTypeUpdate() {
	s.bootstrap()
	// Start with no name field -> object reads as auto-generated.
	s.NoError(s.dbc.CMDB.TypeCreate("srv"))
	body := easyjson.NewJSONObjectWithKeyValue("display_name", easyjson.NewJSON("Named-After-Update"))
	s.NoError(s.dbc.CMDB.ObjectCreate("ssssss222222", "srv", body))
	s.Equal("srv_ssssss_auto", s.readName("ssssss222222"))

	// Declare the name field on the type; the next read must pick it up.
	s.NoError(s.dbc.CMDB.TypeUpdate("srv", typeBodyWithNameField("display_name"), false))
	s.Equal("Named-After-Update", s.readName("ssssss222222"))
}
