package crud_test

// Bug hunt: edge-case object bodies must round-trip intact (deep nesting, unicode
// and special-char values, arrays, empty objects). A corruption here would be a
// serialization/storage defect.
//
// Method on CMDBClientContractTestSuite to reuse its bootstrap.

import (
	"github.com/foliagecp/easyjson"
)

func (s *CMDBClientContractTestSuite) Test_Hunt_ObjectBody_EdgeCasesRoundTrip() {
	s.bootstrap()
	s.NoError(s.dbc.CMDB.TypeCreate("EpType"))

	body := easyjson.NewJSONObject()
	body.SetByPath("nested.deep.deeper.x", easyjson.NewJSON("v"))
	body.SetByPath("special", easyjson.NewJSON("a/b#c@d+e=f-g"))
	body.SetByPath("unicode", easyjson.NewJSON("ünïcödé-世界-🚀"))
	body.SetByPath("arr", easyjson.NewJSON([]string{"a", "b", "c"}))
	body.SetByPath("num", easyjson.NewJSON(1234567890))
	body.SetByPath("empty", easyjson.NewJSONObject())
	body.SetByPath("flag", easyjson.NewJSON(true))

	s.NoError(s.dbc.CMDB.ObjectUpdate("ep-1", body, true, "EpType"))

	data, err := s.dbc.CMDB.ObjectRead("ep-1")
	s.NoError(err)
	s.Equal("v", data.GetByPath("body.nested.deep.deeper.x").AsStringDefault(""), "deep nesting must round-trip")
	s.Equal("ünïcödé-世界-🚀", data.GetByPath("body.unicode").AsStringDefault(""), "unicode value must round-trip")
	s.Equal("a/b#c@d+e=f-g", data.GetByPath("body.special").AsStringDefault(""), "special-char value must round-trip")
	s.Equal(3, data.GetByPath("body.arr").ArraySize(), "array must round-trip")
	s.Equal(int64(1234567890), int64(data.GetByPath("body.num").AsNumericDefault(0)), "number must round-trip")
	s.True(data.GetByPath("body.flag").AsBoolDefault(false), "bool must round-trip")
	s.True(data.GetByPath("body.empty").IsObject(), "empty object must round-trip")
}
