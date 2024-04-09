package crud

import (
	"fmt"
	"testing"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/statefun"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	"github.com/foliagecp/sdk/statefun/test"
	"github.com/stretchr/testify/suite"
)

type LowLevelTestSuite struct {
	test.StatefunTestSuite
}

func TestLowLevelTestSuite(t *testing.T) {
	suite.Run(t, new(LowLevelTestSuite))
}

func (s *LowLevelTestSuite) Test_GraphAPI_CreateVertex_CorrectPayload() {
	typename := "functions.graph.api.vertex.create"

	cfg := *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).SetMaxIdHandlers(-1)
	s.RegisterFunction(typename, LLAPIVertexCreate, cfg)

	err := s.StartRuntime()
	s.NoError(err)

	createdAt := time.Now().Unix()

	payload := easyjson.NewJSONObject()
	payload.SetByPath("body.created_at", easyjson.NewJSON(createdAt))

	result, err := s.Request(sfPlugins.AutoRequestSelect, typename, "1", &payload, nil)
	s.NoError(err)

	s.Equal("ok", result.GetByPath("status").AsStringDefault("failed"))

	wantBody := fmt.Sprintf(`{"created_at":%v}`, createdAt)

	gotBody, err := s.CacheValue("1")
	s.NoError(err)

	s.Equal(wantBody, gotBody.ToString())
}

func (s *LowLevelTestSuite) Test_GraphAPI_UpdateVertex_BeforeVertexCreate() {
	typename := "functions.graph.api.vertex.update"

	cfg := *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).SetMaxIdHandlers(-1)
	s.RegisterFunction(typename, LLAPIVertexUpdate, cfg)

	err := s.StartRuntime()
	s.NoError(err)

	vertexID := "1"
	createdAt := time.Now().Unix()

	payload := easyjson.NewJSONObject()
	payload.SetByPath("body.updated_at", easyjson.NewJSON(createdAt))

	result, err := s.Request(sfPlugins.AutoRequestSelect, typename, vertexID, &payload, nil)
	s.NoError(err)

	wantStatus := "idle"
	s.Equal(wantStatus, result.GetByPath("status").AsStringDefault(""))

	wantDetails := fmt.Sprintf("vertex with id=%v does not exist", s.SetThisDomainPreffix(vertexID))
	s.Equal(wantDetails, result.GetByPath("details").AsStringDefault(""))
}
