package graph

import (
	"encoding/json"
	"io"

	"github.com/foliagecp/sdk/clients/go/db"
	"github.com/foliagecp/sdk/statefun/system"
)

// This file will not be regenerated automatically.
//
// It serves as dependency injection for your app, add any dependencies you require here.
type Resolver struct{}

var DBC *db.DBSyncClient

type JSON map[string]interface{}

// MarshalGQL serializes JSON into format used by GraphQL
func (j JSON) MarshalGQL(w io.Writer) {
	system.MsgOnErrorReturn(json.NewEncoder(w).Encode(j))
}

// UnmarshalGQL deserializes from GraphQL format into JSON
func (j *JSON) UnmarshalGQL(v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, j)
}
