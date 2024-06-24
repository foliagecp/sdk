package graph

import (
	"encoding/json"
	"io"

	"github.com/foliagecp/sdk/clients/go/db"
)

// This file will not be regenerated automatically.
//
// It serves as dependency injection for your app, add any dependencies you require here.
type Resolver struct{}

var DBC *db.DBSyncClient

// JSON определяет тип данных, представляющий JSON-объект
type JSON map[string]interface{}

// MarshalGQL сериализует JSON в формат, используемый GraphQL
func (j JSON) MarshalGQL(w io.Writer) {
	json.NewEncoder(w).Encode(j)
}

// UnmarshalGQL десериализует данные из формата GraphQL в JSON
func (j *JSON) UnmarshalGQL(v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, j)
}
