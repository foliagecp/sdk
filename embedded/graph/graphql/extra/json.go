package extra

import (
	"encoding/json"
	"io"

	"github.com/foliagecp/sdk/statefun/system"
)

// JSON is a JSON object as a GraphQL scalar.
type JSON map[string]interface{}

// MarshalGQL writes the object in the form GraphQL expects.
func (j JSON) MarshalGQL(w io.Writer) {
	system.MsgOnErrorReturn(json.NewEncoder(w).Encode(j))
}

// UnmarshalGQL reads a GraphQL value into the object.
func (j *JSON) UnmarshalGQL(v interface{}) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, j)
}
