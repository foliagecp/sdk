package extra

import (
	"encoding/json"
	"io"
)

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
