

// JSON package.
// Provides everything that is needed for ease and simple operating with JSON data
package json_easy

import (
	"encoding/hex"
	"encoding/json"
	"strconv"
	"strings"
)

// JSON -----------------------------------------------------------------------
type JSON struct {
	Value interface{}
}

func NewJSON(value interface{}) JSON {
	var j JSON
	j.Value = value
	return j
}

func NewJSONBytes(value []byte) JSON {
	codedBytes := hex.EncodeToString(value)
	var j JSON
	j.Value = codedBytes
	return j
}

func NewJSONNull() JSON {
	return NewJSON(nil)
}

func NewJSONObject() JSON {
	return NewJSON(make(map[string]interface{}))
}

func NewJSONObjectWithKeyValue(key string, value JSON) JSON {
	m := make(map[string]interface{})
	m[key] = value.Value
	return NewJSON(m)
}

func NewJSONArray() JSON {
	return NewJSON(make([]interface{}, 0))
}

func (j1 JSON) Equals(j2 JSON) bool {
	if j1.IsObject() && j2.IsObject() {
		if len(j1.ObjectKeys()) == len(j2.ObjectKeys()) {
			for _, key := range j1.ObjectKeys() {
				if !j2.PathExists(key) {
					return false
				}
				if !j1.GetByPath(key).Equals(j2.GetByPath(key)) {
					return false
				}
			}
			return true
		}
		return false
	}
	if j1.IsArray() && j2.IsArray() {
		if j1.ArraySize() == j2.ArraySize() {
			for i := 0; i < j1.ArraySize(); i++ {
				element_found := false
				for j := 0; j < j2.ArraySize(); j++ {
					if j1.GetByPath(strconv.Itoa(i)).Equals(j2.GetByPath(strconv.Itoa(j))) {
						element_found = true
					}
				}
				if !element_found {
					return false
				}
			}
			return true
		}
		return false
	}
	if j1.Value == j2.Value {
		return true
	}
	return false
}

func (j JSON) PathExists(p string) bool {
	path := strings.Split(p, ".")
	if len(path) > 0 && len(path[0]) > 0 { // Not an empty path
		switch j.Value.(type) {
		case map[string]interface{}: // JSON object
			jObj := j.Value.(map[string]interface{})
			if jvRes, ok := jObj[path[0]]; ok {
				jRes := NewJSON(jvRes)
				if len(path) == 1 {
					return true
				} else { // More than one key in path (recursive call is only possible when there is a path's tail)
					return jRes.PathExists(strings.Join(path[1:], "."))
				}
			} else {
				return false
			}
		case []interface{}: // JSON array
			jArr := j.Value.([]interface{})
			if arrayId, err := strconv.Atoi(path[0]); err == nil {
				if 0 <= arrayId && arrayId < len(jArr) {
					if jvRes := jArr[arrayId]; jvRes != nil {
						jRes := NewJSON(jvRes)
						if len(path) == 1 {
							return true
						} else { // More than one key in path (recursive call is only possible when there is a path's tail)
							return jRes.PathExists(strings.Join(path[1:], "."))
						}
					}
				}
			}
			return false
		default: // Not JSON obejct
			return false
		}
	} else { // Empty path can come only from the very first call
		return true // No path, so.. nothing exists in JSON
	}
}

func (j JSON) GetByPath(p string) JSON {
	path := strings.Split(p, ".")
	if len(path) > 0 && len(path[0]) > 0 { // Not an empty path
		switch j.Value.(type) {
		case map[string]interface{}: // JSON object
			jObj := j.Value.(map[string]interface{})
			if jvRes, ok := jObj[path[0]]; ok {
				jRes := NewJSON(jvRes)
				if len(path) == 1 {
					return jRes
				} else { // More than one key in path (recursive call is only possible when there is a path's tail)
					return jRes.GetByPath(strings.Join(path[1:], "."))
				}
			} else {
				//fmt.Printf(`Key "%s" does not exist in JSON object: %s`+"\n", path[0], j.ToString())
				return NewJSONNull()
			}
		case []interface{}: // JSON array
			jArr := j.Value.([]interface{})
			if arrayId, err := strconv.Atoi(path[0]); err == nil {
				if 0 <= arrayId && arrayId < len(jArr) {
					if jvRes := jArr[arrayId]; jvRes != nil {
						jRes := NewJSON(jvRes)
						if len(path) == 1 {
							return jRes
						} else { // More than one key in path (recursive call is only possible when there is a path's tail)
							return jRes.GetByPath(strings.Join(path[1:], "."))
						}
					}
				}
			}
			return NewJSONNull()
		default: // Not JSON obejct
			//fmt.Printf(`Path "%s" tries to accesss not a JSON object's element in object %s`+"\n", p, j.ToString())
			return NewJSONNull()
		}
	} else { // Empty path can come only from the very first call
		return j // No path, so.. return current JSON
	}
}

func (j JSON) GetByPathPtr(p string) *JSON {
	res := j.GetByPath(p)
	return &res
}

func (j JSON) GetPtr() *JSON {
	return &j
}

func jvSetValueByPath(parent *interface{}, parentKeyOrIdForThisValue string, jv *interface{}, p string, v *interface{}, delimiter string) bool {
	path := strings.Split(p, delimiter)
	if len(path) > 0 && len(path[0]) > 0 { // Not an empty path
		switch (*jv).(type) {
		case map[string]interface{}: // JSON object
			jObj := (*jv).(map[string]interface{})
			if len(path) == 1 {
				jObj[path[0]] = *v // Set value anyway (whether key exists on not)
				return true
			} else { // More than one key in path (recursive call is only possible when there is a path's tail)
				jvRes, ok := jObj[path[0]]
				if !ok {
					jObj[path[0]] = map[string]interface{}{}
					jvRes = jObj[path[0]]
				}
				return jvSetValueByPath(jv, path[0], &jvRes, strings.Join(path[1:], delimiter), v, delimiter)
			}
		case []interface{}: // JSON array
			jArr := (*jv).([]interface{})
			if arrayId, err := strconv.Atoi(path[0]); err == nil {
				if 0 <= arrayId && arrayId < len(jArr) { // Array has a place for element
					if len(path) == 1 {
						jArr[arrayId] = *v
						return true
					} else { // More than one key in path (recursive call is only possible when there is a path's tail)
						jvRes := jArr[arrayId]
						return jvSetValueByPath(jv, path[0], &jvRes, strings.Join(path[1:], delimiter), v, delimiter)
					}
				} else { // Array has no place for element
					if len(path) == 1 {
						if arrayId < 0 {
							jvAddValueToArray(&jArr, *v)
						} else {
							jvSetArrayValue(&jArr, arrayId, *v)
						}
						*jv = jArr
						return jvSetValueByPath(nil, "", parent, parentKeyOrIdForThisValue, jv, delimiter)
					}
				}
			}
			return false
		default: // Not JSON obejct
			//fmt.Printf(`Path "%s" tries to accesss not a JSON object's element in object %s`+"\n", p, j.ToString())
			return false
		}
	} else { // Empty path can come only from the very first call
		return false // No path, so.. return false
	}
}

func jvDeepMerge(jv1 *interface{}, jv2 *interface{}) {
	switch (*jv1).(type) {
	case map[string]interface{}: // JSON object
		jObj1 := (*jv1).(map[string]interface{})
		if jObj2, ok := (*jv2).(map[string]interface{}); ok {
			for k2, v2 := range jObj2 {
				if v1, exists := jObj1[k2]; exists {
					jvDeepMerge(&v1, &v2)
					jObj1[k2] = v1
				} else {
					jObj1[k2] = v2
				}
			}
		}
	case []interface{}: // JSON array
		jArr1 := (*jv1).([]interface{})
		if jArr2, ok := (*jv2).([]interface{}); ok {
			for _, v2 := range jArr2 {
				unique := true
				for _, v1 := range jArr1 {
					if NewJSON(v2).Equals(NewJSON(v1)) {
						unique = false
					}
				}
				if unique {
					jvAddValueToArray(&jArr1, v2)
					*jv1 = jArr1
				}
			}
		}
	default: // Not JSON obejct and not JSON array - mere type
		*jv1 = *jv2
	}
}

func jvRemoveValueByPath(jv *interface{}, p string) bool {
	path := strings.Split(p, ".")
	if len(path) > 0 && len(path[0]) > 0 { // Not an empty path
		switch (*jv).(type) {
		case map[string]interface{}: // JSON object
			jObj := (*jv).(map[string]interface{})
			if jRes, keyExists := jObj[path[0]]; true {
				if len(path) == 1 { // Last key in the path
					delete(jObj, path[0]) // Remove key anyway (whether key exists on not)
					return true
				} else { // More than one key in path (recursive call is only possible when there is a path's tail)
					if !keyExists { // Return true cause nothing to remove
						return true
					}
					return jvRemoveValueByPath(&jRes, strings.Join(path[1:], "."))
				}
			}
		case []interface{}: // JSON array
			jArr := (*jv).([]interface{})
			if arrayId, err := strconv.Atoi(path[0]); err == nil {
				if 0 <= arrayId && arrayId < len(jArr) { // Array has a place for element
					if len(path) == 1 {
						jArr[arrayId] = nil
						return true
					} else { // More than one key in path (recursive call is only possible when there is a path's tail)
						return jvRemoveValueByPath(&jArr[arrayId], strings.Join(path[1:], "."))
					}
				}
			}
			return false
		default: // Not JSON obejct
			//fmt.Printf(`Path "%s" tries to accesss not a JSON object's element in object %s`+"\n", p, jvValueToString(jv))
			return false
		}
	} else { // More than one key in path (recursive call is only possible when there is a path's tail)
		*jv = nil // No path, so.. this can be only interpreted as setting current json value to nil
		return true
	}
	return false
}

func jvAddValueToArray(jArray *[]interface{}, v interface{}) bool {
	*jArray = append(*jArray, v)
	return true
}

func jvSetArrayValue(jArray *[]interface{}, id int, v interface{}) bool {
	if id >= 0 && len(*jArray) >= 0 {
		for ok := true; ok; ok = (len(*jArray) <= id) {
			jvAddValueToArray(jArray, nil)
		}
		(*jArray)[id] = v
		return true
	}
	return false
}

func (j *JSON) SetByPath(p string, v JSON) bool {
	return jvSetValueByPath(nil, "", &j.Value, p, &v.Value, ".")
}

func (j *JSON) SetByPathCustomDelimiter(p string, v JSON, delimiter string) bool {
	return jvSetValueByPath(nil, "", &j.Value, p, &v.Value, delimiter)
}

func (j *JSON) DeepMerge(v JSON) {
	if j == nil {
		*j = v
	} else {
		jvDeepMerge(&j.Value, &v.Value)
	}
}

func (j *JSON) RemoveByPath(p string) bool {
	return jvRemoveValueByPath(&j.Value, p)
}

func jvValueToBytes(jv interface{}) []byte {
	bytes, _ := json.Marshal(jv)
	return bytes
}

func jvValueToString(jv interface{}) string {
	return string(jvValueToBytes(jv))
}

func (j JSON) Clone() JSON {
	json, _ := JSONFromString(j.ToString())
	return json
}

func (j JSON) ToBytes() []byte {
	return jvValueToBytes(j.Value)
}

func (j JSON) ToString() string {
	return jvValueToString(j.Value)
}

func (j JSON) IsNull() bool {
	return j.Value == nil
}

func (j JSON) IsObject() bool {
	_, ok := j.AsObject()
	return ok
}

func (j JSON) IsArray() bool {
	_, ok := j.AsArray()
	return ok
}

func (j JSON) IsNumeric() bool {
	_, ok := j.AsNumeric()
	return ok
}

func (j JSON) IsString() bool {
	_, ok := j.AsString()
	return ok
}

func (j JSON) IsBool() bool {
	_, ok := j.AsBool()
	return ok
}

func (j JSON) AsObject() (map[string]interface{}, bool) {
	val, ok := j.Value.(map[string]interface{})
	return val, ok
}

func (j JSON) AsArray() ([]interface{}, bool) {
	val, ok := j.Value.([]interface{})
	return val, ok
}

func (j JSON) AsArrayString() ([]string, bool) {
	if array, ok := j.AsArray(); ok {
		strArray := make([]string, len(array))
		allString := true
		for i, v := range array {
			if vStr, ok := v.(string); ok {
				strArray[i] = vStr
			} else {
				allString = false
			}
		}
		if allString {
			return strArray, allString
		}
	}
	return make([]string, 0), false
}

func (j *JSON) AddToArray(jElem JSON) {
	if jv, ok := j.Value.([]interface{}); ok {
		jvAddValueToArray(&jv, jElem.Value)
		j.Value = jv
	}
}

func (j JSON) ArraySize() int {
	if array, ok := j.AsArray(); ok {
		return len(array)
	}
	return -1
}

func (j JSON) ArrayElement(num int) JSON {
	size := j.ArraySize()
	if size >= 0 && num < size {
		array, _ := j.AsArray()
		return NewJSON(array[num])
	}
	return NewJSONNull()
}

func (j JSON) ObjectKeys() []string {
	if object, ok := j.AsObject(); ok {
		var keys []string
		for key := range object {
			keys = append(keys, key)
		}
		return keys
	}
	return []string{}
}

func (j JSON) KeysCount() int {
	return len(j.ObjectKeys())
}

func (j JSON) IsNonEmptyObject() bool {
	return len(j.ObjectKeys()) > 0
}

func (j JSON) IsNonEmptyArray() bool {
	return j.ArraySize() > 0
}

func (j JSON) AsNumeric() (float64, bool) {
	switch j.Value.(type) {
	case float64:
		return float64(j.Value.(float64)), true
	case float32:
		return float64(j.Value.(float32)), true
	case int:
		return float64(j.Value.(int)), true
	case int64:
		return float64(j.Value.(int64)), true
	case uint:
		return float64(j.Value.(uint)), true
	case uint64:
		return float64(j.Value.(uint64)), true
	case uint32:
		return float64(j.Value.(uint32)), true
	case byte:
		return float64(j.Value.(byte)), true
	default:
		return 0.0, false
	}
}

func (j JSON) AsString() (string, bool) {
	switch j.Value.(type) {
	case string:
		return j.Value.(string), true
	default:
		return "", false
	}
}

func (j JSON) AsBytes() ([]byte, bool) {
	switch j.Value.(type) {
	case string:
		if b, err := hex.DecodeString(j.Value.(string)); err == nil {
			return b, true
		}
		return nil, false
	default:
		return nil, false
	}
}

func (j JSON) AsBool() (bool, bool) {
	switch j.Value.(type) {
	case bool:
		return j.Value.(bool), true
	default:
		return false, false
	}
}

func (j JSON) AsNumericDefault(defaultValue float64) float64 {
	if v, ok := j.AsNumeric(); ok {
		return v
	}
	return defaultValue
}

func (j JSON) AsStringDefault(defaultValue string) string {
	if v, ok := j.AsString(); ok {
		return v
	}
	return defaultValue
}

func (j JSON) AsBoolDefault(defaultValue bool) bool {
	if v, ok := j.AsBool(); ok {
		return v
	}
	return defaultValue
}

func JSONFromBytes(b []byte) (JSON, bool) {
	var j JSON
	if err := json.Unmarshal(b, &j.Value); err != nil {
		return NewJSONNull(), false
	}
	return j, true
}

func JSONFromString(s string) (JSON, bool) {
	s = strings.Replace(s, "\n", "", -1)
	s = strings.Replace(s, "\t", "", -1)
	return JSONFromBytes([]byte(s))
}

func JSONFromArray[T comparable](array []T) JSON {
	new_array := make([]interface{}, len(array))
	for i, v := range array {
		new_array[i] = v
	}
	return NewJSON(new_array)
}

// ----------------------------------------------------------------------------
