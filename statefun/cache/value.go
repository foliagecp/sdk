

package cache

import (
	"errors"
	"strings"
	"sync"
	"time"
)

var (
	ErrInvalidState       = errors.New("invalid state")
	ErrStoreValueNotFound = errors.New("store value not found")
	ErrAlreadyInKeyValue  = errors.New("already in key value")
	ErrSetOldRevision     = errors.New("set value with old revision")
)

type storeValuePath []string

func storeValuePathFromStr(key string) storeValuePath {
	return strings.Split(key, ".")
}

func (k storeValuePath) Path() []string {
	return k[:len(k)-1]
}

func (k storeValuePath) Last() string {
	return k[len(k)-1]
}

func (k storeValuePath) First() string {
	return k[0]
}

type storeValuePather struct {
	path string
	*StoreValue
}

type StoreValue struct {
	parent *StoreValue
	key    string

	mutex    *sync.Mutex
	value    []byte
	revision uint64

	childrenMu *sync.RWMutex
	children   map[string]*StoreValue

	updatedAt int64
}

func newStoreValue(parent *StoreValue, key string, value []byte) *StoreValue {
	return &StoreValue{
		parent:     parent,
		key:        key,
		value:      value,
		mutex:      &sync.Mutex{},
		childrenMu: &sync.RWMutex{},
		children:   map[string]*StoreValue{},
		updatedAt:  time.Now().UnixNano(),
	}
}

func initRoot() *StoreValue {
	return newStoreValue(nil, "", nil)
}

func (v *StoreValue) Update(newValue []byte, revision uint64) error {
	v.mutex.Lock()
	defer v.mutex.Unlock()

	if v.revision >= revision {
		return ErrSetOldRevision
	}

	v.value = newValue
	v.updatedAt = time.Now().UnixNano()
	v.revision = revision

	return nil
}

func (v *StoreValue) Value() []byte {
	out := make([]byte, len(v.value))
	copy(out, v.value)

	return out
}

func (v *StoreValue) Key() string {
	return v.key
}

func (v *StoreValue) Children(key string) (*StoreValue, bool) {
	v.childrenMu.RLock()
	defer v.childrenMu.RUnlock()

	c, ok := v.children[key]

	return c, ok
}

func (v *StoreValue) SaveChild(value *StoreValue) {
	v.childrenMu.Lock()
	v.children[value.key] = value
	v.childrenMu.Unlock()
}

func (v *StoreValue) MkvalueAll(path []string) *StoreValue {
	if len(path) == 0 {
		return v
	}

	current := v
	for i := 0; i < len(path); i++ {
		currentToken := path[i]

		if ch, ok := current.Children(currentToken); ok {
			current = ch
		} else {
			sv := newStoreValue(current, currentToken, nil)
			current.SaveChild(sv)
			current = sv
		}
	}

	return current
}

func (v *StoreValue) SearchChild(path []string) (*StoreValue, error) {
	if len(path) == 0 {
		return v, nil
	}

	current := v
	for i := 0; i < len(path); i++ {
		currentToken := path[i]
		ch, ok := current.Children(currentToken)
		if !ok {
			return nil, ErrStoreValueNotFound
		}

		current = ch
	}

	return current, nil
}

func (v *StoreValue) destroy() bool {
	if v.parent == nil || v.HasChildren() {
		return false
	}

	v.parent.childrenMu.Lock()
	delete(v.parent.children, v.key)
	v.parent.childrenMu.Unlock()

	v.parent.destroy()

	return true
}

func (v *StoreValue) Expired(threshold int64) bool {
	return v.updatedAt < threshold
}

func (v *StoreValue) HasChildren() bool {
	return len(v.children) > 0
}

func (v *StoreValue) Updated() {
	v.mutex.Lock()
	v.updatedAt = time.Now().UnixNano()
	v.mutex.Unlock()
}
