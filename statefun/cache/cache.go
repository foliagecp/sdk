

package cache

import (
	"context"
	"regexp"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/statefun/logger"
	"github.com/foliagecp/sdk/statefun/system"
	"github.com/nats-io/nats.go"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	keyValidationRegexp = regexp.MustCompile(`^[a-zA-Z0-9=_-][a-zA-Z0-9=._-]+[a-zA-Z0-9=_-]$|^[a-zA-Z0-9=_-]*$`)
)

type Store struct {
	cfg         *Config
	kv          nats.KeyValue
	valuesCount *atomic.Int32
	root        *StoreValue

	mutex            *sync.Mutex
	levelSubscribers map[string]nats.KeyWatcher

	taskQueue   chan Task
	lruTreshold int64
}

func New(cfg *Config, keyValue nats.KeyValue) *Store {
	return &Store{
		cfg:              cfg,
		kv:               keyValue,
		valuesCount:      &atomic.Int32{},
		root:             initRoot(),
		mutex:            &sync.Mutex{},
		levelSubscribers: make(map[string]nats.KeyWatcher),
		taskQueue:        make(chan Task, 1024),
	}
}

func (s *Store) Start(ctx context.Context) error {
	watcher, err := s.kv.Watch(s.cfg.kvStorePrefix + ".>")
	if err != nil {
		return err
	}

	go s.process()

	ctxPrepare, cancel := context.WithTimeout(ctx, time.Second*10)
	defer cancel()

	if err := s.prepareStore(ctxPrepare, watcher); err != nil {
		watcher.Stop()
		return err
	}

	go s.readUpdates(ctx, watcher)
	go s.gc(ctx)

	return nil
}

func (s *Store) Set(key string, value []byte) (success bool) {
	start := time.Now()

	defer func() {
		if gaugeVec, err := system.GlobalPrometrics.EnsureGaugeVecSimple("cache_set", "", []string{"id"}); err == nil {
			gaugeVec.With(prometheus.Labels{"id": s.cfg.id}).Set(float64(time.Since(start).Microseconds()))
		}
	}()

	if !keyValidationRegexp.MatchString(key) {
		return false
	}

	rev, err := s.kv.Put(s.toStoreKey(key), value)
	if err != nil {
		return false
	}

	s.taskQueue <- newSetTask(key, value, rev)

	return true
}

func (s *Store) Get(key string) ([]byte, error) {
	start := time.Now()
	path := storeValuePathFromStr(key)

	defer func() {
		if gaugeVec, err := system.GlobalPrometrics.EnsureGaugeVecSimple("cache_get", "", []string{"id"}); err == nil {
			gaugeVec.With(prometheus.Labels{"id": s.cfg.id}).Set(float64(time.Since(start).Microseconds()))
		}
	}()

	c, err := s.root.SearchChild(path)
	if err != nil {
		entry, err := s.kv.Get(s.toStoreKey(key))
		if err != nil {
			return nil, err
		}

		s.taskQueue <- newSetTask(key, entry.Value(), entry.Revision())

		return entry.Value(), nil
	}

	c.Updated()

	return c.Value(), nil
}

func (s *Store) Delete(key string) error {
	if gaugeVec, err := system.GlobalPrometrics.EnsureGaugeVecSimple("cache_del", "", []string{"id"}); err == nil {
		gaugeVec.With(prometheus.Labels{"id": s.cfg.id}).Inc()
	}

	if err := s.kv.Delete(s.toStoreKey(key)); err != nil {
		return err
	}

	s.taskQueue <- newDeleteTask(key)

	return nil
}

func (s *Store) GetKeysByPattern(key string) []string {
	start := time.Now()
	out := make([]string, 0)

	defer func() {
		if gaugeVec, err := system.GlobalPrometrics.EnsureGaugeVecSimple("cache_get_keys_by_pattern", "", []string{"id"}); err == nil {
			gaugeVec.With(prometheus.Labels{"id": s.cfg.id}).Set(float64(time.Since(start).Microseconds()))
		}
	}()

	path := storeValuePathFromStr(key)

	if len(path) == 0 {
		return out
	}

	if len(path) == 1 {
		switch path[0] {
		case "*":
			s.root.childrenMu.RLock()
			for _, sv := range s.root.children {
				out = append(out, sv.key)
			}
			s.root.childrenMu.RUnlock()

			return out
		case ">":
			queue := make([]*storeValuePather, 0)

			s.root.childrenMu.RLock()
			for _, sv := range s.root.children {
				queue = append(queue, &storeValuePather{path: "", StoreValue: sv})
			}
			s.root.childrenMu.RUnlock()

			for len(queue) > 0 {
				lastID := len(queue) - 1

				v := queue[lastID]
				queue = queue[:lastID]

				storeKeyBuilder := strings.Builder{}
				if v.path == "" {
					storeKeyBuilder.WriteString(v.key)
				} else {
					storeKeyBuilder.WriteString(v.path)
					storeKeyBuilder.WriteString(".")
					storeKeyBuilder.WriteString(v.key)
				}

				storeKeyPath := storeKeyBuilder.String()

				v.childrenMu.RLock()
				if len(v.children) == 0 {
					out = append(out, storeKeyPath)
				}

				for _, sv2 := range v.children {
					queue = append(queue, &storeValuePather{path: storeKeyPath, StoreValue: sv2})
				}
				v.childrenMu.RUnlock()
			}

			return out
		}
	}

	switch path.Last() {
	case "*":
		pathWithoutLastKey := path[:len(path)-1]

		value, err := s.root.SearchChild(pathWithoutLastKey)
		if err != nil {
			break
		}

		out := make([]string, 0)

		value.childrenMu.RLock()
		for _, sv := range value.children {
			svKey := make([]string, len(pathWithoutLastKey))
			copy(svKey, pathWithoutLastKey)
			svKey = append(svKey, sv.key)
			out = append(out, strings.Join(svKey, "."))
		}
		value.childrenMu.RUnlock()

		return out
	case ">":
		pathWithoutLastKey := path[:len(path)-1]
		value, err := s.root.SearchChild(pathWithoutLastKey)
		if err != nil {
			break
		}

		queue := make([]*storeValuePather, 0)

		value.childrenMu.RLock()
		for _, sv := range value.children {
			queue = append(queue, &storeValuePather{path: strings.Join(pathWithoutLastKey, "."), StoreValue: sv})
		}
		value.childrenMu.RUnlock()

		out := make([]string, 0)

		for len(queue) > 0 {
			lastID := len(queue) - 1

			e := queue[lastID]
			queue = queue[:lastID]

			storeKeyBuilder := strings.Builder{}
			storeKeyBuilder.WriteString(e.path)
			storeKeyBuilder.WriteString(".")
			storeKeyBuilder.WriteString(e.key)

			storeKeyPath := storeKeyBuilder.String()

			e.childrenMu.RLock()
			if len(e.children) == 0 {
				out = append(out, storeKeyPath)
			}

			for _, sv2 := range e.children {
				queue = append(queue, &storeValuePather{path: storeKeyPath, StoreValue: sv2})
			}
			e.childrenMu.RUnlock()
		}

		return out
	default:
		if _, err := s.root.SearchChild(path); err != nil {
			break
		}

		return []string{key}
	}

	return out
}

func (s *Store) GetAsJSON(key string) (*easyjson.JSON, error) {
	value, err := s.Get(key)
	if err != nil {
		return nil, err
	}

	j, ok := easyjson.JSONFromBytes(value)
	if !ok {
		return easyjson.NewJSONObject().GetPtr(), nil
	}

	return &j, nil
}

func (s *Store) SubscribeLevelCallback(key string, str string) <-chan nats.KeyValueEntry {
	watcher, err := s.kv.Watch(s.toStoreKey(key), nats.IgnoreDeletes())
	if err != nil {
		return nil
	}

	s.mutex.Lock()
	if w, ok := s.levelSubscribers[key]; ok {
		w.Stop()
		delete(s.levelSubscribers, key)
	}

	s.levelSubscribers[key] = watcher
	s.mutex.Unlock()

	return watcher.Updates()
}

func (s *Store) UnsubscribeLevelCallback(key, str string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	w, ok := s.levelSubscribers[key]
	if !ok {
		return
	}

	w.Stop()
	delete(s.levelSubscribers, key)
}

func (s *Store) SetIfDoesNotExist(key string, newValue []byte) bool {
	if _, err := s.kv.Get(s.toStoreKey(key)); err == nil {
		return true
	}

	rev, err := s.kv.Put(s.toStoreKey(key), newValue)
	if err != nil {
		return false
	}

	s.taskQueue <- newSetTask(key, newValue, rev)

	return true
}

func (s *Store) prepareStore(ctx context.Context, watcher nats.KeyWatcher) error {
	values := 0

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case update := <-watcher.Updates():
			if update == nil {
				return nil
			}

			if int32(values) > int32(s.cfg.lruSize) {
				continue
			}

			s.taskQueue <- newSetTask(s.fromStoreKey(update.Key()), update.Value(), update.Revision())
		}
	}
}

func (s *Store) readUpdates(ctx context.Context, watcher nats.KeyWatcher) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case update := <-watcher.Updates():
			if update == nil {
				return nil
			}

			storeKey := s.fromStoreKey(update.Key())

			switch update.Operation() {
			case nats.KeyValuePut:
				s.taskQueue <- newSetTask(storeKey, update.Value(), update.Revision())
			case nats.KeyValueDelete:
				s.taskQueue <- newDeleteTask(storeKey)
			}
		}
	}
}

func (s *Store) process() {
	for {
		task := <-s.taskQueue
		if task == nil {
			continue
		}

		if err := task.Process(s); err != nil {
			logger.Logln(logger.DebugLevel, err)
		}
	}
}

func (s *Store) gc(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(5 * time.Second):
			lruTimes := make([]int64, 0)
			queue := make([]*storeValuePather, 0)
			count := 0

			s.root.childrenMu.RLock()
			for _, sv := range s.root.children {
				queue = append(queue, &storeValuePather{path: "", StoreValue: sv})
			}
			s.root.childrenMu.RUnlock()

			for len(queue) > 0 {
				count++
				lastID := len(queue) - 1

				v := queue[lastID]
				queue = queue[:lastID]

				storeKeyBuilder := strings.Builder{}
				if v.path == "" {
					storeKeyBuilder.WriteString(v.key)
				} else {
					storeKeyBuilder.WriteString(v.path)
					storeKeyBuilder.WriteString(".")
					storeKeyBuilder.WriteString(v.key)
				}

				storeKeyPath := storeKeyBuilder.String()

				if v.Expired(s.lruTreshold) {
					s.taskQueue <- newDeleteTask(storeKeyPath)
				}

				v.childrenMu.RLock()
				for _, sv2 := range v.children {
					queue = append(queue, &storeValuePather{path: storeKeyPath, StoreValue: sv2})
				}
				v.childrenMu.RUnlock()
			}

			if gaugeVec, err := system.GlobalPrometrics.EnsureGaugeVecSimple("cache_values", "", []string{"id"}); err == nil {
				gaugeVec.With(prometheus.Labels{"id": s.cfg.id}).Set(float64(count))
			}

			if len(lruTimes) == 0 {
				continue
			}

			sort.Slice(lruTimes, func(i, j int) bool { return lruTimes[i] > lruTimes[j] })
			if len(lruTimes) > s.cfg.lruSize {
				s.lruTreshold = lruTimes[s.cfg.lruSize-1]
			} else {
				s.lruTreshold = lruTimes[len(lruTimes)-1]
			}
		}
	}
}

func (cs *Store) toStoreKey(key string) string {
	b := strings.Builder{}
	b.Grow(len(cs.cfg.kvStorePrefix) + len(".") + len(key))
	b.WriteString(cs.cfg.kvStorePrefix)
	b.WriteString(".")
	b.WriteString(key)

	return b.String()
}

func (cs *Store) fromStoreKey(key string) string {
	return strings.TrimPrefix(key, cs.cfg.kvStorePrefix+".")
}
