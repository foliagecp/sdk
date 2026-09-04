package cache

// Ворота этапа 2A: с выключенным и включённым ярусом Store обязан отвечать
// одинаково. Тест ходит только через публичные методы — то есть проверяет
// ровно то, что видят CRUD и JPGQL.

import (
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"testing"

	"github.com/foliagecp/easyjson"
	"github.com/stretchr/testify/require"
)

// storeOp — одна операция над кэшем, применимая к любому Store.
type storeOp func(cs *Store)

// storeQuery — один вопрос к кэшу; ответ приводится к строке, чтобы сравнивать
// два хранилища, не зная их устройства.
type storeQuery func(cs *Store) string

func randomOps(rng *rand.Rand, vertices, links, steps int) ([]storeOp, []storeQuery) {
	vid := func(i int) string { return fmt.Sprintf("dom/v-%03d", i) }
	lname := func(i int) string { return fmt.Sprintf("l%03d", i) }
	ltype := func(i int) string { return fmt.Sprintf("t%d", i%4) }
	tgt := func(i int) string { return fmt.Sprintf("dom/tgt-%03d", i%17) }

	var ops []storeOp
	now := int64(1_000_000)
	for step := 0; step < steps; step++ {
		now++
		t := now
		v := vid(rng.Intn(vertices))
		n := rng.Intn(links)
		name, lt, to := lname(n), ltype(n), tgt(n)

		switch rng.Intn(10) {
		case 0: // тело вершины
			body := easyjson.NewJSONObjectWithKeyValue("n", easyjson.NewJSON(rng.Intn(100)))
			ops = append(ops, func(cs *Store) { cs.SetValueJSON(v, &body, false, t) })
		case 1: // удалить тело
			ops = append(ops, func(cs *Store) { cs.DeleteValue(v, false, t) })
		case 2, 3, 4: // создать связь целиком, как это делает CRUD
			ops = append(ops, func(cs *Store) {
				cs.SetValue(fmt.Sprintf(kOutTo, v, name), []byte(lt+"."+to), false, t)
				cs.SetValue(fmt.Sprintf(kOutBody, v, name), []byte(`{"w":1}`), false, t)
				cs.SetValue(fmt.Sprintf(kLinkType, v, lt, to), []byte(name), false, t)
				cs.SetValue(fmt.Sprintf(kIndexType, v, name, lt), nil, false, t)
				cs.SetValue(fmt.Sprintf(kIndexTag, v, name, "tag0"), nil, false, t)
				cs.SetValue(fmt.Sprintf(kInLink, to, v, name), []byte(lt), false, t)
			})
		case 5: // удалить связь целиком
			ops = append(ops, func(cs *Store) {
				for _, k := range cs.GetKeysByPattern(fmt.Sprintf("%s.out.index.%s.>", v, name)) {
					cs.DeleteValue(k, false, t)
				}
				cs.DeleteValue(fmt.Sprintf(kLinkType, v, lt, to), false, t)
				cs.DeleteValue(fmt.Sprintf(kOutBody, v, name), false, t)
				cs.DeleteValue(fmt.Sprintf(kOutTo, v, name), false, t)
				cs.DeleteValue(fmt.Sprintf(kInLink, to, v, name), false, t)
			})
		case 6: // добавить тег
			tag := fmt.Sprintf("tag%d", rng.Intn(3))
			ops = append(ops, func(cs *Store) {
				cs.SetValue(fmt.Sprintf(kIndexTag, v, name, tag), nil, false, t)
			})
		case 7: // снять тег
			tag := fmt.Sprintf("tag%d", rng.Intn(3))
			ops = append(ops, func(cs *Store) {
				cs.DeleteValue(fmt.Sprintf(kIndexTag, v, name, tag), false, t)
			})
		case 8: // запоздавшая запись — оба обязаны отвергнуть одинаково
			ops = append(ops, func(cs *Store) {
				cs.SetValue(fmt.Sprintf(kOutTo, v, name), []byte("stale.dom/stale"), false, t-1000)
			})
		case 9: // ключ рантайма — обязан остаться в дереве в обоих режимах
			ops = append(ops, func(cs *Store) {
				body := easyjson.NewJSONObjectWithKeyValue("rev", easyjson.NewJSON(t))
				cs.SetValueJSON(v+"-lock", &body, false, t)
				ctxKey := fmt.Sprintf("functions.cmdb.api.object.update.%s", v)
				cs.SetValueJSON(ctxKey, &body, false, t)
			})
		}
	}

	var qs []storeQuery
	for i := 0; i < vertices; i++ {
		v := vid(i)
		qs = append(qs,
			func(cs *Store) string { return fmt.Sprintf("exists(%s)=%v", v, cs.Exists(v)) },
			func(cs *Store) string { return fmt.Sprintf("existsJson(%s)=%v", v, cs.ExistsJson(v)) },
			func(cs *Store) string { return fmt.Sprintf("time(%s)=%d", v, cs.GetValueUpdateTime(v)) },
			func(cs *Store) string { return "body(" + v + ")=" + getStr(cs, v) },
			func(cs *Store) string { return "lock=" + getStr(cs, v+"-lock") },
			func(cs *Store) string {
				return "ctx=" + getStr(cs, "functions.cmdb.api.object.update."+v)
			},
		)
		for _, pat := range []string{".out.to.>", ".out.body.>", ".out.index.>", ".ltype.>", ".in.>", ".out.to.*"} {
			pattern := v + pat
			qs = append(qs, func(cs *Store) string {
				keys := cs.GetKeysByPattern(pattern)
				sort.Strings(keys)
				return pattern + "=" + fmt.Sprint(keys)
			})
		}
		for n := 0; n < links; n++ {
			name, lt, to := lname(n), ltype(n), tgt(n)
			for _, key := range []string{
				fmt.Sprintf(kOutTo, v, name),
				fmt.Sprintf(kOutBody, v, name),
				fmt.Sprintf(kLinkType, v, lt, to),
				fmt.Sprintf(kIndexType, v, name, lt),
				fmt.Sprintf(kIndexTag, v, name, "tag0"),
				fmt.Sprintf(kInLink, v, vid(0), name),
			} {
				k := key
				qs = append(qs, func(cs *Store) string {
					return fmt.Sprintf("%s|%v|%d|%s", k, cs.Exists(k), cs.GetValueUpdateTime(k), getStr(cs, k))
				})
			}
		}
	}
	return ops, qs
}

func getStr(cs *Store, key string) string {
	v, err := cs.GetValue(key)
	if err != nil {
		return "<нет>"
	}
	return string(v)
}

func runOps(t *testing.T, tiering bool, ops []storeOp, qs []storeQuery) []string {
	t.Helper()
	restore := SetTieringForTest(tiering)
	defer restore()

	cs := NewStoreForTest("tiering")
	for _, op := range ops {
		op(cs)
	}
	out := make([]string, len(qs))
	for i, q := range qs {
		out[i] = q(cs)
	}
	return out
}

// Test_Tiering_SameAnswers — главный тест: ярус ничего не меняет снаружи.
func Test_Tiering_SameAnswers(t *testing.T) {
	for _, cfg := range []struct{ vertices, links, steps int }{
		{3, 4, 200},
		{5, 20, 2000},
		{2, 60, 4000},
	} {
		t.Run(fmt.Sprintf("v=%d/l=%d", cfg.vertices, cfg.links), func(t *testing.T) {
			rng := rand.New(rand.NewSource(int64(cfg.steps)))
			ops, qs := randomOps(rng, cfg.vertices, cfg.links, cfg.steps)

			off := runOps(t, false, ops, qs)
			on := runOps(t, true, ops, qs)

			require.Equal(t, len(off), len(on))
			diffs := 0
			for i := range off {
				if off[i] != on[i] {
					if diffs < 10 {
						t.Errorf("расхождение:\n  дерево: %s\n  запись: %s", off[i], on[i])
					}
					diffs++
				}
			}
			require.Zero(t, diffs, "всего расхождений: %d из %d вопросов", diffs, len(off))
		})
	}
}

// Test_Tiering_RuntimeKeysStayInTree — контексты и объектные мьютексы записями
// не становятся: они короткоживущие и горячие, и форматом записи не описываются.
func Test_Tiering_RuntimeKeysStayInTree(t *testing.T) {
	restore := SetTieringForTest(true)
	defer restore()

	cs := NewStoreForTest("runtime")
	body := easyjson.NewJSONObjectWithKeyValue("a", easyjson.NewJSON(1))

	cs.SetValueJSON("dom/v", &body, false, 1)                      // вершина
	cs.SetValueJSON("dom/v-lock", &body, false, 1)                 // мьютекс объекта
	cs.SetValueJSON("functions.cmdb.api.x.dom/v", &body, false, 1) // контекст

	require.Equal(t, 1, cs.RecordCountForTest(), "записью обязана стать только вершина")
	require.True(t, cs.ExistsJson("dom/v-lock"))
	require.True(t, cs.ExistsJson("functions.cmdb.api.x.dom/v"))
	require.True(t, cs.ExistsJson("dom/v"))
}

// Test_Tiering_CompactionKeepsAnswers — уплотнение из обхода обслуживания
// ничего не меняет в ответах.
func Test_Tiering_CompactionKeepsAnswers(t *testing.T) {
	restore := SetTieringForTest(true)
	defer restore()

	rng := rand.New(rand.NewSource(77))
	ops, qs := randomOps(rng, 4, 30, 3000)

	cs := NewStoreForTest("compact")
	for _, op := range ops {
		op(cs)
	}
	before := make([]string, len(qs))
	for i, q := range qs {
		before[i] = q(cs)
	}

	n := cs.compactRecords()
	require.Greater(t, n, 0, "записи обязаны были оставить корзины разобранными")

	for i, q := range qs {
		require.Equal(t, before[i], q(cs), "уплотнение изменило ответ")
	}
	require.Zero(t, cs.compactRecords(), "повторное уплотнение не должно ничего находить")
}

// Test_Tiering_ConcurrentWrites — параллельная запись через публичный API.
func Test_Tiering_ConcurrentWrites(t *testing.T) {
	restore := SetTieringForTest(true)
	defer restore()

	cs := NewStoreForTest("conc")
	const goroutines, per = 32, 100

	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < per; i++ {
				id := g*per + i
				name := fmt.Sprintf("l%05d", id)
				now := int64(1000 + id)
				cs.SetValue(fmt.Sprintf(kOutTo, "dom/hub", name), []byte("t."+fmt.Sprintf("dom/o%05d", id)), false, now)
				cs.SetValue(fmt.Sprintf(kIndexType, "dom/hub", name, "t"), nil, false, now)
			}
		}(g)
	}
	wg.Wait()

	for id := 0; id < goroutines*per; id++ {
		name := fmt.Sprintf("l%05d", id)
		key := fmt.Sprintf(kOutTo, "dom/hub", name)
		require.True(t, cs.Exists(key), "потеряна связь %s", name)
		v, err := cs.GetValue(key)
		require.NoError(t, err)
		require.Equal(t, "t."+fmt.Sprintf("dom/o%05d", id), string(v))
	}
	keys := cs.GetKeysByPattern("dom/hub.out.to.>")
	require.Len(t, keys, goroutines*per, "перечисление не сходится с числом записей")
}

// Test_Tiering_TreeStaysEmpty — вершина, ушедшая в запись, не должна
// одновременно расти поддеревом: иначе память не экономится, а тратится дважды.
func Test_Tiering_TreeStaysEmpty(t *testing.T) {
	restore := SetTieringForTest(true)
	defer restore()

	cs := NewStoreForTest("empty")
	now := int64(1_000_000)
	for i := 0; i < 200; i++ {
		v := fmt.Sprintf("dom/v-%03d", i)
		body := easyjson.NewJSONObjectWithKeyValue("n", easyjson.NewJSON(i))
		cs.SetValueJSON(v, &body, false, now)
		for k := 0; k < 5; k++ {
			name := fmt.Sprintf("l%02d", k)
			cs.SetValue(fmt.Sprintf(kOutTo, v, name), []byte("t.dom/o"), false, now)
			cs.SetValue(fmt.Sprintf(kIndexType, v, name, "t"), nil, false, now)
			cs.SetValue(fmt.Sprintf(kInLink, v, "dom/src", name), []byte("t"), false, now)
		}
	}

	require.Equal(t, 200, cs.RecordCountForTest())
	st := cs.StatsForTest()
	require.LessOrEqual(t, st.TotalNodes, 2,
		"дерево обязано остаться пустым: %d узлов на 200 вершин в записях", st.TotalNodes)

	// а служебные ключи по-прежнему в дереве
	body := easyjson.NewJSONObjectWithKeyValue("rev", easyjson.NewJSON(1))
	cs.SetValueJSON("dom/v-000-lock", &body, false, now)
	st = cs.StatsForTest()
	require.Greater(t, st.TotalNodes, 1, "мьютекс объекта обязан лежать в дереве")
}
