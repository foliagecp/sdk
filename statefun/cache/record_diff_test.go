package cache

// Дифференциальный тест этапа 1: одни и те же операции над деревом и над
// записью обязаны давать один и тот же ответ.
//
// Раскладка ключей продублирована из embedded/graph/crud/common.go: тот пакет
// зависит от этого, и импортировать его сюда нельзя. Если раскладка изменится,
// этот тест должен упасть — в этом и смысл.

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	kOutTo     = "%s.out.to.%s"
	kOutBody   = "%s.out.body.%s"
	kLinkType  = "%s.ltype.%s.%s"
	kIndexType = "%s.out.index.%s.type.%s"
	kIndexTag  = "%s.out.index.%s.tag.%s"
	kInLink    = "%s.in.%s.%s"
)

// diffVertex — вершина, которую кладём в оба представления.
type diffVertex struct {
	id   string
	body []byte
	out  []outLink
	in   []inLink
}

func randomDiffVertex(rng *rand.Rand, id string, maxLinks int) diffVertex {
	v := diffVertex{id: id}
	if rng.Intn(10) > 0 {
		v.body = []byte(fmt.Sprintf(`{"name":"%s","n":%d}`, id, rng.Intn(1000)))
	}
	n := rng.Intn(maxLinks + 1)
	for i := 0; i < n; i++ {
		l := outLink{
			Name:       fmt.Sprintf("l%03d", i),
			Type:       fmt.Sprintf("t%d", rng.Intn(4)),
			Target:     fmt.Sprintf("dom/tgt-%03d", rng.Intn(50)),
			UpdateTime: int64(100 + i),
		}
		if rng.Intn(3) == 0 {
			l.Body = []byte(fmt.Sprintf(`{"w":%d}`, i))
		}
		for k := 0; k < rng.Intn(3); k++ {
			l.Tags = append(l.Tags, fmt.Sprintf("tag%d", k))
		}
		v.out = append(v.out, l)
	}
	m := rng.Intn(maxLinks + 1)
	for i := 0; i < m; i++ {
		v.in = append(v.in, inLink{
			From:       fmt.Sprintf("dom/src-%03d", i),
			Name:       fmt.Sprintf("l%03d", rng.Intn(5)),
			Type:       fmt.Sprintf("t%d", rng.Intn(4)),
			UpdateTime: int64(200 + i),
		})
	}
	return v
}

// writeToTree кладёт вершину в дерево так, как это делает CRUD.
func writeToTree(cs *Store, v diffVertex) {
	if v.body != nil {
		cs.SetValue(v.id, v.body, false, 1)
	}
	for _, l := range v.out {
		cs.SetValue(fmt.Sprintf(kOutTo, v.id, l.Name), []byte(l.Type+"."+l.Target), false, l.UpdateTime)
		if l.Body != nil {
			cs.SetValue(fmt.Sprintf(kOutBody, v.id, l.Name), l.Body, false, l.UpdateTime)
		}
		cs.SetValue(fmt.Sprintf(kLinkType, v.id, l.Type, l.Target), []byte(l.Name), false, l.UpdateTime)
		cs.SetValue(fmt.Sprintf(kIndexType, v.id, l.Name, l.Type), nil, false, l.UpdateTime)
		for _, tag := range l.Tags {
			cs.SetValue(fmt.Sprintf(kIndexTag, v.id, l.Name, tag), nil, false, l.UpdateTime)
		}
	}
	for _, l := range v.in {
		cs.SetValue(fmt.Sprintf(kInLink, v.id, l.From, l.Name), []byte(l.Type), false, l.UpdateTime)
	}
}

func recordOf(v diffVertex) *vertexRecord {
	return newVertexRecord(vertexData{
		Body: v.body, BodyTime: 1, Out: v.out, In: v.in,
	}, defaultBucketLinks)
}

// allKeysOf перечисляет каждый ключ, который вершина занимает в дереве.
func allKeysOf(v diffVertex) []string {
	var keys []string
	if v.body != nil {
		keys = append(keys, v.id)
	}
	for _, l := range v.out {
		keys = append(keys, fmt.Sprintf(kOutTo, v.id, l.Name))
		if l.Body != nil {
			keys = append(keys, fmt.Sprintf(kOutBody, v.id, l.Name))
		}
		keys = append(keys, fmt.Sprintf(kLinkType, v.id, l.Type, l.Target))
		keys = append(keys, fmt.Sprintf(kIndexType, v.id, l.Name, l.Type))
		for _, tag := range l.Tags {
			keys = append(keys, fmt.Sprintf(kIndexTag, v.id, l.Name, tag))
		}
	}
	for _, l := range v.in {
		keys = append(keys, fmt.Sprintf(kInLink, v.id, l.From, l.Name))
	}
	sort.Strings(keys)
	return keys
}

func Test_Record_MatchesTree(t *testing.T) {
	rng := rand.New(rand.NewSource(20260904))

	for _, maxLinks := range []int{0, 1, 5, 40, 200} {
		t.Run(fmt.Sprintf("links<=%d", maxLinks), func(t *testing.T) {
			for iter := 0; iter < 40; iter++ {
				id := fmt.Sprintf("dom/v-%03d", iter)
				v := randomDiffVertex(rng, id, maxLinks)

				cs := NewStoreForTest("diff")
				writeToTree(cs, v)
				r := recordOf(v)

				// --- каждый существующий ключ отвечает одинаково
				for _, key := range allKeysOf(v) {
					_, tail := splitVertexKey(key)

					wantVal, wantErr := cs.GetValue(key)
					gotVal, gotOK := r.get(tail)
					require.NoError(t, wantErr, "дерево не знает свой же ключ %s", key)
					require.True(t, gotOK, "запись не знает ключ %s", key)
					require.Equal(t, len(wantVal), len(gotVal), "длина значения %s", key)
					if len(wantVal) > 0 {
						require.Equal(t, wantVal, gotVal, "значение %s", key)
					}

					require.Equal(t, cs.Exists(key), r.exists(tail), "существование %s", key)
					require.Equal(t, cs.GetValueUpdateTime(key), r.updateTime(tail),
						"время обновления %s", key)
				}

				// --- отсутствующие ключи одинаково отсутствуют
				for _, key := range []string{
					id + ".out.to.нет",
					id + ".out.body.нет",
					id + ".ltype.нет.нет",
					id + ".out.index.нет.type.нет",
					id + ".out.index.нет.tag.нет",
					id + ".in.нет.нет",
				} {
					_, tail := splitVertexKey(key)
					require.False(t, cs.Exists(key), "дерево нашло несуществующий %s", key)
					require.False(t, r.exists(tail), "запись нашла несуществующий %s", key)
					require.Equal(t, int64(-1), r.updateTime(tail), "время у несуществующего %s", key)
				}

				// --- перечисление по шаблонам
				patterns := []string{
					id + ".out.to.*",
					id + ".out.to.>",
					id + ".out.body.>",
					id + ".in.>",
					id + ".ltype.>",
					id + ".out.index.>",
					id + ".out.>",
				}
				for _, l := range v.out {
					patterns = append(patterns,
						fmt.Sprintf(kIndexTag, id, l.Name, ">"),
						fmt.Sprintf("%s.out.index.%s.>", id, l.Name))
				}
				for _, p := range patterns {
					want := cs.GetKeysByPattern(p)
					got := r.keysByPattern(id, p)
					sort.Strings(want)
					sort.Strings(got)
					require.Equal(t, want, got, "шаблон %s (вершина %s)", p, id)
				}
			}
		})
	}
}

// Test_Record_MatchesTree_AfterMutations — то же, но после случайной череды
// записей и удалений в обоих представлениях.
func Test_Record_MatchesTree_AfterMutations(t *testing.T) {
	rng := rand.New(rand.NewSource(4092026))
	const id = "dom/v"

	cs := NewStoreForTest("diffmut")
	r := newVertexRecord(vertexData{BodyTime: 0}, defaultBucketLinks)

	live := map[string]outLink{}
	now := int64(10)

	for step := 0; step < 3000; step++ {
		now++
		name := fmt.Sprintf("l%03d", rng.Intn(120))

		switch rng.Intn(4) {
		case 0, 1: // записать связь
			l := outLink{
				Name: name, Type: fmt.Sprintf("t%d", rng.Intn(3)),
				Target: fmt.Sprintf("dom/tgt-%02d", rng.Intn(20)), UpdateTime: now,
			}
			if prev, ok := live[name]; ok {
				// у старой связи ключи по типу/цели надо снять, как это делает CRUD
				cs.DeleteValue(fmt.Sprintf(kLinkType, id, prev.Type, prev.Target), false, now)
				cs.DeleteValue(fmt.Sprintf(kIndexType, id, prev.Name, prev.Type), false, now)
			}
			cs.SetValue(fmt.Sprintf(kOutTo, id, l.Name), []byte(l.Type+"."+l.Target), false, now)
			cs.SetValue(fmt.Sprintf(kLinkType, id, l.Type, l.Target), []byte(l.Name), false, now)
			cs.SetValue(fmt.Sprintf(kIndexType, id, l.Name, l.Type), nil, false, now)
			require.True(t, r.putOutLink(l))
			live[name] = l

		case 2: // удалить связь
			prev, ok := live[name]
			if !ok {
				continue
			}
			cs.DeleteValue(fmt.Sprintf(kOutTo, id, prev.Name), false, now)
			cs.DeleteValue(fmt.Sprintf(kLinkType, id, prev.Type, prev.Target), false, now)
			cs.DeleteValue(fmt.Sprintf(kIndexType, id, prev.Name, prev.Type), false, now)
			require.True(t, r.deleteOutLink(prev.Name, now))
			delete(live, name)

		case 3: // запоздавшая запись — обязана быть отвергнута обоими
			prev, ok := live[name]
			if !ok {
				continue
			}
			stale := prev
			stale.Target = "dom/stale"
			stale.UpdateTime = prev.UpdateTime - 1
			require.False(t, r.putOutLink(stale), "шаг %d: запись сквозь приняла старое время", step)
		}

		if step%97 != 0 {
			continue
		}
		// сверяем оба представления целиком
		for n := 0; n < 120; n++ {
			nm := fmt.Sprintf("l%03d", n)
			key := fmt.Sprintf(kOutTo, id, nm)
			_, tail := splitVertexKey(key)
			require.Equal(t, cs.Exists(key), r.exists(tail), "шаг %d: существование %s", step, key)
			if cs.Exists(key) {
				wantVal, _ := cs.GetValue(key)
				gotVal, _ := r.get(tail)
				require.Equal(t, wantVal, gotVal, "шаг %d: значение %s", step, key)
			}
			require.Equal(t, cs.GetValueUpdateTime(key), r.updateTime(tail),
				"шаг %d: время %s", step, key)
		}
	}
}
