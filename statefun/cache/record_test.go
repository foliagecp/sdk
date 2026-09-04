package cache

import (
	"fmt"
	"math/rand"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func mkOut(i int) outLink {
	t := int64(i + 1)
	lt := fmt.Sprintf("type-%d", i%7)
	tgt := fmt.Sprintf("dom/obj-%05d", i)
	return outLink{
		Name:     fmt.Sprintf("link-%05d", i),
		To:       subValue{Value: lt + "." + tgt, Time: t, Live: true},
		IdxTypes: []subValue{{Value: lt, Time: t, Live: true}},
	}
}

// putOutLinkForTest writes every key of a link, the way CRUD does.
func putOutLinkForTest(r *vertexRecord, l outLink) bool {
	ok := r.setOutTo(l.Name, l.To.Value, l.To.Time)
	for _, it := range l.IdxTypes {
		r.setOutIndexType(l.Name, it.Value, it.Time, it.Live)
	}
	if l.Body.Live {
		r.setOutBody(l.Name, []byte(l.Body.Value), l.Body.Time)
	}
	for _, tg := range l.Tags {
		r.setOutTag(l.Name, tg.Value, tg.Time, tg.Live)
	}
	lt, tgt := splitTypeTarget(l.To.Value)
	r.putPair(pairEntry{Type: lt, Target: tgt, Name: l.Name, UpdateTime: l.To.Time})
	return ok
}

// deleteOutLinkForTest removes every key of a link.
func deleteOutLinkForTest(r *vertexRecord, l outLink, t int64) bool {
	lt, tgt := splitTypeTarget(l.To.Value)
	r.deletePair(lt, tgt, t)
	for _, it := range l.IdxTypes {
		r.setOutIndexType(l.Name, it.Value, t, false)
	}
	r.deleteOutBody(l.Name, t)
	return r.deleteOutTo(l.Name, t)
}

func mkIn(i int) inLink {
	return inLink{
		From:       fmt.Sprintf("dom/src-%05d", i),
		Name:       fmt.Sprintf("link-%05d", i),
		Type:       fmt.Sprintf("type-%d", i%7),
		UpdateTime: int64(i + 1),
	}
}

func buildRecord(t testing.TB, n int) *vertexRecord {
	t.Helper()
	v := vertexData{Body: []byte(`{"name":"v","kind":"k"}`), BodyTime: 1}
	for i := 0; i < n; i++ {
		v.Out = append(v.Out, mkOut(i))
		v.In = append(v.In, mkIn(i))
	}
	return newVertexRecord(v, defaultBucketLinks)
}

// ---------------------------------------------------------------------------
// reading
// ---------------------------------------------------------------------------

func Test_Record_RoundTrip(t *testing.T) {
	for _, n := range []int{0, 1, 8, 31, 32, 33, 500, 5000} {
		t.Run(fmt.Sprintf("links=%d", n), func(t *testing.T) {
			r := buildRecord(t, n)

			body, bt, ok := r.bodyBytes()
			require.True(t, ok)
			require.Equal(t, `{"name":"v","kind":"k"}`, body)
			require.Equal(t, int64(1), bt)

			for i := 0; i < n; i++ {
				want := mkOut(i)
				got, ok := r.lookupOutLink(want.Name)
				require.True(t, ok, "исходящая %s", want.Name)
				require.Equal(t, want.To.Value, got.To.Value)
				require.Equal(t, want.To.Time, got.To.Time)

				lt, tgt, ok := r.lookupOutTarget(want.Name)
				require.True(t, ok)
				wlt, wtgt := splitTypeTarget(want.To.Value)
				require.Equal(t, wlt, lt)
				require.Equal(t, wtgt, tgt)

				wi := mkIn(i)
				gi, ok := r.lookupInLink(wi.From, wi.Name)
				require.True(t, ok, "входящая %s", wi.From)
				require.Equal(t, wi.Type, gi.Type)
				require.Equal(t, wi.UpdateTime, gi.UpdateTime)
			}

			_, ok = r.lookupOutLink("нет такой")
			require.False(t, ok)
			_, ok = r.lookupInLink("нет", "такой")
			require.False(t, ok)

			// перечисление отдаёт ровно то, что положили, по одному разу
			seen := map[string]int{}
			r.rangeOutLinks(func(l outLink) bool { seen[l.Name]++; return true })
			require.Len(t, seen, n)
			for _, c := range seen {
				require.Equal(t, 1, c, "связь перечислена дважды")
			}

			seenIn := map[string]int{}
			r.rangeInLinks(func(l inLink) bool { seenIn[l.From+"/"+l.Name]++; return true })
			require.Len(t, seenIn, n)
			for _, c := range seenIn {
				require.Equal(t, 1, c)
			}
		})
	}
}

func Test_Record_LookupByTypeTarget(t *testing.T) {
	r := buildRecord(t, 100)
	want := mkOut(42)
	wlt, wtgt := splitTypeTarget(want.To.Value)
	got, ok := r.lookupPair(wlt, wtgt)
	require.True(t, ok)
	require.Equal(t, want.Name, got.Name)

	_, ok = r.lookupPair(wlt, "dom/нет")
	require.False(t, ok)
}

// ---------------------------------------------------------------------------
// writing
// ---------------------------------------------------------------------------

func Test_Record_WriteThrough(t *testing.T) {
	r := buildRecord(t, 10)

	// новая связь
	nl := outLink{Name: "link-new", To: subValue{Value: "t.dom/x", Time: 100, Live: true}}
	require.True(t, putOutLinkForTest(r, nl))
	got, ok := r.lookupOutLink("link-new")
	require.True(t, ok)
	require.Equal(t, "dom/x", got.target())

	// замена существующей
	upd := mkOut(3)
	upd.To = subValue{Value: "type-3.dom/moved", Time: 1000, Live: true}
	require.True(t, putOutLinkForTest(r, upd))
	got, _ = r.lookupOutLink(upd.Name)
	require.Equal(t, "dom/moved", got.target())

	// удаление
	require.True(t, deleteOutLinkForTest(r, upd, 2000))
	_, ok = r.lookupOutLink(upd.Name)
	require.False(t, ok, "удалённая связь не должна читаться")

	// остальные на месте
	for i := 0; i < 10; i++ {
		if i == 3 {
			continue
		}
		_, ok := r.lookupOutLink(mkOut(i).Name)
		require.True(t, ok, "связь %d потерялась", i)
	}
}

// Test_Record_LastWriterWins — страж, ради которого существуют надгробия.
func Test_Record_LastWriterWins(t *testing.T) {
	r := buildRecord(t, 4)
	name := mkOut(1).Name

	// запись со временем старше текущего игнорируется
	old := mkOut(1)
	old.To = subValue{Value: "type-1.dom/stale", Time: 0, Live: true}
	require.False(t, putOutLinkForTest(r, old), "старая запись не должна применяться")
	got, _ := r.lookupOutLink(name)
	require.Equal(t, mkOut(1).target(), got.target())

	// после удаления запоздавшая запись не воскрешает связь
	require.True(t, deleteOutLinkForTest(r, mkOut(1), 500))
	late := mkOut(1)
	late.To.Time = 499
	require.False(t, putOutLinkForTest(r, late), "запоздавшая запись воскресила удалённую связь")
	_, ok := r.lookupOutLink(name)
	require.False(t, ok)

	// а более новая — воскрешает, это законно
	fresh := mkOut(1)
	fresh.To.Time = 501
	require.True(t, putOutLinkForTest(r, fresh))
	_, ok = r.lookupOutLink(name)
	require.True(t, ok)

	// тело: тот же страж
	require.False(t, r.putBody([]byte(`{"a":1}`), 0, true))
	require.True(t, r.putBody([]byte(`{"a":1}`), 10, true))
	require.True(t, r.deleteBody(20))
	_, _, ok = r.bodyBytes()
	require.False(t, ok)
	require.False(t, r.putBody([]byte(`{"a":2}`), 19, true), "запоздавшая запись воскресила тело")
	require.True(t, r.putBody([]byte(`{"a":2}`), 21, true))
	body, _, ok := r.bodyBytes()
	require.True(t, ok)
	require.Equal(t, `{"a":2}`, body)
}

// Test_Record_SplitKeepsEverything — расщепление не теряет связей.
func Test_Record_SplitKeepsEverything(t *testing.T) {
	r := newVertexRecord(vertexData{BodyTime: 1}, defaultBucketLinks)

	const n = 5000
	for i := 0; i < n; i++ {
		require.True(t, putOutLinkForTest(r, mkOut(i)), "исходящая %d", i)
		require.True(t, r.putInLink(mkIn(i)), "входящая %d", i)
	}
	require.Greater(t, int(r.out.Load().depth), 0, "справочник обязан был вырасти")

	for i := 0; i < n; i++ {
		got, ok := r.lookupOutLink(mkOut(i).Name)
		require.True(t, ok, "после расщепления потерялась исходящая %d", i)
		require.Equal(t, mkOut(i).target(), got.target())

		wi := mkIn(i)
		_, ok = r.lookupInLink(wi.From, wi.Name)
		require.True(t, ok, "после расщепления потерялась входящая %d", i)
	}

	// перечисление — ровно n, без повторов (корзины делят слоты справочника)
	seen := map[string]int{}
	r.rangeOutLinks(func(l outLink) bool { seen[l.Name]++; return true })
	require.Len(t, seen, n)
	for name, c := range seen {
		require.Equal(t, 1, c, "%s перечислена %d раз", name, c)
	}

	// ни одна корзина не осталась переполненной
	r.out.Load().each(func(b *bucket) bool {
		require.LessOrEqual(t, b.entryCount(), 2*defaultBucketLinks,
			"корзина осталась вдвое больше предела — расщепление не сработало")
		return true
	})
}

// Test_Record_ConcurrentWritesLoseNothing — тест на потерю обновления.
//
// Проверяется счётчиками, а не гонкой: два писателя, изменившие разные связи
// одной корзины, оба декодируют старый блок, и второй затирает первого. Гонки
// данных при этом нет, поэтому -race такую ошибку не видит.
func Test_Record_ConcurrentWritesLoseNothing(t *testing.T) {
	r := newVertexRecord(vertexData{BodyTime: 1}, defaultBucketLinks)

	const goroutines = 64
	const perGoroutine = 200
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				id := g*perGoroutine + i
				if !putOutLinkForTest(r, mkOut(id)) {
					t.Errorf("исходящая %d отвергнута", id)
					return
				}
				if !r.putInLink(mkIn(id)) {
					t.Errorf("входящая %d отвергнута", id)
					return
				}
			}
		}(g)
	}
	wg.Wait()

	total := goroutines * perGoroutine
	for i := 0; i < total; i++ {
		got, ok := r.lookupOutLink(mkOut(i).Name)
		require.True(t, ok, "потеряна исходящая %d", i)
		require.Equal(t, mkOut(i).target(), got.target())

		wi := mkIn(i)
		_, ok = r.lookupInLink(wi.From, wi.Name)
		require.True(t, ok, "потеряна входящая %d", i)
	}

	count := 0
	r.rangeOutLinks(func(outLink) bool { count++; return true })
	require.Equal(t, total, count, "перечисление не сходится с числом записанных связей")
}

// Test_Record_ConcurrentReadWrite — читатели работают без локов, пока корзины
// подменяются под ними.
func Test_Record_ConcurrentReadWrite(t *testing.T) {
	r := buildRecord(t, 200)

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(seed)))
			for {
				select {
				case <-stop:
					return
				default:
				}
				n := rng.Intn(200)
				if _, _, ok := r.lookupOutTarget(mkOut(n).Name); !ok {
					t.Errorf("читатель не нашёл связь %d", n)
					return
				}
			}
		}(i)
	}

	for i := 200; i < 2000; i++ {
		require.True(t, putOutLinkForTest(r, mkOut(i)))
	}
	close(stop)
	wg.Wait()

	for i := 0; i < 2000; i++ {
		_, ok := r.lookupOutLink(mkOut(i).Name)
		require.True(t, ok, "потеряна связь %d", i)
	}
}

// ---------------------------------------------------------------------------
// стоимость
// ---------------------------------------------------------------------------

// Benchmark_Record_Write — Ф-5: стоимость записи не зависит от степени.
func Benchmark_Record_Write(b *testing.B) {
	for _, n := range []int{8, 1000, 10691} {
		b.Run(fmt.Sprintf("degree=%d", n), func(b *testing.B) {
			r := buildRecord(b, n)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				l := mkOut(i % n)
				l.To.Time = int64(1_000_000 + i)
				if !putOutLinkForTest(r, l) {
					b.Fatal("отвергнуто")
				}
			}
		})
	}
}

func Benchmark_Record_Read(b *testing.B) {
	for _, n := range []int{8, 1000, 10691} {
		b.Run(fmt.Sprintf("degree=%d", n), func(b *testing.B) {
			r := buildRecord(b, n)
			names := make([]string, n)
			for i := range names {
				names[i] = mkOut(i).Name
			}
			sort.Strings(names)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, _, ok := r.lookupOutTarget(names[i%n]); !ok {
					b.Fatal("не найдено")
				}
			}
		})
	}
}

// Test_Record_CompactBuckets — уплотнение возвращает память и не меняет ответов.
func Test_Record_CompactBuckets(t *testing.T) {
	r := newVertexRecord(vertexData{BodyTime: 1}, defaultBucketLinks)
	const n = 2000
	for i := 0; i < n; i++ {
		require.True(t, putOutLinkForTest(r, mkOut(i)))
		require.True(t, r.putInLink(mkIn(i)))
	}
	require.Greater(t, r.dirtyBuckets(), 0, "записи обязаны были оставить корзины разобранными")

	before := map[string]outLink{}
	r.rangeOutLinks(func(l outLink) bool { before[l.Name] = l; return true })

	encoded := r.compactBuckets()
	require.Greater(t, encoded, 0)
	require.Equal(t, 0, r.dirtyBuckets(), "после уплотнения разобранных корзин быть не должно")

	after := map[string]outLink{}
	r.rangeOutLinks(func(l outLink) bool { after[l.Name] = l; return true })
	require.Equal(t, len(before), len(after))
	for name, l := range before {
		got, ok := after[name]
		require.True(t, ok, "%s пропала при уплотнении", name)
		require.Equal(t, l.To.Value, got.To.Value)
		require.Equal(t, l.To.Time, got.To.Time)
	}
	for i := 0; i < n; i++ {
		_, ok := r.lookupOutLink(mkOut(i).Name)
		require.True(t, ok, "после уплотнения не читается %d", i)
		wi := mkIn(i)
		_, ok = r.lookupInLink(wi.From, wi.Name)
		require.True(t, ok)
	}

	// повторное уплотнение — ничего не делает
	require.Equal(t, 0, r.compactBuckets())
}

// Test_Record_CompactionReturnsMemory — уплотнение возвращает память.
//
// Утверждение на детерминированной оценке, а не на размере кучи: замер кучи
// внутри одного процесса зависит от того, что успел сделать сборщик, и такой
// тест падает через раз. approxBytes считает то, что действительно хранится, —
// и это же та оценка, которой пользуется регулятор бюджета.
func Test_Record_CompactionReturnsMemory(t *testing.T) {
	for _, links := range []int{40, 400, 4000} {
		t.Run(fmt.Sprintf("links=%d", links), func(t *testing.T) {
			r := newVertexRecord(vertexData{Body: []byte(`{"n":1}`), BodyTime: 1}, defaultBucketLinks)
			for i := 0; i < links; i++ {
				require.True(t, putOutLinkForTest(r, mkOut(i)))
			}
			dirty := r.approxBytes()
			require.Greater(t, r.dirtyBuckets(), 0)

			r.compactBuckets()
			clean := r.approxBytes()

			t.Logf("%d связей: разобранные %d B, уплотнённые %d B, %.2fx",
				links, dirty, clean, float64(dirty)/float64(clean))
			require.Less(t, clean, dirty, "уплотнение обязано возвращать память")
			require.Less(t, float64(clean), 0.6*float64(dirty),
				"уплотнение вернуло меньше 40 %% — форма записи не окупается")
		})
	}
}

func runtimeGC()             { runtime.GC(); runtime.GC() }
func runtimeKeepAlive(v any) { runtime.KeepAlive(v) }
func heapAllocNow() uint64   { var ms runtime.MemStats; runtime.ReadMemStats(&ms); return ms.HeapAlloc }

// Test_Record_PairKeyIsIndependent — ключ (тип, цель) → имя живёт своей жизнью.
//
// Ради этого он и хранится отдельной таблицей: CRUD удаляет его сам, когда у
// связи меняется тип или цель, и вывести такое состояние из таблицы связей
// нельзя — вывод нашёл бы живую связь и вернул её имя.
func Test_Record_PairKeyIsIndependent(t *testing.T) {
	r := newVertexRecord(vertexData{BodyTime: 1}, defaultBucketLinks)

	l := outLink{Name: "l1", To: subValue{Value: "t1.dom/a", Time: 10, Live: true}}
	require.True(t, putOutLinkForTest(r, l))
	require.True(t, r.putPair(pairEntry{Type: "t1", Target: "dom/a", Name: "l1", UpdateTime: 10}))

	p, ok := r.lookupPair("t1", "dom/a")
	require.True(t, ok)
	require.Equal(t, "l1", p.Name)

	// удалить пару, оставив связь живой
	require.True(t, r.deletePair("t1", "dom/a", 20))
	_, ok = r.lookupPair("t1", "dom/a")
	require.False(t, ok, "ключ пары обязан исчезнуть")
	_, ok = r.lookupOutLink("l1")
	require.True(t, ok, "связь удалять никто не просил")

	// страж действует и здесь
	require.False(t, r.putPair(pairEntry{Type: "t1", Target: "dom/a", Name: "l1", UpdateTime: 19}),
		"запоздавшая запись воскресила ключ пары")
	require.True(t, r.putPair(pairEntry{Type: "t1", Target: "dom/a", Name: "l2", UpdateTime: 21}))
	p, ok = r.lookupPair("t1", "dom/a")
	require.True(t, ok)
	require.Equal(t, "l2", p.Name, "победить обязана последняя запись")

	// две связи на одну пару: в дереве остаётся имя последней
	require.True(t, r.setOutTo("a", "t2.dom/b", 30))
	require.True(t, r.putPair(pairEntry{Type: "t2", Target: "dom/b", Name: "a", UpdateTime: 30}))
	require.True(t, r.setOutTo("b", "t2.dom/b", 31))
	require.True(t, r.putPair(pairEntry{Type: "t2", Target: "dom/b", Name: "b", UpdateTime: 31}))
	p, _ = r.lookupPair("t2", "dom/b")
	require.Equal(t, "b", p.Name)
	// обе связи при этом на месте
	_, ok = r.lookupOutLink("a")
	require.True(t, ok)
	_, ok = r.lookupOutLink("b")
	require.True(t, ok)
}

// parseTailBySplit — прежняя реализация через strings.Split, оставленная как
// эталон: новая обязана отвечать ровно то же на любом хвосте.
func parseTailBySplit(tail string) (tailKind, string, string) {
	if tail == "" {
		return tailBody, "", ""
	}
	t := strings.Split(tail, ".")
	switch t[0] {
	case "out":
		if len(t) < 3 {
			return tailUnknown, "", ""
		}
		switch t[1] {
		case "to":
			if len(t) == 3 {
				return tailOutTo, t[2], ""
			}
		case "body":
			if len(t) == 3 {
				return tailOutBody, t[2], ""
			}
		case "index":
			if len(t) == 5 {
				switch t[3] {
				case "type":
					return tailIndexType, t[2], t[4]
				case "tag":
					return tailIndexTag, t[2], t[4]
				}
			}
		}
	case "ltype":
		if len(t) == 3 {
			return tailLinkType, t[1], t[2]
		}
	case "in":
		if len(t) == 3 {
			return tailIn, t[1], t[2]
		}
	}
	return tailUnknown, "", ""
}

func Test_ParseTail_MatchesSplit(t *testing.T) {
	tokens := []string{"", "out", "in", "to", "body", "index", "type", "tag", "ltype",
		"l001", "dom/v-1", "__object", "имя", "a.b", "."}
	var tails []string
	// все хвосты длиной до четырёх токенов плюс настоящие формы
	var build func(prefix string, depth int)
	build = func(prefix string, depth int) {
		if depth == 0 {
			tails = append(tails, prefix)
			return
		}
		for _, tk := range tokens[:9] {
			next := tk
			if prefix != "" {
				next = prefix + "." + tk
			}
			build(next, depth-1)
		}
	}
	build("", 3)
	for _, a := range tokens {
		for _, b := range tokens {
			tails = append(tails,
				"out.to."+a, "out.body."+a, "ltype."+a+"."+b, "in."+a+"."+b,
				"out.index."+a+".type."+b, "out.index."+a+".tag."+b,
				"out.index."+a+"."+b, "out."+a+"."+b, a+"."+b)
		}
	}
	for _, tail := range tails {
		wk, wa, wb := parseTailBySplit(tail)
		gk, ga, gb := parseTail(tail)
		require.Equalf(t, wk, gk, "вид хвоста %q", tail)
		require.Equalf(t, wa, ga, "первый токен %q", tail)
		require.Equalf(t, wb, gb, "второй токен %q", tail)
	}
	t.Logf("сверено хвостов: %d", len(tails))
}

func Benchmark_ParseTail(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		parseTail("out.index.l00042.type.__object")
	}
}
func Benchmark_ParseTailBySplit(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		parseTailBySplit("out.index.l00042.type.__object")
	}
}
