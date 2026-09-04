package cache

import (
	"fmt"
	"math/rand"
	"runtime"
	"sort"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func mkOut(i int) outLink {
	return outLink{
		Name:       fmt.Sprintf("link-%05d", i),
		Type:       fmt.Sprintf("type-%d", i%7),
		Target:     fmt.Sprintf("dom/obj-%05d", i),
		UpdateTime: int64(i + 1),
	}
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
				require.Equal(t, want.Type, got.Type)
				require.Equal(t, want.Target, got.Target)
				require.Equal(t, want.UpdateTime, got.UpdateTime)

				lt, tgt, ok := r.lookupOutTarget(want.Name)
				require.True(t, ok)
				require.Equal(t, want.Type, lt)
				require.Equal(t, want.Target, tgt)

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
	got, ok := r.lookupOutLinkByTypeTarget(want.Type, want.Target)
	require.True(t, ok)
	require.Equal(t, want.Name, got.Name)

	_, ok = r.lookupOutLinkByTypeTarget(want.Type, "dom/нет")
	require.False(t, ok)
}

// ---------------------------------------------------------------------------
// writing
// ---------------------------------------------------------------------------

func Test_Record_WriteThrough(t *testing.T) {
	r := buildRecord(t, 10)

	// новая связь
	nl := outLink{Name: "link-new", Type: "t", Target: "dom/x", UpdateTime: 100}
	require.True(t, r.putOutLink(nl))
	got, ok := r.lookupOutLink("link-new")
	require.True(t, ok)
	require.Equal(t, "dom/x", got.Target)

	// замена существующей
	upd := mkOut(3)
	upd.Target = "dom/moved"
	upd.UpdateTime = 1000
	require.True(t, r.putOutLink(upd))
	got, _ = r.lookupOutLink(upd.Name)
	require.Equal(t, "dom/moved", got.Target)

	// удаление
	require.True(t, r.deleteOutLink(upd.Name, 2000))
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
	old.Target = "dom/stale"
	old.UpdateTime = 0
	require.False(t, r.putOutLink(old), "старая запись не должна применяться")
	got, _ := r.lookupOutLink(name)
	require.Equal(t, mkOut(1).Target, got.Target)

	// после удаления запоздавшая запись не воскрешает связь
	require.True(t, r.deleteOutLink(name, 500))
	late := mkOut(1)
	late.UpdateTime = 499
	require.False(t, r.putOutLink(late), "запоздавшая запись воскресила удалённую связь")
	_, ok := r.lookupOutLink(name)
	require.False(t, ok)

	// а более новая — воскрешает, это законно
	fresh := mkOut(1)
	fresh.UpdateTime = 501
	require.True(t, r.putOutLink(fresh))
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
		require.True(t, r.putOutLink(mkOut(i)), "исходящая %d", i)
		require.True(t, r.putInLink(mkIn(i)), "входящая %d", i)
	}
	require.Greater(t, int(r.out.Load().depth), 0, "справочник обязан был вырасти")

	for i := 0; i < n; i++ {
		got, ok := r.lookupOutLink(mkOut(i).Name)
		require.True(t, ok, "после расщепления потерялась исходящая %d", i)
		require.Equal(t, mkOut(i).Target, got.Target)

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
				if !r.putOutLink(mkOut(id)) {
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
		require.Equal(t, mkOut(i).Target, got.Target)

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
		require.True(t, r.putOutLink(mkOut(i)))
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
				l.UpdateTime = int64(1_000_000 + i)
				if !r.putOutLink(l) {
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
		require.True(t, r.putOutLink(mkOut(i)))
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
		require.Equal(t, l.Target, got.Target)
		require.Equal(t, l.UpdateTime, got.UpdateTime)
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
				require.True(t, r.putOutLink(mkOut(i)))
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
