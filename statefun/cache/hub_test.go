package cache

// Тест 3 из ТЗ: хаб.
//
// В настоящем графе есть вершина `objects`, к которой сходится всё — на
// эталонном дампе 10 692 связи. Формат записи держит связи в корзинах именно
// ради неё: без корзин запись одной связи в такую вершину переписывала бы всю
// вершину, и цена записи росла бы вместе с графом.
//
// Здесь проверяется, что этого не происходит: цена записи в хаб сравнима с
// ценой записи в вершину об одной корзине, поток записей в хаб не отстаёт от
// дерева, и ни одна связь при этом не теряется.

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// hubSize — размер хаба `objects` на эталонном дампе.
const hubSize = 10692

func fillHub(cs *Store, id string, links int) {
	for i := 0; i < links; i++ {
		cs.SetValue(fmt.Sprintf("%s.out.to.l%05d", id, i), []byte("t.dom/tgt"), false, int64(i+1))
	}
}

// perWrite — цена одной записи связи, лучшая из трёх попыток.
func perWrite(cs *Store, id string, from, count int) time.Duration {
	best := time.Duration(1<<62 - 1)
	for attempt := 0; attempt < 3; attempt++ {
		base := from + attempt*count
		started := time.Now()
		for i := 0; i < count; i++ {
			cs.SetValue(fmt.Sprintf("%s.out.to.n%07d", id, base+i),
				[]byte("t.dom/tgt"), false, int64(1_000_000+base+i))
		}
		if d := time.Since(started) / time.Duration(count); d < best {
			best = d
		}
	}
	return best
}

// Test_Hub_WriteCostDoesNotGrowWithTheHub — запись связи в хаб из 10 692 связей
// не должна стоить заметно больше записи в вершину об одной корзине.
func Test_Hub_WriteCostDoesNotGrowWithTheHub(t *testing.T) {
	restore := SetCacheModeForTest("records")
	defer restore()

	small := NewStoreForTest("hub_small")
	fillHub(small, "dom/small", defaultBucketLinks)
	big := NewStoreForTest("hub_big")
	fillHub(big, "dom/big", hubSize)

	perSmall := perWrite(small, "dom/small", 0, 2000)
	perBig := perWrite(big, "dom/big", 0, 2000)
	t.Logf("вершина из %d связей: %s на запись", defaultBucketLinks, perSmall)
	t.Logf("хаб из %d связей:  %s на запись", hubSize, perBig)

	// Хаб в 334 раза больше. Требование ТЗ — «не больше»; с запасом на шум
	// машины и на то, что справочник хаба глубже, берём трёхкратный порог:
	// перезапись всей вершины дала бы три сотни.
	require.Lessf(t, perBig, perSmall*3,
		"запись в хаб (%s) дороже записи в маленькую вершину (%s) — цена растёт с размером вершины",
		perBig, perSmall)

	// Н-4: не более 100 мкс сверх сегодняшней записи.
	require.Lessf(t, perBig-perSmall, 100*time.Microsecond,
		"Н-4: запись в хаб дороже на %s", perBig-perSmall)
}

// Test_Hub_SequentialWritesKeepEverything — десять тысяч последовательных
// записей в хаб: все они на месте, ни одна не потеряна при расщеплениях.
func Test_Hub_SequentialWritesKeepEverything(t *testing.T) {
	restore := SetCacheModeForTest("records")
	defer restore()

	cs := NewStoreForTest("hub_seq")
	const writes = 10000
	started := time.Now()
	for i := 0; i < writes; i++ {
		cs.SetValue(fmt.Sprintf("dom/hub.out.to.l%05d", i),
			[]byte(fmt.Sprintf("t.dom/tgt-%05d", i)), false, int64(i+1))
	}
	took := time.Since(started)

	for i := 0; i < writes; i++ {
		key := fmt.Sprintf("dom/hub.out.to.l%05d", i)
		v, err := cs.GetValue(key)
		require.NoErrorf(t, err, "связь %d потеряна", i)
		require.Equalf(t, fmt.Sprintf("t.dom/tgt-%05d", i), string(v), "связь %d испорчена", i)
	}
	require.Len(t, cs.GetKeysByPattern("dom/hub.out.to.>"), writes,
		"перечисление обязано вернуть все связи хаба")
	t.Logf("%d последовательных записей в хаб за %s (%s на запись)",
		writes, took.Round(time.Millisecond), (took / writes).Round(time.Nanosecond))
}

// Test_Hub_ConcurrentThroughput — 64 горутины пишут различимые связи в один
// хаб, в том числе в одну корзину. Ничего не теряется, и поток записей не
// отстаёт от дерева более чем на пятую часть.
func Test_Hub_ConcurrentThroughput(t *testing.T) {
	const goroutines, perGoroutine = 64, 200
	const total = goroutines * perGoroutine

	run := func(mode string, prefill int) time.Duration {
		restore := SetCacheModeForTest(mode)
		defer restore()

		best := time.Duration(1<<62 - 1)
		for attempt := 0; attempt < 3; attempt++ {
			cs := NewStoreForTest("hub_conc")
			fillHub(cs, "dom/hub", prefill)

			var wg sync.WaitGroup
			started := time.Now()
			for g := 0; g < goroutines; g++ {
				wg.Add(1)
				go func(g int) {
					defer wg.Done()
					for i := 0; i < perGoroutine; i++ {
						// Имена чередуются так, что соседние по номеру
						// попадают в разные корзины, а одинаковые по остатку —
						// в одну: нужны обе ситуации сразу.
						name := fmt.Sprintf("c%03d_%04d", g, i)
						cs.SetValue("dom/hub.out.to."+name,
							[]byte(fmt.Sprintf("t.dom/%s", name)), false, int64(2_000_000+g*perGoroutine+i))
					}
				}(g)
			}
			wg.Wait()
			took := time.Since(started)

			// ничего не потеряно
			missing := 0
			for g := 0; g < goroutines; g++ {
				for i := 0; i < perGoroutine; i++ {
					name := fmt.Sprintf("c%03d_%04d", g, i)
					v, err := cs.GetValue("dom/hub.out.to." + name)
					if err != nil || string(v) != fmt.Sprintf("t.dom/%s", name) {
						missing++
					}
				}
			}
			require.Zerof(t, missing, "режим %s: потеряно или испорчено %d связей из %d", mode, missing, total)
			if took < best {
				best = took
			}
		}
		return best
	}

	prefill := defaultBucketLinks * 4
	if v := os.Getenv("HUB_PREFILL"); v != "" {
		fmt.Sscanf(v, "%d", &prefill)
	}
	tree := run("tree", prefill)
	records := run("records", prefill)
	t.Logf("%d горутин по %d связей в один хаб:", goroutines, perGoroutine)
	t.Logf("  дерево:  %s (%s на запись)", tree.Round(time.Millisecond), (tree / total).Round(time.Nanosecond))
	t.Logf("  записи:  %s (%s на запись)", records.Round(time.Millisecond), (records / total).Round(time.Nanosecond))

	// ТЗ: пропускная способность не ниже 80 % сегодняшней, то есть время не
	// более чем на четверть больше.
	require.Lessf(t, records, tree*5/4,
		"пропускная способность записей (%s) ниже 80%% от дерева (%s)", records, tree)
}
