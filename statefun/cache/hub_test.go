package cache

// Тест 3 из ТЗ: хаб.
//
// В настоящем графе есть вершина `objects`, к которой сходится всё — на
// эталонном дампе 10 692 связи. Формат записи держит связи в корзинах именно
// ради неё: без корзин запись одной связи в такую вершину переписывала бы всю
// вершину, и цена записи росла бы вместе с графом.
//
// Здесь проверяется, что этого не происходит: цена записи в хаб сравнима с
// ценой записи в вершину об одной корзине, и ни одна связь при конкурентной
// записи не теряется. Что поток записей в хаб не отстаёт от дерева, проверяет
// гейт Test_Hub_ConcurrentThroughputGate в hub_perf_test.go (тег perf): это
// замер, а замеру нужны условия, которых у корректностного прогона под -race нет.

import (
	"fmt"
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

// hubBurst — goroutines горутин пишут по perGoroutine различимых связей в хаб
// dom/hub. Имена чередуются так, что соседние по номеру попадают в разные
// корзины, а одинаковые по остатку — в одну: нужны обе ситуации сразу.
func hubBurst(cs *Store, goroutines, perGoroutine int) {
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				name := fmt.Sprintf("c%03d_%04d", g, i)
				cs.SetValue("dom/hub.out.to."+name,
					[]byte(fmt.Sprintf("t.dom/%s", name)), false, int64(2_000_000+g*perGoroutine+i))
			}
		}(g)
	}
	wg.Wait()
}

// hubBurstMissing — сколько связей всплеска потеряно или испорчено.
func hubBurstMissing(cs *Store, goroutines, perGoroutine int) int {
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
	return missing
}

// Test_Hub_ConcurrentWritesKeepEverything — 64 горутины пишут различимые связи
// в один хаб, в том числе в одну корзину, в обоих режимах: ничего не теряется.
//
// Время здесь не меряется. Прежний Test_Hub_ConcurrentThroughput сравнивал
// режимы по одному замеру в ~150 мс под race detector и падал от загрузки
// машины, а не от кода; сравнение скорости теперь делает
// Test_Hub_ConcurrentThroughputGate (hub_perf_test.go, тег perf), этот тест
// оставляет корректность там, где она нужнее всего, — под -race.
func Test_Hub_ConcurrentWritesKeepEverything(t *testing.T) {
	const goroutines, perGoroutine = 64, 200
	for _, mode := range []string{"tree", "records"} {
		func() {
			restore := SetCacheModeForTest(mode)
			defer restore()
			for attempt := 0; attempt < 3; attempt++ {
				cs := NewStoreForTest("hub_conc")
				fillHub(cs, "dom/hub", defaultBucketLinks*4)
				hubBurst(cs, goroutines, perGoroutine)
				missing := hubBurstMissing(cs, goroutines, perGoroutine)
				require.Zerof(t, missing, "режим %s: потеряно или испорчено %d связей из %d",
					mode, missing, goroutines*perGoroutine)
			}
		}()
	}
}
