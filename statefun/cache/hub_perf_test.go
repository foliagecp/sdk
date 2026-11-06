//go:build perf

package cache

// Тест 3 из ТЗ, вторая половина: поток записей в хаб не отстаёт от дерева.
//
// Порог прежний: записи не медленнее дерева больше чем на четверть, то есть
// пропускная способность не ниже 80 %. Меняется способ замера, потому что
// прежний (Test_Hub_ConcurrentThroughput до 0.5.9) не отличал код от машины:
//
//   - Один замер на режим, лучший из трёх, сначала дерево, потом записи. Окно —
//     около 150 мс под race detector и около 10 мс без него: короче или равно
//     периоду планировщика, так что замедление машины в момент одного замера
//     целиком доставалось одному режиму.
//   - Под постоянной конкуренцией за CPU записи и правда теряют больше дерева:
//     запись в корзину держит замок своего слота на всё декодирование и
//     кодирование, расщепление корзины — замок справочника, и вытесненный
//     держатель замка задерживает остальные 63 горутины; на запись уходит
//     вдвое больше памяти (680 Б против 360). Прежний тест превращал и шум, и
//     это свойство в случайный FAIL — одинаково на 0.5.6 и 0.5.8.
//
// Замеры scripts/hub-gate-experiment.sh (контейнер на 4 CPU, сосед на 2 ядра):
// прежний тест под -race — простой 0 FAIL из 20, постоянная нагрузка 2, мигающая
// 6; этот гейт — простой 10 PASS из 10 (медиана 1,09), мигающая 10 из 10 (1,06),
// постоянная — медиана 1,27: свойство кода под нагрузкой, а не шум.
//
// Теперь:
//
//   - Замер — пара «дерево, записи» подряд, порядок в паре чередуется. Каждый
//     замер — серия всплесков на свежих хранилищах суммарно не короче
//     gateSampleFloor; заполнение хаба и сборка мусора вне замера.
//   - До и после каждой пары меряется пропускная способность машины: одна и та
//     же работа на всех P. Пара, вокруг которой она хуже лучшей за прогон
//     больше чем на gateDriftLimit, отбрасывается: её отношение говорит о
//     машине, а не о коде. Постоянная нагрузка сдвигом не считается — пары под
//     ней сравнимы между собой, и отношение под ней настоящее.
//   - Решение — по медиане отношений чистых пар. Если чистых пар меньше
//     gateMinClean, прогон падает как INCONCLUSIVE: он не доказывает ни
//     регрессию, ни её отсутствие.
//   - Под race detector гейт не запускается: инструментированный код меряет не
//     то, что работает в проде. Корректность того же всплеска под -race
//     проверяет Test_Hub_ConcurrentWritesKeepEverything.
//
// Запуск:
//
//	go test -tags perf -count=1 -v -run '^Test_Hub_ConcurrentThroughputGate$' ./statefun/cache
//
// Он же входит в scripts/run-perf-tests.sh --gate и в фазу 1b
// scripts/run-all-tests.sh. Как воспроизвести прежний FAIL и проверить гейт под
// нагрузкой, описывает scripts/hub-gate-experiment.sh.

import (
	"crypto/sha256"
	"fmt"
	"os"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	gateGoroutines   = 64
	gatePerGoroutine = 200
	gateRatioLimit   = 1.25
	gateCleanPairs   = 15
	gateMinClean     = 8
	gateMaxPairs     = 30
	gateDriftLimit   = 1.15
	gateSampleFloor  = 200 * time.Millisecond
)

type hubGateSample struct {
	took    time.Duration
	writes  uint64
	mallocs uint64
	bytes   uint64
}

func (s hubGateSample) add(o hubGateSample) hubGateSample {
	return hubGateSample{s.took + o.took, s.writes + o.writes, s.mallocs + o.mallocs, s.bytes + o.bytes}
}

func (s hubGateSample) String() string {
	w := float64(max(s.writes, 1))
	return fmt.Sprintf("%s, %.1f аллокаций, %.0f Б", (s.took / time.Duration(max(s.writes, 1))).Round(time.Nanosecond),
		float64(s.mallocs)/w, float64(s.bytes)/w)
}

type hubGatePair struct {
	first         string
	pre, post     time.Duration
	tree, records hubGateSample
}

func (p hubGatePair) ratio() float64 { return float64(p.records.took) / float64(p.tree.took) }

// drift — во сколько раз машина вокруг пары медленнее, чем в лучший момент прогона.
func (p hubGatePair) drift(best time.Duration) float64 {
	return float64(max(p.pre, p.post)) / float64(best)
}

// hubGateMeasure — bursts всплесков в режиме mode, каждый на свежем хранилище.
func hubGateMeasure(mode string, bursts, prefill int) hubGateSample {
	restore := SetCacheModeForTest(mode)
	defer restore()
	var s hubGateSample
	var before, after runtime.MemStats
	for b := 0; b < bursts; b++ {
		cs := NewStoreForTest("hub_gate")
		fillHub(cs, "dom/hub", prefill)
		runtime.GC()
		runtime.ReadMemStats(&before)
		started := time.Now()
		hubBurst(cs, gateGoroutines, gatePerGoroutine)
		s.took += time.Since(started)
		runtime.ReadMemStats(&after)
		s.mallocs += after.Mallocs - before.Mallocs
		s.bytes += after.TotalAlloc - before.TotalAlloc
		s.writes += gateGoroutines * gatePerGoroutine
	}
	return s
}

// cpuCapacity — сколько машина сейчас тратит на одну и ту же работу на всех P:
// хеширование без общих данных и замков. Её сдвиг за время пары и есть «машина
// изменилась» — соседний процесс, троттлинг квоты, другой контейнер. Лучший из
// трёх коротких замеров: единичная заминка планировщика в самой калибровке —
// не сдвиг, а занятая соседом машина держится дольше трёх замеров.
func cpuCapacity() time.Duration { return cpuWork(runtime.GOMAXPROCS(0)) }

// cpuParallelism — какую долю номинальной параллельности машина отдаёт сейчас:
// та же работа на одной горутине и на каждой из GOMAXPROCS. На свободной машине
// около 1 (на SMT меньше), под соседом, занявшим часть ядер, — заметно меньше.
// Сдвиг между парами гейт видит сам; постоянную нагрузку — только так, поэтому
// число печатается рядом с вердиктом, но вердикт не меняет.
func cpuParallelism() float64 {
	return float64(cpuWork(1)) / float64(cpuWork(runtime.GOMAXPROCS(0)))
}

func cpuWork(goroutines int) time.Duration {
	buf := make([]byte, 4096)
	best := time.Duration(1<<62 - 1)
	for attempt := 0; attempt < 3; attempt++ {
		var wg sync.WaitGroup
		started := time.Now()
		for i := 0; i < goroutines; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				h := sha256.New()
				for j := 0; j < 2048; j++ {
					h.Write(buf)
				}
				_ = h.Sum(nil)
			}()
		}
		wg.Wait()
		best = min(best, time.Since(started))
	}
	return best
}

func hubGatePassport() string {
	quota := "n/a"
	if b, err := os.ReadFile("/sys/fs/cgroup/cpu.max"); err == nil {
		quota = strings.TrimSpace(string(b))
	}
	return fmt.Sprintf("%s %s/%s NumCPU=%d GOMAXPROCS=%d cgroup cpu.max=%q race=%v",
		runtime.Version(), runtime.GOOS, runtime.GOARCH, runtime.NumCPU(), runtime.GOMAXPROCS(0), quota, raceEnabled)
}

func median(sorted []float64) float64 {
	n := len(sorted)
	if n%2 == 1 {
		return sorted[n/2]
	}
	return (sorted[n/2-1] + sorted[n/2]) / 2
}

func Test_Hub_ConcurrentThroughputGate(t *testing.T) {
	if raceEnabled {
		t.Skip("гейт скорости не запускается под race detector: инструментированный код меряет не то, " +
			"что работает в проде; корректность всплеска под -race проверяет Test_Hub_ConcurrentWritesKeepEverything")
	}
	prefill := defaultBucketLinks * 4
	if v := os.Getenv("HUB_PREFILL"); v != "" {
		fmt.Sscanf(v, "%d", &prefill)
	}

	// Разогрев, он же размер замера: всплесков столько, чтобы замер покрывал
	// много периодов планировщика, а не долю одного.
	warm := hubGateMeasure("tree", 1, prefill)
	hubGateMeasure("records", 1, prefill)
	bursts := max(3, int(gateSampleFloor/max(warm.took, time.Millisecond))+1)

	t.Logf("паспорт: %s", hubGatePassport())
	t.Logf("замер: %d горутин × %d связей в хаб из %d связей, %d всплесков на замер",
		gateGoroutines, gatePerGoroutine, prefill, bursts)
	parallelStart := cpuParallelism()

	var pairs []hubGatePair
	best := time.Duration(1<<62 - 1)
	clean := func() []hubGatePair {
		var out []hubGatePair
		for _, p := range pairs {
			if p.drift(best) <= gateDriftLimit {
				out = append(out, p)
			}
		}
		return out
	}
	for len(pairs) < gateMaxPairs && len(clean()) < gateCleanPairs {
		p := hubGatePair{first: "tree", pre: cpuCapacity()}
		if len(pairs)%2 == 1 {
			p.first = "records"
			p.records = hubGateMeasure("records", bursts, prefill)
			p.tree = hubGateMeasure("tree", bursts, prefill)
		} else {
			p.tree = hubGateMeasure("tree", bursts, prefill)
			p.records = hubGateMeasure("records", bursts, prefill)
		}
		p.post = cpuCapacity()
		best = min(best, p.pre, p.post)
		pairs = append(pairs, p)
	}

	t.Logf("%4s  %-7s  %9s  %9s  %13s  %5s", "пара", "первым", "дерево", "записи", "записи/дерево", "сдвиг")
	for i, p := range pairs {
		mark := ""
		if p.drift(best) > gateDriftLimit {
			mark = "  отброшена: машина изменилась"
		}
		t.Logf("%4d  %-7s  %9s  %9s  %13.3f  %5.2f%s", i+1, p.first,
			p.tree.took.Round(time.Millisecond), p.records.took.Round(time.Millisecond), p.ratio(), p.drift(best), mark)
	}

	ok := clean()
	if len(ok) < gateMinClean {
		t.Fatalf("INCONCLUSIVE: машина вокруг %d пар из %d была медленнее лучшего момента больше чем на %.0f%%, "+
			"чистых пар %d при нужных %d. Такой прогон ничего не говорит о коде ни в одну сторону — "+
			"повторите на машине без посторонней нагрузки.",
			len(pairs)-len(ok), len(pairs), (gateDriftLimit-1)*100, len(ok), gateMinClean)
	}
	ratios := make([]float64, 0, len(ok))
	var tree, records hubGateSample
	for _, p := range ok {
		ratios = append(ratios, p.ratio())
		tree, records = tree.add(p.tree), records.add(p.records)
	}
	sort.Float64s(ratios)
	med := median(ratios)
	parallel := fmt.Sprintf("машина отдавала %.0f%% / %.0f%% номинальной параллельности в начале / в конце",
		parallelStart*100, cpuParallelism()*100)
	t.Logf("чистых пар %d из %d; записи/дерево: мин %.3f, медиана %.3f, макс %.3f; порог медианы %.2f",
		len(ok), len(pairs), ratios[0], med, ratios[len(ratios)-1], gateRatioLimit)
	t.Logf("на запись: дерево %s; записи %s", tree, records)
	t.Logf("%s", parallel)
	if med > gateRatioLimit {
		t.Fatalf("пропускная способность записей ниже 80%% дерева: медиана отношения %.3f по %d чистым парам (порог %.2f); %s. "+
			"Под постоянной нагрузкой на CPU записи теряют больше дерева — это свойство кода, а не шум; "+
			"сравнение с порогом ТЗ делайте на машине без посторонней нагрузки.",
			med, len(ok), gateRatioLimit, parallel)
	}
}
