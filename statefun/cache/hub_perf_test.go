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
// постоянная — медиана 1,27: свойство кода под нагрузкой, а не шум. С
// отбраковкой по разбросу внутри пары, по 5 прогонов: простой 5 PASS (1,10),
// мигающая 5 PASS (1,13), постоянная 5 FAIL (1,29) при 42–51 % номинальной
// параллельности; чистых пар 15 в каждом прогоне.
//
// Теперь:
//
//   - Замер — пара «дерево, записи» подряд, порядок в паре чередуется. Каждый
//     замер — серия всплесков на свежих хранилищах суммарно не короче
//     gateSampleFloor; заполнение хаба и сборка мусора вне замера.
//   - До пары, между её двумя замерами и после меряется пропускная способность
//     машины: одна и та же работа на всех P. Если в самый медленный из этих трёх
//     моментов машина медленнее, чем в самый быстрый, больше чем на
//     gateSpreadLimit, пара отбрасывается: её замеры шли на разной машине, и
//     отношение говорит о машине, а не о коде. Пара на ровно медленной машине
//     сравнима — отношение сокращает абсолютную скорость, — поэтому постоянная
//     нагрузка не отбраковывается. Сравнение с лучшим моментом всего прогона,
//     с которого гейт начинал, отбрасывало и такие пары, а на виртуальной машине
//     с соседями по гипервизору — почти все.
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
// scripts/run-all-tests.sh — там только отчётом: вердикт печатается, но общий
// прогон не роняет. Как воспроизвести прежний FAIL и проверить гейт под
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
	gateSpreadLimit  = 1.15
	gateSampleFloor  = 200 * time.Millisecond
	gateMinBursts    = 3
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
	first          string
	pre, mid, post time.Duration
	tree, records  hubGateSample
}

func (s hubGateSample) perWrite() float64 { return float64(s.took) / float64(max(s.writes, 1)) }

// ratio — во сколько раз запись в режиме записей дольше, чем в дереве. Замеры
// пары могут состоять из разного числа всплесков, поэтому сравнивается время
// одной записи.
func (p hubGatePair) ratio() float64 { return p.records.perWrite() / p.tree.perWrite() }

// spread — во сколько раз машина в самый медленный момент пары (до неё, между
// замерами, после) медленнее, чем в самый быстрый.
func (p hubGatePair) spread() float64 {
	return float64(max(p.pre, p.mid, p.post)) / float64(min(p.pre, p.mid, p.post))
}

// hubGateMeasure — всплески в режиме mode, каждый на свежем хранилище, пока
// их суммарное время не достигнет floor, и не меньше gateMinBursts. Число
// всплесков не задаётся заранее: рабочий всплеск бывает быстрее прогревочного,
// и посчитанное по прогреву число давало замеры короче floor.
func hubGateMeasure(mode string, floor time.Duration, prefill int) hubGateSample {
	restore := SetCacheModeForTest(mode)
	defer restore()
	var s hubGateSample
	var before, after runtime.MemStats
	for b := 0; b < gateMinBursts || s.took < floor; b++ {
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
// хеширование без общих данных и замков. Её разброс внутри пары и есть «машина
// изменилась» — соседний процесс, троттлинг квоты, другой контейнер. Лучший из
// трёх коротких замеров: единичная заминка планировщика в самой калибровке —
// не сдвиг, а занятая соседом машина держится дольше трёх замеров.
func cpuCapacity() time.Duration { return cpuWork(runtime.GOMAXPROCS(0)) }

// cpuParallelism — какую долю номинальной параллельности машина отдаёт сейчас:
// та же работа на одной горутине и на каждой из GOMAXPROCS. На свободной машине
// около 1 (на SMT меньше), под соседом, занявшим часть ядер, — заметно меньше.
// Перемены внутри пары гейт видит сам; постоянную нагрузку — только так, поэтому
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

	// Разогрев: первые всплески медленнее рабочих и в замеры не идут.
	hubGateMeasure("tree", 0, prefill)
	hubGateMeasure("records", 0, prefill)

	t.Logf("паспорт: %s", hubGatePassport())
	t.Logf("замер: %d горутин × %d связей в хаб из %d связей; всплесков в замере не меньше %d, "+
		"пока их суммарное время не достигнет %s — замер покрывает много периодов планировщика, а не долю одного",
		gateGoroutines, gatePerGoroutine, prefill, gateMinBursts, gateSampleFloor)
	parallelStart := cpuParallelism()

	var pairs, ok []hubGatePair
	for len(pairs) < gateMaxPairs && len(ok) < gateCleanPairs {
		p := hubGatePair{first: "tree", pre: cpuCapacity()}
		second := "records"
		if len(pairs)%2 == 1 {
			p.first, second = "records", "tree"
		}
		a := hubGateMeasure(p.first, gateSampleFloor, prefill)
		p.mid = cpuCapacity()
		b := hubGateMeasure(second, gateSampleFloor, prefill)
		p.post = cpuCapacity()
		p.tree, p.records = a, b
		if p.first == "records" {
			p.tree, p.records = b, a
		}
		pairs = append(pairs, p)
		if p.spread() <= gateSpreadLimit {
			ok = append(ok, p)
		}
	}

	t.Logf("%4s  %-7s  %9s  %9s  %13s  %7s", "пара", "первым", "дерево", "записи", "записи/дерево", "разброс")
	for i, p := range pairs {
		mark := ""
		if p.spread() > gateSpreadLimit {
			mark = "  отброшена: машина изменилась внутри пары"
		}
		t.Logf("%4d  %-7s  %9s  %9s  %13.3f  %7.2f%s", i+1, p.first,
			p.tree.took.Round(time.Millisecond), p.records.took.Round(time.Millisecond), p.ratio(), p.spread(), mark)
	}

	if len(ok) < gateMinClean {
		t.Fatalf("INCONCLUSIVE: внутри %d пар из %d машина менялась больше чем на %.0f%%, "+
			"чистых пар %d при нужных %d. Такой прогон ничего не говорит о коде ни в одну сторону — "+
			"повторите на машине без посторонней нагрузки.",
			len(pairs)-len(ok), len(pairs), (gateSpreadLimit-1)*100, len(ok), gateMinClean)
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
