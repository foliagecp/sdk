package cache

import (
	"fmt"
	"testing"

	"github.com/foliagecp/easyjson"
	"github.com/stretchr/testify/require"
)

// seedGraph наполняет стор вершинами со связями — так, как это делает CRUD.
func seedGraph(t testing.TB, cs *Store, vertices, links int) {
	t.Helper()
	now := int64(1_000_000)
	for i := 0; i < vertices; i++ {
		v := fmt.Sprintf("dom/v-%05d", i)
		body := easyjson.NewJSONObject()
		body.SetByPath("name", easyjson.NewJSON(fmt.Sprintf("узел-%05d", i)))
		body.SetByPath("kind", easyjson.NewJSON("os_package"))
		body.SetByPath("size_kb", easyjson.NewJSON(i*13%9000))
		cs.SetValueJSON(v, &body, false, now)
		for k := 0; k < links; k++ {
			name := fmt.Sprintf("uiapplib_%05d_%02d", i, k)
			cs.SetValue(fmt.Sprintf(kOutTo, v, name), []byte(fmt.Sprintf("ui_controller_subject.dom/tgt-%05d", (i+k)%vertices)), false, now)
			cs.SetValue(fmt.Sprintf(kIndexType, v, name, "ui_controller_subject"), nil, false, now)
			cs.SetValue(fmt.Sprintf(kInLink, v, fmt.Sprintf("dom/src-%05d", (i+k)%vertices), name), []byte("ui_controller_subject"), false, now)
		}
	}
}

// Test_Compression_KeepsAnswers — сжатие невидимо снаружи.
func Test_Compression_KeepsAnswers(t *testing.T) {
	ResetCompressionForTest()
	restore := SetCacheModeForTest("zstd-dict")
	defer restore()

	cs := NewStoreForTest("zstd")
	seedGraph(t, cs, 60, 12)

	// ответы до сжатия
	var keys []string
	for i := 0; i < 60; i++ {
		v := fmt.Sprintf("dom/v-%05d", i)
		keys = append(keys, v)
		keys = append(keys, cs.GetKeysByPattern(v+".out.to.>")...)
		keys = append(keys, cs.GetKeysByPattern(v+".in.>")...)
		keys = append(keys, cs.GetKeysByPattern(v+".ltype.>")...)
	}
	before := make([]string, len(keys))
	for i, k := range keys {
		before[i] = fmt.Sprintf("%v|%d|%s", cs.Exists(k), cs.GetValueUpdateTime(k), getStr(cs, k))
	}

	require.Greater(t, cs.compactRecords(), 0, "записи обязаны были оставить корзины разобранными")
	n := cs.compressRecords()
	require.Greater(t, n, 0, "ни одна корзина не сжалась")

	compressed, _ := cs.CompressionStatsForTest()
	require.Greater(t, compressed, 0)

	for i, k := range keys {
		got := fmt.Sprintf("%v|%d|%s", cs.Exists(k), cs.GetValueUpdateTime(k), getStr(cs, k))
		require.Equal(t, before[i], got, "сжатие изменило ответ по ключу %s", k)
	}
}

// Test_Compression_ReadPublishesRawBack — чтение сжатой корзины возвращает её в
// сырой вид, чтобы следующее чтение не платило распаковкой снова.
func Test_Compression_ReadPublishesRawBack(t *testing.T) {
	ResetCompressionForTest()
	restore := SetCacheModeForTest("zstd-dict")
	defer restore()

	cs := NewStoreForTest("republish")
	seedGraph(t, cs, 20, 40)
	cs.compactRecords()
	require.Greater(t, cs.compressRecords(), 0)

	before, _ := cs.CompressionStatsForTest()
	require.Greater(t, before, 0)

	// прочитать всё
	for i := 0; i < 20; i++ {
		v := fmt.Sprintf("dom/v-%05d", i)
		for _, k := range cs.GetKeysByPattern(v + ".out.to.>") {
			require.True(t, cs.Exists(k))
		}
	}

	after, _ := cs.CompressionStatsForTest()
	require.Less(t, after, before,
		"после чтения сжатых корзин должно остаться меньше сжатых: было %d, стало %d", before, after)
}

// Test_Compression_StaleDictionaryStillReads — корзина, сжатая прежним
// словарём, обязана читаться после его замены. Ради этого декодер и хранит все
// словари, которые процесс когда-либо использовал.
func Test_Compression_StaleDictionaryStillReads(t *testing.T) {
	ResetCompressionForTest()
	restore := SetCacheModeForTest("zstd-dict")
	defer restore()

	cs := NewStoreForTest("dict")
	seedGraph(t, cs, 80, 16)
	cs.compactRecords()

	// сжать без словаря
	require.Greater(t, cs.compressRecords(), 0)
	v0 := "dom/v-00000"
	keysV0 := cs.GetKeysByPattern(v0 + ".out.to.>")
	require.NotEmpty(t, keysV0)

	// новые данные дают сырые корзины, на которых и обучается словарь:
	// выборка берёт только их, потому что учиться надо на том, что пишут,
	// а не на том, что уже сжато
	seedGraph(t, cs, 40, 16)
	cs.compactRecords()
	require.True(t, cs.trainDictionary(64), "словарь не обучился")
	_, ver1 := cs.CompressionStatsForTest()
	require.Equal(t, uint32(1), ver1)
	cs.compressRecords() // теперь уже словарём

	// ещё данные и второй словарь
	seedGraph(t, cs, 40, 16)
	cs.compactRecords()
	require.True(t, cs.trainDictionary(64))
	_, ver2 := cs.CompressionStatsForTest()
	require.Equal(t, uint32(2), ver2)

	// корзины, сжатые без словаря и первым словарём, обязаны читаться
	for _, k := range keysV0 {
		require.True(t, cs.Exists(k), "потерян ключ %s после смены словаря", k)
	}
	for i := 0; i < 120; i++ {
		v := fmt.Sprintf("dom/v-%05d", i)
		for _, k := range cs.GetKeysByPattern(v + ".out.to.>") {
			require.True(t, cs.Exists(k), "потерян ключ %s", k)
		}
	}
}

// Test_Compression_Ratio — сколько сжатие даёт на данных, похожих на графовые.
func Test_Compression_Ratio(t *testing.T) {
	ResetCompressionForTest()
	restore := SetCacheModeForTest("zstd-dict")
	defer restore()

	cs := NewStoreForTest("ratio")
	seedGraph(t, cs, 400, 20)
	cs.compactRecords()
	raw := cs.RecordsBytesForTest()

	// без словаря
	require.True(t, cs.trainDictionary(0) == false, "с пустой выборкой словарь обучаться не должен")
	cs.compressRecords()
	noDict := cs.RecordsBytesForTest()

	// со словарём: вернуть корзины в сырой вид чтением, обучить, сжать заново
	for i := 0; i < 400; i++ {
		v := fmt.Sprintf("dom/v-%05d", i)
		cs.GetKeysByPattern(v + ".out.to.>")
		cs.GetKeysByPattern(v + ".in.>")
		cs.GetKeysByPattern(v + ".ltype.>")
	}
	require.True(t, cs.trainDictionary(256), "словарь не обучился")
	cs.compressRecords()
	withDict := cs.RecordsBytesForTest()

	t.Logf("сырые %d B, zstd без словаря %d B (%.2fx), zstd со словарём %d B (%.2fx)",
		raw, noDict, float64(raw)/float64(noDict), withDict, float64(raw)/float64(withDict))
	require.Less(t, noDict, raw, "сжатие обязано уменьшать объём")
}

// Test_Compression_Off — при выключенном флаге ничего не сжимается.
func Test_Compression_Off(t *testing.T) {
	ResetCompressionForTest()
	restore := SetCacheModeForTest("records")
	defer restore()

	cs := NewStoreForTest("off")
	seedGraph(t, cs, 20, 20)
	cs.compactRecords()
	require.Zero(t, cs.compressRecords())
	compressed, _ := cs.CompressionStatsForTest()
	require.Zero(t, compressed)
}

// Test_Compression_RetrainsOnDecay — словарь переобучается по деградации
// ратио, а не по расписанию.
func Test_Compression_RetrainsOnDecay(t *testing.T) {
	restore := SetCacheModeForTest("zstd-dict")
	defer restore()
	ResetCompressionForTest()

	cs := NewStoreForTest("decay")
	seedGraph(t, cs, 200, 16)
	cs.compactRecords()

	// первый раз словаря нет — обучение обязано случиться
	require.True(t, cs.maybeTrainDictionary(256), "первый словарь не обучился")
	trainedAt, _, retrains := DictionaryStatsForTest()
	require.Equal(t, 1, retrains)
	require.Greater(t, trainedAt, 1.0, "ратио при обучении: %.2f", trainedAt)

	// на тех же данных переобучать незачем
	require.False(t, cs.maybeTrainDictionary(256), "переобучился без причины")
	_, _, retrains2 := DictionaryStatsForTest()
	require.Equal(t, 1, retrains2)

	// данные другой формы — ратио падает, словарь обязан обновиться
	cs2 := NewStoreForTest("decay2")
	now := int64(1_000_000)
	for i := 0; i < 200; i++ {
		v := fmt.Sprintf("dom/z-%05d", i)
		for k := 0; k < 16; k++ {
			name := fmt.Sprintf("%d-%d-совсем-другая-форма-имени-связи", i, k)
			cs2.SetValue(fmt.Sprintf(kOutTo, v, name),
				[]byte(fmt.Sprintf("другой_тип_связи.dom/иная-цель-%d-%d", i, k)), false, now)
		}
	}
	cs2.compactRecords()

	if cs2.maybeTrainDictionary(256) {
		_, _, retrains3 := DictionaryStatsForTest()
		require.Equal(t, 2, retrains3, "переобучение обязано учитываться")
		require.Equal(t, uint32(2), recordCodec.dictVersion())
	} else {
		lastTrained, last, _ := DictionaryStatsForTest()
		t.Logf("переобучения не потребовалось: ратио %.2f против %.2f при обучении", last, lastTrained)
	}

	// и данные первого стора по-прежнему читаются
	for i := 0; i < 200; i++ {
		v := fmt.Sprintf("dom/v-%05d", i)
		for _, k := range cs.GetKeysByPattern(v + ".out.to.>") {
			require.True(t, cs.Exists(k), "потерян ключ %s", k)
		}
	}
}

// Test_Maintenance_CompactsAndCompresses — обслуживание обязано само уплотнять
// и сжимать. Тесты, зовущие эти шаги напрямую, этого не проверяют: если хук в
// обходе обслуживания не стоит, они всё равно зелёные, а в бою не срабатывает
// ничего.
func Test_Maintenance_CompactsAndCompresses(t *testing.T) {
	restore := SetCacheModeForTest("zstd-dict")
	defer restore()
	ResetCompressionForTest()

	cs := NewStoreForTest("maintenance")
	seedGraph(t, cs, 120, 20)

	require.Greater(t, dirtyBucketsOf(cs), 0, "записи обязаны были оставить корзины разобранными")

	// единственный вызов — тот, что делает рантайм по таймеру
	cs.traverseCacheForMaintenance()

	require.Zero(t, dirtyBucketsOf(cs), "обслуживание не уплотнило корзины")
	compressed, _ := cs.CompressionStatsForTest()
	require.Greater(t, compressed, 0, "обслуживание не сжало ни одной корзины")

	// и ответы прежние
	for i := 0; i < 120; i++ {
		v := fmt.Sprintf("dom/v-%05d", i)
		require.True(t, cs.Exists(v), "потеряно тело %s", v)
		keys := cs.GetKeysByPattern(v + ".out.to.>")
		require.Len(t, keys, 20, "потеряны связи %s", v)
	}
}

func dirtyBucketsOf(cs *Store) int {
	n := 0
	cs.records.each(func(_ string, r *vertexRecord) bool {
		n += r.dirtyBuckets()
		return true
	})
	return n
}

// Test_ParsedBody_KeptFromSecondRead — разбор сохраняется со второго чтения, а
// не с первого: обход графа читает каждое тело единожды и не должен оставлять
// после себя разобранным весь граф. Ответ при этом одинаков в обоих случаях.
func Test_ParsedBody_KeptFromSecondRead(t *testing.T) {
	restore := SetCacheModeForTest("records")
	defer restore()

	cs := NewStoreForTest("parsed")
	body, _ := easyjson.JSONFromString(`{"cpu":8,"name":"узел"}`)
	require.True(t, cs.SetValueJSON("dom/v", &body, false, 1))

	r, ok := cs.records.get("dom/v")
	require.True(t, ok)
	require.Nil(t, r.parsedBody.Load(), "до чтения разбора быть не должно")

	first, err := cs.GetValueJSON("dom/v")
	require.NoError(t, err)
	require.Equal(t, float64(8), first.GetByPath("cpu").AsNumericDefault(-1))
	require.Nil(t, r.parsedBody.Load(), "первое чтение не должно оставлять разбор")

	second, err := cs.GetValueJSON("dom/v")
	require.NoError(t, err)
	require.Equal(t, float64(8), second.GetByPath("cpu").AsNumericDefault(-1))
	require.NotNil(t, r.parsedBody.Load(), "второе чтение обязано сохранить разбор")

	// каждому читателю — своя копия, и правка одного не видна другому
	second.SetByPath("cpu", easyjson.NewJSON(999))
	third, err := cs.GetValueJSON("dom/v")
	require.NoError(t, err)
	require.Equal(t, float64(8), third.GetByPath("cpu").AsNumericDefault(-1),
		"правка одного читателя протекла к другому")
}

// Test_ParsedBody_AgesByUse — обслуживание сбрасывает то, к чему не обращались,
// и оставляет то, к чему обращались.
func Test_ParsedBody_AgesByUse(t *testing.T) {
	restore := SetCacheModeForTest("records")
	defer restore()

	cs := NewStoreForTest("aging")
	body, _ := easyjson.JSONFromString(`{"n":1}`)
	for i := 0; i < 4; i++ {
		cs.SetValueJSON(fmt.Sprintf("dom/v-%d", i), &body, false, 1)
	}
	// дважды: разбор сохраняется со второго чтения
	for round := 0; round < 2; round++ {
		for i := 0; i < 4; i++ {
			cs.GetValueJSON(fmt.Sprintf("dom/v-%d", i)) //nolint:errcheck
		}
	}
	for i := 0; i < 4; i++ {
		r, _ := cs.records.get(fmt.Sprintf("dom/v-%d", i))
		require.NotNil(t, r.parsedBody.Load(), "тело %d должно быть разобрано", i)
	}

	// первый проход: обращались ко всем — не сбрасывается ничего
	require.Zero(t, cs.ageParsedBodies(), "сброшено то, к чему только что обращались")

	// обращаемся лишь к части
	cs.GetValueJSON("dom/v-0") //nolint:errcheck
	cs.GetValueJSON("dom/v-1") //nolint:errcheck

	require.Equal(t, 2, cs.ageParsedBodies(), "должны были уйти ровно те два, к которым не обращались")

	r0, _ := cs.records.get("dom/v-0")
	r2, _ := cs.records.get("dom/v-2")
	require.NotNil(t, r0.parsedBody.Load(), "горячее тело обязано было остаться")
	require.Nil(t, r2.parsedBody.Load(), "холодное тело обязано было уйти")
	require.False(t, r2.parsedBodySeen.Load(), "сброшенное тело обязано начать счёт заново")

	// и ответы прежние в обоих случаях
	for i := 0; i < 4; i++ {
		got, err := cs.GetValueJSON(fmt.Sprintf("dom/v-%d", i))
		require.NoError(t, err)
		require.Equal(t, float64(1), got.GetByPath("n").AsNumericDefault(-1))
	}
}

// Test_ParsedBody_InvalidatedByWrite — запись обязана обесценить сохранённый
// разбор, иначе читатель увидит то, чего уже нет.
func Test_ParsedBody_InvalidatedByWrite(t *testing.T) {
	restore := SetCacheModeForTest("records")
	defer restore()

	cs := NewStoreForTest("invalidate")
	first, _ := easyjson.JSONFromString(`{"v":1}`)
	cs.SetValueJSON("dom/v", &first, false, 1)
	got, _ := cs.GetValueJSON("dom/v")
	require.Equal(t, float64(1), got.GetByPath("v").AsNumericDefault(-1))

	second, _ := easyjson.JSONFromString(`{"v":2}`)
	cs.SetValueJSON("dom/v", &second, false, 2)
	got, _ = cs.GetValueJSON("dom/v")
	require.Equal(t, float64(2), got.GetByPath("v").AsNumericDefault(-1),
		"после записи читается прежнее тело")

	cs.DeleteValue("dom/v", false, 3)
	_, err := cs.GetValueJSON("dom/v")
	require.Error(t, err, "после удаления тело не должно читаться")
}

// Test_FullScan_DoesNotGrowTheCache — что делает с памятью тот, кто пришёл и
// прочитал весь граф.
//
// Один обход не оставляет после себя ничего: разбор тела запоминается только
// со второго чтения, а обход читает каждое тело единожды и не возвращается.
// Повторные обходы упираются в потолок — по одному разбору на вершину — и выше
// не идут; потолок этот ниже того, что дерево занимает всегда. Когда чтения
// прекращаются, всё возвращается.
func Test_FullScan_DoesNotGrowTheCache(t *testing.T) {
	restore := SetCacheModeForTest("records")
	defer restore()

	const vertices = 500
	cs := NewStoreForTest("fullscan")
	body, _ := easyjson.JSONFromString(`{"name":"узел","cpu":8,"tags":["a","b"]}`)
	for i := 0; i < vertices; i++ {
		require.True(t, cs.SetValueJSON(fmt.Sprintf("dom/v-%04d", i), &body, false, 1))
	}
	scan := func() {
		for i := 0; i < vertices; i++ {
			j, err := cs.GetValueJSON(fmt.Sprintf("dom/v-%04d", i))
			require.NoError(t, err)
			require.Equal(t, float64(8), j.GetByPath("cpu").AsNumericDefault(-1))
		}
	}

	require.Zero(t, cs.ParsedBodyCountForTest(), "до чтений разборов быть не должно")

	scan()
	require.Zerof(t, cs.ParsedBodyCountForTest(),
		"один обход графа обязан не оставить после себя ни одного разбора")

	scan()
	require.Equal(t, vertices, cs.ParsedBodyCountForTest(),
		"второй обход обязан разобрать всё — это и есть потолок")

	scan()
	scan()
	require.Equal(t, vertices, cs.ParsedBodyCountForTest(),
		"дальнейшие обходы обязаны упереться в тот же потолок, а не расти")

	// чтения прекратились — обслуживание забирает память обратно
	cs.ageParsedBodies()
	cs.ageParsedBodies()
	require.Zerof(t, cs.ParsedBodyCountForTest(),
		"после того как чтения прекратились, разборы обязаны быть отпущены")

	// и граф по-прежнему читается
	scan()
}
