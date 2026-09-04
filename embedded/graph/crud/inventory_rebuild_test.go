package crud_test

// Тест 6 из ТЗ: запись-интенсивный проход.
//
// Инвентаризация приходит и переписывает граф целиком, хотя изменилась в нём
// малая часть. Это самый частый вид нагрузки на запись, и именно им ТЗ
// проверяет, достаточно ли варианта A (все вершины — записи, все записи
// сквозные) или нужен вариант B с горячим по записи набором. Условие простое:
// то, что не изменилось, не должно порождать записи вообще — ни в кэше, ни в
// журнале, ни в триггерах.
//
// Проверка структурная, а не по секундомеру: время цикла зависит от машины, а
// «не изменившаяся вершина не переписана» — нет. Время всё равно печатается,
// потому что ради него всё и делалось.

import (
	"fmt"
	"testing"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/clients/go/db"
	"github.com/foliagecp/sdk/embedded/graph/crud"
	"github.com/foliagecp/sdk/statefun/cache"
	"github.com/foliagecp/sdk/statefun/test"
	"github.com/stretchr/testify/suite"
)

type InventoryRebuildTestSuite struct {
	test.StatefunTestSuite
	dbc db.DBSyncClient
}

func TestInventoryRebuildTestSuite(t *testing.T) {
	suite.Run(t, new(InventoryRebuildTestSuite))
}

func (s *InventoryRebuildTestSuite) waitForVertex(id string, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := s.CacheValue(id); err == nil {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	s.T().Fatalf("вершина %q не появилась за %s", id, timeout)
}

// bodyOf — тело в том виде, в каком его шлёт инвентаризация: несколько полей
// обнаружения, из которых меняется одно и только у части вершин.
func bodyOf(i, generation int) easyjson.JSON {
	b := easyjson.NewJSONObject()
	b.SetByPath("name", easyjson.NewJSON(fmt.Sprintf("srv-%03d", i)))
	b.SetByPath("cpu", easyjson.NewJSON(int64(8+i%16)))
	b.SetByPath("rack", easyjson.NewJSON(fmt.Sprintf("R%02d", i%12)))
	b.SetByPath("firmware", easyjson.NewJSON(fmt.Sprintf("v%d.%d", 3, generation)))
	return b
}

func (s *InventoryRebuildTestSuite) Test_RebuildRewritesOnlyWhatChanged() {
	crud.RegisterAllFunctionTypes(s.Runtime())
	s.NoError(s.StartRuntime())
	s.waitForVertex(crud.BUILT_IN_TYPES, 15*time.Second)
	s.waitForVertex(crud.BUILT_IN_OBJECTS, 15*time.Second)

	dbc, err := db.NewDBSyncClientFromRequestFunction(s.Runtime().Request)
	s.NoError(err)
	s.dbc = dbc
	cmdb := dbc.CMDB

	const total = 500
	const changed = total / 2 // вторая половина меняется

	s.NoError(cmdb.TypeCreate("srv"))
	for i := 0; i < total; i++ {
		b := bodyOf(i, 0)
		s.NoError(cmdb.ObjectUpdate(fmt.Sprintf("srv-%03d", i), b, false, "srv"))
	}

	cs := s.Runtime().Domain.Cache()
	timeOf := func(i int) int64 {
		return cs.GetValueUpdateTime(s.SetThisDomainPreffix(fmt.Sprintf("srv-%03d", i)))
	}
	before := make([]int64, total)
	for i := range before {
		before[i] = timeOf(i)
		s.Require().GreaterOrEqual(before[i], int64(0), "вершина %d не записалась", i)
	}
	bytesBefore := cs.RecordsBytesForTest()

	// --- проход инвентаризации: первая половина без изменений
	for i := 0; i < total-changed; i++ {
		b := bodyOf(i, 0)
		s.NoError(cmdb.ObjectUpdate(fmt.Sprintf("srv-%03d", i), b, false, "srv"))
	}
	// --- вторая половина с изменением одного поля
	for i := total - changed; i < total; i++ {
		b := bodyOf(i, 1)
		s.NoError(cmdb.ObjectUpdate(fmt.Sprintf("srv-%03d", i), b, false, "srv"))
	}

	// --- что должно было произойти
	for i := 0; i < total-changed; i++ {
		s.Require().Equalf(before[i], timeOf(i),
			"вершина %d не менялась, но была переписана — проверка на no-op не сработала", i)
	}
	for i := total - changed; i < total; i++ {
		s.Require().Greaterf(timeOf(i), before[i],
			"вершина %d изменилась, но записи не произошло", i)
		got, err := s.dbc.CMDB.ObjectRead(fmt.Sprintf("srv-%03d", i))
		s.Require().NoError(err)
		s.Require().Equal("v3.1", got.GetByPath("body.firmware").AsStringDefault(""),
			"новое значение не доехало до вершины %d", i)
	}

	// --- чего это стоит
	//
	// Меряется полным проходом, а не половиной: половины идут одна за другой,
	// и первая платит за прогрев, из-за чего no-op выходил дороже записи —
	// ровно наоборот действительности. Отсюда прогревочный проход и лучшее из
	// трёх, как в остальных замерах.
	generation := 1
	pass := func(gen int) time.Duration {
		started := time.Now()
		for i := 0; i < total; i++ {
			b := bodyOf(i, gen)
			s.NoError(cmdb.ObjectUpdate(fmt.Sprintf("srv-%03d", i), b, false, "srv"))
		}
		return time.Since(started)
	}
	pass(generation) // прогрев: после него весь граф в поколении 1

	best := func(f func() time.Duration) time.Duration {
		out := time.Duration(1<<62 - 1)
		for a := 0; a < 3; a++ {
			if d := f(); d < out {
				out = d
			}
		}
		return out
	}
	unchangedTook := best(func() time.Duration { return pass(generation) })
	changedTook := best(func() time.Duration {
		generation++
		return pass(generation)
	})

	// и проход без изменений действительно ничего не переписал
	atRest := make([]int64, total)
	for i := range atRest {
		atRest[i] = timeOf(i)
	}
	for i := 0; i < total; i++ {
		b := bodyOf(i, generation)
		s.NoError(cmdb.ObjectUpdate(fmt.Sprintf("srv-%03d", i), b, false, "srv"))
	}
	for i := 0; i < total; i++ {
		s.Require().Equalf(atRest[i], timeOf(i),
			"повторная отправка того же тела переписала вершину %d", i)
	}

	bytesAfter := cs.RecordsBytesForTest()
	perUnchanged := unchangedTook / total
	perChanged := changedTook / total
	s.T().Logf("режим %s: %d вершин, из них изменилось %d", cache.CacheMode(), total, changed)
	s.T().Logf("  проход без изменений: %s на вершину (всего %s)",
		perUnchanged.Round(time.Nanosecond), unchangedTook.Round(time.Millisecond))
	s.T().Logf("  проход с изменениями: %s на вершину (всего %s)",
		perChanged.Round(time.Nanosecond), changedTook.Round(time.Millisecond))
	s.T().Logf("  байт в записях: %d -> %d", bytesBefore, bytesAfter)

	s.Require().Lessf(perUnchanged, perChanged,
		"проход по неизменившимся (%s на вершину) обязан быть дешевле прохода с записью (%s)",
		perUnchanged, perChanged)
	s.Require().Lessf(bytesAfter, bytesBefore*3/2,
		"перезапись графа не должна раздувать записи: было %d, стало %d", bytesBefore, bytesAfter)
}
