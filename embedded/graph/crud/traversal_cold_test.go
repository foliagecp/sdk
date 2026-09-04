package crud_test

// Тест 7 из ТЗ: обход.
//
// JPGQL-запрос, проходящий по холодным вершинам — то есть по таким, к которым
// с прошлого прохода обслуживания никто не обращался: корзины упакованы в
// байты, а в режиме со сжатием ещё и сжаты, тела не разобраны. Это худший для
// записей случай чтения и лучший для дерева, у которого холодного состояния
// нет вовсе — оно всё всегда держит разобранным.
//
// Цифры печатаются для отчёта; тест же требует только того, что от него можно
// требовать независимо от машины: обход обязан вернуть весь граф в любом
// режиме, и холодный обход обязан оставить после себя тёплые записи, а не
// разрастись.

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/foliagecp/easyjson"
	"github.com/foliagecp/sdk/clients/go/db"
	"github.com/foliagecp/sdk/embedded/graph/crud"
	"github.com/foliagecp/sdk/embedded/graph/jpgql"
	"github.com/foliagecp/sdk/statefun/cache"
	"github.com/foliagecp/sdk/statefun/test"
	"github.com/stretchr/testify/suite"
)

type ColdTraversalTestSuite struct {
	test.StatefunTestSuite
	dbc db.DBSyncClient
}

func TestColdTraversalTestSuite(t *testing.T) {
	suite.Run(t, new(ColdTraversalTestSuite))
}

func (s *ColdTraversalTestSuite) bootstrap() {
	crud.RegisterAllFunctionTypes(s.Runtime())
	jpgql.RegisterAllFunctionTypes(s.Runtime())
	s.NoError(s.StartRuntime())
	for _, v := range []string{crud.BUILT_IN_TYPES, crud.BUILT_IN_OBJECTS} {
		deadline := time.Now().Add(15 * time.Second)
		for time.Now().Before(deadline) {
			if _, err := s.CacheValue(v); err == nil {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	dbc, err := db.NewDBSyncClientFromRequestFunction(s.Runtime().Request)
	s.NoError(err)
	s.dbc = dbc
}

// buildFan строит хаб с n исходящими связями одного типа и возвращает его id.
func (s *ColdTraversalTestSuite) buildFan(prefix string, n int) string {
	cmdb := s.dbc.CMDB
	typeName := prefix + "_t"
	s.NoError(cmdb.TypeCreate(typeName))
	s.NoError(cmdb.TypesLinkCreate(typeName, typeName, prefix+"_rel", nil))

	hub := prefix + "-hub"
	body := easyjson.NewJSONObjectWithKeyValue("name", easyjson.NewJSON(hub))
	s.NoError(cmdb.ObjectUpdate(hub, body, false, typeName))
	for i := 0; i < n; i++ {
		id := fmt.Sprintf("%s-%05d", prefix, i)
		b := easyjson.NewJSONObject()
		b.SetByPath("name", easyjson.NewJSON(id))
		b.SetByPath("cpu", easyjson.NewJSON(int64(8+i%16)))
		b.SetByPath("rack", easyjson.NewJSON(fmt.Sprintf("R%02d", i%12)))
		s.NoError(cmdb.ObjectUpdate(id, b, false, typeName))
		s.NoError(cmdb.ObjectsLinkCreate(hub, id, fmt.Sprintf("l%05d", i), nil))
	}
	return hub
}

// coldTraversal остужает кэш и меряет обход, лучший из трёх — каждый раз
// заново остудив.
func (s *ColdTraversalTestSuite) coldTraversal(hub, linkType string, want int) time.Duration {
	cs := s.Runtime().Domain.Cache()
	query := fmt.Sprintf(".*[l:type('%s')]", linkType)
	best := time.Duration(1<<62 - 1)
	for attempt := 0; attempt < 3; attempt++ {
		// два прохода: первый гасит признаки обращения, второй отпускает
		cs.RunMaintenanceForTest()
		cs.RunMaintenanceForTest()

		started := time.Now()
		ids, err := s.dbc.Query.JPGQLCtraQuery(hub, query)
		took := time.Since(started)
		s.Require().NoError(err)
		s.Require().Lenf(ids, want, "обход обязан вернуть все %d вершин", want)
		if took < best {
			best = took
		}
	}
	return best
}

// cacheWalk — вклад самого кэша в обход: перечислить связи хаба и прочитать
// цель каждой, по холодному. Это то, что мы меняли, без движка запросов
// вокруг.
func (s *ColdTraversalTestSuite) cacheWalk(hub string, want int) time.Duration {
	cs := s.Runtime().Domain.Cache()
	domHub := s.SetThisDomainPreffix(hub)
	pattern := fmt.Sprintf(crud.OutLinkTargetKeyPrefPattern+">", domHub)

	best := time.Duration(1<<62 - 1)
	for attempt := 0; attempt < 3; attempt++ {
		cs.RunMaintenanceForTest()
		cs.RunMaintenanceForTest()

		started := time.Now()
		keys := cs.GetKeysByPattern(pattern)
		n := 0
		for _, k := range keys {
			if v, err := cs.GetValue(k); err == nil && len(v) > 0 {
				n++
			}
		}
		took := time.Since(started)
		// want + 1: у всякого объекта есть ещё связь __type на свой тип.
		s.Require().Equalf(want+1, n, "кэш обязан отдать все %d связей хаба", want+1)
		if took < best {
			best = took
		}
	}
	return best
}

func (s *ColdTraversalTestSuite) Test_ColdTraversal() {
	s.bootstrap()
	cs := s.Runtime().Domain.Cache()

	sizes := []int{1000}
	if os.Getenv("TRAVERSAL_BIG") != "" {
		// Десять тысяч вершин ТЗ просит, но сквозной запрос по ним занимает
		// двадцать секунд, и почти всё это — движок запросов: он делает около
		// восьми обращений на вершину, и с ростом графа их становится больше,
		// а не столько же. Держать это в обычном прогоне значит платить
		// двадцать секунд за измерение чужой работы.
		sizes = append(sizes, 10000)
	}

	for _, n := range sizes {
		prefix := fmt.Sprintf("tr%d", n)
		hub := s.buildFan(prefix, n)

		walk := s.cacheWalk(hub, n)
		query := s.coldTraversal(hub, prefix+"_rel", n)

		vertices, bytes, buckets, compressed, _, parsed := cs.RecordStatsForTest()
		s.T().Logf("режим %s, %d вершин:", cache.CacheMode(), n)
		s.T().Logf("  обход по кэшу:   %s (%s на связь)",
			walk.Round(time.Microsecond), (walk / time.Duration(n)).Round(time.Nanosecond))
		s.T().Logf("  запрос JPGQL:    %s (%s на вершину)",
			query.Round(time.Millisecond), (query / time.Duration(n)).Round(time.Microsecond))
		s.T().Logf("  записей %d, байт %d, корзин %d, из них сжатых %d, тел разобрано %d",
			vertices, bytes, buckets, compressed, parsed)

		// Холодный обход читает связи, а не тела, поэтому разбирать тела ему
		// незачем — и он не должен оставлять их разобранными после себя.
		s.Require().Zerof(parsed,
			"обход по связям оставил %d разобранных тел — он их не читает", parsed)
	}
}
