package cache

import (
	"fmt"
	"sync"
	"testing"

	"github.com/foliagecp/easyjson"
	"github.com/stretchr/testify/require"
)

// Test_IntermediateNode_DoesNotBlockLaterWrite — узел, созданный по пути к
// другому ключу, не должен мешать записи по нему самому.
//
// Навигация создаёт промежуточные узлы, и если такому узлу поставить текущее
// время, страж last-writer-wins в Put отбросит любую последующую запись с явным
// временем меньше этого штампа. Промежуточному узлу защищать нечего: значения у
// него нет, и первая же запись по этому ключу — первая настоящая.
func Test_IntermediateNode_DoesNotBlockLaterWrite(t *testing.T) {
	restore := SetCacheModeForTest("tree")
	defer restore()

	cs := NewStoreForTest("intermediate")

	// Запись ключа под вершиной создаёт узел самой вершины попутно.
	require.True(t, cs.SetValue("dom/v.out.to.l1", []byte("t.dom/other"), false, 1000))
	require.False(t, cs.Exists("dom/v"), "у вершины пока нет тела")

	// Тело вершины со временем, заведомо меньшим системных часов, но большим
	// времени операции, создавшей промежуточный узел.
	body := easyjson.NewJSONObjectWithKeyValue("cpu", easyjson.NewJSON(8))
	require.True(t, cs.SetValueJSON("dom/v", &body, false, 2000))

	require.True(t, cs.Exists("dom/v"), "запись тела вершины потерялась")
	require.Equal(t, int64(2000), cs.GetValueUpdateTime("dom/v"))
	got, err := cs.GetValueJSON("dom/v")
	require.NoError(t, err)
	require.Equal(t, float64(8), got.GetByPath("cpu").AsNumericDefault(-1))

	// И ключ, ради которого узел создавался, на месте.
	v, err := cs.GetValue("dom/v.out.to.l1")
	require.NoError(t, err)
	require.Equal(t, "t.dom/other", string(v))
}

// Test_IntermediateNode_GuardStillProtectsRealValues — страж обязан работать
// там, где значение действительно было.
func Test_IntermediateNode_GuardStillProtectsRealValues(t *testing.T) {
	restore := SetCacheModeForTest("tree")
	defer restore()

	cs := NewStoreForTest("guard")

	require.True(t, cs.SetValue("dom/v", []byte("новое"), false, 2000))
	require.True(t, cs.SetValue("dom/v", []byte("старое"), false, 1000))
	got, err := cs.GetValue("dom/v")
	require.NoError(t, err)
	require.Equal(t, "новое", string(got), "запись со старым временем не должна побеждать")

	// после удаления запоздавшая запись не воскрешает ключ
	cs.DeleteValue("dom/v", false, 3000)
	require.False(t, cs.Exists("dom/v"))
	require.True(t, cs.SetValue("dom/v", []byte("воскрешение"), false, 2500))
	require.False(t, cs.Exists("dom/v"), "запоздавшая запись воскресила удалённый ключ")
}

// Test_IntermediateNode_ConcurrentVertexWrite — тот же сценарий, но так, как он
// возникает в проде: одна операция пишет связь, другая — тело той же вершины,
// и время у второй взято раньше, чем первая создала промежуточный узел.
func Test_IntermediateNode_ConcurrentVertexWrite(t *testing.T) {
	restore := SetCacheModeForTest("tree")
	defer restore()

	const vertices = 200
	cs := NewStoreForTest("concurrent")

	var wg sync.WaitGroup
	for i := 0; i < vertices; i++ {
		v := fmt.Sprintf("dom/v-%03d", i)
		// время тела взято ДО того, как связь создаст промежуточный узел
		bodyTime := int64(1000 + i)
		linkTime := int64(500 + i)

		wg.Add(2)
		go func() {
			defer wg.Done()
			cs.SetValue(v+".out.to.l1", []byte("t.dom/x"), false, linkTime)
		}()
		go func() {
			defer wg.Done()
			body := easyjson.NewJSONObjectWithKeyValue("n", easyjson.NewJSON(1))
			cs.SetValueJSON(v, &body, false, bodyTime)
		}()
	}
	wg.Wait()

	lost := 0
	for i := 0; i < vertices; i++ {
		if !cs.Exists(fmt.Sprintf("dom/v-%03d", i)) {
			lost++
		}
	}
	require.Zero(t, lost, "потеряно тел вершин: %d из %d", lost, vertices)
}
