package cache

import (
	"fmt"
	"sync"
	"testing"

	"github.com/foliagecp/easyjson"
	"github.com/stretchr/testify/require"
)

// Test_IntermediateNode_DoesNotBlockLaterWrite — a node created on the way to
// another key must not get in the way of a write to the node itself.
//
// Navigation creates intermediate nodes, and if such a node is given the
// current time, the last-writer-wins guard in Put drops any later write whose
// explicit time is below that stamp. An intermediate node has nothing to
// protect: it has no value, and the very first write to its key is the first
// real one.
func Test_IntermediateNode_DoesNotBlockLaterWrite(t *testing.T) {
	restore := SetCacheModeForTest("tree")
	defer restore()

	cs := NewStoreForTest("intermediate")

	// Writing a key under the vertex creates the vertex's own node on the way.
	require.True(t, cs.SetValue("dom/v.out.to.l1", []byte("t.dom/other"), false, 1000))
	require.False(t, cs.Exists("dom/v"), "the vertex has no body yet")

	// The vertex body, with a time certainly below the system clock but above
	// the time of the operation that created the intermediate node.
	body := easyjson.NewJSONObjectWithKeyValue("cpu", easyjson.NewJSON(8))
	require.True(t, cs.SetValueJSON("dom/v", &body, false, 2000))

	require.True(t, cs.Exists("dom/v"), "the write of the vertex body was lost")
	require.Equal(t, int64(2000), cs.GetValueUpdateTime("dom/v"))
	got, err := cs.GetValueJSON("dom/v")
	require.NoError(t, err)
	require.Equal(t, float64(8), got.GetByPath("cpu").AsNumericDefault(-1))

	// And the key the node was created for is in place.
	v, err := cs.GetValue("dom/v.out.to.l1")
	require.NoError(t, err)
	require.Equal(t, "t.dom/other", string(v))
}

// Test_IntermediateNode_GuardStillProtectsRealValues — the guard must still
// work where a value really was.
func Test_IntermediateNode_GuardStillProtectsRealValues(t *testing.T) {
	restore := SetCacheModeForTest("tree")
	defer restore()

	cs := NewStoreForTest("guard")

	require.True(t, cs.SetValue("dom/v", []byte("новое"), false, 2000))
	// The return value now means "the write landed": a write the guard refused
	// must say so, or the caller takes a dropped write for a successful one.
	require.False(t, cs.SetValue("dom/v", []byte("старое"), false, 1000))
	got, err := cs.GetValue("dom/v")
	require.NoError(t, err)
	require.Equal(t, "новое", string(got), "a write with an older time must not win")

	// after a delete, a late write does not resurrect the key
	cs.DeleteValue("dom/v", false, 3000)
	require.False(t, cs.Exists("dom/v"))
	require.False(t, cs.SetValue("dom/v", []byte("воскрешение"), false, 2500))
	require.False(t, cs.Exists("dom/v"), "a late write resurrected the deleted key")
}

// Test_IntermediateNode_ConcurrentVertexWrite — the same scenario, but as it
// arises in production: one operation writes a link, another the body of the
// same vertex, and the second one's time is taken before the first creates
// the intermediate node.
func Test_IntermediateNode_ConcurrentVertexWrite(t *testing.T) {
	restore := SetCacheModeForTest("tree")
	defer restore()

	const vertices = 200
	cs := NewStoreForTest("concurrent")

	var wg sync.WaitGroup
	for i := 0; i < vertices; i++ {
		v := fmt.Sprintf("dom/v-%03d", i)
		// the body time is taken BEFORE the link creates the intermediate node
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
	require.Zero(t, lost, "vertex bodies lost: %d of %d", lost, vertices)
}
