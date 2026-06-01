package cache

// White-box tests for the adaptive children-container of StoreValue.
//
// A node with no children carries no container at all (c1 == nil && more ==
// nil). The single-child fast path keeps the one child inline in c1 (88% of
// non-leaf nodes in a real graph). Past one child the node uses a COW
// parallel-slice overflow, upgrading to an 8-shard ShardedMap past
// overflowToShardedThreshold so hot high-fanout nodes keep write striping.
// These tests pin those invariants and verify the concurrent paths are safe.

import (
	"fmt"
	"sync"
	"testing"
)

// Test_StoreValue_FreshNodeHasNoContainer verifies that a newly created node (a
// leaf, the overwhelming majority) holds neither the single-child pointer nor
// an overflow container. This is the property that removes millions of empty
// 8-shard maps from a large graph.
func Test_StoreValue_FreshNodeHasNoContainer(t *testing.T) {
	csv := &StoreValue{}

	if csv.c1.Load() != nil || csv.more.Load() != nil {
		t.Fatalf("fresh node must have empty container (c1=%v more=%v)", csv.c1.Load(), csv.more.Load())
	}
	if n := csv.storeLen(); n != 0 {
		t.Fatalf("fresh node storeLen must be 0, got %d", n)
	}
}

// Test_StoreValue_LoadChildOnEmpty verifies read paths tolerate an empty
// container without panicking and report "no child".
func Test_StoreValue_LoadChildOnEmpty(t *testing.T) {
	csv := &StoreValue{}

	if child, ok := csv.LoadChild("anything"); ok || child != nil {
		t.Fatalf("LoadChild on empty must return (nil,false), got (%v,%v)", child, ok)
	}

	called := false
	csv.Range(func(key, value interface{}) bool {
		called = true
		return true
	})
	if called {
		t.Fatalf("Range over empty container must not invoke the callback")
	}
}

// Test_StoreValue_SingleChildFastPath verifies the first child lands in the
// inline c1 fast path (no overflow allocated), and is retrievable.
func Test_StoreValue_SingleChildFastPath(t *testing.T) {
	parent := &StoreValue{}

	child := &StoreValue{value: []byte("v"), flags: flagValueExists}
	actual, loaded := parent.StoreChild("c1", child)
	if loaded {
		t.Fatalf("first StoreChild must report loaded=false")
	}
	if actual != child {
		t.Fatalf("StoreChild must return the inserted child on first insert")
	}

	if parent.c1.Load() != child {
		t.Fatalf("first child must occupy the inline c1 fast path")
	}
	if parent.more.Load() != nil {
		t.Fatalf("a single-child node must NOT allocate an overflow container")
	}
	if n := parent.storeLen(); n != 1 {
		t.Fatalf("storeLen must be 1 after one child, got %d", n)
	}

	got, ok := parent.LoadChild("c1")
	if !ok || got != child {
		t.Fatalf("inserted child must be retrievable, got (%v,%v)", got, ok)
	}
	if child.parent != parent || child.keyInParent != "c1" {
		t.Fatalf("StoreChild must wire child.parent and child.keyInParent")
	}
}

// Test_StoreValue_AdaptiveTransitions walks the container through all three
// representations — inline single (1), COW slice (2..threshold), sharded
// (>threshold) — and checks every inserted child stays retrievable and the
// count is exact across each transition.
func Test_StoreValue_AdaptiveTransitions(t *testing.T) {
	parent := &StoreValue{}
	total := overflowToShardedThreshold + 5 // cross both transitions

	for i := 0; i < total; i++ {
		key := fmt.Sprintf("k%d", i)
		parent.StoreChild(key, &StoreValue{value: []byte(key), flags: flagValueExists})

		// After 1 child: inline. After 2..threshold: slice overflow. After
		// >threshold: sharded.
		switch {
		case i == 0:
			if parent.c1.Load() == nil || parent.more.Load() != nil {
				t.Fatalf("after 1 child: expected inline c1, no overflow")
			}
		case i+1 <= overflowToShardedThreshold:
			m := parent.more.Load()
			if parent.c1.Load() != nil || m == nil || m.sharded != nil {
				t.Fatalf("after %d children: expected COW-slice overflow", i+1)
			}
		default:
			m := parent.more.Load()
			if m == nil || m.sharded == nil {
				t.Fatalf("after %d children (> %d): expected sharded overflow", i+1, overflowToShardedThreshold)
			}
		}

		if got := parent.storeLen(); got != i+1 {
			t.Fatalf("storeLen must be %d, got %d", i+1, got)
		}
	}

	// All children retrievable after both transitions.
	for i := 0; i < total; i++ {
		key := fmt.Sprintf("k%d", i)
		if got, ok := parent.LoadChild(key); !ok || got == nil {
			t.Fatalf("child %q lost across transitions", key)
		}
	}

	// Range visits exactly the inserted set.
	seen := map[string]bool{}
	parent.Range(func(k, v interface{}) bool {
		seen[k.(string)] = true
		return true
	})
	if len(seen) != total {
		t.Fatalf("Range must visit %d children, saw %d", total, len(seen))
	}
}

// Test_StoreValue_DeleteChild covers removal in each representation.
func Test_StoreValue_DeleteChild(t *testing.T) {
	parent := &StoreValue{}

	// single -> empty
	parent.StoreChild("only", &StoreValue{flags: flagValueExists})
	parent.deleteChild("only")
	if parent.storeLen() != 0 || parent.c1.Load() != nil {
		t.Fatalf("deleting the single child must empty the node")
	}

	// build up to slice, delete from middle
	for i := 0; i < 4; i++ {
		parent.StoreChild(fmt.Sprintf("s%d", i), &StoreValue{flags: flagValueExists})
	}
	parent.deleteChild("s1")
	if parent.storeLen() != 3 {
		t.Fatalf("slice delete must leave 3, got %d", parent.storeLen())
	}
	if _, ok := parent.LoadChild("s1"); ok {
		t.Fatalf("deleted slice child must be gone")
	}
	if _, ok := parent.LoadChild("s2"); !ok {
		t.Fatalf("sibling must survive slice delete")
	}
}

// Test_StoreValue_ConcurrentFirstChild stresses concurrent inserts of distinct
// children into a fresh parent (crosses inline->slice->sharded under racing
// writers). All children must survive; run with -race.
func Test_StoreValue_ConcurrentFirstChild(t *testing.T) {
	const n = 256
	parent := &StoreValue{}

	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("k%d", i)
			parent.StoreChild(key, &StoreValue{value: []byte(key), flags: flagValueExists})
		}(i)
	}
	wg.Wait()

	if got := parent.storeLen(); got != n {
		t.Fatalf("expected %d children after concurrent inserts, got %d", n, got)
	}
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("k%d", i)
		if _, ok := parent.LoadChild(key); !ok {
			t.Fatalf("child %q lost in concurrent insertion", key)
		}
	}
}

// Test_StoreValue_ConcurrentSameKeyDedup verifies that when many goroutines
// race to insert the SAME key, exactly one wins and all callers observe the
// same surviving child (LoadOrStore semantics preserved through the adaptive
// container).
func Test_StoreValue_ConcurrentSameKeyDedup(t *testing.T) {
	const n = 128
	parent := &StoreValue{}

	var wg sync.WaitGroup
	wg.Add(n)
	results := make([]*StoreValue, n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			actual, _ := parent.StoreChild("dup", &StoreValue{value: []byte(fmt.Sprintf("%d", i)), flags: flagValueExists})
			results[i] = actual
		}(i)
	}
	wg.Wait()

	if got := parent.storeLen(); got != 1 {
		t.Fatalf("same-key races must collapse to a single child, got storeLen=%d", got)
	}
	winner, ok := parent.LoadChild("dup")
	if !ok {
		t.Fatalf("the single child must be retrievable")
	}
	for i, r := range results {
		if r != winner {
			t.Fatalf("goroutine %d observed a different child than the survivor", i)
		}
	}
}
