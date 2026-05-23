package cache

// White-box tests for the lazy children-container ("store") of StoreValue.
//
// The memory win of PR #1 rests on a single invariant: a node that has no
// children carries no ShardedMap at all (store == nil). The container is
// allocated only on the first child insertion. These tests pin that
// invariant and verify the concurrent allocation race is safe.

import (
	"fmt"
	"sync"
	"testing"
)

// Test_StoreValue_FreshNodeHasNoContainer verifies that a newly created
// node (a leaf, the overwhelming majority) holds no ShardedMap. This is
// the property that removes ~4.4M empty maps from a large graph.
func Test_StoreValue_FreshNodeHasNoContainer(t *testing.T) {
	csv := &StoreValue{}

	if sm := csv.loadStore(); sm != nil {
		t.Fatalf("fresh node must have nil store, got %p", sm)
	}
	if n := csv.storeLen(); n != 0 {
		t.Fatalf("fresh node storeLen must be 0, got %d", n)
	}
}

// Test_StoreValue_LoadChildOnNilStore verifies read paths tolerate a nil
// container without panicking and report "no child".
func Test_StoreValue_LoadChildOnNilStore(t *testing.T) {
	csv := &StoreValue{}

	if child, ok := csv.LoadChild("anything"); ok || child != nil {
		t.Fatalf("LoadChild on nil store must return (nil,false), got (%v,%v)", child, ok)
	}

	// Range over a nil store must be a no-op (and must not panic).
	called := false
	csv.Range(func(key, value interface{}) bool {
		called = true
		return true
	})
	if called {
		t.Fatalf("Range over nil store must not invoke the callback")
	}
}

// Test_StoreValue_ContainerAllocatedOnFirstChild verifies the container
// appears exactly when the first child is stored, and the child is then
// retrievable.
func Test_StoreValue_ContainerAllocatedOnFirstChild(t *testing.T) {
	parent := &StoreValue{}

	if parent.loadStore() != nil {
		t.Fatalf("precondition: parent must start with nil store")
	}

	child := &StoreValue{value: []byte("v"), valueExists: true}
	actual, loaded := parent.StoreChild("c1", child)
	if loaded {
		t.Fatalf("first StoreChild must report loaded=false")
	}
	if actual != child {
		t.Fatalf("StoreChild must return the inserted child on first insert")
	}

	if parent.loadStore() == nil {
		t.Fatalf("container must be allocated after first child insertion")
	}
	if n := parent.storeLen(); n != 1 {
		t.Fatalf("storeLen must be 1 after one child, got %d", n)
	}

	got, ok := parent.LoadChild("c1")
	if !ok || got != child {
		t.Fatalf("inserted child must be retrievable, got (%v,%v)", got, ok)
	}

	// Parent/key wiring set by StoreChild.
	if child.parent != parent || child.keyInParent != "c1" {
		t.Fatalf("StoreChild must wire child.parent and child.keyInParent")
	}
}

// Test_StoreValue_ensureStoreIdempotent verifies repeated ensureStore
// returns the same container instance (no reallocation).
func Test_StoreValue_ensureStoreIdempotent(t *testing.T) {
	csv := &StoreValue{}
	a := csv.ensureStore()
	b := csv.ensureStore()
	if a == nil || a != b {
		t.Fatalf("ensureStore must return the same non-nil container, got a=%p b=%p", a, b)
	}
}

// Test_StoreValue_ConcurrentFirstChild stresses the lazy-allocation race:
// many goroutines insert distinct children into a fresh parent at once.
// All children must survive and exactly one container must back them.
func Test_StoreValue_ConcurrentFirstChild(t *testing.T) {
	const n = 256
	parent := &StoreValue{}

	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			key := fmt.Sprintf("k%d", i)
			parent.StoreChild(key, &StoreValue{value: []byte(key), valueExists: true})
		}(i)
	}
	wg.Wait()

	if parent.loadStore() == nil {
		t.Fatalf("container must exist after concurrent inserts")
	}
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
// same surviving child (LoadOrStore semantics preserved through lazy alloc).
func Test_StoreValue_ConcurrentSameKeyDedup(t *testing.T) {
	const n = 128
	parent := &StoreValue{}

	var wg sync.WaitGroup
	wg.Add(n)
	results := make([]*StoreValue, n)
	for i := 0; i < n; i++ {
		go func(i int) {
			defer wg.Done()
			actual, _ := parent.StoreChild("dup", &StoreValue{value: []byte(fmt.Sprintf("%d", i)), valueExists: true})
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
