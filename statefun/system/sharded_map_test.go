package system

import (
	"reflect"
	"sort"
	"testing"
	"unicode/utf8"
)

// --- helpers ---

func asSetRunes(rs []rune) map[rune]struct{} {
	m := make(map[rune]struct{}, len(rs))
	for _, r := range rs {
		m[r] = struct{}{}
	}
	return m
}

func asSetStrings(ss []string) map[string]struct{} {
	m := make(map[string]struct{}, len(ss))
	for _, s := range ss {
		m[s] = struct{}{}
	}
	return m
}

// --- expandCharClass tests (package-internal) ---

func Test_expandCharClass_BasicRanges(t *testing.T) {
	rs, err := expandCharClass("a-cA-B0-2")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got := asSetRunes(rs)

	// Expect: a,b,c ; A,B ; 0,1,2
	for _, r := range []rune{'a', 'b', 'c', 'A', 'B', '0', '1', '2'} {
		if _, ok := got[r]; !ok {
			t.Fatalf("expected rune %q in set", r)
		}
	}
}

func Test_expandCharClass_DashAsLiteral(t *testing.T) {
	rs, err := expandCharClass("ab-")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got := asSetRunes(rs)
	// '-' must be treated as a literal here.
	if _, ok := got['-']; !ok {
		t.Fatalf("expected literal '-' in set")
	}

	// Also ensure that "a-_" treats '-' as literal (not a valid range)
	rs2, err := expandCharClass("a-_")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	got2 := asSetRunes(rs2)
	for _, r := range []rune{'a', '-', '_'} {
		if _, ok := got2[r]; !ok {
			t.Fatalf("expected rune %q in set for spec 'a-_'", r)
		}
	}
}

func Test_expandCharClass_InvalidRange(t *testing.T) {
	_, err := expandCharClass("z-a")
	if err == nil {
		t.Fatalf("expected error for invalid range z-a, got nil")
	}
}

func Test_expandCharClass_EmptySpec(t *testing.T) {
	_, err := expandCharClass("")
	if err == nil {
		t.Fatalf("expected error for empty spec, got nil")
	}
}

// --- constructor tests ---

func Test_New_DeduplicatesAndAddsDefaultShard(t *testing.T) {
	// 'a' repeated, expect unique {a,b,c} + 1 default shard => 4 total
	sm, err := New("abca")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if sm.ShardCount() != 4 {
		t.Fatalf("expected 4 shards (3 uniques + 1 default), got %d", sm.ShardCount())
	}

	// Verify that default index points to the last shard.
	if sm.defaultIndex != sm.ShardCount()-1 {
		t.Fatalf("expected defaultIndex == last shard, got %d of %d", sm.defaultIndex, sm.ShardCount())
	}
}

func Test_MustNew_PanicsOnError(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatalf("expected panic from MustNew on bad spec")
		}
	}()
	_ = MustNew("") // empty spec must panic
}

// --- shard selection & basic ops ---

func Test_ShardSelectionByFirstRune(t *testing.T) {
	sm := MustNew("ab/")

	// Directly inspect mapping for first runes (same package access).
	idxA, okA := sm.runeToIndex['a']
	idxB, okB := sm.runeToIndex['b']
	idxSlash, okSlash := sm.runeToIndex['/']
	if !(okA && okB && okSlash) {
		t.Fatalf("expected runeToIndex to contain 'a','b','/'")
	}

	// a* -> idxA
	if got := sm.chooseShard("apple"); got != idxA {
		t.Fatalf("chooseShard('apple') -> %d, want %d", got, idxA)
	}
	// b* -> idxB
	if got := sm.chooseShard("beta"); got != idxB {
		t.Fatalf("chooseShard('beta') -> %d, want %d", got, idxB)
	}
	// /* -> idxSlash
	if got := sm.chooseShard("/route"); got != idxSlash {
		t.Fatalf("chooseShard('/route') -> %d, want %d", got, idxSlash)
	}
	// empty key -> default
	if got := sm.chooseShard(""); got != sm.defaultIndex {
		t.Fatalf("chooseShard(empty) -> %d, want default %d", got, sm.defaultIndex)
	}
	// non-listed first rune (e.g., emoji) -> default
	if got := sm.chooseShard("💎gem"); got != sm.defaultIndex {
		t.Fatalf("chooseShard(emoji*) -> %d, want default %d", got, sm.defaultIndex)
	}
	// invalid UTF-8 -> default
	invalid := string([]byte{0xff, 'a'})
	if utf8.ValidString(invalid) {
		t.Fatalf("constructed string must be invalid UTF-8 for test")
	}
	if got := sm.chooseShard(invalid); got != sm.defaultIndex {
		t.Fatalf("chooseShard(invalidUTF8) -> %d, want default %d", got, sm.defaultIndex)
	}
}

func Test_SetGetDeleteHas(t *testing.T) {
	sm := MustNew("a-zA-Z0-9/=_$#@$%+-")

	sm.Set("apple", 42)
	sm.Set("Zorro", "ok")
	sm.Set("/route", true)

	if v, ok := sm.Get("apple"); !ok || v.(int) != 42 {
		t.Fatalf("Get('apple') = (%v,%v), want (42,true)", v, ok)
	}
	if !sm.Has("/route") {
		t.Fatalf("Has('/route') = false, want true")
	}
	sm.Delete("Zorro")
	if _, ok := sm.Get("Zorro"); ok {
		t.Fatalf("Zorro should be deleted")
	}
}

func Test_LenKeysSnapshotClear(t *testing.T) {
	sm := MustNew("ab")
	keysIn := []string{"apple", "beta", "alpha", "bravo", "a1", "b2"}
	for i, k := range keysIn {
		sm.Set(k, i)
	}
	if sm.Len() != len(keysIn) {
		t.Fatalf("Len() = %d, want %d", sm.Len(), len(keysIn))
	}

	// Keys: compare as sets (order not guaranteed)
	gotKeys := sm.Keys()
	sort.Strings(gotKeys)
	wantSet := asSetStrings(keysIn)
	for _, k := range gotKeys {
		if _, ok := wantSet[k]; !ok {
			t.Fatalf("unexpected key in Keys(): %q", k)
		}
	}

	// Snapshot: map equality by content
	snap := sm.Snapshot()
	if len(snap) != len(keysIn) {
		t.Fatalf("Snapshot len = %d, want %d", len(snap), len(keysIn))
	}
	for k := range wantSet {
		if _, ok := snap[k]; !ok {
			t.Fatalf("snapshot missing key %q", k)
		}
	}

	// Clear
	sm.Clear()
	if sm.Len() != 0 {
		t.Fatalf("Len() after Clear = %d, want 0", sm.Len())
	}
	if len(sm.Keys()) != 0 {
		t.Fatalf("Keys() after Clear should be empty")
	}
}

// --- concurrency tests ---

func Test_ConcurrentAccess(t *testing.T) {
	sm := MustNew("a-zA-Z0-9/=_$#@$%+-")

	// Launch multiple goroutines performing sets/gets in parallel.
	N := 8
	M := 1000
	errCh := make(chan error, N)

	done := make(chan struct{})
	for g := 0; g < N; g++ {
		go func(id int) {
			defer func() {
				if r := recover(); r != nil {
					errCh <- &panicError{r}
				}
			}()
			// write
			for i := 0; i < M; i++ {
				key := []string{"apple", "Beta", "/route", "Zorro", "9lives"}[i%5] + "_" + itoa(i) + "_" + itoa(id)
				sm.Set(key, i+id)
			}
			// read
			for i := 0; i < M; i++ {
				key := []string{"apple", "Beta", "/route", "Zorro", "9lives"}[i%5] + "_" + itoa(i) + "_" + itoa(id)
				_, _ = sm.Get(key)
			}
			// delete a few
			for i := 0; i < M; i += 10 {
				key := []string{"apple", "Beta", "/route", "Zorro", "9lives"}[i%5] + "_" + itoa(i) + "_" + itoa(id)
				sm.Delete(key)
			}
			errCh <- nil
		}(g)
	}

	// Wait for all goroutines to finish
	for i := 0; i < N; i++ {
		if err := <-errCh; err != nil {
			t.Fatalf("goroutine failed: %v", err)
		}
	}
	close(done)

	// Basic sanity: after deletes of every 10th item, there must still be items left.
	if sm.Len() == 0 {
		t.Fatalf("expected non-zero length after concurrent ops")
	}
}

// --- small utilities for tests ---

// panicError wraps recovered panics to implement error for channels.
type panicError struct{ v interface{} }

func (p *panicError) Error() string { return "panic: " + reflect.TypeOf(p.v).String() }

// itoa is a small integer-to-string helper without pulling strconv in tests.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}
