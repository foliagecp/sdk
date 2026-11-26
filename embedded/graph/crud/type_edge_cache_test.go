package crud

// The cache of types links that belong to another domain.
//
// Their owner does not announce changes, so a cached objects-link type is
// trusted for typeEdgeTTL from the moment its read began: a change made by
// another runtime is resolved here no later than that. A read overtaken by a
// purge of the cache stores nothing, and neither does a read that failed.

import (
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func typeEdgeCacheForTest(t *testing.T, ttl time.Duration) {
	t.Helper()
	saved := typeEdgeTTL
	typeEdgeTTL = ttl
	cachePurgeTypeEdges()
	t.Cleanup(func() {
		typeEdgeTTL = saved
		cachePurgeTypeEdges()
	})
}

// One runtime cached the type, another changed it: the first resolves the new
// type once the entry it holds is typeEdgeTTL old, and not from the cache
// after that.
func Test_TypeEdgeCache_ChangeMadeElsewhereIsSeenWithinTTL(t *testing.T) {
	const ttl = 300 * time.Millisecond
	typeEdgeCacheForTest(t, ttl)

	declared, reads := "t1", 0
	resolve := func() string {
		lt, err := cachedObjectsLinkType("hub/A", "hub/B", func() (string, error) {
			reads++
			return declared, nil
		})
		require.NoError(t, err)
		return lt
	}

	require.Equal(t, "t1", resolve())
	changedAt := time.Now()
	declared = "t2"
	if lt := resolve(); time.Since(changedAt) < ttl/2 {
		require.Equal(t, "t1", lt, "a fresh entry answers without a read")
		require.Equal(t, 1, reads)
	}

	deadline := changedAt.Add(ttl + 100*time.Millisecond)
	for resolve() != "t2" {
		require.Truef(t, time.Now().Before(deadline), "still resolving the old type %s after the change", time.Since(changedAt))
		time.Sleep(10 * time.Millisecond)
	}
	require.Equal(t, 2, reads, "one read per lifetime of an entry, not one per call")
}

// The age of an entry counts from the start of its read: a value that was
// already old when a slow read returned is not trusted for a full lifetime
// more.
func Test_TypeEdgeCache_EntryAgeCountsFromTheStartOfItsRead(t *testing.T) {
	const ttl = 300 * time.Millisecond
	typeEdgeCacheForTest(t, ttl)

	_, err := cachedObjectsLinkType("hub/A", "hub/B", func() (string, error) {
		time.Sleep(ttl + 50*time.Millisecond)
		return "t1", nil
	})
	require.NoError(t, err)
	_, fresh := cacheGetTypeEdge("hub/A", "hub/B")
	require.False(t, fresh)
}

func Test_TypeEdgeCache_ReadOvertakenByPurgeStoresNothing(t *testing.T) {
	typeEdgeCacheForTest(t, time.Minute)

	reading, purged := make(chan struct{}), make(chan struct{})
	done := make(chan string, 1)
	go func() {
		lt, _ := cachedObjectsLinkType("hub/A", "hub/B", func() (string, error) {
			close(reading)
			<-purged
			return "t1", nil // read before the purge, returned after it
		})
		done <- lt
	}()
	<-reading
	cachePurgeTypeEdges()
	close(purged)
	require.Equal(t, "t1", <-done, "the caller still gets what it read")

	_, cached := cacheGetTypeEdge("hub/A", "hub/B")
	require.False(t, cached, "but the value from before the purge is not in the cache after it")

	lt, err := cachedObjectsLinkType("hub/A", "hub/B", func() (string, error) { return "t2", nil })
	require.NoError(t, err)
	require.Equal(t, "t2", lt)
}

func Test_TypeEdgeCache_FailedReadStoresNothing(t *testing.T) {
	typeEdgeCacheForTest(t, time.Minute)

	_, err := cachedObjectsLinkType("hub/A", "hub/B", func() (string, error) { return "", errors.New("no such link") })
	require.Error(t, err)
	_, cached := cacheGetTypeEdge("hub/A", "hub/B")
	require.False(t, cached)
	require.Zero(t, TypeEdgeCacheSizeForTest())
}
