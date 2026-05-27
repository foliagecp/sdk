package debug

import (
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestStore() *ExportSessionStore {
	return &ExportSessionStore{sessions: make(map[string]*exportSession)}
}

// mustPut stores data and returns just the session ID, discarding chunk metadata.
func mustPut(s *ExportSessionStore, data string) string {
	id, _, _ := s.Put(data)
	return id
}

// expireSession backdates a session's TTL so the next cleanup pass removes it.
func expireSession(s *ExportSessionStore, id string) {
	s.mu.Lock()
	if sess, ok := s.sessions[id]; ok {
		sess.expiresAt = time.Now().Add(-time.Minute)
	}
	s.mu.Unlock()
}

// runCleanup executes one cleanup pass (same logic as cleanupLoop body).
func runCleanup(s *ExportSessionStore) {
	now := time.Now()
	s.mu.Lock()
	for id, sess := range s.sessions {
		if now.After(sess.expiresAt) {
			delete(s.sessions, id)
		}
	}
	s.mu.Unlock()
}

// ---- Put ----------------------------------------------------------------

func TestExportSessionStore_Put_ReturnsNonEmptyID(t *testing.T) {
	s := newTestStore()
	id := mustPut(s, "data")
	assert.NotEmpty(t, id)
}

func TestExportSessionStore_Put_ReturnsUniqueIDs(t *testing.T) {
	s := newTestStore()
	ids := make(map[string]struct{}, 10)
	for i := 0; i < 10; i++ {
		id := mustPut(s, "x")
		ids[id] = struct{}{}
	}
	assert.Len(t, ids, 10)
}

// ---- TotalChunks --------------------------------------------------------

func TestExportSessionStore_TotalChunks_Sizes(t *testing.T) {
	s := newTestStore()

	cases := []struct {
		name       string
		dataLen    int
		wantChunks int
	}{
		{"single byte", 1, 1},
		{"exact one chunk", chunkSize, 1},
		{"one byte over chunk", chunkSize + 1, 2},
		{"exact two chunks", 2 * chunkSize, 2},
		{"two chunks minus one", 2*chunkSize - 1, 2},
		{"three chunks not aligned", 3*chunkSize - 7, 3},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			id := mustPut(s, strings.Repeat("x", tc.dataLen))
			total, cs, err := s.TotalChunks(id)
			require.NoError(t, err)
			assert.Equal(t, tc.wantChunks, total)
			assert.Equal(t, chunkSize, cs)
		})
	}
}

func TestExportSessionStore_TotalChunks_UnknownSession(t *testing.T) {
	s := newTestStore()
	_, _, err := s.TotalChunks("no-such-id")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

// ---- GetChunk -----------------------------------------------------------

func TestExportSessionStore_GetChunk_AssemblesFullData(t *testing.T) {
	s := newTestStore()

	// 2.5 chunks worth of distinguishable data
	data := strings.Repeat("a", chunkSize) +
		strings.Repeat("b", chunkSize) +
		strings.Repeat("c", chunkSize/2)

	id := mustPut(s, data)
	total, _, err := s.TotalChunks(id)
	require.NoError(t, err)
	require.Equal(t, 3, total)

	var assembled strings.Builder
	for i := 0; i < total; i++ {
		chunk, last, err := s.GetChunk(id, i)
		require.NoError(t, err, "chunk %d", i)
		assembled.WriteString(chunk)

		if i < total-1 {
			assert.False(t, last, "chunk %d should not be last", i)
		} else {
			assert.True(t, last, "last chunk must set last=true")
		}
	}

	assert.Equal(t, data, assembled.String())
}

func TestExportSessionStore_GetChunk_SingleChunk(t *testing.T) {
	s := newTestStore()
	id := mustPut(s, "hello world")

	chunk, last, err := s.GetChunk(id, 0)
	require.NoError(t, err)
	assert.Equal(t, "hello world", chunk)
	assert.True(t, last)
}

func TestExportSessionStore_GetChunk_Idempotent(t *testing.T) {
	s := newTestStore()
	id := mustPut(s, strings.Repeat("z", chunkSize+1))

	for i := 0; i < 5; i++ {
		c0a, last0a, err := s.GetChunk(id, 0)
		require.NoError(t, err)
		c0b, last0b, _ := s.GetChunk(id, 0)
		assert.Equal(t, c0a, c0b)
		assert.Equal(t, last0a, last0b)
	}
}

func TestExportSessionStore_GetChunk_OutOfRange(t *testing.T) {
	s := newTestStore()
	id := mustPut(s, "small")

	_, _, err := s.GetChunk(id, 1)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "out of range")
}

func TestExportSessionStore_GetChunk_UnknownSession(t *testing.T) {
	s := newTestStore()
	_, _, err := s.GetChunk("ghost", 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

// ---- FinishSession ------------------------------------------------------

func TestExportSessionStore_FinishSession_RemovesSession(t *testing.T) {
	s := newTestStore()
	id := mustPut(s, "data")

	_, _, err := s.GetChunk(id, 0)
	require.NoError(t, err)

	s.FinishSession(id)

	_, _, err = s.GetChunk(id, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestExportSessionStore_FinishSession_NoopOnMissing(t *testing.T) {
	s := newTestStore()
	assert.NotPanics(t, func() { s.FinishSession("ghost") })
}

// ---- TTL cleanup --------------------------------------------------------

func TestExportSessionStore_Cleanup_RemovesExpiredSession(t *testing.T) {
	s := newTestStore()
	id := mustPut(s, "data")

	expireSession(s, id)
	runCleanup(s)

	_, _, err := s.GetChunk(id, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not found")
}

func TestExportSessionStore_Cleanup_PreservesValidSession(t *testing.T) {
	s := newTestStore()
	expired := mustPut(s, "old")
	valid := mustPut(s, "new")

	expireSession(s, expired)
	runCleanup(s)

	_, _, err := s.GetChunk(expired, 0)
	require.Error(t, err)

	_, _, err = s.GetChunk(valid, 0)
	require.NoError(t, err)
}

// ---- Concurrency --------------------------------------------------------

func TestExportSessionStore_ConcurrentReads(t *testing.T) {
	s := newTestStore()
	data := strings.Repeat("x", chunkSize+1) // two chunks
	id := mustPut(s, data)
	total, _, err := s.TotalChunks(id)
	require.NoError(t, err)

	var wg sync.WaitGroup
	const workers = 20
	results := make([]string, workers)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			var b strings.Builder
			for c := 0; c < total; c++ {
				chunk, _, _ := s.GetChunk(id, c)
				b.WriteString(chunk)
			}
			results[idx] = b.String()
		}(i)
	}

	wg.Wait()
	for i, got := range results {
		assert.Equal(t, data, got, "worker %d got wrong data", i)
	}
}

func TestExportSessionStore_ConcurrentPutFinish(t *testing.T) {
	s := newTestStore()

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id := mustPut(s, "ephemeral")
			_, _, err := s.GetChunk(id, 0)
			assert.NoError(t, err)
			s.FinishSession(id)
		}()
	}
	wg.Wait()
}
