// Foliage graph store debug package.
// Provides debug stateful functions for the graph store
package debug

import (
	"fmt"
	"sync"
	"time"

	"github.com/foliagecp/sdk/statefun/system"
	"github.com/google/uuid"
)

// maxChunkSize caps EXPORT_CHUNK_SIZE to leave headroom for JSON encoding
// overhead on the wire. At 512 KB raw the worst-case JSON-escaped payload
// is ~3 MB, well within the typical NATS max_payload of 8–64 MB.
const maxChunkSize = 512 * 1024

// chunkSize is the byte size of each export chunk.
// 128 KB is conservative: JSON/XML escaping of body content can expand raw bytes
// significantly (up to ×6 for all-control-character payloads), and the chunk
// string is embedded in a JSON envelope on the wire, so the actual NATS message
// can be larger than chunkSize.
// Override with EXPORT_CHUNK_SIZE env var (bytes); clamped to [1, 512 KB].
var chunkSize = func() int {
	v := system.GetEnvMustProceed("EXPORT_CHUNK_SIZE", 128<<10)
	if v <= 0 {
		return 128 << 10
	}
	if v > maxChunkSize {
		return maxChunkSize
	}
	return v
}()

// sessionTTL is how long an export session survives without reads. A var so
// tests can shrink it via SetSessionTTLForTest.
var sessionTTL = 10 * time.Minute

type exportSession struct {
	data      string
	chunkSize int
	expiresAt time.Time
}

// ExportSessionStore holds large export payloads in memory while the client fetches them chunk by chunk.
// Sessions are bound to this runtime process — restart or rebalance invalidates all sessions.
type ExportSessionStore struct {
	mu       sync.Mutex
	sessions map[string]*exportSession
}

var defaultStore = func() *ExportSessionStore {
	s := &ExportSessionStore{sessions: make(map[string]*exportSession)}
	go s.cleanupLoop()
	return s
}()

// Put stores the export data and returns the session ID, total chunk count, and chunk size.
func (s *ExportSessionStore) Put(data string) (sessionID string, totalChunks int, cs int) {
	sessionID = uuid.New().String()
	cs = chunkSize
	totalChunks = (len(data) + cs - 1) / cs
	s.mu.Lock()
	s.sessions[sessionID] = &exportSession{
		data:      data,
		chunkSize: cs,
		expiresAt: time.Now().Add(sessionTTL),
	}
	s.mu.Unlock()
	return sessionID, totalChunks, cs
}

// TotalChunks returns the number of chunks and the chunk size for a session.
func (s *ExportSessionStore) TotalChunks(sessionID string) (total int, chunkSize int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.sessions[sessionID]
	if !ok {
		return 0, 0, fmt.Errorf("session %q not found", sessionID)
	}
	total = (len(sess.data) + sess.chunkSize - 1) / sess.chunkSize
	return total, sess.chunkSize, nil
}

// GetChunk returns a data slice for the given chunk index.
// Reading is idempotent — the session is not deleted. Retry of any chunk is possible until
// FinishSession is called or the TTL expires.
// Each successful read extends the session TTL to prevent expiry during slow transfers.
func (s *ExportSessionStore) GetChunk(sessionID string, index int) (data string, last bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.sessions[sessionID]
	if !ok {
		return "", false, fmt.Errorf("session %q not found or expired (runtime restarted?)", sessionID)
	}
	start := index * sess.chunkSize
	if start >= len(sess.data) {
		return "", false, fmt.Errorf("chunk index %d out of range", index)
	}
	end := start + sess.chunkSize
	if end > len(sess.data) {
		end = len(sess.data)
	}
	sess.expiresAt = time.Now().Add(sessionTTL)
	return sess.data[start:end], end == len(sess.data), nil
}

// FinishSession explicitly frees the session. The client calls this after successful assembly.
func (s *ExportSessionStore) FinishSession(sessionID string) {
	s.mu.Lock()
	delete(s.sessions, sessionID)
	s.mu.Unlock()
}

func (s *ExportSessionStore) cleanupLoop() {
	ticker := time.NewTicker(2 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		now := time.Now()
		s.mu.Lock()
		for id, sess := range s.sessions {
			if now.After(sess.expiresAt) {
				delete(s.sessions, id)
			}
		}
		s.mu.Unlock()
	}
}

// SessionCountForTest returns the number of live export sessions. Test-only.
func SessionCountForTest() int {
	defaultStore.mu.Lock()
	defer defaultStore.mu.Unlock()
	return len(defaultStore.sessions)
}

// SetSessionTTLForTest overrides the export-session TTL so tests can expire
// abandoned sessions in seconds instead of 10 minutes. Set it before starting
// any exports. The background sweep still ticks every 2 minutes — pair with
// SweepExpiredForTest for deterministic expiry.
func SetSessionTTLForTest(ttl time.Duration) { sessionTTL = ttl }

// SweepExpiredForTest synchronously removes expired sessions, independent of
// the background cleanup ticker. Test-only.
func SweepExpiredForTest() {
	now := time.Now()
	defaultStore.mu.Lock()
	for id, sess := range defaultStore.sessions {
		if now.After(sess.expiresAt) {
			delete(defaultStore.sessions, id)
		}
	}
	defaultStore.mu.Unlock()
}
