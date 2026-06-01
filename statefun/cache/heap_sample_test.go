package cache

// Heap-profile sample test. NOT part of normal CI.
//
// Loads keys extracted from a real KV bucket dump (/tmp/keys.idx by default;
// override via HEAP_SAMPLE_FILE) into a real tree of *StoreValue nodes — the
// exact same node type the production cache uses — and writes a heap profile.
// Goals:
//
//   1. Quantify how much of the in-memory cache is the tree-node overhead
//      itself (per-key) vs the stored values.
//   2. Compare two configurations on the SAME inputs:
//        a) load all keys
//        b) skip "<id>.out.index.<name>.type.<type>" keys (the redundant
//           type-index we discussed removing) and report the delta.
//
// Safety on a 16 GB host: the loaded fraction is configurable via
// HEAP_SAMPLE_FRACTION (default 0.10 = 10 %). With ~560 K records in the
// reference dump, 10 % is ~56 K records and stays well under a few GB. Raise
// the fraction only when you know what your machine can hold.
//
// Run:
//
//   go test ./statefun/cache/ -run TestHeapSample -v -count=1
//   HEAP_SAMPLE_FRACTION=0.10 HEAP_SAMPLE_EXCLUDE_TYPE_INDEX=true \
//     go test ./statefun/cache/ -run TestHeapSample -v -count=1
//
// The heap profile is written to /tmp/heap_sample.pprof (override via
// HEAP_SAMPLE_OUT). Inspect with: go tool pprof -top /tmp/heap_sample.pprof
//
// The test is a no-op (PASS, "skipped") when the index file is missing, so it
// does not break a normal `go test ./...` run.

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"runtime"
	"runtime/pprof"
	"strconv"
	"strings"
	"testing"
	"time"
)

const (
	defaultIndexFile = "/tmp/keys.idx"
	defaultFraction  = 0.10
	defaultOutFile   = "/tmp/heap_sample.pprof"
)

func TestHeapSample(t *testing.T) {
	idxPath := envDefault("HEAP_SAMPLE_FILE", defaultIndexFile)
	if _, err := os.Stat(idxPath); err != nil {
		t.Skipf("heap sample skipped: %s missing (set HEAP_SAMPLE_FILE to override)", idxPath)
		return
	}

	fraction := envFloat("HEAP_SAMPLE_FRACTION", defaultFraction)
	if fraction <= 0 || fraction > 1 {
		t.Fatalf("invalid HEAP_SAMPLE_FRACTION=%v (need 0<x<=1)", fraction)
	}
	excludeTypeIdx := envBool("HEAP_SAMPLE_EXCLUDE_TYPE_INDEX", false)
	outPath := envDefault("HEAP_SAMPLE_OUT", defaultOutFile)

	t.Logf("CONFIG  fraction=%.2f  excludeTypeIndex=%v  index=%s  out=%s",
		fraction, excludeTypeIdx, idxPath, outPath)

	// Baseline memstats (before loading) ------------------------------------
	runtime.GC()
	var ms0 runtime.MemStats
	runtime.ReadMemStats(&ms0)

	// Synthetic root: same struct shape the real cache root has.
	root := newSyntheticRoot()

	// Stream-load keys -------------------------------------------------------
	f, err := os.Open(idxPath)
	if err != nil {
		t.Fatalf("open index: %v", err)
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 1<<20)

	var (
		seen, loaded         int
		skippedTypeIdx       int
		skippedByFraction    int
		bytesValueSizeTotal  int64
		valueBufBytesAlloced int64
	)
	keepEvery := int(1.0 / fraction)
	if keepEvery < 1 {
		keepEvery = 1
	}

	start := time.Now()
	for {
		var lb [2]byte
		if _, err := io.ReadFull(r, lb[:]); err != nil {
			break
		}
		keyLen := int(binary.LittleEndian.Uint16(lb[:]))
		keyBuf := make([]byte, keyLen)
		if _, err := io.ReadFull(r, keyBuf); err != nil {
			break
		}
		key := string(keyBuf)
		var vl [4]byte
		if _, err := io.ReadFull(r, vl[:]); err != nil {
			break
		}
		valLen := int(binary.LittleEndian.Uint32(vl[:]))
		seen++

		if excludeTypeIdx && isTypeIndexKey(key) {
			skippedTypeIdx++
			continue
		}
		if seen%keepEvery != 0 {
			skippedByFraction++
			continue
		}

		// Build the chain by walking dot-separated tokens, creating any
		// missing StoreValue intermediates. Same shape getLastKeyToken
		// AndItsParentCacheStoreValue produces in production.
		current := root
		tokens := splitDots(key)
		for i := 0; i < len(tokens)-1; i++ {
			current = childOrCreate(current, tokens[i])
		}
		leaf := childOrCreate(current, tokens[len(tokens)-1])
		// Synthesize a value of the recorded size (content does not matter
		// for allocator sizing — only the byte-slice length does).
		if valLen > 0 {
			b := make([]byte, valLen)
			leaf.value = b
			valueBufBytesAlloced += int64(valLen)
		}
		leaf.setValueExists(true)
		leaf.valueUpdateTime = time.Now().UnixNano()

		bytesValueSizeTotal += int64(valLen)
		loaded++
	}
	elapsed := time.Since(start)

	// Force GC and capture heap state after loading -------------------------
	runtime.GC()
	runtime.GC()
	var ms1 runtime.MemStats
	runtime.ReadMemStats(&ms1)

	// Write heap profile ----------------------------------------------------
	if pf, err := os.Create(outPath); err == nil {
		_ = pprof.WriteHeapProfile(pf)
		pf.Close()
	}

	// Report ----------------------------------------------------------------
	t.Logf("LOAD: seen=%d  loaded=%d  skipped_fraction=%d  skipped_type_idx=%d  in=%s",
		seen, loaded, skippedByFraction, skippedTypeIdx, elapsed.Truncate(time.Millisecond))
	t.Logf("VALUE BYTES (recorded sizes): %.1f MB  (alloced via synthetic []byte slices)",
		float64(valueBufBytesAlloced)/(1<<20))
	t.Logf("HEAP DELTA  (after-load vs before-load):")
	t.Logf("  HeapAlloc:   %s -> %s   (delta %s)",
		humanBytes(ms0.HeapAlloc), humanBytes(ms1.HeapAlloc),
		signedDelta(ms0.HeapAlloc, ms1.HeapAlloc))
	t.Logf("  HeapInuse:   %s -> %s   (delta %s)",
		humanBytes(ms0.HeapInuse), humanBytes(ms1.HeapInuse),
		signedDelta(ms0.HeapInuse, ms1.HeapInuse))
	t.Logf("  HeapObjects: %d -> %d   (+%d)",
		ms0.HeapObjects, ms1.HeapObjects, ms1.HeapObjects-ms0.HeapObjects)
	if loaded > 0 && ms1.HeapAlloc > ms0.HeapAlloc {
		grew := ms1.HeapAlloc - ms0.HeapAlloc
		bytesPerRec := grew / uint64(loaded)
		t.Logf("AVG HEAP COST PER LOADED RECORD: ~%d B   (incl. value buffer)", bytesPerRec)
		t.Logf("    (value bytes share: %.1f MB / %.1f MB = %.1f%%)",
			float64(valueBufBytesAlloced)/(1<<20),
			float64(grew)/(1<<20),
			100*float64(valueBufBytesAlloced)/float64(grew))
	}

	t.Logf("HEAP PROFILE WRITTEN: %s", outPath)
	t.Logf("  Inspect: go tool pprof -top %s", outPath)
	t.Logf("           go tool pprof -alloc_space %s", outPath)

	// CRITICAL: without KeepAlive the compiler may decide `root` is dead after
	// the load loop and let GC reclaim the entire tree before we take the heap
	// snapshot, which makes the measurement useless. Anchor it.
	runtime.KeepAlive(root)
}

// --- helpers ----------------------------------------------------------------

// newSyntheticRoot constructs a StoreValue that mimics the real cache root's
// initialization (see NewCacheStore in cache.go). Crucially, we DO NOT need a
// nats KV / JetStream — we only exercise the in-memory tree.
func newSyntheticRoot() *StoreValue {
	return &StoreValue{
		valueUpdateTime: -1,
	}
}

func childOrCreate(parent *StoreValue, key string) *StoreValue {
	if c, ok := parent.LoadChild(key); ok {
		return c
	}
	child := &StoreValue{
		valueUpdateTime: time.Now().UnixNano(),
	}
	actual, _ := parent.StoreChild(key, child)
	return actual
}

func splitDots(s string) []string {
	return strings.Split(s, ".")
}

// isTypeIndexKey matches "store.<id>.out.index.<name>.type.<type>".
// Used for the "no redundant type-index" A/B comparison.
func isTypeIndexKey(key string) bool {
	parts := strings.Split(key, ".")
	if len(parts) < 6 {
		return false
	}
	if parts[0] != "store" {
		return false
	}
	// look for ".out.index.<name>.type." anywhere from offset 2
	for i := 2; i+4 < len(parts); i++ {
		if parts[i] == "out" && parts[i+1] == "index" && parts[i+3] == "type" {
			return true
		}
	}
	return false
}

func envDefault(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envFloat(k string, def float64) float64 {
	if v := os.Getenv(k); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return f
		}
	}
	return def
}

func envBool(k string, def bool) bool {
	if v := os.Getenv(k); v != "" {
		b, err := strconv.ParseBool(v)
		if err == nil {
			return b
		}
	}
	return def
}

func signedDelta(before, after uint64) string {
	if after >= before {
		return "+" + humanBytes(after-before)
	}
	return "-" + humanBytes(before-after)
}

func humanBytes(n uint64) string {
	const (
		KB = 1 << 10
		MB = 1 << 20
		GB = 1 << 30
	)
	switch {
	case n >= GB:
		return fmt.Sprintf("%.2f GB", float64(n)/GB)
	case n >= MB:
		return fmt.Sprintf("%.2f MB", float64(n)/MB)
	case n >= KB:
		return fmt.Sprintf("%.2f KB", float64(n)/KB)
	default:
		return fmt.Sprintf("%d B", n)
	}
}
