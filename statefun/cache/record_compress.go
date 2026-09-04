package cache

// Compressing cold buckets.
//
// A bucket is a block of bytes with a great deal of structure repeated across
// the whole graph — the same link types, the same target prefixes, the same
// body keys, in vertex after vertex. zstd with a dictionary trained on a sample
// of real buckets exploits exactly that, and it is why compression is worth
// more here than a general-purpose ratio would suggest.
//
// WHEN IT HAPPENS. Never on the write path and never on the read path: writing
// leaves a bucket decoded, maintenance encodes it, and the regulator compresses
// what has stayed cold. A read of a compressed bucket decompresses it and
// publishes the raw form back, so a bucket being read repeatedly stops paying
// for its own compression — the regulator will compress it again once it goes
// quiet, and that hysteresis is the whole point.
//
// DICTIONARIES ARE VERSIONED, AND OLD ONES KEEP WORKING. A zstd frame carries
// the id of the dictionary it was built with, and the decoder is given every
// dictionary this process has ever used. So a bucket compressed with a stale
// dictionary, or with none at all, still reads — which is what makes retraining
// safe to do in the background while the graph is being served.

import (
	"fmt"
	"strings"
	"sync"

	lg "github.com/foliagecp/sdk/statefun/logger"
	"github.com/foliagecp/sdk/statefun/system"
	"github.com/klauspost/compress/zstd"
)

// compressionEnabled is the CACHE_RECORD_COMPRESSION switch.
var compressionEnabled = strings.EqualFold(
	system.GetEnvMustProceed[string]("CACHE_RECORD_COMPRESSION", "off"), "zstd")

// CompressionEnabled reports whether cold buckets are compressed.
func CompressionEnabled() bool { return compressionEnabled }

// SetCompressionForTest flips the switch and returns a function restoring it.
func SetCompressionForTest(on bool) func() {
	prev := compressionEnabled
	compressionEnabled = on
	return func() { compressionEnabled = prev }
}

// minCompressBytes is the size below which compressing is not worth a frame
// header. A bucket of a few short links is already smaller than the overhead.
var minCompressBytes = system.GetEnvMustProceed[int]("CACHE_COMPRESS_MIN_BYTES", 256)

// codec holds the encoder for the current dictionary and a decoder that knows
// every dictionary used so far.
type codec struct {
	mu sync.RWMutex

	enc     *zstd.Encoder // encodes with the newest dictionary, or none
	dec     *zstd.Decoder // decodes anything this process ever wrote
	dicts   [][]byte      // every dictionary, oldest first
	version uint32        // how many dictionaries have been installed
	samples int           // buckets sampled towards the next dictionary
}

// recordCodec is process-wide, not per store: a bucket has no reference to the
// store it belongs to, and threading one through every read to reach a codec
// would cost more than sharing does. Dictionaries are shared as a consequence —
// harmless, since a zstd frame names the dictionary it was built with, so a
// bucket compressed under one is decoded under it whatever else has been
// installed since.
var recordCodec = newCodec()

func newCodec() *codec {
	c := &codec{}
	enc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(zstd.SpeedDefault))
	if err != nil {
		return c // compression stays unavailable; buckets remain raw
	}
	dec, err := zstd.NewReader(nil)
	if err != nil {
		return c
	}
	c.enc, c.dec = enc, dec
	return c
}

func (c *codec) ready() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.enc != nil && c.dec != nil
}

// compress returns the compressed form, or ok=false when it is not worth it.
func (c *codec) compress(raw string) (string, bool) {
	c.mu.RLock()
	enc := c.enc
	c.mu.RUnlock()
	if enc == nil || len(raw) < minCompressBytes {
		return "", false
	}
	out := enc.EncodeAll([]byte(raw), nil)
	if len(out) >= len(raw) {
		return "", false // incompressible: keep the bytes we have
	}
	return string(out), true
}

func (c *codec) decompress(packed string) (string, bool) {
	c.mu.RLock()
	dec := c.dec
	c.mu.RUnlock()
	if dec == nil {
		return "", false
	}
	out, err := dec.DecodeAll([]byte(packed), nil)
	if err != nil {
		return "", false
	}
	return string(out), true
}

// installDict trains a dictionary on the given samples and starts using it.
// The decoder keeps every previous dictionary, so buckets compressed earlier
// stay readable.
func (c *codec) installDict(samples [][]byte) error {
	if len(samples) < 8 {
		return fmt.Errorf("too few samples: %d", len(samples))
	}
	c.mu.Lock()
	nextID := c.version + 1
	c.mu.Unlock()

	// Contents feeds the entropy tables; History IS the dictionary's content —
	// the window later compressions match against. Leaving History empty
	// produces a dictionary of size zero, which the builder rightly refuses.
	dict, err := zstd.BuildDict(zstd.BuildDictOptions{
		ID:       nextID,
		Contents: samples,
		History:  dictHistory(samples, dictBytes),
		// A zstd dictionary carries three "recent offsets" seeding the
		// matcher. Left at zero the encoder rejects the dictionary outright;
		// 1/4/8 are the conventional defaults.
		Offsets: [3]int{1, 4, 8},
		Level:   zstd.SpeedDefault,
	})
	if err != nil {
		return fmt.Errorf("build: %w", err)
	}

	enc, err := zstd.NewWriter(nil,
		zstd.WithEncoderLevel(zstd.SpeedDefault),
		zstd.WithEncoderDict(dict))
	if err != nil {
		return fmt.Errorf("encoder: %w", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	all := append(append([][]byte(nil), c.dicts...), dict)
	dec, err := zstd.NewReader(nil, zstd.WithDecoderDicts(all...))
	if err != nil {
		return fmt.Errorf("decoder: %w", err)
	}
	c.enc, c.dec, c.dicts, c.version = enc, dec, all, nextID
	return nil
}

// dictBytes caps the dictionary's content. Bigger matches more but costs
// memory in every encoder and decoder that holds it.
var dictBytes = system.GetEnvMustProceed[int]("CACHE_DICT_BYTES", 64*1024)

// dictHistory builds the dictionary's content from the samples: the most
// recent bytes, up to the cap. zstd matches against the END of the history, so
// the tail is what earns its keep.
func dictHistory(samples [][]byte, limit int) []byte {
	if limit <= 0 {
		return nil
	}
	total := 0
	first := len(samples)
	for i := len(samples) - 1; i >= 0; i-- {
		total += len(samples[i])
		first = i
		if total >= limit {
			break
		}
	}
	out := make([]byte, 0, total)
	for _, s := range samples[first:] {
		out = append(out, s...)
	}
	if len(out) > limit {
		out = out[len(out)-limit:]
	}
	return out
}

// dictVersion is how many dictionaries have been installed; 0 means none.
func (c *codec) dictVersion() uint32 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.version
}

// ---------------------------------------------------------------------------
// buckets
// ---------------------------------------------------------------------------

// compressed returns the bucket in compressed form, or nil when it should stay
// as it is (already compressed, still decoded, too small, incompressible).
func (b *bucket) compressedForm() *bucket {
	if b == nil || b.decoded || b.compressed {
		return nil
	}
	out, ok := recordCodec.compress(b.data)
	if !ok {
		return nil
	}
	return &bucket{data: out, compressed: true, localDepth: b.localDepth}
}

// rawForm returns the bucket with its bytes decompressed. The original is
// returned untouched when it is not compressed.
func (b *bucket) rawForm() *bucket {
	if b == nil || !b.compressed {
		return b
	}
	raw, ok := recordCodec.decompress(b.data)
	if !ok {
		return b
	}
	return &bucket{data: raw, localDepth: b.localDepth}
}

// readable returns a bucket a lookup can read, decompressing if needed and
// publishing the raw form back so the next read does not repeat the work.
//
// Publishing takes the slot lock only if it is free: a read must never wait on
// a writer, and missing the chance costs one more decompression, not
// correctness.
func (s *bucketSlot) readable() *bucket {
	b := s.ptr.Load()
	if b == nil || !b.compressed {
		return b
	}
	raw := b.rawForm()
	if raw != b && s.mu.TryLock() {
		if s.ptr.Load() == b { // nobody changed it while we worked
			s.ptr.Store(raw)
		}
		s.mu.Unlock()
	}
	return raw
}

// ---------------------------------------------------------------------------
// the record
// ---------------------------------------------------------------------------

// compressBuckets compresses every encoded bucket of the record and returns how
// many it compressed. Decoded buckets are skipped: they were just written, and
// compacting them into bytes comes first (compactBuckets).
func (r *vertexRecord) compressBuckets() int {
	if !compressionEnabled || !recordCodec.ready() {
		return 0
	}
	n := 0
	n += compressDir(r.out.Load())
	n += compressDir(r.in.Load())
	n += compressDir(r.pairs.Load())
	return n
}

func compressDir(d *bucketDir) int {
	if d == nil {
		return 0
	}
	n := 0
	seen := make(map[*bucketSlot]struct{}, len(d.slots))
	for _, s := range d.slots {
		if s == nil {
			continue
		}
		if _, dup := seen[s]; dup {
			continue
		}
		seen[s] = struct{}{}

		s.mu.Lock()
		b := s.ptr.Load()
		if c := b.compressedForm(); c != nil {
			s.ptr.Store(c)
			n++
		}
		s.mu.Unlock()
	}
	return n
}

// compressedBuckets counts buckets currently held compressed.
func (r *vertexRecord) compressedBuckets() int {
	n := 0
	count := func(d *bucketDir) {
		if d == nil {
			return
		}
		seen := make(map[*bucketSlot]struct{}, len(d.slots))
		for _, s := range d.slots {
			if s == nil {
				continue
			}
			if _, dup := seen[s]; dup {
				continue
			}
			seen[s] = struct{}{}
			if b := s.ptr.Load(); b != nil && b.compressed {
				n++
			}
		}
	}
	count(r.out.Load())
	count(r.in.Load())
	count(r.pairs.Load())
	return n
}

// sampleBuckets collects raw bucket bytes towards training a dictionary.
func (r *vertexRecord) sampleBuckets(limit int, out *[][]byte) {
	collect := func(d *bucketDir) {
		if d == nil || len(*out) >= limit {
			return
		}
		seen := make(map[*bucketSlot]struct{}, len(d.slots))
		for _, s := range d.slots {
			if s == nil || len(*out) >= limit {
				return
			}
			if _, dup := seen[s]; dup {
				continue
			}
			seen[s] = struct{}{}
			b := s.ptr.Load()
			if b == nil || b.decoded || b.compressed || len(b.data) < minCompressBytes {
				continue
			}
			*out = append(*out, []byte(b.data))
		}
	}
	collect(r.out.Load())
	collect(r.in.Load())
	collect(r.pairs.Load())
}

// ---------------------------------------------------------------------------
// the store
// ---------------------------------------------------------------------------

// compressRecords compresses cold buckets across the store and returns how many
// it compressed. Called from the maintenance pass, after compaction.
func (cs *Store) compressRecords() int {
	if !compressionEnabled || !tieringEnabled || cs.records == nil {
		return 0
	}
	n := 0
	cs.records.each(func(_ string, r *vertexRecord) bool {
		n += r.compressBuckets()
		return true
	})
	return n
}

// trainDictionary samples raw buckets and installs a dictionary built from
// them, returning whether it installed one.
//
// Retraining is driven by how well the current dictionary still does on fresh
// data rather than by a schedule: a dictionary that stopped matching the graph
// costs ratio, and one that still matches costs nothing to keep.
func (cs *Store) trainDictionary(sampleLimit int) bool {
	if !compressionEnabled || !tieringEnabled || cs.records == nil {
		return false
	}
	samples := make([][]byte, 0, sampleLimit)
	cs.records.each(func(_ string, r *vertexRecord) bool {
		r.sampleBuckets(sampleLimit, &samples)
		return len(samples) < sampleLimit
	})
	if err := recordCodec.installDict(samples); err != nil {
		// Not fatal: buckets keep compressing without a dictionary. Silent
		// failure would be, though — a ratio quietly worse than it should be
		// is exactly the kind of thing nobody notices.
		lg.Logf(lg.WarnLevel, "cache: dictionary training skipped: %s", err)
		return false
	}
	return true
}

// CompressionStatsForTest reports how many buckets are held compressed and the
// dictionary version in use.
func (cs *Store) CompressionStatsForTest() (compressed int, dictVersion uint32) {
	if cs.records != nil {
		cs.records.each(func(_ string, r *vertexRecord) bool {
			compressed += r.compressedBuckets()
			return true
		})
	}
	return compressed, recordCodec.dictVersion()
}

// ---------------------------------------------------------------------------
// keeping the dictionary useful
// ---------------------------------------------------------------------------

// dictState remembers how well the current dictionary did when it was trained,
// so a later measurement can say whether it still does.
type dictState struct {
	mu        sync.Mutex
	trainedAt float64 // compression ratio measured right after training
	lastRatio float64 // ratio on the most recent sample
	retrains  int
}

var dictTracker dictState

// dictDecayTolerance is how much worse than at training time the ratio may get
// before the dictionary is rebuilt.
// dictSampleLimit is how many buckets a training pass looks at.
var dictSampleLimit = system.GetEnvMustProceed[int]("CACHE_DICT_SAMPLES", 256)

var dictDecayTolerance = float64(system.GetEnvMustProceed[int]("CACHE_DICT_DECAY_PCT", 20)) / 100

// measureRatio compresses a sample with the current encoder and reports how
// much smaller it got. Zero means there was nothing to measure.
func (c *codec) measureRatio(samples [][]byte) float64 {
	if len(samples) == 0 {
		return 0
	}
	raw, packed := 0, 0
	for _, s := range samples {
		out, ok := c.compress(string(s))
		raw += len(s)
		if ok {
			packed += len(out)
		} else {
			packed += len(s)
		}
	}
	if packed == 0 {
		return 0
	}
	return float64(raw) / float64(packed)
}

// maybeTrainDictionary trains a dictionary when there is none, and retrains
// when the current one has stopped earning its keep.
//
// Driven by measurement rather than by a schedule: a dictionary that still
// matches the graph costs nothing to keep, and one that stopped matching shows
// up as a worse ratio on fresh buckets. Retraining is safe at any moment —
// every dictionary ever installed stays in the decoder, so what was compressed
// with an older one still reads.
func (cs *Store) maybeTrainDictionary(sampleLimit int) bool {
	if !compressionEnabled || !tieringEnabled || cs.records == nil {
		return false
	}
	samples := make([][]byte, 0, sampleLimit)
	cs.records.each(func(_ string, r *vertexRecord) bool {
		r.sampleBuckets(sampleLimit, &samples)
		return len(samples) < sampleLimit
	})
	if len(samples) < 8 {
		return false
	}

	ratio := recordCodec.measureRatio(samples)

	dictTracker.mu.Lock()
	dictTracker.lastRatio = ratio
	trainedAt := dictTracker.trainedAt
	dictTracker.mu.Unlock()

	needed := recordCodec.dictVersion() == 0 ||
		(trainedAt > 0 && ratio < trainedAt*(1-dictDecayTolerance))
	if !needed {
		return false
	}

	if err := recordCodec.installDict(samples); err != nil {
		lg.Logf(lg.WarnLevel, "cache: dictionary training skipped: %s", err)
		return false
	}

	dictTracker.mu.Lock()
	dictTracker.trainedAt = recordCodec.measureRatio(samples)
	dictTracker.retrains++
	dictTracker.mu.Unlock()
	return true
}

// DictionaryStatsForTest reports the ratio measured at training time, the ratio
// on the latest sample, and how many times the dictionary has been rebuilt.
func DictionaryStatsForTest() (trainedAt, lastRatio float64, retrains int) {
	dictTracker.mu.Lock()
	defer dictTracker.mu.Unlock()
	return dictTracker.trainedAt, dictTracker.lastRatio, dictTracker.retrains
}

// ResetCompressionForTest puts the codec back to having no dictionary, so one
// test's training does not decide another's behaviour — the codec is
// process-wide by design, and tests share a process.
func ResetCompressionForTest() {
	recordCodec.mu.Lock()
	defer recordCodec.mu.Unlock()
	fresh := newCodec()
	recordCodec.enc, recordCodec.dec = fresh.enc, fresh.dec
	recordCodec.dicts, recordCodec.version = nil, 0
	ResetDictionaryStatsForTest()
}

// ResetDictionaryStatsForTest clears the tracker so a test starts clean.
func ResetDictionaryStatsForTest() {
	dictTracker.mu.Lock()
	defer dictTracker.mu.Unlock()
	// Fields only: assigning the whole struct would replace the mutex this
	// call is holding, and the deferred unlock would then release a different,
	// unlocked one.
	dictTracker.trainedAt = 0
	dictTracker.lastRatio = 0
	dictTracker.retrains = 0
}
