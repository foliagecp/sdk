package cache

// Compact vertex records — the storage form behind CACHE_TIERING.
//
// WHY. A vertex in the tree costs about 2.6 KB per link: nine nodes, each with
// its own key string, container and pointers, for thirty-odd bytes of actual
// data. The same vertex written as one pointer-free byte block costs a few
// hundred. That is the difference between a graph that fits in memory and one
// that does not.
//
// WHAT A RECORD HOLDS. Everything CRUD writes under a vertex key:
//
//	V                                  vertex body
//	V.out.to.<name>                    "<type>.<target>"
//	V.out.body.<name>                  link body
//	V.ltype.<type>.<target>            "<name>"
//	V.out.index.<name>.type.<type>     (empty marker)
//	V.out.index.<name>.tag.<tag>       (empty marker)
//	V.in.<from>.<name>                 "<type>"
//
// so out-links carry name, type, target, body, tags and an update time, and
// in-links carry source, name, type and a time. Every one of those keys is
// answered from the record without rebuilding a tree.
//
// BUCKETS, NOT WHOLE RECORDS. Writes rewrite one bucket, never the record: on a
// real graph 0.56% of vertices hold 41.5% of all links (`objects` alone has
// 10 692), and re-encoding such a vertex per write would make creating an
// object cost tens of milliseconds. Links are distributed over buckets by the
// hash of their name (out) or source (in), with an extendible-hashing
// directory, so a write costs the size of one bucket regardless of degree.
//
// TOMBSTONES. Deleting a key in the tree leaves a tombstone carrying the delete
// time, and it exists for exactly one reason: a late write with an older
// timestamp must not resurrect the key (the last-writer-wins guard in Put).
// The time itself is NOT observable — GetValueUpdateTime returns -1 for a
// tombstone, the same as for a missing key — so a record stores the guard and
// nothing else, and a tombstoned entry reads as absent.

import (
	"encoding/binary"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/foliagecp/sdk/statefun/system"
)

const (
	recordVersion = 1

	// recordHeaderLen is the fixed prefix; every offset below is from the
	// start of the record.
	recordHeaderLen = 32

	// flagBodyTombstoned marks a vertex whose body was deleted: the guard time
	// stays in the header, reads see nothing.
	flagBodyTombstoned uint8 = 1 << 0

	// flagBodyJSON records that the body was written as a JSON value rather
	// than as bytes. The tree keeps the same distinction in its node flags, and
	// ExistsJson answers by it, so a record that lost it could not give the
	// same answer as the tree.
	flagBodyJSON uint8 = 1 << 1

	// entryTombstoned marks a link entry that is a guard rather than a link.
	entryTombstoned uint8 = 1 << 0
)

// defaultBucketLinks is K from the spec: the number of links a bucket holds
// before it splits. 32 keeps a bucket rewrite near 100 µs, and an ordinary
// vertex inside a single bucket per direction.
var defaultBucketLinks = system.GetEnvMustProceed[int]("CACHE_BUCKET_LINKS", 32)

// record is an immutable encoded vertex. Immutable is what lets readers walk it
// without locks: a write publishes a new block, it never edits one in place.
type record string

// outLink is the decoded form of one outgoing link.
type outLink struct {
	Name       string
	Type       string
	Target     string
	Body       []byte // raw JSON, nil when the link has no body
	Tags       []string
	UpdateTime int64
	Tombstone  bool
}

// inLink is the decoded form of one incoming link.
type inLink struct {
	From       string
	Name       string
	Type       string
	UpdateTime int64
	Tombstone  bool
}

// vertexData is what an encoder is handed and a decoder can reproduce.
type vertexData struct {
	Body     []byte // raw JSON of the vertex body, nil when absent
	BodyTime int64
	BodyDead bool
	BodyJSON bool
	Out      []outLink
	In       []inLink
	OutDepth uint8 // directory depth; 0 means a single bucket
	InDepth  uint8
}

// ---------------------------------------------------------------------------
// the record itself
// ---------------------------------------------------------------------------
//
// A record is NOT one contiguous block. Buckets are separate immutable blocks
// behind atomic pointers, because that is the only shape in which writing one
// link costs one bucket: with everything in a single blob, changing a link
// would rebuild the whole vertex, which is exactly what buckets exist to avoid
// (`objects` has 10 692 links).
//
// Readers take no locks: they load the directory pointer, index it by hash and
// read an immutable bucket. A writer publishes a new bucket into the slot.

type vertexRecord struct {
	// head is the header plus the vertex body, replaced wholesale on a body
	// write and therefore held behind an atomic pointer — a reader must never
	// see a half-written body:
	//   0  1  version
	//   1  1  flags
	//   2  2  (reserved)
	//   4  8  body update time
	//  12  …  body bytes
	head atomic.Pointer[string]

	out atomic.Pointer[bucketDir]
	in  atomic.Pointer[bucketDir]

	// headMu serializes body writes; dirMu serializes structural changes to a
	// directory (splitting a bucket, doubling the directory). Ordinary link
	// writes take neither — they take the lock of their own slot.
	headMu sync.Mutex
	dirMu  sync.Mutex
}

const recordHeadLen = 12

// bucketDir is an extendible-hashing directory: 2^depth slots, several of
// which may point at the same bucket after a split.
type bucketDir struct {
	depth uint8

	// Several directory entries may hold the SAME *bucketSlot — that is what
	// extendible hashing does before a bucket has been split to the global
	// depth. The indirection matters: a write publishes a new block into the
	// slot, and every entry sharing that slot must see it. Publishing into one
	// directory entry instead would leave the others on the old block, and the
	// link would be readable from both — which is exactly the bug this shape
	// prevents.
	slots []*bucketSlot
}

// bucketSlot owns one bucket: the block itself behind an atomic pointer for
// lock-free readers, and the lock a writer takes to replace it. The lock lives
// with the slot rather than the directory entry, so entries that share a slot
// share its lock too, and two writers to the same bucket serialize even when
// they arrived through different entries.
type bucketSlot struct {
	mu  sync.Mutex
	ptr atomic.Pointer[bucket]
}

func newBucketSlot(b *bucket) *bucketSlot {
	s := &bucketSlot{}
	s.ptr.Store(b)
	return s
}

// bucket is an immutable encoded block of entries plus the local depth that
// extendible hashing needs to decide whether a split must grow the directory.
type bucket struct {
	data       string
	localDepth uint8
}

func makeHead(body []byte, bodyTime int64, bodyDead, bodyJSON bool) string {
	buf := make([]byte, recordHeadLen, recordHeadLen+len(body))
	buf[0] = recordVersion
	if bodyDead {
		buf[1] |= flagBodyTombstoned
	}
	if bodyJSON {
		buf[1] |= flagBodyJSON
	}
	binary.LittleEndian.PutUint64(buf[4:], uint64(bodyTime))
	buf = append(buf, body...)
	return string(buf)
}

func (r *vertexRecord) headStr() string {
	if r == nil {
		return ""
	}
	if h := r.head.Load(); h != nil {
		return *h
	}
	return ""
}

func (r *vertexRecord) valid() bool {
	h := r.headStr()
	return len(h) >= recordHeadLen && h[0] == recordVersion
}

func (r *vertexRecord) flags() uint8 { return r.headStr()[1] }

// bodyBytes returns the vertex body. A tombstoned body reads as absent — the
// guard time stays for writers only.
func (r *vertexRecord) bodyBytes() (string, int64, bool) {
	if !r.valid() || r.flags()&flagBodyTombstoned != 0 {
		return "", -1, false
	}
	h := r.headStr()
	if len(h) == recordHeadLen {
		return "", -1, false
	}
	return h[recordHeadLen:], int64(le64(h, 4)), true
}

// bodyIsJSON reports whether the body was stored as a JSON value.
func (r *vertexRecord) bodyIsJSON() bool {
	return r.valid() && r.flags()&flagBodyJSON != 0
}

// bodyGuardTime is the time a writer must beat, tombstone or not.
func (r *vertexRecord) bodyGuardTime() int64 {
	if !r.valid() {
		return -1
	}
	return int64(le64(r.headStr(), 4))
}

func (d *bucketDir) slotIndex(h uint32) int {
	if d.depth == 0 {
		return 0
	}
	return int(h & ((1 << d.depth) - 1))
}

func (d *bucketDir) slotFor(h uint32) *bucketSlot {
	if d == nil || len(d.slots) == 0 {
		return nil
	}
	return d.slots[d.slotIndex(h)]
}

func (d *bucketDir) bucketFor(h uint32) *bucket {
	if s := d.slotFor(h); s != nil {
		return s.ptr.Load()
	}
	return nil
}

// each visits every distinct bucket once: after a split two slots share one
// bucket, and visiting it twice would list its links twice.
func (d *bucketDir) each(fn func(b *bucket) bool) {
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
		b := s.ptr.Load()
		if b == nil {
			continue
		}
		if !fn(b) {
			return
		}
	}
}

// ---------------------------------------------------------------------------
// buckets
// ---------------------------------------------------------------------------
//
// A bucket is: 4 bytes count, then count 4-byte offsets of its entries (from
// the bucket start), then the entries themselves, sorted by their key. The
// offset table is what makes a lookup a binary search instead of a scan — which
// is the whole reason a record with ten thousand links answers as fast as one
// with thirty.

func bucketCount(b string) int {
	if len(b) < 4 {
		return 0
	}
	return int(le32(b, 0))
}

func bucketEntry(b string, i int) string {
	off := int(le32(b, 4+i*4))
	if off <= 0 || off > len(b) {
		return ""
	}
	end := len(b)
	if i+1 < bucketCount(b) {
		if next := int(le32(b, 4+(i+1)*4)); next > off && next <= len(b) {
			end = next
		}
	}
	return b[off:end]
}

// bucketSearch finds the entry whose key equals want, comparing with keyOf.
func bucketSearch(b string, want string, keyOf func(entry string) string) (string, bool) {
	lo, hi := 0, bucketCount(b)-1
	for lo <= hi {
		mid := int(uint(lo+hi) >> 1)
		e := bucketEntry(b, mid)
		switch k := keyOf(e); {
		case k < want:
			lo = mid + 1
		case k > want:
			hi = mid - 1
		default:
			return e, true
		}
	}
	return "", false
}

// ---------------------------------------------------------------------------
// entry encoding
// ---------------------------------------------------------------------------

func putUvarint(dst []byte, v uint64) []byte {
	var tmp [binary.MaxVarintLen64]byte
	n := binary.PutUvarint(tmp[:], v)
	return append(dst, tmp[:n]...)
}

func putBytes(dst []byte, b []byte) []byte {
	dst = putUvarint(dst, uint64(len(b)))
	return append(dst, b...)
}

func putString(dst []byte, s string) []byte {
	dst = putUvarint(dst, uint64(len(s)))
	return append(dst, s...)
}

// Entry decoding is written as plain functions over (text, index) rather than a
// reader struct on purpose: a &reader{} taken inside a lookup escapes to the
// heap, and outLinkName is called once per step of the binary search — that
// alone put six allocations on every read. Passing the index back keeps the
// whole path allocation-free.

func readUvarint(s string, i int) (uint64, int) {
	var v uint64
	var shift uint
	for i < len(s) {
		b := s[i]
		i++
		if b < 0x80 {
			return v | uint64(b)<<shift, i
		}
		v |= uint64(b&0x7f) << shift
		shift += 7
		if shift > 63 {
			return 0, len(s)
		}
	}
	return 0, len(s)
}

func readStr(s string, i int) (string, int) {
	n, i := readUvarint(s, i)
	if i+int(n) > len(s) {
		return "", len(s)
	}
	return s[i : i+int(n)], i + int(n)
}

func readInt64(s string, i int) (int64, int) {
	if i+8 > len(s) {
		return 0, len(s)
	}
	return int64(le64(s, i)), i + 8
}

// out-link entry: name, flags, time, type, target, body, tags
func encodeOutLink(dst []byte, l outLink) []byte {
	dst = putString(dst, l.Name)
	var f byte
	if l.Tombstone {
		f |= entryTombstoned
	}
	dst = append(dst, f)
	var t [8]byte
	binary.LittleEndian.PutUint64(t[:], uint64(l.UpdateTime))
	dst = append(dst, t[:]...)
	if l.Tombstone {
		return dst // a guard carries nothing else
	}
	dst = putString(dst, l.Type)
	dst = putString(dst, l.Target)
	dst = putBytes(dst, l.Body)
	dst = putUvarint(dst, uint64(len(l.Tags)))
	for _, tag := range l.Tags {
		dst = putString(dst, tag)
	}
	return dst
}

func decodeOutLink(entry string) outLink {
	var l outLink
	i := 0
	l.Name, i = readStr(entry, i)
	if i >= len(entry) {
		return l
	}
	l.Tombstone = entry[i]&entryTombstoned != 0
	i++
	l.UpdateTime, i = readInt64(entry, i)
	if l.Tombstone {
		return l
	}
	l.Type, i = readStr(entry, i)
	l.Target, i = readStr(entry, i)
	var body string
	body, i = readStr(entry, i)
	if len(body) > 0 {
		l.Body = []byte(body)
	}
	var n uint64
	n, i = readUvarint(entry, i)
	if n > 0 {
		l.Tags = make([]string, n)
		for k := range l.Tags {
			l.Tags[k], i = readStr(entry, i)
		}
	}
	return l
}

// outLinkName reads only the name — the key a bucket is sorted by. Used by the
// binary search, so it must not decode anything else.
func outLinkName(entry string) string {
	n, _ := readStr(entry, 0)
	return n
}

// outLinkTarget decodes only what the `V.out.to.<name>` key needs — type and
// target — skipping body and tags, which a reader of that key never looks at.
func outLinkTarget(entry string) (linkType, target string, tombstoned bool, ok bool) {
	i := 0
	_, i = readStr(entry, i)
	if i >= len(entry) {
		return "", "", false, false
	}
	tombstoned = entry[i]&entryTombstoned != 0
	i++
	_, i = readInt64(entry, i)
	if tombstoned {
		return "", "", true, true
	}
	linkType, i = readStr(entry, i)
	target, _ = readStr(entry, i)
	return linkType, target, false, true
}

// in-link entry: from, name, flags, time, type
func encodeInLink(dst []byte, l inLink) []byte {
	dst = putString(dst, l.From)
	dst = putString(dst, l.Name)
	var f byte
	if l.Tombstone {
		f |= entryTombstoned
	}
	dst = append(dst, f)
	var t [8]byte
	binary.LittleEndian.PutUint64(t[:], uint64(l.UpdateTime))
	dst = append(dst, t[:]...)
	if l.Tombstone {
		return dst
	}
	return putString(dst, l.Type)
}

func decodeInLink(entry string) inLink {
	var l inLink
	i := 0
	l.From, i = readStr(entry, i)
	l.Name, i = readStr(entry, i)
	if i >= len(entry) {
		return l
	}
	l.Tombstone = entry[i]&entryTombstoned != 0
	i++
	l.UpdateTime, i = readInt64(entry, i)
	if !l.Tombstone {
		l.Type, _ = readStr(entry, i)
	}
	return l
}

// inLinkKey is the sort key of an in-link: source then name, joined by a byte
// that cannot occur in either (both are cache key tokens).
func inLinkKey(entry string) string {
	from, i := readStr(entry, 0)
	name, _ := readStr(entry, i)
	return from + "\x00" + name
}

func makeInLinkKey(from, name string) string { return from + "\x00" + name }

// ---------------------------------------------------------------------------
// building a record
// ---------------------------------------------------------------------------

func hashToken(s string) uint32 {
	// FNV-1a inline: hash.Hash32 would allocate on a path taken once per read.
	var h uint32 = 2166136261
	for i := 0; i < len(s); i++ {
		h ^= uint32(s[i])
		h *= 16777619
	}
	return h
}

// depthFor picks a directory depth that keeps buckets near K entries.
func depthFor(n, k int) uint8 {
	if n <= k || k <= 0 {
		return 0
	}
	d := uint8(0)
	for (n>>d) > k && d < 16 {
		d++
	}
	return d
}

func buildDir[T any](items []T, keyOf func(T) string, depth uint8, encode func([]T) string,
	sortLess func(a, b T) bool) *bucketDir {
	n := 1 << depth
	groups := make([][]T, n)
	for _, it := range items {
		idx := 0
		if depth > 0 {
			idx = int(hashToken(keyOf(it)) & ((1 << depth) - 1))
		}
		groups[idx] = append(groups[idx], it)
	}
	d := &bucketDir{depth: depth, slots: make([]*bucketSlot, n)}
	for i := range groups {
		sort.Slice(groups[i], func(a, b int) bool { return sortLess(groups[i][a], groups[i][b]) })
		d.slots[i] = newBucketSlot(&bucket{data: encode(groups[i]), localDepth: depth})
	}
	return d
}

// newVertexRecord lays a vertex out as a head plus two bucket directories.
func newVertexRecord(v vertexData, bucketLinks int) *vertexRecord {
	if bucketLinks <= 0 {
		bucketLinks = defaultBucketLinks
	}
	outDepth := v.OutDepth
	if outDepth == 0 {
		outDepth = depthFor(len(v.Out), bucketLinks)
	}
	inDepth := v.InDepth
	if inDepth == 0 {
		inDepth = depthFor(len(v.In), bucketLinks)
	}

	r := &vertexRecord{}
	h := makeHead(v.Body, v.BodyTime, v.BodyDead, v.BodyJSON)
	r.head.Store(&h)
	r.out.Store(buildDir(v.Out, func(l outLink) string { return l.Name }, outDepth,
		encodeOutBucket, func(a, b outLink) bool { return a.Name < b.Name }))
	r.in.Store(buildDir(v.In, func(l inLink) string { return l.From }, inDepth,
		encodeInBucket, func(a, b inLink) bool {
			if a.From != b.From {
				return a.From < b.From
			}
			return a.Name < b.Name
		}))
	return r
}

func encodeOutBucket(links []outLink) string {
	head := make([]byte, 4+len(links)*4)
	binary.LittleEndian.PutUint32(head[0:], uint32(len(links)))
	body := head
	for i, l := range links {
		binary.LittleEndian.PutUint32(body[4+i*4:], uint32(len(body)))
		body = encodeOutLink(body, l)
	}
	return string(body)
}

func encodeInBucket(links []inLink) string {
	head := make([]byte, 4+len(links)*4)
	binary.LittleEndian.PutUint32(head[0:], uint32(len(links)))
	body := head
	for i, l := range links {
		binary.LittleEndian.PutUint32(body[4+i*4:], uint32(len(body)))
		body = encodeInLink(body, l)
	}
	return string(body)
}

// ---------------------------------------------------------------------------
// lookups
// ---------------------------------------------------------------------------

// lookupOutTarget answers `V.out.to.<name>` — the hottest key of a traversal —
// without materializing the link: type and target only, no body, no tags, no
// allocation on the path.
func (r *vertexRecord) lookupOutTarget(name string) (linkType, target string, ok bool) {
	b := r.out.Load().bucketFor(hashToken(name))
	if b == nil {
		return "", "", false
	}
	e, found := bucketSearch(b.data, name, outLinkName)
	if !found {
		return "", "", false
	}
	lt, tgt, tomb, valid := outLinkTarget(e)
	if !valid || tomb {
		return "", "", false
	}
	return lt, tgt, true
}

func (r *vertexRecord) lookupOutLink(name string) (outLink, bool) {
	l, ok := r.lookupOutLinkGuard(name)
	if !ok || l.Tombstone {
		return outLink{}, false
	}
	return l, true
}

// lookupOutLinkGuard returns the entry even when it is a tombstone: this is
// what a writer consults before deciding whether its timestamp may apply.
func (r *vertexRecord) lookupOutLinkGuard(name string) (outLink, bool) {
	b := r.out.Load().bucketFor(hashToken(name))
	if b == nil {
		return outLink{}, false
	}
	e, found := bucketSearch(b.data, name, outLinkName)
	if !found {
		return outLink{}, false
	}
	return decodeOutLink(e), true
}

func (r *vertexRecord) lookupInLink(from, name string) (inLink, bool) {
	l, ok := r.lookupInLinkGuard(from, name)
	if !ok || l.Tombstone {
		return inLink{}, false
	}
	return l, true
}

func (r *vertexRecord) lookupInLinkGuard(from, name string) (inLink, bool) {
	b := r.in.Load().bucketFor(hashToken(from))
	if b == nil {
		return inLink{}, false
	}
	e, found := bucketSearch(b.data, makeInLinkKey(from, name), inLinkKey)
	if !found {
		return inLink{}, false
	}
	return decodeInLink(e), true
}

// rangeOutLinks visits every live outgoing link. Order is by bucket, not by
// name — the tree's own enumeration makes no ordering promise either.
func (r *vertexRecord) rangeOutLinks(fn func(outLink) bool) {
	r.out.Load().each(func(b *bucket) bool {
		for i, n := 0, bucketCount(b.data); i < n; i++ {
			l := decodeOutLink(bucketEntry(b.data, i))
			if l.Tombstone {
				continue
			}
			if !fn(l) {
				return false
			}
		}
		return true
	})
}

func (r *vertexRecord) rangeInLinks(fn func(inLink) bool) {
	r.in.Load().each(func(b *bucket) bool {
		for i, n := 0, bucketCount(b.data); i < n; i++ {
			l := decodeInLink(bucketEntry(b.data, i))
			if l.Tombstone {
				continue
			}
			if !fn(l) {
				return false
			}
		}
		return true
	})
}

// lookupOutLinkByTypeTarget answers the `V.ltype.<type>.<target>` key shape,
// which CRUD uses to find a link's name from its endpoints.
//
// That key is NOT a property of one link: it is keyed by the pair, so two links
// of the same type to the same target — legal, since a link is identified by
// its name — share it, and the tree holds whichever was written last. A record
// has no write order, so it resolves the pair by the newest update time, with
// the name as a deterministic tie-break. Exactness for simultaneous writes to a
// colliding pair would need the mapping stored in its own right; the shapes
// CRUD produces carry increasing operation times, so the newest link is the one
// that wrote the key.
func (r *vertexRecord) lookupOutLinkByTypeTarget(linkType, target string) (outLink, bool) {
	var found outLink
	var ok bool
	r.rangeOutLinks(func(l outLink) bool {
		if l.Type != linkType || l.Target != target {
			return true
		}
		if !ok || l.UpdateTime > found.UpdateTime ||
			(l.UpdateTime == found.UpdateTime && l.Name > found.Name) {
			found, ok = l, true
		}
		return true
	})
	return found, ok
}

// ---------------------------------------------------------------------------
// small helpers
// ---------------------------------------------------------------------------

func le32(s string, i int) uint32 {
	return uint32(s[i]) | uint32(s[i+1])<<8 | uint32(s[i+2])<<16 | uint32(s[i+3])<<24
}

func le64(s string, i int) uint64 {
	return uint64(le32(s, i)) | uint64(le32(s, i+4))<<32
}
