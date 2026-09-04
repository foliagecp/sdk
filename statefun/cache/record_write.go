package cache

// Writing into a compact vertex without rebuilding it.
//
// A write touches ONE bucket. That is the whole point: on a real graph
// 0.56% of vertices hold 41.5% of the links, and `objects` — the enumeration
// root every object hangs from — has 10 692 of them. Re-encoding such a vertex
// on every object creation would cost tens of milliseconds; re-encoding one
// bucket of 32 costs microseconds, whatever the degree.
//
// CONCURRENCY. Buckets are immutable; a writer publishes a replacement into an
// atomic slot, so readers never lock and never see a half-written block.
// Publishing alone is not enough, though: two writers changing different links
// of the SAME bucket would both decode the old block and the second would erase
// the first. So a write holds the lock of its own slot for the whole
// decode-modify-encode-publish, and only that lock. Writes to different buckets
// of the same vertex proceed in parallel.
//
// Structural changes — splitting a bucket, doubling the directory — take the
// record's dirMu instead, publish a whole new directory, and are invisible to
// readers, who load the directory pointer once. A slot writer that finds the
// directory replaced under it simply retries against the new one.
//
// These locks are the record's own; they are never held together with a node
// lock of the tree, so the cache's "no path holds two node locks" invariant —
// the thing that makes its shared lock pool deadlock-free — is untouched.
//
// LAST WRITER WINS. Every entry carries the time of the operation that wrote
// it, and a write with an older time is dropped. Deleting leaves a tombstone
// carrying the delete time for exactly that reason: without it a late write
// would resurrect a deleted link. A tombstone is invisible to readers.

import (
	"sort"
)

// bucketWriteResult says what a mutation did, so the caller can decide whether
// the bucket now needs splitting.
type bucketWriteResult struct {
	applied bool // false when the last-writer-wins guard rejected the write
	count   int  // entries in the bucket after the write, tombstones included
}

// withOutSlot runs fn under the lock of the slot serving key, retrying if the
// directory was replaced (split or doubling) while we waited for the lock.
func (r *vertexRecord) withOutSlot(key string, fn func(b *bucket) (*bucket, bucketWriteResult)) (*bucketDir, bucketWriteResult) {
	for {
		d := r.out.Load()
		s := d.slots[d.slotIndex(hashToken(key))]
		s.mu.Lock()
		if r.out.Load() != d {
			// The directory changed under us; the slot we locked may no longer
			// serve this key. Start over against the current one.
			s.mu.Unlock()
			continue
		}
		nb, res := fn(s.ptr.Load())
		if nb != nil {
			s.ptr.Store(nb)
		}
		s.mu.Unlock()
		return d, res
	}
}

func (r *vertexRecord) withInSlot(key string, fn func(b *bucket) (*bucket, bucketWriteResult)) (*bucketDir, bucketWriteResult) {
	for {
		d := r.in.Load()
		s := d.slots[d.slotIndex(hashToken(key))]
		s.mu.Lock()
		if r.in.Load() != d {
			s.mu.Unlock()
			continue
		}
		nb, res := fn(s.ptr.Load())
		if nb != nil {
			s.ptr.Store(nb)
		}
		s.mu.Unlock()
		return d, res
	}
}

// ---------------------------------------------------------------------------
// outgoing links
// ---------------------------------------------------------------------------

// mutateOutLink applies a change to one key of a link, creating the entry if it
// is not there yet, and publishes the bucket.
//
// CRUD writes a link as several separate keys, in no guaranteed order and each
// through its own cache call, and later rewrites or deletes them one at a time.
// A write of one key therefore merges into whatever the others left behind, and
// each key keeps its own time and its own live flag.
func (r *vertexRecord) mutateOutLink(name string, apply func(*outLink) bool) bool {
	d, res := r.withOutSlot(name, func(b *bucket) (*bucket, bucketWriteResult) {
		links := copyOutEntries(b)
		i := sort.Search(len(links), func(i int) bool { return links[i].Name >= name })

		l := outLink{Name: name}
		exists := i < len(links) && links[i].Name == name
		if exists {
			l = links[i]
		}
		if !apply(&l) {
			return nil, bucketWriteResult{applied: false}
		}
		if exists {
			links[i] = l
		} else {
			links = append(links, outLink{})
			copy(links[i+1:], links[i:])
			links[i] = l
		}
		return &bucket{outs: links, decoded: true, localDepth: b.localDepth},
			bucketWriteResult{applied: true, count: len(links)}
	})
	if res.applied && res.count > r.bucketLimit() {
		r.splitOut(d, name)
	}
	return res.applied
}

// setOutTo writes `out.to.<name>` = "<type>.<target>".
func (r *vertexRecord) setOutTo(name, value string, t int64) bool {
	return r.mutateOutLink(name, func(l *outLink) bool {
		if t < l.To.Time {
			return false
		}
		l.To = subValue{Value: value, Time: t, Live: true}
		return true
	})
}

func (r *vertexRecord) deleteOutTo(name string, t int64) bool {
	return r.mutateOutLink(name, func(l *outLink) bool {
		if t < l.To.Time {
			return false
		}
		l.To = subValue{Time: t}
		return true
	})
}

// setOutBody writes `out.body.<name>`.
func (r *vertexRecord) setOutBody(name string, body []byte, t int64) bool {
	return r.mutateOutLink(name, func(l *outLink) bool {
		if t < l.Body.Time {
			return false
		}
		l.Body = subValue{Value: string(body), Time: t, Live: true}
		return true
	})
}

func (r *vertexRecord) deleteOutBody(name string, t int64) bool {
	return r.mutateOutLink(name, func(l *outLink) bool {
		if t < l.Body.Time {
			return false
		}
		l.Body = subValue{Time: t}
		return true
	})
}

// setOutIndexType writes `out.index.<name>.type.<type>`; the type is part of
// the key, so several may coexist, and each has its own time.
func (r *vertexRecord) setOutIndexType(name, linkType string, t int64, live bool) bool {
	return r.mutateOutLink(name, func(l *outLink) bool {
		subs, ok := putSub(l.IdxTypes, linkType, t, live)
		if !ok {
			return false
		}
		l.IdxTypes = subs
		return true
	})
}

// setOutTag writes `out.index.<name>.tag.<tag>`.
func (r *vertexRecord) setOutTag(name, tag string, t int64, live bool) bool {
	return r.mutateOutLink(name, func(l *outLink) bool {
		subs, ok := putSub(l.Tags, tag, t, live)
		if !ok {
			return false
		}
		l.Tags = subs
		return true
	})
}

// ---------------------------------------------------------------------------
// incoming links
// ---------------------------------------------------------------------------

func (r *vertexRecord) putInLink(l inLink) bool {
	d, res := r.withInSlot(l.From, func(b *bucket) (*bucket, bucketWriteResult) {
		links, ok := applyInLink(copyInEntries(b), l)
		if !ok {
			return nil, bucketWriteResult{applied: false}
		}
		return &bucket{ins: links, decoded: true, localDepth: b.localDepth},
			bucketWriteResult{applied: true, count: len(links)}
	})
	if res.applied && res.count > r.bucketLimit() {
		r.splitIn(d, l.From)
	}
	return res.applied
}

func (r *vertexRecord) deleteInLink(from, name string, t int64) bool {
	_, res := r.withInSlot(from, func(b *bucket) (*bucket, bucketWriteResult) {
		links, ok := applyInLink(copyInEntries(b), inLink{From: from, Name: name, UpdateTime: t, Tombstone: true})
		if !ok {
			return nil, bucketWriteResult{applied: false}
		}
		return &bucket{ins: links, decoded: true, localDepth: b.localDepth},
			bucketWriteResult{applied: true, count: len(links)}
	})
	return res.applied
}

func applyInLink(links []inLink, l inLink) ([]inLink, bool) {
	key := makeInLinkKey(l.From, l.Name)
	i := sort.Search(len(links), func(i int) bool {
		return makeInLinkKey(links[i].From, links[i].Name) >= key
	})
	if i < len(links) && links[i].From == l.From && links[i].Name == l.Name {
		if l.UpdateTime < links[i].UpdateTime {
			return nil, false
		}
		links[i] = l
		return links, true
	}
	links = append(links, inLink{})
	copy(links[i+1:], links[i:])
	links[i] = l
	return links, true
}

// copyOutEntries returns the bucket's entries in a slice the caller may mutate.
// Copying is what keeps published buckets immutable: a reader that loaded this
// bucket must keep seeing what it loaded, so the writer works on its own slice
// and publishes a new bucket around it.
func copyOutEntries(b *bucket) []outLink {
	if b == nil {
		return nil
	}
	if b.decoded {
		out := make([]outLink, len(b.outs), len(b.outs)+1)
		copy(out, b.outs)
		return out
	}
	n := bucketCount(b.data)
	links := make([]outLink, 0, n+1)
	for i := 0; i < n; i++ {
		links = append(links, decodeOutLink(bucketEntry(b.data, i)))
	}
	return links
}

func copyInEntries(b *bucket) []inLink {
	if b == nil {
		return nil
	}
	if b.decoded {
		out := make([]inLink, len(b.ins), len(b.ins)+1)
		copy(out, b.ins)
		return out
	}
	n := bucketCount(b.data)
	links := make([]inLink, 0, n+1)
	for i := 0; i < n; i++ {
		links = append(links, decodeInLink(bucketEntry(b.data, i)))
	}
	return links
}

// ---------------------------------------------------------------------------
// the vertex body
// ---------------------------------------------------------------------------

func (r *vertexRecord) putBody(body []byte, t int64, asJSON bool) bool {
	r.headMu.Lock()
	defer r.headMu.Unlock()
	if t < r.bodyGuardTime() {
		return false
	}
	h := makeHead(body, t, false, asJSON)
	r.head.Store(&h)
	return true
}

func (r *vertexRecord) deleteBody(t int64) bool {
	r.headMu.Lock()
	defer r.headMu.Unlock()
	if t < r.bodyGuardTime() {
		return false
	}
	h := makeHead(nil, t, true, r.bodyIsJSON())
	r.head.Store(&h)
	return true
}

// ---------------------------------------------------------------------------
// splitting
// ---------------------------------------------------------------------------

func (r *vertexRecord) bucketLimit() int {
	if defaultBucketLinks > 0 {
		return defaultBucketLinks
	}
	return 32
}

// splitOut splits the bucket serving key, doubling the directory when the
// bucket's local depth has caught up with the global one. Done under dirMu and
// published as a whole new directory, so readers see one or the other.
func (r *vertexRecord) splitOut(seen *bucketDir, key string) {
	r.dirMu.Lock()
	defer r.dirMu.Unlock()

	d := r.out.Load()
	if d != seen {
		return // somebody already restructured; their split covers this too
	}
	old := d.slots[d.slotIndex(hashToken(key))]

	// The bucket is read under its own lock and the new directory is published
	// before that lock is released. Without it a writer that passed its
	// directory check a moment earlier would publish into the old block after
	// we copied it, and its link would vanish with the block — a lost update
	// no race detector reports, because there is no data race.
	old.mu.Lock()
	defer old.mu.Unlock()

	b := old.ptr.Load()
	if b == nil || b.entryCount() <= r.bucketLimit() || b.localDepth >= maxDirDepth {
		return
	}

	links := b.outEntries()
	nd := growDirIfNeeded(d, b)
	splitSlots(nd, old, b.localDepth, func(bit int) *bucket {
		var g []outLink
		for _, l := range links {
			if int((hashToken(l.Name)>>b.localDepth)&1) == bit {
				g = append(g, l)
			}
		}
		sort.Slice(g, func(a, c int) bool { return g[a].Name < g[c].Name })
		return &bucket{outs: g, decoded: true, localDepth: b.localDepth + 1}
	})
	r.out.Store(nd)
}

func (r *vertexRecord) splitIn(seen *bucketDir, key string) {
	r.dirMu.Lock()
	defer r.dirMu.Unlock()

	d := r.in.Load()
	if d != seen {
		return
	}
	old := d.slots[d.slotIndex(hashToken(key))]
	old.mu.Lock()
	defer old.mu.Unlock()

	b := old.ptr.Load()
	if b == nil || b.entryCount() <= r.bucketLimit() || b.localDepth >= maxDirDepth {
		return
	}

	links := b.inEntries()
	nd := growDirIfNeeded(d, b)
	splitSlots(nd, old, b.localDepth, func(bit int) *bucket {
		var g []inLink
		for _, l := range links {
			if int((hashToken(l.From)>>b.localDepth)&1) == bit {
				g = append(g, l)
			}
		}
		sort.Slice(g, func(a, c int) bool {
			if g[a].From != g[c].From {
				return g[a].From < g[c].From
			}
			return g[a].Name < g[c].Name
		})
		return &bucket{ins: g, decoded: true, localDepth: b.localDepth + 1}
	})
	r.in.Store(nd)
}

// maxDirDepth caps directory growth. A hash that refuses to separate its keys
// would otherwise double the directory forever; leaving the bucket oversized
// costs speed, never correctness.
const maxDirDepth = 16

// growDirIfNeeded returns a directory in which the given bucket can be split:
// the same entries (a fresh slice over the SAME slots, so sharing and locks
// survive) when its local depth is below the global depth, or one of twice the
// size when it is not.
func growDirIfNeeded(d *bucketDir, b *bucket) *bucketDir {
	depth := d.depth
	if b.localDepth >= d.depth {
		depth = d.depth + 1
	}
	n := 1 << depth
	nd := &bucketDir{depth: depth, slots: make([]*bucketSlot, n)}
	for i := 0; i < n; i++ {
		nd.slots[i] = d.slots[i&(len(d.slots)-1)]
	}
	return nd
}

// splitSlots replaces every directory entry that pointed at old with one of two
// new slots, chosen by the bit just past the bucket's local depth. Entries that
// shared old keep sharing whichever half they now belong to.
func splitSlots(nd *bucketDir, old *bucketSlot, localDepth uint8, make func(bit int) *bucket) {
	zero := newBucketSlot(make(0))
	one := newBucketSlot(make(1))
	for i := range nd.slots {
		if nd.slots[i] != old {
			continue
		}
		if (i>>localDepth)&1 == 0 {
			nd.slots[i] = zero
		} else {
			nd.slots[i] = one
		}
	}
}

// ---------------------------------------------------------------------------
// giving the memory back
// ---------------------------------------------------------------------------

// compactBuckets re-encodes every bucket a write left decoded, and reports how
// many it encoded.
//
// Writing deliberately does not encode: CRUD writes a link as several keys into
// the same bucket, and encoding per key made a link write more expensive than
// the tree. The cost of that choice is that a written bucket holds Go objects
// until somebody asks for the memory back — which is what this is for, called
// from the maintenance pass. Reads work on either form, so this is never on a
// read path and never changes an answer.
func (r *vertexRecord) compactBuckets() int {
	n := 0
	n += compactDir(r.out.Load())
	n += compactDir(r.in.Load())
	n += compactDir(r.pairs.Load())
	return n
}

func compactDir(d *bucketDir) int {
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
		if b != nil && b.decoded {
			s.ptr.Store(b.encoded())
			n++
		}
		s.mu.Unlock()
	}
	return n
}

// dirtyBuckets counts buckets still holding decoded entries.
func (r *vertexRecord) dirtyBuckets() int {
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
			if b := s.ptr.Load(); b != nil && b.decoded {
				n++
			}
		}
	}
	count(r.out.Load())
	count(r.in.Load())
	count(r.pairs.Load())
	return n
}
