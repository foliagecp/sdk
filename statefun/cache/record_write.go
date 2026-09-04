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
	"sync/atomic"
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
		idx := d.slotIndex(hashToken(key))
		s := d.slots[idx].Load()
		s.mu.Lock()
		// Two ways the ground can move: the directory was replaced by a
		// doubling, or this entry was pointed at a different slot by a split
		// that did not need one. Either means the slot just locked may no
		// longer serve this key.
		if r.out.Load() != d || d.slots[idx].Load() != s {
			// The directory changed under us; the slot we locked may no longer
			// serve this key. Start over against the current one.
			s.mu.Unlock()
			continue
		}
		// A compressed bucket is decompressed here rather than by the
		// reader's opportunistic path: the writer already holds the slot
		// lock, and it is about to replace the block anyway.
		nb, res := fn(s.ptr.Load().rawForm())
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
		idx := d.slotIndex(hashToken(key))
		s := d.slots[idx].Load()
		s.mu.Lock()
		// Two ways the ground can move: the directory was replaced by a
		// doubling, or this entry was pointed at a different slot by a split
		// that did not need one. Either means the slot just locked may no
		// longer serve this key.
		if r.in.Load() != d || d.slots[idx].Load() != s {
			s.mu.Unlock()
			continue
		}
		// A compressed bucket is decompressed here rather than by the
		// reader's opportunistic path: the writer already holds the slot
		// lock, and it is about to replace the block anyway.
		nb, res := fn(s.ptr.Load().rawForm())
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
		cur := b.outEntries()
		i := sort.Search(len(cur), func(i int) bool { return cur[i].Name >= name })
		exists := i < len(cur) && cur[i].Name == name

		l := outLink{Name: name}
		if exists {
			l = *cur[i]
		}
		if !apply(&l) {
			return nil, bucketWriteResult{applied: false}
		}

		// An entry is never edited where it lies: a reader walks this slice
		// without a lock and must keep seeing what it started with. So the
		// changed entry becomes a new object and only the slice of pointers is
		// rebuilt — 8 bytes an entry instead of 128, which is the difference
		// between a bulk load allocating twice the graph and twenty times it.
		var links []*outLink
		switch {
		case exists:
			links = make([]*outLink, len(cur), spareFor(len(cur)))
			copy(links, cur)
			links[i] = &l

		case b != nil && b.decoded && i == len(cur) && cap(cur) > len(cur):
			// Writing past the end of the published slice leaves everything a
			// reader can reach exactly as it was, so the spare room left by
			// the last copy is used instead of making another one. Links
			// arriving in name order — which is how a load and a rebuild both
			// deliver them — take this path every time.
			links = cur[:len(cur)+1]
			links[len(cur)] = &l

		default:
			links = make([]*outLink, len(cur)+1, spareFor(len(cur)+1))
			copy(links, cur[:i])
			links[i] = &l
			copy(links[i+1:], cur[i:])
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

// spareFor is how much room a fresh copy of a bucket keeps beyond what it
// holds. A bucket is copied to be written to, and the write that follows is
// often an append; leaving room means the appends after it cost nothing at
// all. The ceiling is about the split threshold, so the spare is never large.
func spareFor(n int) int {
	room := n + n/2 + 1
	if ceiling := 2*defaultBucketLinks + 1; room > ceiling {
		room = ceiling
	}
	if room < n {
		// The ceiling must never cut below what the caller already holds. A
		// bucket can outgrow it: a split runs after the write that overfilled
		// the bucket, so under concurrent writers several may land before it
		// does, and a bucket at the maximum directory depth never splits at
		// all. Returning less than n made make() panic outright.
		room = n
	}
	return room
}

func (r *vertexRecord) putInLink(l inLink) bool {
	d, res := r.withInSlot(l.From, func(b *bucket) (*bucket, bucketWriteResult) {
		links, ok := applyInLinkTo(b.inEntries(), b != nil && b.decoded, l)
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
		links, ok := applyInLinkTo(b.inEntries(), b != nil && b.decoded,
			inLink{From: from, Name: name, UpdateTime: t, Tombstone: true})
		if !ok {
			return nil, bucketWriteResult{applied: false}
		}
		return &bucket{ins: links, decoded: true, localDepth: b.localDepth},
			bucketWriteResult{applied: true, count: len(links)}
	})
	return res.applied
}

// applyInLinkTo builds the bucket's new entry slice with l in it, or reports
// that the guard refused the write. Like the outgoing side, no entry is edited
// where it lies.
func applyInLinkTo(cur []*inLink, decoded bool, l inLink) ([]*inLink, bool) {
	key := makeInLinkKey(l.From, l.Name)
	i := sort.Search(len(cur), func(i int) bool {
		return makeInLinkKey(cur[i].From, cur[i].Name) >= key
	})
	if i < len(cur) && cur[i].From == l.From && cur[i].Name == l.Name {
		if l.UpdateTime < cur[i].UpdateTime {
			return nil, false
		}
		out := make([]*inLink, len(cur), spareFor(len(cur)))
		copy(out, cur)
		out[i] = &l
		return out, true
	}
	if decoded && i == len(cur) && cap(cur) > len(cur) {
		out := cur[: len(cur)+1 : cap(cur)]
		out[len(cur)] = &l
		return out, true
	}
	out := make([]*inLink, len(cur)+1, spareFor(len(cur)+1))
	copy(out, cur[:i])
	out[i] = &l
	copy(out[i+1:], cur[i:])
	return out, true
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
	r.parsedBody.Store(nil) // the kept parse is of the body that just went away
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
	r.parsedBody.Store(nil)
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
	old := d.slots[d.slotIndex(hashToken(key))].Load()

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
	splitSlots(nd, old, b.localDepth, hashToken(key), func(bit int) *bucket {
		var g []*outLink
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
	old := d.slots[d.slotIndex(hashToken(key))].Load()
	old.mu.Lock()
	defer old.mu.Unlock()

	b := old.ptr.Load()
	if b == nil || b.entryCount() <= r.bucketLimit() || b.localDepth >= maxDirDepth {
		return
	}

	links := b.inEntries()
	nd := growDirIfNeeded(d, b)
	splitSlots(nd, old, b.localDepth, hashToken(key), func(bit int) *bucket {
		var g []*inLink
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
// growDirIfNeeded returns the directory the split must publish into: the same
// one when the bucket has room below the global depth, a doubled copy when it
// does not.
//
// Returning the same directory is the point. A bucket below the global depth
// splits without the directory changing size, and that is the common case —
// after one doubling, every bucket at the old depth can split without another.
// Copying the directory anyway cost O(entries) per split, under the lock that
// serializes every writer to the vertex, which on a hub of 30 000 links made
// concurrent writes three times dearer than the tree's.
func growDirIfNeeded(d *bucketDir, b *bucket) *bucketDir {
	if b.localDepth < d.depth {
		return d
	}
	depth := d.depth + 1
	n := 1 << depth
	nd := &bucketDir{depth: depth, slots: make([]atomic.Pointer[bucketSlot], n)}
	for i := 0; i < n; i++ {
		nd.slots[i].Store(d.slots[i&(len(d.slots)-1)].Load())
	}
	return nd
}

// splitSlots replaces every directory entry that pointed at old with one of two
// new slots, chosen by the bit just past the bucket's local depth. Entries that
// shared old keep sharing whichever half they now belong to.
// splitSlots replaces every directory entry that pointed at old with one of two
// new slots, chosen by the bit just past the bucket's local depth. Entries that
// shared old keep sharing whichever half they now belong to.
//
// Which entries those are is computed, not searched for: a bucket at local
// depth L is reachable exactly from the entries whose low L bits are its hash
// prefix, so they are prefix, prefix+2^L, prefix+2^(L+1)... Scanning the whole
// directory instead cost O(entries) per split while holding the lock that
// serializes every writer to the vertex — on a hub with thousands of entries
// that, and not the split itself, was the cost.
func splitSlots(nd *bucketDir, old *bucketSlot, localDepth uint8, prefix uint32, make func(bit int) *bucket) {
	zero := newBucketSlot(make(0))
	one := newBucketSlot(make(1))
	step := 1 << localDepth
	for i := int(prefix) & (step - 1); i < len(nd.slots); i += step {
		if nd.slots[i].Load() != old {
			continue
		}
		if (i>>localDepth)&1 == 0 {
			nd.slots[i].Store(zero)
		} else {
			nd.slots[i].Store(one)
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
	for i := range d.slots {
		s := d.slots[i].Load()
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
		for i := range d.slots {
			s := d.slots[i].Load()
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

// ageParsedBodies drops the body parses nobody asked for since the last pass
// and keeps the ones somebody did, reporting how many it dropped. This is what
// bounds how much of the graph is held as trees: the working set, not the
// graph.
func (cs *Store) ageParsedBodies() int {
	if !tieringEnabled() || cs.records == nil {
		return 0
	}
	n := 0
	cs.records.each(func(_ string, r *vertexRecord) bool {
		if r.ageParsedBody() {
			n++
		}
		return true
	})
	return n
}

// dropParsedBodies releases every kept body parse across the store.
func (cs *Store) dropParsedBodies() int {
	if !tieringEnabled() || cs.records == nil {
		return 0
	}
	n := 0
	cs.records.each(func(_ string, r *vertexRecord) bool {
		if r.dropParsedBody() {
			n++
		}
		return true
	})
	return n
}
