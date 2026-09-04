package cache

// The (type, target) -> name table.
//
// `V.ltype.<type>.<target>` is a key in its own right, and it does NOT belong to
// a link: a link is identified by its name, so a vertex may hold two links of
// the same type to the same target under different names, and then both write
// this one key — the later one wins. CRUD also deletes it on its own, when a
// link changes type or target.
//
// Deriving it from the link table therefore cannot be right: a derivation would
// find both links and could not represent the key having been deleted while the
// links live on. It is stored, with its own time and its own tombstone, in a
// third directory of the same bucketed shape as the other two.

import "sort"

// pairEntry is one (type, target) -> name mapping.
type pairEntry struct {
	Type       string
	Target     string
	Name       string
	UpdateTime int64
	Tombstone  bool
}

func makePairKey(linkType, target string) string { return linkType + "\x00" + target }

func encodePairEntry(dst []byte, p pairEntry) []byte {
	dst = putString(dst, p.Type)
	dst = putString(dst, p.Target)
	var f byte
	if p.Tombstone {
		f |= entryTombstoned
	}
	dst = append(dst, f)
	var t [8]byte
	putUint64(t[:], uint64(p.UpdateTime))
	dst = append(dst, t[:]...)
	if p.Tombstone {
		return dst
	}
	return putString(dst, p.Name)
}

func decodePairEntry(entry string) pairEntry {
	var p pairEntry
	i := 0
	p.Type, i = readStr(entry, i)
	p.Target, i = readStr(entry, i)
	if i >= len(entry) {
		return p
	}
	p.Tombstone = entry[i]&entryTombstoned != 0
	i++
	p.UpdateTime, i = readInt64(entry, i)
	if !p.Tombstone {
		p.Name, _ = readStr(entry, i)
	}
	return p
}

func pairEntryKey(entry string) string {
	t, i := readStr(entry, 0)
	tgt, _ := readStr(entry, i)
	return makePairKey(t, tgt)
}

func encodePairBucket(pairs []*pairEntry) string {
	head := make([]byte, 4+len(pairs)*4)
	putUint32(head[0:], uint32(len(pairs)))
	body := head
	for i, p := range pairs {
		putUint32(body[4+i*4:], uint32(len(body)))
		body = encodePairEntry(body, *p)
	}
	return string(body)
}

func (b *bucket) pairEntries() []*pairEntry {
	if b == nil {
		return nil
	}
	if b.decoded {
		return b.pairs
	}
	n := bucketCount(b.data)
	out := make([]*pairEntry, n)
	for i := 0; i < n; i++ {
		p := decodePairEntry(bucketEntry(b.data, i))
		out[i] = &p
	}
	return out
}

// applyPairTo builds the bucket's new entry slice with p in it, or reports
// that the guard refused the write. No entry is edited where it lies: a reader
// walks this slice without a lock, so the changed entry becomes a new object
// and only the pointers are rebuilt.
func applyPairTo(cur []*pairEntry, decoded bool, p pairEntry) ([]*pairEntry, bool) {
	key := makePairKey(p.Type, p.Target)
	i := sort.Search(len(cur), func(i int) bool {
		return makePairKey(cur[i].Type, cur[i].Target) >= key
	})
	if i < len(cur) && cur[i].Type == p.Type && cur[i].Target == p.Target {
		if p.UpdateTime < cur[i].UpdateTime {
			return nil, false
		}
		out := make([]*pairEntry, len(cur), spareFor(len(cur)))
		copy(out, cur)
		out[i] = &p
		return out, true
	}
	if decoded && i == len(cur) && cap(cur) > len(cur) {
		// Past the end of what any reader can see — the spare room is free.
		out := cur[: len(cur)+1 : cap(cur)]
		out[len(cur)] = &p
		return out, true
	}
	out := make([]*pairEntry, len(cur)+1, spareFor(len(cur)+1))
	copy(out, cur[:i])
	out[i] = &p
	copy(out[i+1:], cur[i:])
	return out, true
}

func searchPairSlice(pairs []*pairEntry, linkType, target string) (*pairEntry, bool) {
	key := makePairKey(linkType, target)
	i := sort.Search(len(pairs), func(i int) bool {
		return makePairKey(pairs[i].Type, pairs[i].Target) >= key
	})
	if i < len(pairs) && pairs[i].Type == linkType && pairs[i].Target == target {
		return pairs[i], true
	}
	return nil, false
}

// lookupPair answers `V.ltype.<type>.<target>`.
func (r *vertexRecord) lookupPair(linkType, target string) (pairEntry, bool) {
	p, ok := r.lookupPairGuard(linkType, target)
	if !ok || p.Tombstone {
		return pairEntry{}, false
	}
	return p, true
}

func (r *vertexRecord) lookupPairGuard(linkType, target string) (pairEntry, bool) {
	b := r.pairs.Load().bucketFor(hashToken(makePairKey(linkType, target)))
	if b == nil {
		return pairEntry{}, false
	}
	if b.decoded {
		p, found := searchPairSlice(b.pairs, linkType, target)
		if !found {
			return pairEntry{}, false
		}
		return *p, true
	}
	e, found := bucketSearch(b.data, makePairKey(linkType, target), pairEntryKey)
	if !found {
		return pairEntry{}, false
	}
	return decodePairEntry(e), true
}

func (r *vertexRecord) rangePairs(fn func(pairEntry) bool) {
	r.pairs.Load().each(func(b *bucket) bool {
		for _, p := range b.pairEntries() {
			if p.Tombstone {
				continue
			}
			if !fn(*p) {
				return false
			}
		}
		return true
	})
}

// putPair writes the mapping, honouring the last-writer-wins guard.
func (r *vertexRecord) putPair(p pairEntry) bool {
	key := makePairKey(p.Type, p.Target)
	d, res := r.withPairSlot(key, func(b *bucket) (*bucket, bucketWriteResult) {
		pairs, ok := applyPairTo(b.pairEntries(), b != nil && b.decoded, p)
		if !ok {
			return nil, bucketWriteResult{applied: false}
		}
		return &bucket{pairs: pairs, decoded: true, localDepth: b.localDepth},
			bucketWriteResult{applied: true, count: len(pairs)}
	})
	if res.applied && res.count > r.bucketLimit() {
		r.splitPairs(d, key)
	}
	return res.applied
}

func (r *vertexRecord) deletePair(linkType, target string, t int64) bool {
	_, res := r.withPairSlot(makePairKey(linkType, target), func(b *bucket) (*bucket, bucketWriteResult) {
		pairs, ok := applyPairTo(b.pairEntries(), b != nil && b.decoded,
			pairEntry{Type: linkType, Target: target, UpdateTime: t, Tombstone: true})
		if !ok {
			return nil, bucketWriteResult{applied: false}
		}
		return &bucket{pairs: pairs, decoded: true, localDepth: b.localDepth},
			bucketWriteResult{applied: true, count: len(pairs)}
	})
	return res.applied
}

func applyPair(pairs []pairEntry, p pairEntry) ([]pairEntry, bool) {
	key := makePairKey(p.Type, p.Target)
	i := sort.Search(len(pairs), func(i int) bool {
		return makePairKey(pairs[i].Type, pairs[i].Target) >= key
	})
	if i < len(pairs) && pairs[i].Type == p.Type && pairs[i].Target == p.Target {
		if p.UpdateTime < pairs[i].UpdateTime {
			return nil, false
		}
		pairs[i] = p
		return pairs, true
	}
	pairs = append(pairs, pairEntry{})
	copy(pairs[i+1:], pairs[i:])
	pairs[i] = p
	return pairs, true
}

func (r *vertexRecord) withPairSlot(key string, fn func(b *bucket) (*bucket, bucketWriteResult)) (*bucketDir, bucketWriteResult) {
	for {
		d := r.pairs.Load()
		idx := d.slotIndex(hashToken(key))
		s := d.slots[idx].Load()
		s.mu.Lock()
		// Two ways the ground can move: the directory was replaced by a
		// doubling, or this entry was pointed at a different slot by a split
		// that did not need one. Either means the slot just locked may no
		// longer serve this key.
		if r.pairs.Load() != d || d.slots[idx].Load() != s {
			s.mu.Unlock()
			continue
		}
		nb, res := fn(s.ptr.Load().rawForm())
		if nb != nil {
			s.ptr.Store(nb)
		}
		s.mu.Unlock()
		return d, res
	}
}

func (r *vertexRecord) splitPairs(seen *bucketDir, key string) {
	r.dirMu.Lock()
	defer r.dirMu.Unlock()

	d := r.pairs.Load()
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
	entries := b.pairEntries()
	nd := growDirIfNeeded(d, b)
	splitSlots(nd, old, b.localDepth, hashToken(key), func(bit int) *bucket {
		var g []*pairEntry
		for _, p := range entries {
			if int((hashToken(makePairKey(p.Type, p.Target))>>b.localDepth)&1) == bit {
				g = append(g, p)
			}
		}
		sort.Slice(g, func(a, c int) bool {
			return makePairKey(g[a].Type, g[a].Target) < makePairKey(g[c].Type, g[c].Target)
		})
		return &bucket{pairs: g, decoded: true, localDepth: b.localDepth + 1}
	})
	r.pairs.Store(nd)
}
