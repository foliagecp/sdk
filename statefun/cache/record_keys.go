package cache

// Answering cache keys from a record.
//
// A vertex in the tree is a subtree of keys; a vertex in a record is a block of
// bytes. Everything the cache is asked about a compact vertex is answered here,
// by recognising the shape of the key and reading the corresponding field —
// never by rebuilding the subtree.
//
// The shapes are the ones CRUD writes (embedded/graph/crud/common.go). Listed
// as the suffix after the vertex id, which is what this file calls the tail:
//
//	(empty)                       the vertex body
//	out.to.<name>                 "<type>.<target>"
//	out.body.<name>               the link body
//	ltype.<type>.<target>         "<name>"
//	out.index.<name>.type.<type>  present, empty value
//	out.index.<name>.tag.<tag>    present, empty value
//	in.<from>.<name>              "<type>"
//
// Every token here is a single token: object ids, link names, types and tags
// cannot contain a dot, since a dot is what separates cache key tokens.
//
// A tombstoned entry is absent for every question asked in this file. The
// delete time it carries is for writers only — the tree answers -1 for the
// update time of a deleted key, exactly as for one that never existed, and a
// record must not be more informative than the tree.

import (
	"sort"
	"strings"
)

// splitVertexKey cuts a cache key into the vertex id and the tail. The vertex
// id is the first token; the tail is what a record has to interpret.
func splitVertexKey(key string) (vertexID, tail string) {
	if i := strings.IndexByte(key, '.'); i >= 0 {
		return key[:i], key[i+1:]
	}
	return key, ""
}

// tailKind is the recognised shape of a tail.
type tailKind int

const (
	tailUnknown   tailKind = iota
	tailBody               // (empty)
	tailOutTo              // out.to.<name>
	tailOutBody            // out.body.<name>
	tailLinkType           // ltype.<type>.<target>
	tailIndexType          // out.index.<name>.type.<type>
	tailIndexTag           // out.index.<name>.tag.<tag>
	tailIn                 // in.<from>.<name>
)

// parseTail recognises a tail and returns its parts. a and b are the shape's
// variable tokens in the order they appear.
//
// The tail is cut by scanning and not with strings.Split: a Split allocates a
// slice on every single operation a record answers — every read, every
// existence probe, every write — and there are at most five tokens here with
// fixed meanings. On a load of 130 000 keys that Split alone was 16 MB.
//
// No variable token may contain a dot: link names are checked against a
// regexp that excludes it, and vertex ids separate their domain with a slash.
// A tail that has one anyway is not a shape a record knows, exactly as before.
func parseTail(tail string) (k tailKind, a, b string) {
	if tail == "" {
		return tailBody, "", ""
	}
	head, rest, ok := cutDot(tail)
	if !ok {
		return tailUnknown, "", ""
	}
	switch head {
	case "out":
		what, arg, ok := cutDot(rest)
		if !ok {
			return tailUnknown, "", ""
		}
		switch what {
		case "to":
			if !hasDot(arg) {
				return tailOutTo, arg, ""
			}
		case "body":
			if !hasDot(arg) {
				return tailOutBody, arg, ""
			}
		case "index":
			name, kindAndValue, ok := cutDot(arg)
			if !ok || hasDot(name) {
				return tailUnknown, "", ""
			}
			idx, value, ok := cutDot(kindAndValue)
			if !ok || hasDot(value) {
				return tailUnknown, "", ""
			}
			switch idx {
			case "type":
				return tailIndexType, name, value
			case "tag":
				return tailIndexTag, name, value
			}
		}
	case "ltype":
		linkType, target, ok := cutDot(rest)
		if ok && !hasDot(linkType) && !hasDot(target) {
			return tailLinkType, linkType, target
		}
	case "in":
		from, name, ok := cutDot(rest)
		if ok && !hasDot(from) && !hasDot(name) {
			return tailIn, from, name
		}
	}
	return tailUnknown, "", ""
}

// cutDot splits at the first dot. Both halves share the original string's
// bytes, so nothing is copied.
func cutDot(s string) (before, after string, found bool) {
	for i := 0; i < len(s); i++ {
		if s[i] == '.' {
			return s[:i], s[i+1:], true
		}
	}
	return s, "", false
}

func hasDot(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] == '.' {
			return true
		}
	}
	return false
}

// get answers GetValue for a tail. The second result says whether the key
// exists at all — a key with an empty value (an index marker) exists.
func (r *vertexRecord) get(tail string) ([]byte, bool) {
	k, a, b := parseTail(tail)
	return r.getParsed(k, a, b)
}

// getParsed answers a key whose tail is already recognised. The dispatch parses
// the tail to decide the key is a record's at all, so it hands the answer over
// rather than making this parse it again.
func (r *vertexRecord) getParsed(k tailKind, a, b string) ([]byte, bool) {
	v, _, ok := r.lookupParsed(k, a, b)
	if !ok {
		return nil, false
	}
	if v == "" {
		// Index and tag keys exist with no value; the tree stores nothing for
		// them either.
		return nil, true
	}
	return []byte(v), true
}

func (r *vertexRecord) exists(tail string) bool {
	k, a, b := parseTail(tail)
	return r.existsParsed(k, a, b)
}

// existsParsed answers existence without building the value.
func (r *vertexRecord) existsParsed(k tailKind, a, b string) bool {
	_, _, ok := r.lookupParsed(k, a, b)
	return ok
}

// updateTime mirrors Store.GetValueUpdateTime: the time of a live key, -1 for
// one that is absent — deleted included. Each key of a link carries its own
// time, because in the tree each is its own node and CRUD does write them
// apart.
func (r *vertexRecord) updateTime(tail string) int64 {
	k, a, b := parseTail(tail)
	return r.updateTimeParsed(k, a, b)
}

func (r *vertexRecord) updateTimeParsed(k tailKind, a, b string) int64 {
	_, t, ok := r.lookupParsed(k, a, b)
	if !ok {
		return -1
	}
	return t
}

// lookupParsed is the one read path of a record: it resolves a recognised key
// to its value and time in a single descent. Value, existence and time are
// three questions about the same entry, and a traversal asks all three, so they
// share one lookup instead of repeating it — and none of them decodes more of
// the entry than the answer needs.
func (r *vertexRecord) lookupParsed(k tailKind, a, b string) (value string, t int64, ok bool) {
	switch k {
	case tailBody:
		body, bt, found := r.bodyBytes()
		if !found {
			return "", -1, false
		}
		return body, bt, true

	case tailOutTo:
		// The stored value is already "<type>.<target>" — what the tree holds
		// under this key — so it is handed back untouched.
		to, found := r.lookupOutTo(a)
		if !found || !to.Live {
			return "", -1, false
		}
		return to.Value, to.Time, true

	case tailOutBody:
		l, found := r.lookupOutLink(a)
		if !found || !l.Body.Live {
			return "", -1, false
		}
		return l.Body.Value, l.Body.Time, true

	case tailLinkType:
		p, found := r.lookupPair(a, b)
		if !found {
			return "", -1, false
		}
		return p.Name, p.UpdateTime, true

	case tailIndexType:
		l, found := r.lookupOutLink(a)
		if !found {
			return "", -1, false
		}
		if v, has := findSub(l.IdxTypes, b); has && v.Live {
			return "", v.Time, true
		}
		return "", -1, false

	case tailIndexTag:
		l, found := r.lookupOutLink(a)
		if !found {
			return "", -1, false
		}
		if v, has := findSub(l.Tags, b); has && v.Live {
			return "", v.Time, true
		}
		return "", -1, false

	case tailIn:
		l, found := r.lookupInLink(a, b)
		if !found {
			return "", -1, false
		}
		return l.Type, l.UpdateTime, true
	}
	return "", -1, false
}

func containsString(ss []string, s string) bool {
	for _, v := range ss {
		if v == s {
			return true
		}
	}
	return false
}

// ---------------------------------------------------------------------------
// enumeration
// ---------------------------------------------------------------------------

// eachTail enumerates every key the vertex owns, as a tail. Only families whose
// fixed part can still match want are generated: asking for `out.body.>` on a
// vertex with ten thousand links should not build the index and in-link keys
// too.
// pinnedEntry reports the single entry a tail prefix determines, if it
// determines one: a link name for the outgoing side, a (from, name) pair for
// the incoming one.
//
// This is what makes a traversal affordable. The query engine asks for the
// indices of each link in turn — `<vertex>.out.index.<name>.>` — and answering
// each of those by walking every link of the vertex costs N per question and
// therefore N squared for the vertex. On a hub with a thousand links that was
// a hundredfold on the whole query. Everything such a prefix can match lives in
// one entry, so the enumeration goes straight to it.
func pinnedEntry(want string) (kind tailKind, name string, ok bool) {
	cut := func(s string) (string, bool) {
		i := strings.IndexByte(s, '.')
		if i < 0 {
			return "", false // the name is not complete yet: it may still grow
		}
		return s[:i], true
	}
	switch {
	case strings.HasPrefix(want, "out.index."):
		n, done := cut(want[len("out.index."):])
		return tailIndexType, n, done
	case strings.HasPrefix(want, "out.body."):
		n, done := cut(want[len("out.body."):])
		return tailOutBody, n, done
	case strings.HasPrefix(want, "out.to."):
		n, done := cut(want[len("out.to."):])
		return tailOutTo, n, done
	case strings.HasPrefix(want, "in."):
		n, done := cut(want[len("in."):])
		return tailIn, n, done
	}
	return tailUnknown, "", false
}

func (r *vertexRecord) eachTail(want string, fn func(tail string) bool) {
	if kind, name, ok := pinnedEntry(want); ok {
		r.eachTailOfEntry(kind, name, want, fn)
		return
	}
	mayMatch := func(prefix string) bool {
		if want == "" {
			return true
		}
		return strings.HasPrefix(prefix, want) || strings.HasPrefix(want, prefix)
	}

	if mayMatch("") && want == "" {
		if _, _, ok := r.bodyBytes(); ok {
			if !fn("") {
				return
			}
		}
	}

	if mayMatch("out.") {
		stop := false
		r.rangeOutLinks(func(l outLink) bool {
			emit := func(t string) bool {
				if !strings.HasPrefix(t, want) {
					return true
				}
				if !fn(t) {
					stop = true
					return false
				}
				return true
			}
			if l.To.Live && !emit("out.to."+l.Name) {
				return false
			}
			if l.Body.Live && !emit("out.body."+l.Name) {
				return false
			}
			for _, v := range l.IdxTypes {
				if v.Live && !emit("out.index."+l.Name+".type."+v.Value) {
					return false
				}
			}
			for _, v := range l.Tags {
				if v.Live && !emit("out.index."+l.Name+".tag."+v.Value) {
					return false
				}
			}
			return true
		})
		if stop {
			return
		}
	}

	if mayMatch("ltype.") {
		stop := false
		r.rangePairs(func(p pairEntry) bool {
			t := "ltype." + p.Type + "." + p.Target
			if !strings.HasPrefix(t, want) {
				return true
			}
			if !fn(t) {
				stop = true
				return false
			}
			return true
		})
		if stop {
			return
		}
	}

	if mayMatch("in.") {
		r.rangeInLinks(func(l inLink) bool {
			t := "in." + l.From + "." + l.Name
			if !strings.HasPrefix(t, want) {
				return true
			}
			return fn(t)
		})
	}
}

// eachTailOfEntry emits the tails of the one entry a prefix pinned down.
func (r *vertexRecord) eachTailOfEntry(kind tailKind, name, want string, fn func(tail string) bool) {
	emit := func(t string) bool {
		if !strings.HasPrefix(t, want) {
			return true
		}
		return fn(t)
	}
	if kind == tailIn {
		// The prefix names the source vertex; that source's links share one
		// bucket, so the whole answer is there.
		stop := false
		r.rangeInLinks(func(l inLink) bool {
			if l.From != name {
				return true
			}
			if !emit("in." + l.From + "." + l.Name) {
				stop = true
				return false
			}
			return true
		})
		_ = stop
		return
	}

	l, ok := r.lookupOutLink(name)
	if !ok {
		return
	}
	if l.To.Live && !emit("out.to."+l.Name) {
		return
	}
	if l.Body.Live && !emit("out.body."+l.Name) {
		return
	}
	for _, v := range l.IdxTypes {
		if v.Live && !emit("out.index."+l.Name+".type."+v.Value) {
			return
		}
	}
	for _, v := range l.Tags {
		if v.Live && !emit("out.index."+l.Name+".tag."+v.Value) {
			return
		}
	}
}

// keysByPattern mirrors Store.GetKeysByPattern for the keys of one vertex:
// a trailing `*` matches exactly one more token, a trailing `>` matches any
// depth, anything else is an exact key. Only keys that exist are returned.
func (r *vertexRecord) keysByPattern(vertexID, pattern string) []string {
	// An empty result is an empty slice, not nil: Store.GetKeysByPattern builds
	// its result with make(), and Ф-2 asks for the same answer, not an
	// equivalent one.
	out := []string{}

	id, tail := splitVertexKey(pattern)
	if id != vertexID {
		return out
	}

	// Deduplicated, because a key can be produced more than once: `ltype` is
	// keyed by (type, target), and two links of the same type to the same
	// target share it. Store.GetKeysByPattern collects into a map for the same
	// reason — in the tree such a key is one node.
	seen := map[string]struct{}{}
	add := func(k string) {
		if _, dup := seen[k]; dup {
			return
		}
		seen[k] = struct{}{}
		out = append(out, k)
	}

	switch {
	case strings.HasSuffix(tail, "*"):
		prefix := strings.TrimSuffix(tail, "*")
		r.eachTail(prefix, func(t string) bool {
			// exactly one token past the prefix
			if rest := t[len(prefix):]; rest != "" && !strings.Contains(rest, ".") {
				add(vertexID + "." + t)
			}
			return true
		})
	case strings.HasSuffix(tail, ">"):
		prefix := strings.TrimSuffix(tail, ">")
		r.eachTail(prefix, func(t string) bool {
			if len(t) > len(prefix) {
				add(vertexID + "." + t)
			}
			return true
		})
	default:
		if r.exists(tail) {
			add(pattern)
		}
	}
	sort.Strings(out)
	return out
}
