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
func parseTail(tail string) (k tailKind, a, b string) {
	if tail == "" {
		return tailBody, "", ""
	}
	t := strings.Split(tail, ".")
	switch t[0] {
	case "out":
		if len(t) < 3 {
			return tailUnknown, "", ""
		}
		switch t[1] {
		case "to":
			if len(t) == 3 {
				return tailOutTo, t[2], ""
			}
		case "body":
			if len(t) == 3 {
				return tailOutBody, t[2], ""
			}
		case "index":
			if len(t) == 5 {
				switch t[3] {
				case "type":
					return tailIndexType, t[2], t[4]
				case "tag":
					return tailIndexTag, t[2], t[4]
				}
			}
		}
	case "ltype":
		if len(t) == 3 {
			return tailLinkType, t[1], t[2]
		}
	case "in":
		if len(t) == 3 {
			return tailIn, t[1], t[2]
		}
	}
	return tailUnknown, "", ""
}

// get answers GetValue for a tail. The second result says whether the key
// exists at all — a key with an empty value (an index marker) exists.
func (r *vertexRecord) get(tail string) ([]byte, bool) {
	switch k, a, b := parseTail(tail); k {
	case tailBody:
		body, _, ok := r.bodyBytes()
		if !ok {
			return nil, false
		}
		return []byte(body), true

	case tailOutTo:
		lt, target, ok := r.lookupOutTarget(a)
		if !ok {
			return nil, false
		}
		return []byte(lt + "." + target), true

	case tailOutBody:
		l, ok := r.lookupOutLink(a)
		if !ok || !l.Body.Live {
			return nil, false
		}
		return []byte(l.Body.Value), true

	case tailLinkType:
		p, ok := r.lookupPair(a, b)
		if !ok {
			return nil, false
		}
		return []byte(p.Name), true

	case tailIndexType:
		l, ok := r.lookupOutLink(a)
		if !ok {
			return nil, false
		}
		if v, found := findSub(l.IdxTypes, b); found && v.Live {
			return nil, true // marker: exists, empty value
		}
		return nil, false

	case tailIndexTag:
		l, ok := r.lookupOutLink(a)
		if !ok {
			return nil, false
		}
		if v, found := findSub(l.Tags, b); found && v.Live {
			return nil, true
		}
		return nil, false

	case tailIn:
		l, ok := r.lookupInLink(a, b)
		if !ok {
			return nil, false
		}
		return []byte(l.Type), true
	}
	return nil, false
}

func (r *vertexRecord) exists(tail string) bool {
	_, ok := r.get(tail)
	return ok
}

// updateTime mirrors Store.GetValueUpdateTime: the time of a live key, -1 for
// one that is absent — deleted included. Each key of a link carries its own
// time, because in the tree each is its own node and CRUD does write them
// apart.
func (r *vertexRecord) updateTime(tail string) int64 {
	switch k, a, b := parseTail(tail); k {
	case tailBody:
		if _, t, ok := r.bodyBytes(); ok {
			return t
		}
	case tailOutTo:
		if l, ok := r.lookupOutLink(a); ok && l.To.Live {
			return l.To.Time
		}
	case tailOutBody:
		if l, ok := r.lookupOutLink(a); ok && l.Body.Live {
			return l.Body.Time
		}
	case tailIndexType:
		if l, ok := r.lookupOutLink(a); ok {
			if v, found := findSub(l.IdxTypes, b); found && v.Live {
				return v.Time
			}
		}
	case tailIndexTag:
		if l, ok := r.lookupOutLink(a); ok {
			if v, found := findSub(l.Tags, b); found && v.Live {
				return v.Time
			}
		}
	case tailLinkType:
		if p, ok := r.lookupPair(a, b); ok {
			return p.UpdateTime
		}
	case tailIn:
		if l, ok := r.lookupInLink(a, b); ok {
			return l.UpdateTime
		}
	}
	return -1
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
func (r *vertexRecord) eachTail(want string, fn func(tail string) bool) {
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
