package crud_test

// Shared graph-consistency invariant checker for the adversarial hunts.
//
// For an existing object vertex it asserts the local CMDB invariants:
//   - out.to and out.body carry the same set of link names (no orphan body, no
//     body-less target);
//   - the number of ltype entries equals the number of out-links;
//   - every out.index entry references a real out-link (no dangling index);
//   - the object has exactly one __type link (no orphan, no duplicate).

import (
	"fmt"
	"strings"
	"testing"

	"github.com/foliagecp/sdk/embedded/graph/crud"
	"github.com/foliagecp/sdk/statefun/cache"
	"github.com/stretchr/testify/assert"
)

// namesUnderPrefix returns the first token (the link name) of every key under
// "<domID>.<infix>." — e.g. infix "out.to" -> the link names, infix "out.index"
// -> the link names owning an index entry.
func namesUnderPrefix(c *cache.Store, domID, infix string) map[string]struct{} {
	out := map[string]struct{}{}
	prefix := domID + "." + infix + "."
	for _, k := range c.GetKeysByPattern(domID + "." + infix + ".>") {
		rest := strings.TrimPrefix(k, prefix)
		if i := strings.IndexByte(rest, '.'); i >= 0 {
			rest = rest[:i]
		}
		out[rest] = struct{}{}
	}
	return out
}

func assertObjectConsistent(t *testing.T, c *cache.Store, domID string) {
	t.Helper()
	toNames := namesUnderPrefix(c, domID, "out.to")
	bodyNames := namesUnderPrefix(c, domID, "out.body")
	assert.Equalf(t, toNames, bodyNames, "[%s] out.to and out.body link-name sets must match", domID)

	for n := range namesUnderPrefix(c, domID, "out.index") {
		_, ok := toNames[n]
		assert.Truef(t, ok, "[%s] dangling out.index for non-existent link %q", domID, n)
	}

	ltypeAll := c.GetKeysByPattern(domID + ".ltype.>")
	assert.Lenf(t, ltypeAll, len(toNames), "[%s] ltype entries must match out-link count", domID)

	typeLinks := c.GetKeysByPattern(fmt.Sprintf(crud.OutLinkTypeKeyPrefPattern+"%s.>", domID, crud.TO_TYPELINK))
	assert.Lenf(t, typeLinks, 1, "[%s] object must have exactly one __type link", domID)
}
