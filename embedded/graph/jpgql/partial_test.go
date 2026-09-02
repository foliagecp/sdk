package jpgql

// The truncation half of the partial contract: a traversal cut short by the
// query-depth-spreading timeout or by backpressure already counts what it
// skipped — it just never said so above the stats, and the answer came back as
// a plain OK.
//
// Unit-level on purpose: making a real traversal time out reliably needs a
// graph big enough that the test becomes a race against the clock. What has to
// hold is the mapping — skipped paths mean a partial answer — and that is
// exact.

import (
	"testing"

	"github.com/foliagecp/easyjson"
	"github.com/stretchr/testify/require"
)

func resultWithSkips(depth, timeout, backpressure int) easyjson.JSON {
	r := easyjson.NewJSONObjectWithKeyValue("uuids", easyjson.NewJSONObject())
	r.SetByPath("stats.paths_skipped.depth", easyjson.NewJSON(depth))
	r.SetByPath("stats.paths_skipped.timeout", easyjson.NewJSON(timeout))
	r.SetByPath("stats.paths_skipped.backpressure", easyjson.NewJSON(backpressure))
	return r
}

func Test_MarkPartial(t *testing.T) {
	for _, c := range []struct {
		name                         string
		depth, timeout, backpressure int
		partial                      bool
	}{
		{"nothing skipped", 0, 0, 0, false},
		{"cut by the qds timeout", 0, 1, 0, true},
		{"cut by backpressure", 0, 0, 1, true},
		{"both", 0, 3, 2, true},
		// A depth limit is the caller's own instruction, not a failure to
		// deliver: the answer is exactly what was asked for.
		{"stopped at the requested depth", 5, 0, 0, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			r := resultWithSkips(c.depth, c.timeout, c.backpressure)
			markPartial(&r)
			require.Equal(t, c.partial, r.GetByPath("partial").AsBoolDefault(!c.partial))
		})
	}
}

func Test_MarkPartial_AlwaysStatesItself(t *testing.T) {
	r := resultWithSkips(0, 0, 0)
	markPartial(&r)
	require.True(t, r.PathExists("partial"),
		"the flag is always present, so a consumer never has to read absence as a promise")
}
