package main

import (
	"fmt"
	"sort"

	"github.com/foliagecp/easyjson"
	lg "github.com/foliagecp/sdk/statefun/logger"
	sfMediators "github.com/foliagecp/sdk/statefun/mediator"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"

	"github.com/foliagecp/sdk/statefun"
)

type filterQueryTest struct {
	name     string
	query    string
	expected []string
}

func RunFilterQueries(runtime *statefun.Runtime) {
	lg.Logln(lg.DebugLevel, ">>> Test started: filter queries (v_has fallback, v_array_has, l_has fallback, l_array_has)")

	tests := []filterQueryTest{
		// 1. v_has indexed top-level string (uses bodyindex)
		{
			name:     "v_has indexed string ==",
			query:    ".*[v_has('val2','string','==','Hello World')]",
			expected: []string{"hub/a"},
		},
		// 2. v_has indexed top-level numeric > (uses bodyindex)
		{
			name:     "v_has indexed numeric >",
			query:    ".*[v_has('val1','numeric','>','20')]",
			expected: []string{"hub/b"},
		},
		// 3. v_has fallback: nested field string == (with colons in value)
		{
			name:     "v_has fallback nested MAC ==",
			query:    ".*[v_has('info.mac','string','==','aa:bb:cc:dd:ee:02')]",
			expected: []string{"hub/b"},
		},
		// 4. v_has fallback: nested field — multiple results
		{
			name:     "v_has fallback nested multiple results",
			query:    ".*[v_has('info.role','string','==','spine')]",
			expected: []string{"hub/a", "hub/c"},
		},
		// 5. v_has fallback: deep nested numeric ==
		{
			name:     "v_has fallback deep nested numeric ==",
			query:    ".*[v_has('config.mtu','numeric','==','9000')]",
			expected: []string{"hub/c"},
		},
		// 6. v_array_has: string array ==
		{
			name:     "v_array_has string == (prod)",
			query:    ".*[v_array_has('tags','string','==','prod')]",
			expected: []string{"hub/a"},
		},
		// 7. v_array_has: string array == — multiple results
		{
			name:     "v_array_has string == (staging)",
			query:    ".*[v_array_has('tags','string','==','staging')]",
			expected: []string{"hub/b", "hub/c"},
		},
		// 8. v_array_has: numeric array ==
		{
			name:     "v_array_has numeric == (port 3)",
			query:    ".*[v_array_has('ports','numeric','==','3')]",
			expected: []string{"hub/b"},
		},
		// 9. v_array_has: numeric array > (ports > 3 means has element > 3, i.e. port 4)
		{
			name:     "v_array_has numeric > (port > 3)",
			query:    ".*[v_array_has('ports','numeric','>','3')]",
			expected: []string{"hub/b"},
		},
		// 10. v_array_has: string array != (tags != 'core' means has at least one element != 'core')
		{
			name:     "v_array_has string != (not core)",
			query:    ".*[v_array_has('tags','string','!=','core')]",
			expected: []string{"hub/a", "hub/b", "hub/c"},
		},
		// 11. l_has fallback: nested field in link body
		{
			name:     "l_has fallback nested numeric >",
			query:    ".*[l_has('metrics.bw','numeric','>','800')]",
			expected: []string{"hub/a", "hub/c"},
		},
		// 12. l_array_has: array in link body ==
		{
			name:     "l_array_has string == (bgp)",
			query:    ".*[l_array_has('protocols','string','==','bgp')]",
			expected: []string{"hub/a", "hub/b"},
		},
	}

	allPassed := true
	for i, tt := range tests {
		payload := easyjson.NewJSONObject()
		payload.SetByPath("query", easyjson.NewJSON(tt.query))

		reply, err := runtime.Request(sfPlugins.GolangLocalRequest, "functions.graph.api.query.jpgql.ctra", "rt", &payload, nil)
		om := sfMediators.OpMsgFromSfReply(reply, err)

		uuids := om.Data.GetByPath("uuids").ObjectKeys()
		sort.Strings(uuids)
		sort.Strings(tt.expected)

		queryNano := om.Data.GetByPath("stats.duration.query_nano").AsNumericDefault(0)
		queryUs := queryNano / 1000

		passed := "PASS"
		if !strSliceEqual(uuids, tt.expected) {
			passed = "FAIL"
			allPassed = false
		}

		lg.Logf(lg.InfoLevel, "[FILTER TEST %2d] %-7s | %6.0f us | %-45s | got=%v expected=%v | %s",
			i+1, passed, queryUs, tt.name, uuids, tt.expected, tt.query)
	}

	if allPassed {
		lg.Logln(lg.InfoLevel, "<<< All filter query tests PASSED")
	} else {
		lg.Logln(lg.ErrorLevel, "<<< Some filter query tests FAILED")
	}

	fmt.Println("<<< Test ended: filter queries")
}

func strSliceEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
