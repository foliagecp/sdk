// Package leak hosts the SDK memory-leak test suite (build tag `leak`).
//
// The suite pressures CRUD, queries (jpgql/fpl), batch, function contexts and
// the cache store in fixed churn cycles and verifies, per scenario, that (a)
// deterministic counters return exactly to their post-warmup baseline and (b)
// heap drift over the measured cycles is statistically indistinguishable from
// zero at the 3-sigma level. Run it via scripts/run-leak-tests.sh or directly:
//
//	go test -tags leak -count=1 -v ./tests/leak/
package leak
