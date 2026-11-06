//go:build !race

package cache

// raceEnabled says the test binary is instrumented by the race detector, whose
// overhead makes wall-clock comparisons say nothing about the uninstrumented code.
const raceEnabled = false
