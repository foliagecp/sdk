package system

import (
	"bytes"
	"fmt"
	"runtime"
	"runtime/pprof"
	"sort"
	"time"

	"github.com/google/pprof/profile"
)

func StartHeapWatcher(intervalMins float32) {
	now := time.Now()
	fmt.Printf("heapwatcher: active (%.2f mins interval)\n", intervalMins)
	var initial, prev *profile.Profile
	ticker := time.NewTicker(time.Duration(intervalMins) * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		formatted := now.Format("2006-01-02 15:04:05")
		fmt.Printf("heapwatcher: %s\n", formatted)

		runtime.GC()
		time.Sleep(100 * time.Millisecond)

		var buf bytes.Buffer
		if err := pprof.WriteHeapProfile(&buf); err != nil {
			fmt.Printf("heapwatcher: failed to write profile: %v\n", err)
			continue
		}

		prof, err := profile.Parse(&buf)
		if err != nil {
			fmt.Printf("heapwatcher: failed to parse profile: %v\n", err)
			continue
		}

		if initial != nil && prev != nil {
			printGrowthAndUsage(initial, prev, prof)
		}

		prev = prof
		if initial == nil {
			initial = prof
		}
	}
}

func printGrowthAndUsage(initial, previous, current *profile.Profile) {
	type change struct {
		name       string
		deltaPrev  int64
		deltaStart int64
		currentUse int64
	}

	// find inuse_space index
	inuseIdx := -1
	for i, t := range current.SampleType {
		if t.Type == "inuse_space" {
			inuseIdx = i
			break
		}
	}
	if inuseIdx == -1 {
		fmt.Println("heapwatcher: inuse_space not found in profile")
		return
	}

	summarize := func(p *profile.Profile) map[string]int64 {
		m := make(map[string]int64)
		for _, s := range p.Sample {
			name := "unknown"
			for _, loc := range s.Location {
				for _, line := range loc.Line {
					if line.Function != nil && line.Function.Name != "" {
						name = line.Function.Name
						break
					}
				}
				if name != "unknown" {
					break
				}
			}
			m[name] += s.Value[inuseIdx]
		}
		return m
	}

	initMap := summarize(initial)
	prevMap := summarize(previous)
	currMap := summarize(current)

	var changes []change
	seen := make(map[string]struct{})
	for k := range initMap {
		seen[k] = struct{}{}
	}
	for k := range prevMap {
		seen[k] = struct{}{}
	}
	for k := range currMap {
		seen[k] = struct{}{}
	}

	for name := range seen {
		cur := currMap[name]
		dPrev := cur - prevMap[name]
		dStart := cur - initMap[name]
		if dPrev != 0 || dStart != 0 {
			changes = append(changes, change{name, dPrev, dStart, cur})
		}
	}

	sort.Slice(changes, func(i, j int) bool {
		return abs(changes[i].deltaPrev) > abs(changes[j].deltaPrev)
	})

	if len(changes) == 0 {
		fmt.Println("heapwatcher: no change in memory usage")
		return
	}

	fmt.Println("heapwatcher: memory usage change and current inuse by function:")
	for _, c := range changes {
		fmt.Printf("    %-40s Δprev: %+8.2f KB Δstart: %+8.2f KB Current: %8.2f KB\n",
			c.name,
			float64(c.deltaPrev)/1024,
			float64(c.deltaStart)/1024,
			float64(c.currentUse)/1024,
		)
	}
}

func abs(x int64) int64 {
	if x < 0 {
		return -x
	}
	return x
}
