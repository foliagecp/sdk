package main

import (
	"os"
)

// simulating crash of the runtime
func crashRuntime() {
	os.Exit(1)
}
