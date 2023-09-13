// Copyright 2023 NJWS Inc.

// Foliage tests main package.
// Provides configurable running of different test samples that goes along with the SDK.
package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/foliagecp/sdk/tests/basic"
)

func main() {
	helpFlag := flag.Bool("h", false, "Show help message")
	helpFlagAlias := flag.Bool("help", false, "Show help message (alias)")

	flag.Parse()

	if *helpFlag || *helpFlagAlias || flag.NArg() == 0 {
		fmt.Println("usage: tests <test_name>")
		flag.PrintDefaults()
		return
	}

	testName := flag.Arg(0)

	switch testName {
	case "basic":
		defer basic.Start()
	default:
		fmt.Printf("Test named \"%s\" not found!\n", testName)
		return
	}

	err := os.Chdir(fmt.Sprintf("./%s", testName))
	if err != nil {
		fmt.Printf("ERROR: Could not chdir to test \"%s\": %s\n", testName, err)
		os.Exit(1)
	}
}
