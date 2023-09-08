// Copyright 2023 NJWS Inc.

// Foliage tests main package.
// Provides configurable running of different test samples that goes along with the SDK.
package main

import (
	"fmt"
	"log"
	"os"

	"github.com/foliagecp/sdk/statefun/system"
	"github.com/foliagecp/sdk/tests/basic"
	"github.com/joho/godotenv"
)

var TestsRootDir string = system.GetEnvMustProceed("TESTS_ROOT_DIR", "./")

func helpInfo() {
	fmt.Println("usage: tests <test_name>")
	fmt.Println("This help: tests -h | --help")
}

func missingTestInfo(testName string) {
	fmt.Printf("Test named \"%s\" not found!\n", testName)
}

func main() {
	// TODO: use existing libraries for reading args
	argsWithoutProg := os.Args[1:]
	if len(argsWithoutProg) == 0 || argsWithoutProg[0] == "-h" || argsWithoutProg[0] == "--help" {
		helpInfo()
		return
	}
	testName := argsWithoutProg[0]
	switch testName {
	case "basic":
		defer basic.Start()
	default:
		missingTestInfo(testName)
		return
	}

	envFile := fmt.Sprintf("%s/%s/.env", TestsRootDir, testName)
	err := godotenv.Load(envFile)
	if err != nil {
		log.Fatalf("ERROR: Cannon load env file %s: %s\n", envFile, err)
	}
	system.MsgOnErrorReturn(os.Chdir(fmt.Sprintf("%s/%s", TestsRootDir, testName)))
}
