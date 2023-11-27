

// Foliage tests main package.
// Provides configurable running of different test samples that goes along with the SDK.
package main

import (
	"flag"
	"fmt"
	"os"

	lg "github.com/foliagecp/sdk/statefun/logger"
	"github.com/foliagecp/sdk/tests/basic"
)

func main() {
	helpFlag := flag.Bool("h", false, "Show help message")
	helpFlagAlias := flag.Bool("help", false, "Show help message (alias)")
	logLevelFlag := flag.Int("ll", int(lg.InfoLevel), "Log level (0-6): panic, fatal, error, warn, info, debug, trace")
	logReportCallerFlag := flag.Bool("lrp", false, "Log report caller shows file name and line number where log originates from")

	flag.Parse()

	if *helpFlag || *helpFlagAlias || flag.NArg() == 0 {
		fmt.Println("usage: tests [option] <test_name>")
		fmt.Println("Options:")
		flag.PrintDefaults()
		return
	}

	lg.SetOutputLevel(lg.LogLevel(*logLevelFlag))
	lg.SetReportCaller(*logReportCallerFlag)

	testName := flag.Arg(flag.NArg() - 1)

	switch testName {
	case "basic":
		defer basic.Start()
	default:
		lg.Logf(lg.ErrorLevel, "Test named \"%s\" not found!\n", testName)
		return
	}

	err := os.Chdir(fmt.Sprintf("./%s", testName))
	if err != nil {
		lg.Logf(lg.ErrorLevel, "Could not chdir to test \"%s\": %s\n", testName, err)
		os.Exit(1)
	}
}
