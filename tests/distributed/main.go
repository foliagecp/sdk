// Foliage tests main package.
// Provides configurable running of different test samples that goes along with the SDK.
package main

import (
	"flag"
	"fmt"
	"os"

	lg "github.com/foliagecp/sdk/statefun/logger"
)

func main() {
	helpFlag :=
		flag.Bool(
			"h",
			false,
			"Show help message",
		)
	helpFlagAlias :=
		flag.Bool(
			"help",
			false,
			"Show help message (alias)",
		)
	logLevelFlag :=
		flag.Int(
			"ll",
			int(lg.InfoLevel), // == 2
			"Log level [0;6]: panic, fatal, error, warn, info, debug, trace",
		)
	logReportCallerFlag :=
		flag.Bool(
			"lrp",
			false,
			"Log report caller shows file name and line number where log originates from",
		)

	flag.Parse()

	if *helpFlag || *helpFlagAlias {
		fmt.Println("usage: foliage [option]")
		fmt.Println("Options:")
		flag.PrintDefaults()
		return
	}

	if *logLevelFlag < -2 || *logLevelFlag > 6 {
		fmt.Println("Please select logging level from [0;6]")
		return
	}

	lg.SetDefaultOptions(
		os.Stdout,
		// subtract and multiply, because each level has a factor of 4: -8, -4, 0, 4, 8, 12, 16
		lg.LogLevel((4-*logLevelFlag)*4),
		*logReportCallerFlag,
	)

	Start()
}
