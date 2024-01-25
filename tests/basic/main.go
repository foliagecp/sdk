// Foliage tests main package.
// Provides configurable running of different test samples that goes along with the SDK.
package main

import (
	"flag"
	"fmt"

	lg "github.com/foliagecp/sdk/statefun/logger"
)

func main() {
	helpFlag := flag.Bool("h", false, "Show help message")
	helpFlagAlias := flag.Bool("help", false, "Show help message (alias)")
	logLevelFlag := flag.Int("ll", int(lg.InfoLevel), "Log level (0-6): panic, fatal, error, warn, info, debug, trace")
	logReportCallerFlag := flag.Bool("lrp", false, "Log report caller shows file name and line number where log originates from")

	flag.Parse()

	if *helpFlag || *helpFlagAlias {
		fmt.Println("usage: foliage [option]")
		fmt.Println("Options:")
		flag.PrintDefaults()
		return
	}

	lg.SetOutputLevel(lg.LogLevel(*logLevelFlag))
	lg.SetReportCaller(*logReportCallerFlag)

	Start()
}
