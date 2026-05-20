// Foliage export test package.
// Tests the full export pipeline: CMDB CRUD → WAL → ExportCommitter → Export Stream → PG Dumper → PostgreSQL.
package main

import (
	"flag"
	"fmt"
	"os"

	lg "github.com/foliagecp/sdk/statefun/logger"
)

func main() {
	helpFlag := flag.Bool("h", false, "Show help message")
	logLevelFlag := flag.Int("ll", int(lg.InfoLevel), "Log level [0;6]: panic, fatal, error, warn, info, debug, trace")
	logReportCallerFlag := flag.Bool("lrp", false, "Log report caller")

	flag.Parse()

	if *helpFlag {
		fmt.Println("usage: foliage-export-test [option]")
		fmt.Println("Options:")
		flag.PrintDefaults()
		return
	}

	if *logLevelFlag < -2 || *logLevelFlag > 6 {
		fmt.Println("Please select logging level from [0;6]")
		return
	}

	lg.SetDefaultOptions(os.Stdout, lg.LogLevel((4-*logLevelFlag)*4), *logReportCallerFlag)

	Start()
}
