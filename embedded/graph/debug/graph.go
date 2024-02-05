//go:build !cgo
// +build !cgo

package debug

import (
	"github.com/foliagecp/sdk/statefun/logger"
	sfplugins "github.com/foliagecp/sdk/statefun/plugins"
)

func LLAPIPrintGraph(executor sfplugins.StatefunExecutor, contextProcessor *sfplugins.StatefunContextProcessor) {
	logger.Logln(logger.InfoLevel, "Need to enable CGO")
}
