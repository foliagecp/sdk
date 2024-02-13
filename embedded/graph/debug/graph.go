//go:build !cgo
// +build !cgo

package debug

import (
	"github.com/foliagecp/sdk/statefun/logger"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
)

func LLAPIPrintGraph(executor sfPlugins.StatefunExecutor, contextProcessor *sfPlugins.StatefunContextProcessor) {
	logger.Logln(logger.InfoLevel, "Need to enable CGO")
}
