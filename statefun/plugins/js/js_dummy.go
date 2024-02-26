//go:build !(cgo && !graph_debug)

package js

import (
	"fmt"

	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
)

type StatefunExecutorPluginJS struct {
}

var (
	cgoError = fmt.Errorf("CGO is not enabled")
)

func StatefunExecutorPluginJSContructor(alias string, source string) sfPlugins.StatefunExecutor {
	return &StatefunExecutorPluginJS{}
}

func (sfejs *StatefunExecutorPluginJS) Run(ctx *sfPlugins.StatefunContextProcessor) error {
	return cgoError
}

func (sfejs *StatefunExecutorPluginJS) BuildError() error {
	return cgoError
}
