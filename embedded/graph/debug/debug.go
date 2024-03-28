// Copyright 2023 NJWS Inc.

// Foliage graph store debug package.
// Provides debug stateful functions for the graph store
package debug

import (
	"github.com/foliagecp/sdk/statefun"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
)

func RegisterAllFunctionTypes(runtime *statefun.Runtime) {
	statefun.NewFunctionType(runtime, "functions.graph.api.object.debug.print.graph", LLAPIPrintGraph, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect))
}
