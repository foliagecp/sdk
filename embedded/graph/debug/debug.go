// Foliage graph store debug package.
// Provides debug stateful functions for the graph store
package debug

import (
	"github.com/foliagecp/sdk/statefun"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
)

const (
	MAX_ACK_WAIT_MS = 60 * 1000
)

func RegisterAllFunctionTypes(runtime *statefun.Runtime) {
	statefun.NewFunctionType(runtime, "functions.graph.api.object.debug.print.graph", LLAPIPrintGraph, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).SetMsgAckWaitMs(MAX_ACK_WAIT_MS))
	statefun.NewFunctionType(runtime, "functions.graph.api.import", LLAPIImportGraph, *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).SetMsgAckWaitMs(MAX_ACK_WAIT_MS))
}
