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

// RegisterAllFunctionTypes registers all graph debug/import statefun handlers.
//
// Chunked export uses a single function type (print.graph) for all three
// operations: graph traversal, get_chunk, and finish_session. The operation
// is selected by the export_action payload field. Using one function type
// guarantees that all operations are served by the same runtime process —
// the statefun single-instance lock is per function type, so separate types
// would have independent locks and could land on different processes, making
// the in-memory ExportSessionStore invisible across runtimes.
func RegisterAllFunctionTypes(runtime *statefun.Runtime) {
	cfg := *statefun.NewFunctionTypeConfig().SetAllowedRequestProviders(sfPlugins.AutoRequestSelect).SetMsgAckWaitMs(MAX_ACK_WAIT_MS)
	statefun.NewFunctionType(runtime, "functions.graph.api.object.debug.print.graph", LLAPIPrintGraph, cfg)
	statefun.NewFunctionType(runtime, "functions.graph.api.import", LLAPIImportGraph, cfg)
}
