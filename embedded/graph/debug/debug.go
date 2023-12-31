// Copyright 2023 NJWS Inc.

// Foliage graph store debug package.
// Provides debug stateful functions for the graph store
package debug

import (
	"fmt"

	"github.com/foliagecp/sdk/statefun"
	sfplugins "github.com/foliagecp/sdk/statefun/plugins"
)

func RegisterAllFunctionTypes(runtime *statefun.Runtime) {
	statefun.NewFunctionType(runtime, "functions.graph.ll.api.object.debug.print", LLAPIObjectDebugPrint, *statefun.NewFunctionTypeConfig())
	statefun.NewFunctionType(runtime, "functions.graph.ll.api.object.debug.print.graph", LLAPIPrintGraph, *statefun.NewFunctionTypeConfig())
}

/*
Prints to caonsole the content of an object the function being called on along with all its input and output links.
*/
func LLAPIObjectDebugPrint(executor sfplugins.StatefunExecutor, contextProcessor *sfplugins.StatefunContextProcessor) {
	self := contextProcessor.Self

	objectContext := contextProcessor.GetObjectContext()
	fmt.Printf("************************* Object's body (id=%s):\n", self.ID)
	fmt.Println(objectContext.ToString())
	fmt.Printf("************************* In links:\n")
	for _, key := range contextProcessor.GlobalCache.GetKeysByPattern(self.ID + ".in.oid_ltp-nil.>") {
		fmt.Println(key)
	}
	fmt.Printf("************************* Out links:\n")
	for _, key := range contextProcessor.GlobalCache.GetKeysByPattern(self.ID + ".out.ltp_oid-bdy.>") {
		fmt.Println(key)
		if j, err := contextProcessor.GlobalCache.GetValueAsJSON(key); err == nil {
			fmt.Println(j.ToString())
		}
	}
	fmt.Println()
}
