// Copyright 2023 NJWS Inc.

// Foliage graph store common package.
// Provides shared functions for other graph store packages
package common

import (
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	sfSystem "github.com/foliagecp/sdk/statefun/system"
)

func GetQueryID(contextProcessor *sfPlugins.StatefunContextProcessor) string {
	var queryID string
	if s, ok := contextProcessor.Payload.GetByPath("query_id").AsString(); ok {
		queryID = s
	} else {
		queryID = sfSystem.GetUniqueStrID()
	}
	return queryID
}
