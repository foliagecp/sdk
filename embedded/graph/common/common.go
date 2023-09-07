// Copyright 2023 NJWS Inc.

// Foliage graph DBMS common package.
// Provides shared functions for other graph DBMS packages
package common

import (
	"fmt"
	"json_easy"

	sfplugins "github.com/foliagecp/sdk/statefun/plugins"
	sfSystem "github.com/foliagecp/sdk/statefun/system"
)

const QUERY_RESULT_TOPIC = "functions.graph.query"

func GetQueryId(contextProcessor *sfplugins.StatefunContextProcessor) string {
	var queryId string
	if s, ok := contextProcessor.Payload.GetByPath("query_id").AsString(); ok {
		queryId = s
	} else {
		queryId = sfSystem.GetUniqueStrId()
	}
	return queryId
}

func ReplyQueryId(queryId string, result *json_easy.JSON, contextProcessor *sfplugins.StatefunContextProcessor) {
	if result != nil && contextProcessor != nil {
		if len(contextProcessor.Caller.Typename) == 0 || len(contextProcessor.Caller.ID) == 0 {
			contextProcessor.Egress(QUERY_RESULT_TOPIC+"."+queryId, result) // Publish result to NATS topic (no JetStream)
		} else {
			contextProcessor.Call(contextProcessor.Caller.Typename, contextProcessor.Caller.ID, result, nil)
		}
	} else {
		fmt.Println("ERROR: replyQueryId")
	}
}
