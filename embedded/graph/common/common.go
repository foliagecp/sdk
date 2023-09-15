

// Foliage graph store common package.
// Provides shared functions for other graph store packages
package common

import (
	"fmt"

	"github.com/foliagecp/easyjson"

	sfplugins "github.com/foliagecp/sdk/statefun/plugins"
	sfSystem "github.com/foliagecp/sdk/statefun/system"
)

const QueryResultTopic = "functions.graph.query"

func GetQueryID(contextProcessor *sfplugins.StatefunContextProcessor) string {
	var queryID string
	if s, ok := contextProcessor.Payload.GetByPath("query_id").AsString(); ok {
		queryID = s
	} else {
		queryID = sfSystem.GetUniqueStrID()
	}
	return queryID
}

func ReplyQueryID(queryID string, result *easyjson.JSON, contextProcessor *sfplugins.StatefunContextProcessor) {
	if result != nil && contextProcessor != nil {
		if len(contextProcessor.Caller.Typename) == 0 || len(contextProcessor.Caller.ID) == 0 {
			contextProcessor.Egress(QueryResultTopic+"."+queryID, result) // Publish result to NATS topic (no JetStream)
		} else {
			contextProcessor.Call(contextProcessor.Caller.Typename, contextProcessor.Caller.ID, result, nil)
		}
	} else {
		fmt.Println("ERROR: replyQueryId")
	}
}
