// Copyright 2023 NJWS Inc.

// Foliage graph store common package.
// Provides shared functions for other graph store packages
package common

import (
	"github.com/foliagecp/easyjson"

	lg "github.com/foliagecp/sdk/statefun/logger"
	"github.com/foliagecp/sdk/statefun/plugins"
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
		if contextProcessor.Reply != nil {
			contextProcessor.Reply.With(result)
		} else {
			if len(contextProcessor.Caller.Typename) == 0 || len(contextProcessor.Caller.ID) == 0 { // Signal was sent from a NATS cli
				sfSystem.MsgOnErrorReturn(contextProcessor.Signal(plugins.JetstreamGlobalSignal, QueryResultTopic, queryID, result, nil)) // Publish result to NATS special query reply topic (no JetStream)
			} // else do not reply to a signal from another statefun
		}
	} else {
		lg.Logln(lg.WarnLevel, "replyQueryId has no target to reply")
	}
}
