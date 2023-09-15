// Copyright 2023 NJWS Inc.

// Foliage graph store jpgql package.
// Provides stateful functions of json-path graph query language for the graph store
package jpgql

import (
	"fmt"
	"strings"
	"time"

	"github.com/foliagecp/easyjson"

	"github.com/foliagecp/sdk/embedded/graph/common"
	"github.com/foliagecp/sdk/statefun"
	"github.com/foliagecp/sdk/statefun/cache"
	sfPlugins "github.com/foliagecp/sdk/statefun/plugins"
	sfSystem "github.com/foliagecp/sdk/statefun/system"
)

/*
Uses JPGQL call-tree result aggregation algorithm to find objects

Request:

	payload: json - required
		// Initial request from caller
		query_id: string - optional // ID for this query.
		jpgql_query: string - required // Json path query
		call: json - optional // A call to be done on found targets
			typename: string - required // Typename to be called
			payload: json - required // Data for typename to be called with

		// Self-requests to descendants: (ID is composite: <object_id>===<process_id> - for async execution)
		query_id: string - required // ID for this query.
		caller_aggregation_id: string - required // Id which descendants will send to caller when sending its results
		jpgql_query: string - required // Json path query
		call: json - optional // A call to be done on found targets
			typename: string - required // Typename to be called
			payload: json - required // Data for typename to be called with

	options: json - optional
		eval_timeout_sec: int - optional // Execution timeout

Reply:

	payload: json
		query_id: string // ID for this query.
		aggregation_id: string // Id which to use to aggregate result
		result: []string // Found objects
*/
func LLAPIQueryJPGQLCallTreeResultAggregation(executor sfPlugins.StatefunExecutor, contextProcessor *sfPlugins.StatefunContextProcessor) {
	jpgqlEvaluationTimeoutSec := 30
	if v, ok := contextProcessor.Options.GetByPath("eval_timeout_sec").AsNumeric(); ok {
		jpgqlEvaluationTimeoutSec = int(v)
	}

	var rootProcess bool = true
	c := strings.Count(contextProcessor.Self.ID, "===")
	if c == 1 {
		rootProcess = false
	} else if c > 1 {
		fmt.Printf("ERROR LLAPIQueryJPGQLCallTreeResultAggregation: contextProcessor.Self.ID for descendant must be composite according to the following format: <object_id>===<process_id>\n")
		return
	}

	payload := contextProcessor.Payload
	context := contextProcessor.GetFunctionContext()

	//fmt.Println(contextProcessor.Self.ID+" | Context:", context.ToString())

	if rootProcess {
		queryID := common.GetQueryID(contextProcessor)

		processID := sfSystem.GetUniqueStrID()
		payload.SetByPath("caller_aggregation_id", easyjson.NewJSON(processID))
		payload.SetByPath("query_id", easyjson.NewJSON(queryID))
		contextProcessor.Call(contextProcessor.Self.Typename, contextProcessor.Self.ID+"==="+processID, payload, nil)

		keyBase := fmt.Sprintf("jpgql_ctra.%s.%s", contextProcessor.Self.ID, processID)

		chacheUpdatedChannel := contextProcessor.GlobalCache.SubscribeLevelCallback(keyBase+".*", processID)
		go func(chacheUpdatedChannel chan cache.KeyValue) {
			startedEvaluating := sfSystem.GetCurrentTimeNs()
			for {
				select {
				case kv := <-chacheUpdatedChannel:
					//fmt.Println("____________ UPDATE FROM CACHE!!!!")
					key := kv.Key.(string)
					value := kv.Value.([]byte)
					if key == "result" {
						if result, ok := easyjson.JSONFromBytes(value); ok {
							contextProcessor.GlobalCache.UnsubscribeLevelCallback(keyBase+".*", processID)
							common.ReplyQueryID(queryID, &result, contextProcessor)
							return
						}
					}
				case <-time.After(1 * time.Second):
					if startedEvaluating+int64(jpgqlEvaluationTimeoutSec)*int64(time.Second) < sfSystem.GetCurrentTimeNs() {
						contextProcessor.GlobalCache.UnsubscribeLevelCallback(keyBase+".*", processID)

						//fmt.Println(processID + "::: " + "LLAPIQueryJPGQLCallTreeResultAggregation evaluation timeout!")
						errorString := "LLAPIQueryJPGQLCallTreeResultAggregation evaluation timeout!"
						fmt.Println(errorString)

						result := easyjson.NewJSONObject()
						result.SetByPath("status", easyjson.NewJSON("failed"))
						result.SetByPath("result", easyjson.NewJSON(errorString))
						common.ReplyQueryID(queryID, &result, contextProcessor)
						return
					}
				}
			}
		}(chacheUpdatedChannel)
	} else {
		idTokens := strings.Split(contextProcessor.Self.ID, "===")
		if len(idTokens) != 2 {
			fmt.Printf("ERROR LLAPIQueryJPGQLCallTreeResultAggregation: contextProcessor.Self.ID for descendant must be composite according to the following format: <object_id>===<process_id>\n")
			return
		}
		var thisObjectID string = idTokens[0]
		var processID string = idTokens[1]

		var queryID string
		if s, ok := payload.GetByPath("query_id").AsString(); ok {
			queryID = s
		} else {
			fmt.Printf("Error LLAPIQueryJPGQLCallTreeResultAggregation: this function was called by another LLAPIQueryJPGQLCallTreeResultAggregation - \"query_id\" must exist")
			return
		}

		//fmt.Println(processID+"::: "+thisObjectID+" | Context:", context.ToString())

		getState := func() (int, error) { // 0 - query from parent, 1 - aggregate from child
			if payload.PathExists("jpgql_query") {
				if payload.PathExists("result") {
					return -1, fmt.Errorf("Error LLAPIQueryJPGQLCallTreeResultAggregation: invalid request: \"jpgql_query\" and \"result\" cannot be presented simultaneously")
				}
				return 0, nil
			}
			if payload.PathExists("aggregation_id") && payload.PathExists("result") {
				if payload.PathExists("jpgql_query") {
					return -1, fmt.Errorf("Error LLAPIQueryJPGQLCallTreeResultAggregation: invalid request: \"jpgql_query\" and \"result\" cannot be presented simultaneously")
				}
				return 1, nil
			}
			return -1, fmt.Errorf(`Error LLAPIQueryJPGQLCallTreeResultAggregation: invalid request: either "jpgql_query" or "result"+"aggregation_id" must exist in payload`)
		}

		getQuery := func() (string, error) {
			jpQuery, ok := payload.GetByPath("jpgql_query").AsString()
			if !ok || len(jpQuery) == 0 {
				return "", fmt.Errorf("Error LLAPIQueryJPGQLCallTreeResultAggregation: \"jpgql_query\" must be a string with len>0")
			}
			return jpQuery, nil
		}

		registerAggregationID := func(query string) (string, bool) { // returns: query_id for descendants and aggreagtion, ok
			aggregationID := sfSystem.GetHashStr(queryID + "_" + query)
			if context.PathExists(aggregationID + "_result") {
				return aggregationID, false
			}
			callerAggregationID := ""
			if aggregationID, ok := payload.GetByPath("caller_aggregation_id").AsString(); ok {
				callerAggregationID = aggregationID
			}
			context.SetByPath(aggregationID+"_result", easyjson.NewJSONObject())
			context.SetByPath(aggregationID+"_callbacks", easyjson.NewJSON(0)) // Stores counter of callbacks from descendants
			context.SetByPath(aggregationID+"_reply_object_id", easyjson.NewJSON(contextProcessor.Caller.ID))
			context.SetByPath(aggregationID+"_caller_aggregation_id", easyjson.NewJSON(callerAggregationID))
			if call := payload.GetByPath("call"); call.IsObject() {
				context.SetByPath(aggregationID+"_call", call)
			}
			return aggregationID, true
		}

		unregisterAggregationQueryID := func(aggregationID string) {
			context.RemoveByPath(aggregationID + "_result")                // Release this object from this query from specific parent
			context.RemoveByPath(aggregationID + "_callbacks")             // Release this object from this query from specific parent
			context.RemoveByPath(aggregationID + "_reply_object_id")       // Release this object from this query from specific parent
			context.RemoveByPath(aggregationID + "_caller_aggregation_id") // Release this object from this query from specific parent
			context.RemoveByPath(aggregationID + "_call")                  // Release this object from this query from specific parent
		}

		replyCaller := func(thisFunctionAggregationID string, replyPayload *easyjson.JSON) error {
			//fmt.Println("----------->>> PRERESULT ")
			replyObjectID, ok := context.GetByPath(thisFunctionAggregationID + "_reply_object_id").AsString()
			if !ok {
				return fmt.Errorf("Error LLAPIQueryJPGQLCallTreeResultAggregation: no valid reply_object_id")
			}
			if len(replyObjectID) > 0 && strings.Count(replyObjectID, "===") == 1 {
				contextProcessor.Call(contextProcessor.Self.Typename, replyObjectID, replyPayload, nil)
			} else {
				if context.PathExists(thisFunctionAggregationID + "_call") {
					if call := context.GetByPath(thisFunctionAggregationID + "_call"); call.IsObject() {
						if typename, ok := call.GetByPath("typename").AsString(); ok {
							if callPayload := call.GetByPath("payload"); callPayload.IsObject() {
								if resultObjectsMap, ok := replyPayload.GetByPath("result").AsObject(); ok {
									for objectID := range resultObjectsMap {
										contextProcessor.Call(typename, objectID, &callPayload, nil)
									}
								} else {
									fmt.Printf("ERROR LLAPIQueryJPGQLCallTreeResultAggregation cannot make call on target: no result objects\n")
								}
							} else {
								fmt.Printf("ERROR LLAPIQueryJPGQLCallTreeResultAggregation cannot make call on target: call payload is not a JSON object\n")
							}
						} else {
							fmt.Printf("ERROR LLAPIQueryJPGQLCallTreeResultAggregation cannot make call on target: call typename is not a string\n")
						}
					}
				}
				result := easyjson.NewJSONObject()
				result.SetByPath("status", easyjson.NewJSON("ok"))
				result.SetByPath("result", replyPayload.GetByPath("result"))

				callerAggregationID, _ := replyPayload.GetByPath("aggregation_id").AsString()
				//fmt.Println("----------->>> RESULT " + result.ToString())
				contextProcessor.GlobalCache.SetValue(fmt.Sprintf("jpgql_ctra.%s.%s.result", thisObjectID, callerAggregationID), result.ToBytes(), false, -1, "")
			}
			unregisterAggregationQueryID(thisFunctionAggregationID)
			return nil
		}

		replyCallerPreventSameQueryCall := func() error {
			if callerAggregationID, ok := payload.GetByPath("caller_aggregation_id").AsString(); ok {
				if strings.Count(contextProcessor.Caller.ID, "===") == 1 {
					replyPayload := easyjson.NewJSONObject()
					replyPayload.SetByPath("query_id", easyjson.NewJSON(queryID))
					replyPayload.SetByPath("aggregation_id", easyjson.NewJSON(callerAggregationID))
					replyPayload.SetByPath("result", easyjson.NewJSONObject())
					contextProcessor.Call(contextProcessor.Self.Typename, contextProcessor.Caller.ID, &replyPayload, nil)
				}
				return nil
			}
			return fmt.Errorf("ERROR replyCallerLoopPrevent: callerAggregationID does not exist for object_id=%s", thisObjectID)
		}

		state, err := getState()
		if err != nil { // Cannot get current state
			fmt.Println(err.Error())
			return
		}

		if state == 0 {
			//fmt.Println(processID+"::: 0 "+thisObjectID+" | Context:", context.ToString())
			//fmt.Println(processID + ":0:: " + "(" + thisObjectID + ") " + "1")
			currentObjectLinksQuery, err := getQuery()
			if err != nil {
				fmt.Println(err.Error())
				return
			}

			thisFunctionAggregationID, uniqueParentAndQuery := registerAggregationID(currentObjectLinksQuery)
			//fmt.Println(processID + ":0:: " + "(" + thisObjectID + ") " + "2")
			callerAggregationID, ok := context.GetByPath(thisFunctionAggregationID + "_caller_aggregation_id").AsString()
			if !ok {
				fmt.Printf("ERROR LLAPIQueryJPGQLCallTreeResultAggregation: no valid caller_aggregation_id on state=0 for object_id=%s\n", thisObjectID)
				return
			}
			//fmt.Println(processID + ":0:: " + "(" + thisObjectID + ") " + "3")
			if !(uniqueParentAndQuery) { // This query from that parent was already registered
				if err := replyCallerPreventSameQueryCall(); err != nil {
					fmt.Println(err.Error())
					return
				}
			} else {
				//fmt.Println(processID + ":0:: " + "(" + thisObjectID + ") " + "4")
				queryHeadLinkType, queryHeadFilter, queryTail, anyDepthStop, err := GetQueryHeadAndTailsParts(currentObjectLinksQuery)
				if err != nil {
					fmt.Printf("ERROR LLAPIQueryJPGQLCallTreeResultAggregation: currentObjectLinksQuery is invalid: %s\n", err)
					return
				}
				//fmt.Println(processID + ":0:: " + "(" + thisObjectID + ") " + "5")
				resultObjects := GetObjectIDsFromLinkTypeAndLinkFilterQueryWithAnyDepthStop(contextProcessor.GlobalCache, thisObjectID, queryHeadLinkType, queryHeadFilter, anyDepthStop)
				//fmt.Println("======== RESULT OBJECTS: " + fmt.Sprintln(resultObjects))

				//fmt.Println(processID + ":0:: " + "(" + thisObjectID + ") " + "6")
				if len(resultObjects) == 0 { // If no links found (no matter if queryTail exists) - return result empty objects array immediately without aggregation
					replyPayload := easyjson.NewJSONObject()
					replyPayload.SetByPath("query_id", easyjson.NewJSON(queryID))
					replyPayload.SetByPath("aggregation_id", easyjson.NewJSON(callerAggregationID))
					replyPayload.SetByPath("result", easyjson.NewJSONObject())
					//fmt.Println(processID + ":0:: " + "(" + thisObjectID + ") " + "7")
					if err := replyCaller(thisFunctionAggregationID, &replyPayload); err != nil {
						fmt.Println(err.Error())
						return
					}
					//fmt.Println(processID+"::: 0:0 "+thisObjectID+" | Context:", context.ToString())
				} else { // There are objects to pass tail query to - store result objects in aggregation array
					//fmt.Println(processID + ":0:: " + "(" + thisObjectID + ") " + "8")
					objectsToReturnAsAResult := map[string]bool{}
					nextCalls := 0
					for objectID, anyDepthStopped := range resultObjects {
						nextQuery := queryTail
						if anyDepthStopped == 1 {
							nextQuery = anyDepthStop.QueryTail
						}
						if len(nextQuery) == 0 { // jpgql_query ended!!!!
							objectsToReturnAsAResult[objectID] = true
						} else {
							nextPayload := easyjson.NewJSONObject()
							nextPayload.SetByPath("query_id", easyjson.NewJSON(queryID))
							nextPayload.SetByPath("caller_aggregation_id", easyjson.NewJSON(thisFunctionAggregationID))
							nextPayload.SetByPath("jpgql_query", easyjson.NewJSON(nextQuery))
							//fmt.Println(processID+"::: 0:0.1 "+thisObjectID+" | CHILD:", objectID)
							contextProcessor.Call(contextProcessor.Self.Typename, objectID+"==="+processID, &nextPayload, nil)
							nextCalls++
						}
					}
					//fmt.Println(processID + ":0:: " + "(" + thisObjectID + ") " + "9")
					immediateAggregationResult := easyjson.NewJSON(objectsToReturnAsAResult)
					context.SetByPath(thisFunctionAggregationID+"_result", immediateAggregationResult)
					if nextCalls == 0 { // All descendant objects on links are result ones
						replyPayload := easyjson.NewJSONObject()
						replyPayload.SetByPath("query_id", easyjson.NewJSON(queryID))
						replyPayload.SetByPath("aggregation_id", easyjson.NewJSON(callerAggregationID))
						replyPayload.SetByPath("result", immediateAggregationResult)
						if err := replyCaller(thisFunctionAggregationID, &replyPayload); err != nil {
							fmt.Println(err.Error())
							return
						}
						//fmt.Println(processID+"::: 0:1 "+thisObjectID+" | Context:", context.ToString())
					} else { // There are descendants to aggregate result from
						context.SetByPath(thisFunctionAggregationID+"_callbacks", easyjson.NewJSON(nextCalls)) // Store counter of callbacks from descendants
						//fmt.Println(processID+"::: 0:2 "+thisObjectID+" | Context:", context.ToString())
					}
					//fmt.Println(processID + ":0:: " + "(" + thisObjectID + ") " + "10")
				}
			}
		} else { // Aggregation state - got call from descendant to aggregate its result
			//fmt.Println(processID+"::: 1 "+thisObjectID+" | Context:", context.ToString())
			//fmt.Println(processID + ":1:: " + "(" + thisObjectID + ") " + "11")
			thisFunctionAggregationID, ok := payload.GetByPath("aggregation_id").AsString()
			if !ok {
				fmt.Printf("ERROR LLAPIQueryJPGQLCallTreeResultAggregation: \"aggregationID\" must be a string\n")
				return
			}
			result, ok := payload.GetByPath("result").AsObject()
			if !ok {
				fmt.Printf("ERROR LLAPIQueryJPGQLCallTreeResultAggregation: \"result\" must be a string array\n")
				return
			}
			callbacksFloat, ok := context.GetByPath(thisFunctionAggregationID + "_callbacks").AsNumeric()
			if !ok || callbacksFloat < 0 {
				fmt.Printf("ERROR LLAPIQueryJPGQLCallTreeResultAggregation: no valid callbacks counter for result aggregation for object_id=%s\n", thisObjectID)
				return
			}
			callbacks := int(callbacksFloat)

			//fmt.Println(processID + ":1:: " + "(" + thisObjectID + ") " + "12")
			callbacks--
			totalResult, _ := context.GetByPath(thisFunctionAggregationID + "_result").AsObject()
			totalResult = sfSystem.MergeMaps[interface{}](totalResult, result)
			context.SetByPath(thisFunctionAggregationID+"_result", easyjson.NewJSON(totalResult))
			context.SetByPath(thisFunctionAggregationID+"_callbacks", easyjson.NewJSON(callbacks))

			//fmt.Println(processID + ":1:: " + "(" + thisObjectID + ") " + "13: ")

			if callbacks == 0 { // Aggregated from all descendants
				//fmt.Println(processID+"::: 1:0 "+thisObjectID+" | Context:", context.ToString())
				callerAggregationID, ok := context.GetByPath(thisFunctionAggregationID + "_caller_aggregation_id").AsString()
				if !ok {
					fmt.Printf("ERROR LLAPIQueryJPGQLCallTreeResultAggregation: no valid caller_aggregation_id on state=1 for object_id=%s\n", thisObjectID)
					return
				}

				replyPayload := easyjson.NewJSONObject()
				replyPayload.SetByPath("query_id", easyjson.NewJSON(queryID))
				replyPayload.SetByPath("aggregation_id", easyjson.NewJSON(callerAggregationID))
				replyPayload.SetByPath("result", easyjson.NewJSON(totalResult))
				//fmt.Println(processID + ":1:: " + "(" + thisObjectID + ") " + "13.1")
				if err := replyCaller(thisFunctionAggregationID, &replyPayload); err != nil {
					fmt.Println(err.Error())
					return
				}
			}
			//fmt.Println(processID + ":1:: " + "(" + thisObjectID + ") " + "14")
		}
		//fmt.Println(processID+"::: UC "+thisObjectID+" | Context:", context.ToString())
		contextProcessor.SetFunctionContext(context)
		//fmt.Println(processID + "::: UCCCCCCCCCCCCCC " + thisObjectID)
		//ttt := contextProcessor.GetFunctionContext()
		//fmt.Println(processID+"::: UCCCCCCCCCC TTTTTTTTTTTTT "+thisObjectID+" | Context:", ttt.ToString())
		//fmt.Println(processID + "::: " + "(" + thisObjectID + ") " + "16")
	}
}

/*
Uses JPGQL direct cache result aggregation algorithm to find objects

Request:

	payload: json - required
		// Initial request from caller:
		query_id: string - optional // ID for this query.
		jpgql_query: string - required // Json path query
		call: json - optional // A call to be done on found targets
			typename: string - required // Typename to be called
			payload: json - required // Data for typename to be called with

		// Self-requests to descendants: (ID is composite: <object_id>===<process_id> - for async execution)
		aggregation_id: string - required // Original ID for the search query.
		jpgql_query: string - required // Json path query
		call: json - optional // A call to be done on found targets
			typename: string - required // Typename to be called
			payload: json - required // Data for typename to be called with

	options: json - optional
		eval_timeout_sec: int - optional // Execution timeout

Reply:

	payload: json
		query_id: string // ID for this query.
		aggregation_id: string // Id which to use to aggregate result
		result: []string // Found objects
*/
func LLAPIQueryJPGQLDirectCacheResultAggregation(executor sfPlugins.StatefunExecutor, contextProcessor *sfPlugins.StatefunContextProcessor) {
	modifiedTypename := "jpgql_dcra"

	jpgqlEvaluationTimeoutSec := 30
	if v, ok := contextProcessor.Options.GetByPath("eval_timeout_sec").AsNumeric(); ok {
		jpgqlEvaluationTimeoutSec = int(v)
	}

	payload := contextProcessor.Payload
	var call *easyjson.JSON = nil
	if j := payload.GetByPath("call"); j.IsObject() {
		call = &j
	}

	var rootProcess bool = true
	c := strings.Count(contextProcessor.Self.ID, "===")
	if c == 1 {
		rootProcess = false
	} else if c > 1 {
		fmt.Printf("ERROR LLAPIQueryJPGQLDirectCacheResultAggregation: contextProcessor.Self.ID for descendant must be composite according to the following format: <object_id>===<process_id>\n")
		return
	}

	var currentQuery string
	if v, ok := payload.GetByPath("jpgql_query").AsString(); ok && len(v) > 0 {
		currentQuery = v
	} else {
		fmt.Println("ERROR LLAPIQueryJPGQLDirectCacheResultAggregation: \"jpgql_query\" must be a string with len>0")
		return
	}

	//fmt.Println(contextProcessor.Self.ID)

	initPendingProcess := func(objectID string, objectQuery string, aggregationID string) bool {
		//fmt.Println("initPendingProcess 1", objectID)
		pendingProcessID := sfSystem.GetHashStr(objectID + "_" + objectQuery)
		//fmt.Println("initPendingProcess 2", objectID)

		return contextProcessor.GlobalCache.SetValueIfDoesNotExist(fmt.Sprintf("%s.%s.pending.%s", modifiedTypename, aggregationID, pendingProcessID), []byte{1}, true, -1)
	}

	if rootProcess {
		queryID := common.GetQueryID(contextProcessor)

		aggregationID := sfSystem.GetUniqueStrID()
		chacheUpdatedChannel := contextProcessor.GlobalCache.SubscribeLevelCallback(fmt.Sprintf("%s.%s.pending.%s", modifiedTypename, aggregationID, "*"), aggregationID)

		go func(chacheUpdatedChannel chan cache.KeyValue) {
			startedEvaluating := sfSystem.GetCurrentTimeNs()
			pendingMap := map[string]bool{}
			resultObjects := []string{}
			for {
				select {
				case kv := <-chacheUpdatedChannel:
					key := kv.Key.(string)
					value := kv.Value.([]byte)
					//fmt.Println("DCRA: " + key + " " + fmt.Sprintln(value))
					if len(value) <= 1 { // Result can be: 0x0 - one byte when pending is in progress, [] - empty array (2 bytes), ["a", "b", ...] - non empty array (more than 2 bytes)
						if _, ok := pendingMap[key]; !ok {
							pendingMap[key] = true
						}
					} else {
						pendingMap[key] = false
						if v, ok := easyjson.JSONFromBytes(value); ok && v.IsNonEmptyArray() {
							if resultArray, ok2 := v.AsArrayString(); ok2 {
								resultObjects = append(resultObjects, resultArray...)
							}
						}

						pendingDone := true
						for _, v := range pendingMap {
							if v {
								pendingDone = false
							}
						}

						if pendingDone {
							//fmt.Println("--!! Returning result (all pending done):")
							for k := range pendingMap {
								//fmt.Println("--!! " + k)
								contextProcessor.GlobalCache.DeleteValue(k, true, -1, "")
							}
							contextProcessor.GlobalCache.UnsubscribeLevelCallback(fmt.Sprintf("%s.%s.pending.%s", modifiedTypename, aggregationID, "*"), aggregationID)

							resultMap := easyjson.NewJSONObject()
							for _, resObj := range resultObjects {
								resultMap.SetByPath(resObj, easyjson.NewJSON(true))
							}
							result := easyjson.NewJSONObject()
							result.SetByPath("status", easyjson.NewJSON("ok"))
							result.SetByPath("result", resultMap)
							common.ReplyQueryID(queryID, &result, contextProcessor)

							return
						}
					}
				case <-time.After(1 * time.Second):
					if startedEvaluating+int64(jpgqlEvaluationTimeoutSec)*int64(time.Second) < sfSystem.GetCurrentTimeNs() {
						contextProcessor.GlobalCache.UnsubscribeLevelCallback(fmt.Sprintf("%s.%s.pending.%s", modifiedTypename, aggregationID, "*"), aggregationID)

						errorString := "LLAPIQueryJPGQLDirectCacheResultAggregation evaluation timeout!"
						fmt.Println(errorString)

						result := easyjson.NewJSONObject()
						result.SetByPath("status", easyjson.NewJSON("failed"))
						result.SetByPath("result", easyjson.NewJSON(errorString))
						common.ReplyQueryID(queryID, &result, contextProcessor)
						return
					}
				}
			}
		}(chacheUpdatedChannel)

		if initPendingProcess(contextProcessor.Self.ID, currentQuery, aggregationID) {
			nextPayload := easyjson.NewJSONObject()
			nextPayload.SetByPath("aggregation_id", easyjson.NewJSON(aggregationID))
			nextPayload.SetByPath("jpgql_query", easyjson.NewJSON(currentQuery))
			if call != nil {
				nextPayload.SetByPath("call", *call)
			}
			contextProcessor.Call(contextProcessor.Self.Typename, contextProcessor.Self.ID+"==="+sfSystem.GetUniqueStrID(), &nextPayload, nil)
		}
	} else {
		idTokens := strings.Split(contextProcessor.Self.ID, "===")
		var thisObjectID string = idTokens[0]
		//var originalQueryId string = idTokens[1]

		var aggregationID string
		if s, ok := payload.GetByPath("aggregation_id").AsString(); ok {
			aggregationID = s
		} else {
			fmt.Printf("ERROR LLAPIQueryJPGQLDirectCacheResultAggregation for descendant: aggregation_id is invalid, must be string\n")
			return
		}

		thisProcessID := sfSystem.GetHashStr(thisObjectID + "_" + currentQuery)

		thisPendingDone := func(foundObjects *[]string) bool {
			contextProcessor.GlobalCache.SetValue(fmt.Sprintf("%s.%s.pending.%s", modifiedTypename, aggregationID, thisProcessID), easyjson.JSONFromArray(*foundObjects).ToBytes(), true, -1, "")
			//fmt.Println("-----------> PENDING DONE " + thisObjectID + ": " + fmt.Sprintf("%s.%s.pending.%s", modifiedTypename, aggregationID, thisProcessID))
			return true
		}
		queryHeadLinkType, queryHeadFilter, queryTail, anyDepthStop, err := GetQueryHeadAndTailsParts(currentQuery)
		if err != nil {
			fmt.Printf("ERROR LLAPIQueryJPGQLDirectCacheResultAggregation: currentQuery is invalid: %s\n", err)
			return
		}
		resultObjects := GetObjectIDsFromLinkTypeAndLinkFilterQueryWithAnyDepthStop(contextProcessor.GlobalCache, thisObjectID, queryHeadLinkType, queryHeadFilter, anyDepthStop)

		foundObjects := []string{}
		if len(resultObjects) > 0 { // There are objects to pass tail query to - store result objects in aggregation array
			for objectID, anyDepthStopped := range resultObjects {
				nextQuery := queryTail
				if anyDepthStopped == 1 {
					nextQuery = anyDepthStop.QueryTail
				}
				if len(nextQuery) == 0 { // jpgql_query ended!!!!
					if call != nil {
						if typename, ok := call.GetByPath("typename").AsString(); ok {
							if callPayload := call.GetByPath("payload"); callPayload.IsObject() {
								contextProcessor.Call(typename, objectID, &callPayload, nil)
							} else {
								fmt.Printf("ERROR LLAPIQueryJPGQLDirectCacheResultAggregation cannot make call on target %s: call payload is not a JSON object\n", objectID)
							}
						} else {
							fmt.Printf("ERROR LLAPIQueryJPGQLDirectCacheResultAggregation cannot make call on target %s: call typename is not a string\n", objectID)
						}
					}
					//fmt.Println("RESULT " + objectID)
					foundObjects = append(foundObjects, objectID)
				} else {
					if initPendingProcess(objectID, nextQuery, aggregationID) {
						//fmt.Println("Going to call " + objectID)
						nextPayload := easyjson.NewJSONObject()
						nextPayload.SetByPath("aggregation_id", easyjson.NewJSON(aggregationID))
						nextPayload.SetByPath("jpgql_query", easyjson.NewJSON(nextQuery))
						if call != nil {
							nextPayload.SetByPath("call", *call)
						}
						contextProcessor.Call(contextProcessor.Self.Typename, objectID+"==="+sfSystem.GetUniqueStrID(), &nextPayload, nil)
					}
				}
			}
		}

		thisPendingDone(&foundObjects)
	}
}

func RegisterAllFunctionTypes(runtime *statefun.Runtime, jpgqlEvaluationTimeoutSec int) {
	options := easyjson.NewJSONObjectWithKeyValue("eval_timeout_sec", easyjson.NewJSON(jpgqlEvaluationTimeoutSec))
	statefun.NewFunctionType(runtime, "functions.graph.ll.api.query.jpgql.ctra", LLAPIQueryJPGQLCallTreeResultAggregation, *statefun.NewFunctionTypeConfig().SetOptions(&options))
	statefun.NewFunctionType(runtime, "functions.graph.ll.api.query.jpgql.dcra", LLAPIQueryJPGQLDirectCacheResultAggregation, *statefun.NewFunctionTypeConfig().SetOptions(&options))
}
