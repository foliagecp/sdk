// Copyright 2023 NJWS Inc.

package jpgql

import (
	"fmt"
	"json_easy"
	"strings"
	"time"

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
		queryId := common.GetQueryId(contextProcessor)

		processId := sfSystem.GetUniqueStrId()
		payload.SetByPath("caller_aggregation_id", json_easy.NewJSON(processId))
		payload.SetByPath("query_id", json_easy.NewJSON(queryId))
		contextProcessor.Call(contextProcessor.Self.Typename, contextProcessor.Self.ID+"==="+processId, payload, nil)

		keyBase := fmt.Sprintf("jpgql_ctra.%s.%s", contextProcessor.Self.ID, processId)

		chacheUpdatedChannel := contextProcessor.GlobalCache.SubscribeLevelCallback(keyBase+".*", processId)
		go func(chacheUpdatedChannel chan cache.KeyValue) {
			startedEvaluating := sfSystem.GetCurrentTimeNs()
			for true {
				select {
				case kv := <-chacheUpdatedChannel:
					//fmt.Println("____________ UPDATE FROM CACHE!!!!")
					key := kv.Key.(string)
					value := kv.Value.([]byte)
					if key == "result" {
						if result, ok := json_easy.JSONFromBytes(value); ok {
							contextProcessor.GlobalCache.UnsubscribeLevelCallback(keyBase+".*", processId)
							common.ReplyQueryId(queryId, &result, contextProcessor)
							return
						}
					}
				case <-time.After(1 * time.Second):
					if startedEvaluating+int64(jpgqlEvaluationTimeoutSec)*int64(time.Second) < sfSystem.GetCurrentTimeNs() {
						contextProcessor.GlobalCache.UnsubscribeLevelCallback(keyBase+".*", processId)

						//fmt.Println(processId + "::: " + "LLAPIQueryJPGQLCallTreeResultAggregation evaluation timeout!")
						errorString := "LLAPIQueryJPGQLCallTreeResultAggregation evaluation timeout!"
						fmt.Println(errorString)

						result := json_easy.NewJSONObject()
						result.SetByPath("status", json_easy.NewJSON("failed"))
						result.SetByPath("result", json_easy.NewJSON(errorString))
						common.ReplyQueryId(queryId, &result, contextProcessor)
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
		var thisObjectId string = idTokens[0]
		var processId string = idTokens[1]

		var queryId string
		if s, ok := payload.GetByPath("query_id").AsString(); ok {
			queryId = s
		} else {
			fmt.Printf("ERROR LLAPIQueryJPGQLCallTreeResultAggregation: this function was called by another LLAPIQueryJPGQLCallTreeResultAggregation - \"query_id\" must exist\n")
			return
		}

		//fmt.Println(processId+"::: "+thisObjectId+" | Context:", context.ToString())

		getState := func() (int, error) { // 0 - query from parent, 1 - aggregate from child
			if payload.PathExists("jpgql_query") {
				if payload.PathExists("result") {
					return -1, fmt.Errorf("ERROR LLAPIQueryJPGQLCallTreeResultAggregation: invalid request: \"jpgql_query\" and \"result\" cannot be presented simultaneously\n")
				}
				return 0, nil
			}
			if payload.PathExists("aggregation_id") && payload.PathExists("result") {
				if payload.PathExists("jpgql_query") {
					return -1, fmt.Errorf("ERROR LLAPIQueryJPGQLCallTreeResultAggregation: invalid request: \"jpgql_query\" and \"result\" cannot be presented simultaneously\n")
				}
				return 1, nil
			}
			return -1, fmt.Errorf(`ERROR LLAPIQueryJPGQLCallTreeResultAggregation: invalid request: either "jpgql_query" or "result"+"aggregation_id" must exist in payload\n`)
		}

		getQuery := func() (string, error) {
			if jpQuery, ok := payload.GetByPath("jpgql_query").AsString(); !ok || len(jpQuery) == 0 {
				return "", fmt.Errorf("ERROR LLAPIQueryJPGQLCallTreeResultAggregation: \"jpgql_query\" must be a string with len>0\n")
			} else {
				return jpQuery, nil
			}
		}

		registerAggregationId := func(query string) (string, bool) { // returns: query_id for descendants and aggreagtion, ok
			aggregationId := sfSystem.GetHashStr(queryId + "_" + query)
			if context.PathExists(aggregationId + "_result") {
				return aggregationId, false
			} else {
				callerAggregationId := ""
				if aggregationId, ok := payload.GetByPath("caller_aggregation_id").AsString(); ok {
					callerAggregationId = aggregationId
				}
				context.SetByPath(aggregationId+"_result", json_easy.NewJSONObject())
				context.SetByPath(aggregationId+"_callbacks", json_easy.NewJSON(0)) // Stores counter of callbacks from descendants
				context.SetByPath(aggregationId+"_reply_object_id", json_easy.NewJSON(contextProcessor.Caller.ID))
				context.SetByPath(aggregationId+"_caller_aggregation_id", json_easy.NewJSON(callerAggregationId))
				if call := payload.GetByPath("call"); call.IsObject() {
					context.SetByPath(aggregationId+"_call", call)
				}
				return aggregationId, true
			}
		}

		unregisterAggregationQueryId := func(aggregationId string) {
			context.RemoveByPath(aggregationId + "_result")                // Release this object from this query from specific parent
			context.RemoveByPath(aggregationId + "_callbacks")             // Release this object from this query from specific parent
			context.RemoveByPath(aggregationId + "_reply_object_id")       // Release this object from this query from specific parent
			context.RemoveByPath(aggregationId + "_caller_aggregation_id") // Release this object from this query from specific parent
			context.RemoveByPath(aggregationId + "_call")                  // Release this object from this query from specific parent
		}

		replyCaller := func(thisFunctionAggregationId string, replyPayload *json_easy.JSON) error {
			//fmt.Println("----------->>> PRERESULT ")
			replyObjectId, ok := context.GetByPath(thisFunctionAggregationId + "_reply_object_id").AsString()
			if !ok {
				return fmt.Errorf("ERROR LLAPIQueryJPGQLCallTreeResultAggregation: no valid reply_object_id\n")
			}
			if len(replyObjectId) > 0 && strings.Count(replyObjectId, "===") == 1 {
				contextProcessor.Call(contextProcessor.Self.Typename, replyObjectId, replyPayload, nil)
			} else {
				if context.PathExists(thisFunctionAggregationId + "_call") {
					if call := context.GetByPath(thisFunctionAggregationId + "_call"); call.IsObject() {
						if typename, ok := call.GetByPath("typename").AsString(); ok {
							if callPayload := call.GetByPath("payload"); callPayload.IsObject() {
								if resultObjectsMap, ok := replyPayload.GetByPath("result").AsObject(); ok {
									for objectId := range resultObjectsMap {
										contextProcessor.Call(typename, objectId, &callPayload, nil)
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
				result := json_easy.NewJSONObject()
				result.SetByPath("status", json_easy.NewJSON("ok"))
				result.SetByPath("result", replyPayload.GetByPath("result"))

				callerAggregationId, _ := replyPayload.GetByPath("aggregation_id").AsString()
				//fmt.Println("----------->>> RESULT " + result.ToString())
				contextProcessor.GlobalCache.SetValue(fmt.Sprintf("jpgql_ctra.%s.%s.result", thisObjectId, callerAggregationId), result.ToBytes(), false, -1, "")
			}
			unregisterAggregationQueryId(thisFunctionAggregationId)
			return nil
		}

		replyCallerPreventSameQueryCall := func() error {
			if callerAggregationId, ok := payload.GetByPath("caller_aggregation_id").AsString(); ok {
				if strings.Count(contextProcessor.Caller.ID, "===") == 1 {
					replyPayload := json_easy.NewJSONObject()
					replyPayload.SetByPath("query_id", json_easy.NewJSON(queryId))
					replyPayload.SetByPath("aggregation_id", json_easy.NewJSON(callerAggregationId))
					replyPayload.SetByPath("result", json_easy.NewJSONObject())
					contextProcessor.Call(contextProcessor.Self.Typename, contextProcessor.Caller.ID, &replyPayload, nil)
				}
				return nil
			}
			return fmt.Errorf("ERROR replyCallerLoopPrevent: callerAggregationId does not exist for object_id=%s", thisObjectId)
		}

		state, err := getState()
		if err != nil { // Cannot get current state
			fmt.Printf(err.Error())
			return
		}

		if state == 0 {
			//fmt.Println(processId+"::: 0 "+thisObjectId+" | Context:", context.ToString())
			//fmt.Println(processId + ":0:: " + "(" + thisObjectId + ") " + "1")
			currentObjectLinksQuery, err := getQuery()
			if err != nil {
				fmt.Printf(err.Error())
				return
			}

			thisFunctionAggregationId, uniqueParentAndQuery := registerAggregationId(currentObjectLinksQuery)
			//fmt.Println(processId + ":0:: " + "(" + thisObjectId + ") " + "2")
			callerAggregationId, ok := context.GetByPath(thisFunctionAggregationId + "_caller_aggregation_id").AsString()
			if !ok {
				fmt.Printf("ERROR LLAPIQueryJPGQLCallTreeResultAggregation: no valid caller_aggregation_id on state=0 for object_id=%s\n", thisObjectId)
				return
			}
			//fmt.Println(processId + ":0:: " + "(" + thisObjectId + ") " + "3")
			if !(uniqueParentAndQuery) { // This query from that parent was already registered
				if err := replyCallerPreventSameQueryCall(); err != nil {
					fmt.Printf(err.Error())
					return
				}
			} else {
				//fmt.Println(processId + ":0:: " + "(" + thisObjectId + ") " + "4")
				queryHeadLinkType, queryHeadFilter, queryTail, anyDepthStop, err := GetQueryHeadAndTailsParts(currentObjectLinksQuery)
				if err != nil {
					fmt.Printf("ERROR LLAPIQueryJPGQLCallTreeResultAggregation: currentObjectLinksQuery is invalid: %s\n", err)
					return
				}
				//fmt.Println(processId + ":0:: " + "(" + thisObjectId + ") " + "5")
				resultObjects := GetObjectIDsFromLinkTypeAndLinkFilterQueryWithAnyDepthStop(contextProcessor.GlobalCache, thisObjectId, queryHeadLinkType, queryHeadFilter, anyDepthStop)
				//fmt.Println("======== RESULT OBJECTS: " + fmt.Sprintln(resultObjects))

				//fmt.Println(processId + ":0:: " + "(" + thisObjectId + ") " + "6")
				if len(resultObjects) == 0 { // If no links found (no matter if queryTail exists) - return result empty objects array immediately without aggregation
					replyPayload := json_easy.NewJSONObject()
					replyPayload.SetByPath("query_id", json_easy.NewJSON(queryId))
					replyPayload.SetByPath("aggregation_id", json_easy.NewJSON(callerAggregationId))
					replyPayload.SetByPath("result", json_easy.NewJSONObject())
					//fmt.Println(processId + ":0:: " + "(" + thisObjectId + ") " + "7")
					if err := replyCaller(thisFunctionAggregationId, &replyPayload); err != nil {
						fmt.Printf(err.Error())
						return
					}
					//fmt.Println(processId+"::: 0:0 "+thisObjectId+" | Context:", context.ToString())
				} else { // There are objects to pass tail query to - store result objects in aggregation array
					//fmt.Println(processId + ":0:: " + "(" + thisObjectId + ") " + "8")
					objectsToReturnAsAResult := map[string]bool{}
					nextCalls := 0
					for objectId, anyDepthStopped := range resultObjects {
						nextQuery := queryTail
						if anyDepthStopped == 1 {
							nextQuery = anyDepthStop.QueryTail
						}
						if len(nextQuery) == 0 { // jpgql_query ended!!!!
							objectsToReturnAsAResult[objectId] = true
						} else {
							nextPayload := json_easy.NewJSONObject()
							nextPayload.SetByPath("query_id", json_easy.NewJSON(queryId))
							nextPayload.SetByPath("caller_aggregation_id", json_easy.NewJSON(thisFunctionAggregationId))
							nextPayload.SetByPath("jpgql_query", json_easy.NewJSON(nextQuery))
							//fmt.Println(processId+"::: 0:0.1 "+thisObjectId+" | CHILD:", objectId)
							contextProcessor.Call(contextProcessor.Self.Typename, objectId+"==="+processId, &nextPayload, nil)
							nextCalls++
						}
					}
					//fmt.Println(processId + ":0:: " + "(" + thisObjectId + ") " + "9")
					immediateAggregationResult := json_easy.NewJSON(objectsToReturnAsAResult)
					context.SetByPath(thisFunctionAggregationId+"_result", immediateAggregationResult)
					if nextCalls == 0 { // All descendant objects on links are result ones
						replyPayload := json_easy.NewJSONObject()
						replyPayload.SetByPath("query_id", json_easy.NewJSON(queryId))
						replyPayload.SetByPath("aggregation_id", json_easy.NewJSON(callerAggregationId))
						replyPayload.SetByPath("result", immediateAggregationResult)
						if err := replyCaller(thisFunctionAggregationId, &replyPayload); err != nil {
							fmt.Printf(err.Error())
							return
						}
						//fmt.Println(processId+"::: 0:1 "+thisObjectId+" | Context:", context.ToString())
					} else { // There are descendants to aggregate result from
						context.SetByPath(thisFunctionAggregationId+"_callbacks", json_easy.NewJSON(nextCalls)) // Store counter of callbacks from descendants
						//fmt.Println(processId+"::: 0:2 "+thisObjectId+" | Context:", context.ToString())
					}
					//fmt.Println(processId + ":0:: " + "(" + thisObjectId + ") " + "10")
				}
			}
		} else { // Aggregation state - got call from descendant to aggregate its result
			//fmt.Println(processId+"::: 1 "+thisObjectId+" | Context:", context.ToString())
			//fmt.Println(processId + ":1:: " + "(" + thisObjectId + ") " + "11")
			thisFunctionAggregationId, ok := payload.GetByPath("aggregation_id").AsString()
			if !ok {
				fmt.Printf("ERROR LLAPIQueryJPGQLCallTreeResultAggregation: \"aggregationId\" must be a string\n")
				return
			}
			result, ok := payload.GetByPath("result").AsObject()
			if !ok {
				fmt.Printf("ERROR LLAPIQueryJPGQLCallTreeResultAggregation: \"result\" must be a string array\n")
				return
			}
			callbacksFloat, ok := context.GetByPath(thisFunctionAggregationId + "_callbacks").AsNumeric()
			if !ok || callbacksFloat < 0 {
				fmt.Printf("ERROR LLAPIQueryJPGQLCallTreeResultAggregation: no valid callbacks counter for result aggregation for object_id=%s\n", thisObjectId)
				return
			}
			callbacks := int(callbacksFloat)

			//fmt.Println(processId + ":1:: " + "(" + thisObjectId + ") " + "12")
			callbacks--
			totalResult, _ := context.GetByPath(thisFunctionAggregationId + "_result").AsObject()
			totalResult = sfSystem.MergeMaps(totalResult, result)
			context.SetByPath(thisFunctionAggregationId+"_result", json_easy.NewJSON(totalResult))
			context.SetByPath(thisFunctionAggregationId+"_callbacks", json_easy.NewJSON(callbacks))

			//fmt.Println(processId + ":1:: " + "(" + thisObjectId + ") " + "13: ")

			if callbacks == 0 { // Aggregated from all descendants
				//fmt.Println(processId+"::: 1:0 "+thisObjectId+" | Context:", context.ToString())
				callerAggregationId, ok := context.GetByPath(thisFunctionAggregationId + "_caller_aggregation_id").AsString()
				if !ok {
					fmt.Printf("ERROR LLAPIQueryJPGQLCallTreeResultAggregation: no valid caller_aggregation_id on state=1 for object_id=%s\n", thisObjectId)
					return
				}

				replyPayload := json_easy.NewJSONObject()
				replyPayload.SetByPath("query_id", json_easy.NewJSON(queryId))
				replyPayload.SetByPath("aggregation_id", json_easy.NewJSON(callerAggregationId))
				replyPayload.SetByPath("result", json_easy.NewJSON(totalResult))
				//fmt.Println(processId + ":1:: " + "(" + thisObjectId + ") " + "13.1")
				if err := replyCaller(thisFunctionAggregationId, &replyPayload); err != nil {
					fmt.Printf(err.Error())
					return
				}
			}
			//fmt.Println(processId + ":1:: " + "(" + thisObjectId + ") " + "14")
		}
		//fmt.Println(processId+"::: UC "+thisObjectId+" | Context:", context.ToString())
		contextProcessor.SetFunctionContext(context)
		//fmt.Println(processId + "::: UCCCCCCCCCCCCCC " + thisObjectId)
		//ttt := contextProcessor.GetFunctionContext()
		//fmt.Println(processId+"::: UCCCCCCCCCC TTTTTTTTTTTTT "+thisObjectId+" | Context:", ttt.ToString())
		//fmt.Println(processId + "::: " + "(" + thisObjectId + ") " + "16")
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
	var call *json_easy.JSON = nil
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

	initPendingProcess := func(objectId string, objectQuery string, aggregationId string) bool {
		//fmt.Println("initPendingProcess 1", objectId)
		pendingProcessId := sfSystem.GetHashStr(objectId + "_" + objectQuery)
		//fmt.Println("initPendingProcess 2", objectId)

		return contextProcessor.GlobalCache.SetValueIfDoesNotExist(fmt.Sprintf("%s.%s.pending.%s", modifiedTypename, aggregationId, pendingProcessId), []byte{1}, true, -1)
	}

	if rootProcess {
		queryId := common.GetQueryId(contextProcessor)

		aggregationId := sfSystem.GetUniqueStrId()
		chacheUpdatedChannel := contextProcessor.GlobalCache.SubscribeLevelCallback(fmt.Sprintf("%s.%s.pending.%s", modifiedTypename, aggregationId, "*"), aggregationId)

		go func(chacheUpdatedChannel chan cache.KeyValue) {
			startedEvaluating := sfSystem.GetCurrentTimeNs()
			pendingMap := map[string]bool{}
			resultObjects := []string{}
			for true {
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
						if v, ok := json_easy.JSONFromBytes(value); ok && v.IsNonEmptyArray() {
							if resultArray, ok2 := v.AsArrayString(); ok2 {
								for _, r := range resultArray {
									resultObjects = append(resultObjects, r)
								}
							}
						}

						pendingDone := true
						for _, v := range pendingMap {
							if v == true {
								pendingDone = false
							}
						}

						if pendingDone {
							//fmt.Println("--!! Returning result (all pending done):")
							for k := range pendingMap {
								//fmt.Println("--!! " + k)
								contextProcessor.GlobalCache.DeleteValue(k, true, -1, "")
							}
							contextProcessor.GlobalCache.UnsubscribeLevelCallback(fmt.Sprintf("%s.%s.pending.%s", modifiedTypename, aggregationId, "*"), aggregationId)

							resultMap := json_easy.NewJSONObject()
							for _, resObj := range resultObjects {
								resultMap.SetByPath(resObj, json_easy.NewJSON(true))
							}
							result := json_easy.NewJSONObject()
							result.SetByPath("status", json_easy.NewJSON("ok"))
							result.SetByPath("result", resultMap)
							common.ReplyQueryId(queryId, &result, contextProcessor)

							return
						}
					}
				case <-time.After(1 * time.Second):
					if startedEvaluating+int64(jpgqlEvaluationTimeoutSec)*int64(time.Second) < sfSystem.GetCurrentTimeNs() {
						contextProcessor.GlobalCache.UnsubscribeLevelCallback(fmt.Sprintf("%s.%s.pending.%s", modifiedTypename, aggregationId, "*"), aggregationId)

						errorString := "LLAPIQueryJPGQLDirectCacheResultAggregation evaluation timeout!"
						fmt.Println(errorString)

						result := json_easy.NewJSONObject()
						result.SetByPath("status", json_easy.NewJSON("failed"))
						result.SetByPath("result", json_easy.NewJSON(errorString))
						common.ReplyQueryId(queryId, &result, contextProcessor)
						return
					}
				}
			}
		}(chacheUpdatedChannel)

		if initPendingProcess(contextProcessor.Self.ID, currentQuery, aggregationId) {
			nextPayload := json_easy.NewJSONObject()
			nextPayload.SetByPath("aggregation_id", json_easy.NewJSON(aggregationId))
			nextPayload.SetByPath("jpgql_query", json_easy.NewJSON(currentQuery))
			if call != nil {
				nextPayload.SetByPath("call", *call)
			}
			contextProcessor.Call(contextProcessor.Self.Typename, contextProcessor.Self.ID+"==="+sfSystem.GetUniqueStrId(), &nextPayload, nil)
		}
	} else {
		idTokens := strings.Split(contextProcessor.Self.ID, "===")
		var thisObjectId string = idTokens[0]
		//var originalQueryId string = idTokens[1]

		var aggregationId string
		if s, ok := payload.GetByPath("aggregation_id").AsString(); ok {
			aggregationId = s
			rootProcess = false
		} else {
			fmt.Printf("ERROR LLAPIQueryJPGQLDirectCacheResultAggregation for descendant: aggregation_id is invalid, must be string\n")
			return
		}

		thisProcessId := sfSystem.GetHashStr(thisObjectId + "_" + currentQuery)

		thisPendingDone := func(foundObjects *[]string) bool {
			contextProcessor.GlobalCache.SetValue(fmt.Sprintf("%s.%s.pending.%s", modifiedTypename, aggregationId, thisProcessId), json_easy.JSONFromArray(*foundObjects).ToBytes(), true, -1, "")
			//fmt.Println("-----------> PENDING DONE " + thisObjectId + ": " + fmt.Sprintf("%s.%s.pending.%s", modifiedTypename, aggregationId, thisProcessId))
			return true
		}
		queryHeadLinkType, queryHeadFilter, queryTail, anyDepthStop, err := GetQueryHeadAndTailsParts(currentQuery)
		if err != nil {
			fmt.Printf("ERROR LLAPIQueryJPGQLDirectCacheResultAggregation: currentQuery is invalid: %s\n", err)
			return
		}
		resultObjects := GetObjectIDsFromLinkTypeAndLinkFilterQueryWithAnyDepthStop(contextProcessor.GlobalCache, thisObjectId, queryHeadLinkType, queryHeadFilter, anyDepthStop)

		foundObjects := []string{}
		if len(resultObjects) > 0 { // There are objects to pass tail query to - store result objects in aggregation array
			for objectId, anyDepthStopped := range resultObjects {
				nextQuery := queryTail
				if anyDepthStopped == 1 {
					nextQuery = anyDepthStop.QueryTail
				}
				if len(nextQuery) == 0 { // jpgql_query ended!!!!
					if call != nil {
						if typename, ok := call.GetByPath("typename").AsString(); ok {
							if callPayload := call.GetByPath("payload"); callPayload.IsObject() {
								contextProcessor.Call(typename, objectId, &callPayload, nil)
							} else {
								fmt.Printf("ERROR LLAPIQueryJPGQLDirectCacheResultAggregation cannot make call on target %s: call payload is not a JSON object\n", objectId)
							}
						} else {
							fmt.Printf("ERROR LLAPIQueryJPGQLDirectCacheResultAggregation cannot make call on target %s: call typename is not a string\n", objectId)
						}
					}
					//fmt.Println("RESULT " + objectId)
					foundObjects = append(foundObjects, objectId)
				} else {
					if initPendingProcess(objectId, nextQuery, aggregationId) {
						//fmt.Println("Going to call " + objectId)
						nextPayload := json_easy.NewJSONObject()
						nextPayload.SetByPath("aggregation_id", json_easy.NewJSON(aggregationId))
						nextPayload.SetByPath("jpgql_query", json_easy.NewJSON(nextQuery))
						if call != nil {
							nextPayload.SetByPath("call", *call)
						}
						contextProcessor.Call(contextProcessor.Self.Typename, objectId+"==="+sfSystem.GetUniqueStrId(), &nextPayload, nil)
					}
				}
			}
		}

		thisPendingDone(&foundObjects)
	}
}

func RegisterAllFunctionTypes(runtime *statefun.Runtime, jpgqlEvaluationTimeoutSec int) {
	options := json_easy.NewJSONObjectWithKeyValue("eval_timeout_sec", json_easy.NewJSON(jpgqlEvaluationTimeoutSec))
	statefun.NewFunctionType(runtime, "functions.graph.ll.api.query.jpgql.ctra", LLAPIQueryJPGQLCallTreeResultAggregation, statefun.NewFunctionTypeConfig().SetOptions(&options))
	statefun.NewFunctionType(runtime, "functions.graph.ll.api.query.jpgql.dcra", LLAPIQueryJPGQLDirectCacheResultAggregation, statefun.NewFunctionTypeConfig().SetOptions(&options))
}
