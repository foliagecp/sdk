// Copyright 2023 NJWS Inc.

/*
Functions available in this script 

// Get typename of the stateful function
statefun_getSelfTypename() -> string
// Get id of the stateful function
statefun_getSelfId() -> string
// Get the stateful function's caller typename
statefun_getCallerTypename() -> string
// Get the stateful function's caller id
statefun_getCallerId() -> string
// Get the stateful function's JSON context
statefun_getFunctionContext() -> string(json)
// Get JSON context of the stateful function's object
statefun_getObjectContext() -> string(json)
// Get the stateful function's JSON payload
statefun_getPayload() -> string(json)
// Get the stateful function's JSON options
statefun_getOptions() -> string(json)

// Set the stateful function's JSON context
statefun_setFunctionContext(<string of JSON>) -> int(status)
// Set the stateful function object's JSON context
statefun_setObjectContext(<string of JSON>) -> int(status)
// Set the stateful function's JSON request reply data
statefun_setRequestReplyData(<string of JSON>) -> int(status)

// Signal a stateful function by its typename and id
statefun_signal(<int of signal provider>, <string of typename>, <string of id>, <string with JSON payload>, <string with JSON options>) -> int(status)
// Synchronously call a stateful function by its typename and id (string)
statefun_request(<int of request provider>, <string of typename>, <string of id>, <string with JSON payload>, <string with JSON options>) -> string(json)|int(err status)
// Print arbitrary values
print(v1, v2, ...)
*/

var context = JSON.parse(statefun_getFunctionContext())
var options = JSON.parse(statefun_getOptions())
context.counter = context.counter + options.increment
print("++ Function context's counter value incrementated by JS by", options.increment)
var contextStr = JSON.stringify(context)
statefun_setFunctionContext(contextStr)

statefun_signal(0, "test.basic", "egress", contextStr, "") // Send function's context data to the egress/nats-topic "test.basic.egress"