# JavaScript stateful function plugin
This plugin allows a stateful function to use javascript-defined logic based on v8 engine embedded into golang runtime.

### JavaScript predefined functions
Predefined functions in JavaScript code that provide access to a stateful function's context are the following:

```json
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
```