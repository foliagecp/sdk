# JavaScript stateful function plugin
This plugin allows a stateful function to use javascript-defined logic based on v8 engine embedded into golang runtime.

### JavaScript predefined functions
Predefined functions in JavaScript code that provide access to a stateful function's context are the following:

```json
statefun_getSelfTypename() // gets typename of function (string)
statefun_getSelfId() // gets id of function (string)
statefun_getCallerTypename() // gets caller typename of function (string)
statefun_getCallerId() // gets caller id of function (string)
statefun_getFunctionContext() // gets JSON context of function (string)
statefun_setFunctionContext(<string of JSON>) // set JSON context of function (string)
statefun_getObjectContext() // gets JSON context of object (string)
statefun_setObjectContext(<string of JSON>) // set JSON context of object (string)
statefun_getPayload() // gets JSON payload of function (string)
statefun_getOptions() // gets JSON options of function (string)
statefun_call(<string of typename>, <string of id>, <string with JSON payload>, <string with JSON options>) // calls stateful function by typename and id
statefun_egress(<nats topic>, <string with JSON payload>) // sends payload data to an egress
print(v1, v2, ...) // prints argument values
```