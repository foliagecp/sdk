# Customize existing
## Create your first foliage stateful function
### 1. In main.go `RegisterFunctionTypes` function add the following code to register a new typename function:
```go
// Create new typename function "functions.app1.my.typename" each stateful instance of which uses go function "myTypeFunction"
myft := NewFunctionType(js, "functions.app1.my.typename", myTypeFunction, true)
// Add TypenameExecutorPlugin which will provide StatefunExecutor for each stateful instance for this typename function (skip this if TypenameExecutorPlugin is not needed)
if content, err := ioutil.ReadFile("./my_type_function_plugin.js"); err == nil {
    // Assign JavaScript StatefunExecutor for TypenameExecutorPlugin
    myft.executor = sfplugins.NewTypenameExecutor(string(content), sfplugins.StatefunExecutorPluginJSContructor)
}
// Register created typename function
RegisterFunctionType(myft)
```
Do not forget to create `my_type_function_plugin.js` if planning to use StatefunExecutorPlugin

### 2. Create go function "myTypeFunction" to be used by your typename function
It should correspond the following type
```go
type FunctionHandler func(*sfplugins.StatefunExecutor, sfplugins.StatefunContextProcessor)
```
For example:
```go
func myTypeFunction(executor *sfplugins.StatefunExecutor, contextProcessor sfplugins.StatefunContextProcessor) {
    contextProcessor.Call("functions.app1.json.master", contextProcessor.Self.ID, contextProcessor.Payload)
}
```

## 3. Making "functions.app1.my.typename" to call "functions.app1.json.master"
At the end of the body of "myTypeFunction" add the following code:
```
contextProcessor.Call("functions.app1.json.master", contextProcessor.Self.ID, contextProcessor.Payload)
```

### 4. Working with context within the function:
Use `contextProcessor` variable to call needed methods

Do not forget to remove NATS stream from NATS if new typename was added!!!

## Make stateful function to use JavaScript (v8)
The master function `functions.app1.json.master` shows how JavaScript code can be used for defining stateful function's logic. To enable the usage of the JavaScript in master function in main.go set the enableJS flag as follows:
```go
var enableJS bool = true
```
This will activate `master_function_test.js`'s code:
```js
var context = JSON.parse(statefun_getFunctionContext())
context.counter = context.counter + 1
statefun_setFunctionContext(JSON.stringify(context))
```
Making it to run on each call of the `functions.app1.json.master` and causing the value of the `counter` key of the function's context to increase faster by one – `counter`'s value will be increased by 2 to each call.

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
statefun_call(<nats topic>, <string with JSON payload>) // sends payload data to an egress
print(v1, v2, ...) // prints argument values
```

When you are done rebuild and test function again going through clauses 1 and 2 of this README

# Write your own application
Do the following steps to write your own application:
1. Define objects the designed application is working with  
2. Put objects in the graph database
    - Create directly via the designed application (for e.g. once at the bootstrap)
    - Import via another foliage application (adapter)
3. Create all foliage stateful functions the designed application consists from
4. Organize them through the asynchronous calls (signals) handled by NATS in a the desired way
    - Use statefun's context to store data between the calls
    - Use object's context

## Example of a test application for json template based WebUI
https://github.com/foliagecp/foliage-nats-test-statefun/blob/fix/ui-stub/ui_client.go


