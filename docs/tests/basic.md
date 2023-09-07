# Basic test sample
The basic test sample provides a single stateful function "`functions.tests.basic.master`".

The documentation about the basic test sample can be found [here.](https://pkg.go.dev/github.com/foliagecp/sdk/tests/basic/)

## Default customization
Basic customization is availble via the corresponding `.env` file. For detailed description of variables go [here](https://pkg.go.dev/github.com/foliagecp/sdk/tests/basic/#pkg-variables)

### Call a stateful function
#### 1. Start whatching logs stateful function does:
```sh
docker logs tests-runtime-1 --tail 100 -f
```
#### 2. Get into nats-io container:
```sh
docker exec -it tests-io-1 sh
```
#### 3. Send message:
```sh
nats pub --count=1 -s nats://nats:foliage@nats:4222 functions.tests.basic.master.id "{\"payload\":{\"foo\":\"bar\"}, \"options\":{\"increment\":10}}"
```

## Advanced customization

### Create your own stateful function
#### 1. In main.go `RegisterFunctionTypes` function add the following code to register a new typename function:
```go
// Create new typename function "functions.tests.basic.custom" each stateful instance of which uses go function "MasterFunction"
ft := statefun.NewFunctionType(runtime, "functions.tests.basic.custom", MasterFunction, statefun.NewFunctionTypeConfig())
// Add TypenameExecutorPlugin which will provide StatefunExecutor for each stateful instance for this typename function (skip this if TypenameExecutorPlugin is not needed)
if content, err := os.ReadFile("./custom_function_plugin.js"); err == nil {
    // Assign JavaScript StatefunExecutor for TypenameExecutorPlugin
    ft.SetExecutor(jsFileName, string(content), sfPluginJS.StatefunExecutorPluginJSContructor)
}
```
Do not forget to create `custom_function_plugin.js` if planning to use StatefunExecutorPlugin

#### 2. Create go function "CustomFunction" to be used by your typename function
It should corresponds the following type
```go
type FunctionHandler func(sfPlugins.StatefunExecutor, *sfPlugins.StatefunContextProcessor)
```
For example:
```go
func CustomFunction(executor sfPlugins.StatefunExecutor, contextProcessor *sfPlugins.StatefunContextProcessor) {
    contextProcessor.Call("functions.tests.basic.master", contextProcessor.Self.ID, contextProcessor.Payload)
}
```

#### 3. Making "functions.tests.basic.custom" to call "functions.tests.basic.master"
At the end of the body of "CustomFunction" add the following code:
```
contextProcessor.Call("functions.tests.basic.master", contextProcessor.Self.ID, contextProcessor.Payload, contextProcessor.Options)
```

#### 4. Working with context within the function:
Use - [contextProcessor](https://pkg.go.dev/github.com/foliagecp/sdk/statefun/plugins/#StatefunContextProcessor) variable to call needed methods

> Do not forget to remove NATS stream from NATS server if new function typename was added!!!






