Here's the content with issues fixed and presented in a single Markdown file:

# Basic Test Sample

The basic test sample provides a single stateful function called "`functions.tests.basic.master`". The documentation about the basic test sample can be found [here](https://pkg.go.dev/github.com/foliagecp/sdk/tests/basic/).

## Default Customization

Basic customization is available via the corresponding `.env` file. For a detailed description of variables, refer [here](https://pkg.go.dev/github.com/foliagecp/sdk/tests/basic/#pkg-variables).

### Calling a Stateful Function

1. Start watching logs of the stateful function:

```sh
docker logs tests-runtime-1 --tail 100 -f
```

2. Get into the nats-io container:

```sh
docker exec -it tests-io-1 sh
```

3. Send a message:

```sh
nats pub --count=1 -s nats://nats:foliage@nats:4222 functions.tests.basic.master.id "{\"payload\":{\"foo\":\"bar\"}, \"options\":{\"increment\":10}}"
```

## Advanced Customization

### Creating Your Own Stateful Function

1. In `main.go`, in the `RegisterFunctionTypes` function, add the following code to register a new typename function:

```go
// Create a new typename function "functions.tests.basic.custom," each stateful instance of which uses the Go function "MasterFunction."
ft := statefun.NewFunctionType(runtime, "functions.tests.basic.custom", MasterFunction, statefun.NewFunctionTypeConfig())

// Add TypenameExecutorPlugin, which will provide StatefunExecutor for each stateful instance for this typename function (skip this if TypenameExecutorPlugin is not needed).
if content, err := os.ReadFile("./custom_function_plugin.js"); err == nil {
    // Assign JavaScript StatefunExecutor for TypenameExecutorPlugin.
    ft.SetExecutor(jsFileName, string(content), sfPluginJS.StatefunExecutorPluginJSConstructor)
}
```

Don't forget to create `custom_function_plugin.js` if you plan to use the StatefunExecutorPlugin.

2. Create a Go function "CustomFunction" to be used by your typename function. It should correspond to the following type:

```go
type FunctionLogicHandler func(sfPlugins.StatefunExecutor, *sfPlugins.StatefunContextProcessor)
```

For example:

```go
func CustomFunction(executor sfPlugins.StatefunExecutor, contextProcessor *sfPlugins.StatefunContextProcessor) {
    contextProcessor.Call("functions.tests.basic.master", contextProcessor.Self.ID, contextProcessor.Payload)
}
```

3. Making "functions.tests.basic.custom" Call "functions.tests.basic.master"

At the end of the body of "CustomFunction," add the following code:

```go
contextProcessor.Call("functions.tests.basic.master", contextProcessor.Self.ID, contextProcessor.Payload, contextProcessor.Options)
```

4. Working with Context Within the Function:

Use the [contextProcessor](https://pkg.go.dev/github.com/foliagecp/sdk/statefun/plugins/#StatefunContextProcessor) variable to call the needed methods.

> Do not forget to remove NATS stream from NATS server if a new function typename was added!!!