# External API

Access to the foliage application API from third-party systems is represented by invoking the stateful foliage function with the possibility of receiving a response if provided. Requests and responses are passed in JSON format. There are 2 ways to invoke the function:
- **Signal** – asynchronous function call with the ability to receive a response (if provided by the function's code) to a predefined topic known as "egress".
- **Request** – synchronous function call with the ability to receive a response (if provided by the function's code) upon completion of the call.

## Function Description

Suppose in some foliage application, a function named `functions.app.api.test` is registered, which returns a response both for synchronous and asynchronous calls at the end of its execution:

```go
...

func apiTest(exec plugins.StatefunExecutor, ctx *plugins.StatefunContextProcessor) {
    var data *easyjson.JSON

    ...

    ctx.Reply.With(data) // Replies sync request
    ctx.Egress(data) // Replies async signal
}

...

if runtime, err := statefun.NewRuntime(*statefun.NewRuntimeConfigSimple(NatsURL, "test")); err == nil {

    ...

    statefun.NewFunctionType(
        runtime,
        "functions.app.api.test",
        apiTest,
        *statefun.NewFunctionTypeConfig().SetServiceState(true),
    )

    ...

}
```

Setting `SetServiceState(true)` when configuring the foliage function indicates its ability to handle synchronous requests, not just signals.

## Invocation and Response Retrieval

### Addresses
Signals and synchronous function calls are made by passing data to topics, whose format is determined as follows:

#### Signal
Topic format for transmitting a signal to an arbitrary function:  
```
signal.<function_typename>.<id>
```
For the function in the example above, this topic would appear as ```signal.functions.app.api.test.foo```, where `foo` is the object identifier relative to which the function operates.

#### Egress
Topic format to which the function will send its response (if provided by its logic) to the received signal:
```
egress.<function_typename>.<id>
```
For the function in the example above, this topic would appear as `egress.functions.app.api.test.foo`.

#### Request
Topic format for transmitting a signal to an arbitrary function:
```
request.<function_typename>.<id>
```
For the function in the example above, this topic would appear as `request.functions.app.api.test.foo`.

### Data Payload
To invoke such a function using either of the two methods (as with any other foliage function), JSON data of the following format must be passed to it:
```json
{
    "payload": { // Optional or required depending on function's behavior
        ... // The fields are defined by the developer of the function
    },
    "options": { // Optional
        ... // The fields are defined by the developer of the function
    }
}
```

As a response (if provided), JSON will also be returned, the format of which also depends entirely on the invoked function.

## Function Invocation Methods
Let's consider ways to invoke the function through the `nats` Command Line Tool utility.

**Signal**
1. Subscribe to the topic to await a response:
```shell
nats -s nats://nats:4222 sub egress.functions.app.api.test.foo
```
2. Send a signal to the function:
```shell
nats -s nats://nats:4222 pub signal.functions.app.api.test.foo '{"payload":{...}}'
```

**Request**
1. Synchronous function call:
```shell
nats -s nats://nats:4222 req request.functions.app.api.test.foo '{"payload":{...}}'
```

### WebSocket
Detailed implementation of the aforementioned function invocation methods, but already based on WebSocket, is outlined in the following links:
- [NATS Protocol Reference](https://docs.nats.io/reference/reference-protocols/nats-protocol)
- [nats.ws GitHub Repository](https://github.com/nats-io/nats.ws)
- [Sending Request/Reply with NATS](https://docs.nats.io/using-nats/developer/sending/request_reply)