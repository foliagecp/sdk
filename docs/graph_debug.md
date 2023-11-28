# Graph Debug Print

<img align="center" width="1024" src="./pics/GraphDebug.png">

## Description

Print Graph from certain vertex using Graphviz

### Request:
```json
payload: {
	"ext": "dot" | "png" | "svg", // set the output graph file extension, optional, default: "dot"
	"verbose": true | false, // add inbound links, optional, default: false
	"depth": uint // depth of graph, optional, default: 256
}
```

## Get Started

1. Import the following package:
```go
    import graphDebug "github.com/foliagecp/sdk/embedded/graph/debug"
```

2. Register debug functions to your Foliage Runtime
```go
    runtime, err := statefun.NewRuntime(runtimeCfg)
    if err != nil {...}

    graphDebug.RegisterAllFunctionTypes(runtime)
```

3. Send [**request**](#request) to NATS topic.

**In this case, we want to print graph from "Service" to depth 2**

**Change the identifier of the object from which you want to print the graph**

```sh
nats pub -s nats://nats:foliage@nats:4222 functions.graph.api.object.debug.print.graph.root {\"payload\":{\"depth\":2}}
```
**The result of this function will be the file "graph.dot", which is shown on the first picture**

4. By default, ext="dot", so you can copy the content of "graph.dot" file and paste into sites to view the graph
 - https://dreampuf.github.io/GraphvizOnline/
 - https://edotor.net/

5. To print entire graph use:
```sh
nats pub -s nats://nats:foliage@nats:4222 functions.graph.api.object.debug.print.graph.root {}
```