# Graph Debug Print

<img align="center" width="1024" src="./pics/GraphDebug.png">

## Description

Export graph from certain vertex using dot or graphml format 

### Request:
```json
payload: {
	"format": "dot" | "graphml", // required
	"depth": uint, // optional, depth of graph, optional, default: -1 - infinity
    "json2xml": bool, // optional, if graphml format is chosen – whether to store vertex and link bodies in JSON or XML.
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
nats pub -s nats://nats:foliage@nats:4222 signal.hub.functions.graph.api.object.debug.print.graph.root {\"payload\":{\"depth\":\"dot\",\"depth\":2}}
```
**The result of this function will be the file "graph.dot", which is shown on the first picture**

4. By default, ext="dot", so you can copy the content of "graph.dot" file and paste into sites to view the graph
 - https://dreampuf.github.io/GraphvizOnline/
 - https://edotor.net/

5. To print entire graph use:
```sh
nats pub -s nats://nats:foliage@nats:4222 signal.hub.functions.graph.api.object.debug.print.graph.root {\"payload\":{\"depth\":\"dot\"}}
```