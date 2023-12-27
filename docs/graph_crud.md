# Graph CRUD

1. Subscribe and listen for result: 
```sh
nats sub -s nats://nats:foliage@nats:4222 functions.graph.query.QUERYID
```
2. Call one of the [functions](#functions)
3. To log object with all its links use: `functions.graph.api.object.debug.print.<object_id>`, example:
```sh
nats pub --count=1 -s nats://nats:foliage@nats:4222 functions.graph.api.object.debug.print.root "{}"
```
4. Inspect nats KV store via `io` container: 
```sh
nats -s nats://nats:foliage@nats:4222 kv ls -v --display-value common_kv_store
```

## Functions

- [functions.graph.api.vertex.create](#functionsgraphllapiobjectcreateobject_id) <!-- omit in toc -->
- [functions.graph.api.vertex.update](#functionsgraphllapiobjectupdateobject_id)
- [functions.graph.api.vertex.delete](#functionsgraphllapiobjectdeleteobject_id)
- [functions.graph.api.link.create](#functionsgraphllapilinkcreateobject_id)
- [functions.graph.api.link.update](#functionsgraphllapilinkupdateobject_id)
- [functions.graph.api.link.delete](#functionsgraphllapilinkdeleteobject_id)

### functions.graph.api.vertex.create.<object_id>

[Description](https://pkg.go.dev/github.com/foliagecp/sdk/embedded/graph/crud/#LLAPIVertexCreate)

Example:  
```sh
nats pub --count=1 -s nats://nats:foliage@nats:4222 functions.graph.api.vertex.create.root "{\"payload\":{\"query_id\":\"QUERYID\", \"body\":{\"name\":\"root\"}}}"
```

### functions.graph.api.vertex.update.<object_id>

[Description](https://pkg.go.dev/github.com/foliagecp/sdk/embedded/graph/crud/#LLAPIVertexUpdate)

Example:  
```sh
nats pub --count=1 -s nats://nats:foliage@nats:4222 functions.graph.api.vertex.update.root "{\"payload\":{\"query_id\":\"QUERYID\", \"body\":{\"label\":\"some\"}}}"
```

### functions.graph.api.vertex.delete.<object_id>

[Description](https://pkg.go.dev/github.com/foliagecp/sdk/embedded/graph/crud/#LLAPIVertexDelete)

Example:  
```sh
nats pub --count=1 -s nats://nats:foliage@nats:4222 functions.graph.api.vertex.delete.root "{\"payload\":{\"query_id\":\"QUERYID\"}}"
```

### functions.graph.api.link.create.<object_id>

[Description](https://pkg.go.dev/github.com/foliagecp/sdk/embedded/graph/crud/#LLAPILinkCreate)

Example:  
```sh
nats pub --count=1 -s nats://nats:foliage@nats:4222 functions.graph.api.link.create.root "{\"payload\":{\"query_id\":\"QUERYID\", \"descendant_uuid\":\"a\", \"link_type\": \"type1\", \"link_body\":{\"tags\":[\"t1\", \"t2\"]}}}"
```

### functions.graph.api.link.update.<object_id>

[Description](https://pkg.go.dev/github.com/foliagecp/sdk/embedded/graph/crud/#LLAPILinkUpdate)

Example:  
```sh
nats pub --count=1 -s nats://nats:foliage@nats:4222 functions.graph.api.link.update.root "{\"payload\":{\"query_id\":\"QUERYID\", \"descendant_uuid\":\"a\", \"link_type\": \"type1\", \"link_body\":{\"tags\":[\"t4\"]}}}"
```

### functions.graph.api.link.delete.<object_id>

[Description](https://pkg.go.dev/github.com/foliagecp/sdk/embedded/graph/crud/#LLAPILinkDelete)

Example:  
```sh
nats pub --count=1 -s nats://nats:foliage@nats:4222 functions.graph.api.link.delete.root "{\"payload\":{\"query_id\":\"QUERYID\", \"descendant_uuid\":\"a\", \"link_type\": \"type1\"}}"
```