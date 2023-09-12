# Graph CRUD

1. Subscribe and listen for result: 
```sh
nats sub -s nats://nats:foliage@nats:4222 functions.graph.query.QUERYID
```
2. Call one of the following functions
3. To log object with all its links use: `functions.graph.ll.api.object.debug.print.<object_id>`, example:
```sh
nats pub --count=1 -s nats://nats:foliage@nats:4222 functions.graph.ll.api.object.debug.print.root "{}"
```
4. Inspect nats KV store: `nats -s nats://nats:foliage@nats:4222 kv ls -v --display-value app1_kv_store` via `io` container

## Functions

- [functions.graph.ll.api.object.create](#functionsgraphllapiobjectcreateobject_id) <!-- omit in toc -->
- [functions.graph.ll.api.object.update](#functionsgraphllapiobjectupdateobject_id)
- [functions.graph.ll.api.object.delete](#functionsgraphllapiobjectdeleteobject_id)
- [functions.graph.ll.api.link.create](#functionsgraphllapilinkcreateobject_id)
- [functions.graph.ll.api.link.update](#functionsgraphllapilinkupdateobject_id)
- [functions.graph.ll.api.link.delete](#functionsgraphllapilinkdeleteobject_id)

### functions.graph.ll.api.object.create.<object_id>

[Description](https://pkg.go.dev/github.com/foliagecp/sdk/embedded/graph/crud/#LLAPIObjectCreate)

Example:  
```sh
nats pub --count=1 -s nats://nats:foliage@nats:4222 functions.graph.ll.api.object.create.root "{\"payload\":{\"query_id\":\"QUERYID\", \"body\":{\"name\":\"root\"}}}"
```

### functions.graph.ll.api.object.update.<object_id>

[Description](https://pkg.go.dev/github.com/foliagecp/sdk/embedded/graph/crud/#LLAPIObjectUpdate)

Example:  
```sh
nats pub --count=1 -s nats://nats:foliage@nats:4222 functions.graph.ll.api.object.update.root "{\"payload\":{\"query_id\":\"QUERYID\", \"body\":{\"label\":\"some\"}}}"
```

### functions.graph.ll.api.object.delete.<object_id>

[Description](https://pkg.go.dev/github.com/foliagecp/sdk/embedded/graph/crud/#LLAPIObjectDelete)

Example:  
```json
nats pub --count=1 -s nats://nats:foliage@nats:4222 functions.graph.ll.api.object.delete.root "{\"payload\":{\"query_id\":\"QUERYID\"}}"
```

### functions.graph.ll.api.link.create.<object_id>

[Description](https://pkg.go.dev/github.com/foliagecp/sdk/embedded/graph/crud/#LLAPILinkCreate)

Example:  
```sh
nats pub --count=1 -s nats://nats:foliage@nats:4222 functions.graph.ll.api.link.create.root "{\"payload\":{\"query_id\":\"QUERYID\", \"descendant_uuid\":\"a\", \"link_type\": \"type1\", \"link_body\":{\"tags\":[\"t1\", \"t2\"]}}}"
```

### functions.graph.ll.api.link.update.<object_id>

[Description](https://pkg.go.dev/github.com/foliagecp/sdk/embedded/graph/crud/#LLAPILinkUpdate)

Example:  
```sh
nats pub --count=1 -s nats://nats:foliage@nats:4222 functions.graph.ll.api.link.update.root "{\"payload\":{\"query_id\":\"QUERYID\", \"descendant_uuid\":\"a\", \"link_type\": \"type1\", \"link_body\":{\"tags\":[\"t4\"]}}}"
```

### functions.graph.ll.api.link.delete.<object_id>

[Description](https://pkg.go.dev/github.com/foliagecp/sdk/embedded/graph/crud/#LLAPILinkDelete)

Example:  
```sh
nats pub --count=1 -s nats://nats:foliage@nats:4222 functions.graph.ll.api.link.delete.root "{\"payload\":{\"query_id\":\"QUERYID\", \"descendant_uuid\":\"a\", \"link_type\": \"type1\"}}"
```