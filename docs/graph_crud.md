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
Creates an object in the graph with an id the function being called with. Preliminarily deletes an existing one with the same id, if present.
If caller is not empty returns result to the caller else returns result to the nats topic.

`payload` arguments:
```json
	query_id: string - optional // ID for this query. Transaction id for operations with the cache. Do not use the same for concurrent graph modify operations.
	body: json - required // Body for object to be created with.
        <key>: <type> - optional // Any additional key and value to be stored in objects's body.
```

Example:  
```sh
nats pub --count=1 -s nats://nats:foliage@nats:4222 functions.graph.ll.api.object.create.root "{\"payload\":{\"query_id\":\"QUERYID\", \"body\":{\"name\":\"root\"}}}"
```

### functions.graph.ll.api.object.update.<object_id>
Updates an object in the graph with an id the function being called with. Merges the old object's body with the new one. Creates a new one if the object does not exist.
If caller is not empty returns result to the caller else returns result to the nats topic.

`payload` arguments:
```json
	query_id: string - optional // ID for this query. Transaction id for operations with the cache. Do not use the same for concurrent graph modify operations.
	body: json - required // Body for object to be created with.
        <key>: <type> - optional // Any additional key and value to be stored in objects's body.
```

Example:  
```sh
nats pub --count=1 -s nats://nats:foliage@nats:4222 functions.graph.ll.api.object.update.root "{\"payload\":{\"query_id\":\"QUERYID\", \"body\":{\"label\":\"some\"}}}"
```

### functions.graph.ll.api.object.delete.<object_id>
Deletes an object with an id the function being called with from the graph and deletes all links related to it.
If caller is not empty returns result to the caller else returns result to the nats topic.

`payload` arguments:
```json
	query_id: string - optional // ID for this query. Transaction id for operations with the cache. Do not use the same for concurrent graph modify operations.
```

Example:  
```json
nats pub --count=1 -s nats://nats:foliage@nats:4222 functions.graph.ll.api.object.delete.root "{\"payload\":{\"query_id\":\"QUERYID\"}}"
```

### functions.graph.ll.api.link.create.<object_id>
Creates a link of type="link_type" from an object with id the funcion being called with to an object with id="descendant_uuid".
Preliminarily deletes an existing link with the same type leading to the same descendant if present.
If caller is not empty returns result to the caller else returns result to the nats topic.

`payload` arguments:
```json
	query_id: string - optional // ID for this query. Transaction id for operations with the cache. Do not use the same for concurrent graph modify operations.
	descendant_uuid: string - optional // ID for descendant object. If not defined random UUID will be generated. If a descandant with the specified uuid does not exist - will be created with empty body.
	link_type: string - optional // Type of link leading to descendant. If not defined random UUID will be used.
	link_body: json - optional // Body for link leading to descendant.
		tags: []string - optional // Defines link tags.
        <key>: <type> - optional // Any additional key and value to be stored in link's body.
```

Example:  
```sh
nats pub --count=1 -s nats://nats:foliage@nats:4222 functions.graph.ll.api.link.create.root "{\"payload\":{\"query_id\":\"QUERYID\", \"descendant_uuid\":\"a\", \"link_type\": \"type1\", \"link_body\":{\"tags\":[\"t1\", \"t2\"]}}}"
```

### functions.graph.ll.api.link.update.<object_id>
Updates a link of type="link_type" from an object with id the funcion being called with to an object with id="descendant_uuid".
Merges the old link's body with the new one. Creates a new one if the link does not exist.
If caller is not empty returns result to the caller else returns result to the nats topic.

`payload` arguments:
```json
	query_id: string - optional // ID for this query. Transaction id for operations with the cache. Do not use the same for concurrent graph modify operations.
	descendant_uuid: string - required // ID for descendant object. If a descandant with the specified uuid does not exist - will be created with empty body.
	link_type: string - required // Type of link leading to descendant.
	link_body: json - required // Body for link leading to descendant.
		tags: []string - optional // Defines link tags.
        <key>: <type> - optional // Any additional key and value to be stored in link's body.
```

Example:  
```sh
nats pub --count=1 -s nats://nats:foliage@nats:4222 functions.graph.ll.api.link.update.root "{\"payload\":{\"query_id\":\"QUERYID\", \"descendant_uuid\":\"a\", \"link_type\": \"type1\", \"link_body\":{\"tags\":[\"t4\"]}}}"
```

### functions.graph.ll.api.link.delete.<object_id>
Delete a link of type="link_type" from an object with id the funcion being called with to an object with id="descendant_uuid".
If caller is not empty returns result to the caller else returns result to the nats topic.

`payload` arguments:
```json
	query_id: string - optional // ID for this query. Transaction id for operations with the cache. Do not use the same for concurrent graph modify operations.
	descendant_uuid: string - required // ID for descendant object.
	link_type: string - required // Type of link leading to descendant.
```

Example:  
```sh
nats pub --count=1 -s nats://nats:foliage@nats:4222 functions.graph.ll.api.link.delete.root "{\"payload\":{\"query_id\":\"QUERYID\", \"descendant_uuid\":\"a\", \"link_type\": \"type1\"}}"
```