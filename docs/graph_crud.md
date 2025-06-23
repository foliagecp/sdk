# Graph CRUD

This document describes the low-level CRUD LLAPI functions for vertices and links in the Foliage graph store.

## Setup and Debug

1. **Subscribe to results:**

   ```sh
   nats sub -s nats://nats:foliage@nats:4222 functions.graph.query.QUERYID
   ```
2. **Invoke one of the functions** (see [Functions](#functions)).
3. **Log an object with all its links:**

   ```sh
   nats pub --count=1 -s nats://nats:foliage@nats:4222 signal.<domain>.functions.graph.api.object.debug.print.<object_id> "{}"
   ```
4. **Inspect the common KV store:**

   ```sh
   nats -s nats://nats:foliage@nats:4222 kv ls -v --display-value common_kv_store
   ```

---

## Functions

* [Vertex Create](#functionsgraphapivertexcreateobject_id)
* [Vertex Update](#functionsgraphapivertexupdateobject_id)
* [Vertex Delete](#functionsgraphapivertexdeleteobject_id)
* [Vertex Read](#functionsgraphapivertexreadobject_id)
* [Link Create](#functionsgraphapilinkcreateobject_id)
* [Link Update](#functionsgraphapilinkupdateobject_id)
* [Link Delete](#functionsgraphapilinkdeleteobject_id)
* [Link Read](#functionsgraphapilinkreadobject_id)

---

### functions.graph.api.vertex.create.\<object\_id>

**Function:** LLAPIVertexCreate

**Request:**

```json
{
  "payload": {
    "query_id": "<string>",   // required
    "body": { /* optional JSON object */ }
  },
  "options": {
    "op_stack": <bool>         // optional
  }
}
```

**Reply:**

```json
{
  "payload": {
    "status": "<string>",
    "details": "<string>",
    "data": {
      "op_stack": [ /* optional array */ ]
    }
  }
}
```

---

### functions.graph.api.vertex.update.\<object\_id>

**Function:** LLAPIVertexUpdate

**Request:**

```json
{
  "payload": {
    "query_id": "<string>",       // required
    "body": { /* optional JSON object */ },
    "upsert": <bool>,               // optional, default false
    "replace": <bool>               // optional, default false
  },
  "options": {
    "op_stack": <bool>             // optional
  }
}
```

**Behavior:**

* If `upsert=true`, creates vertex if missing.
* If `replace=true`, replaces the full body; otherwise merges.

---

### functions.graph.api.vertex.delete.\<object\_id>

**Function:** LLAPIVertexDelete

**Request:**

```json
{
  "payload": {
    "query_id": "<string>"     // required
  },
  "options": {
    "op_stack": <bool>           // optional
  }
}
```

**Behavior:**
Deletes the vertex and all its incoming and outgoing links.

---

### functions.graph.api.vertex.read.\<object\_id>

**Function:** LLAPIVertexRead

**Request:**

```json
{
  "payload": {
    "query_id": "<string>",   // required
    "details": <bool>           // optional, default false
  },
  "options": {
    "op_stack": <bool>         // optional
  }
}
```

**Response Data:**

* `body`: the vertex’s JSON body
* if `details=true`, also:

  * `links.out.names`: array of outgoing link names
  * `links.out.types`: array of outgoing link types
  * `links.out.ids`: array of target vertex IDs
  * `links.in`: array of `{ from: <vertex_id>, name: <link_name> }`

---

### functions.graph.api.link.create.\<object\_id>

**Function:** LLAPILinkCreate

**Request (outgoing):**

```json
{
  "payload": {
    "query_id": "<string>",   // required
    "to": "<string>",         // required: target vertex ID
    "name": "<string>",       // required: unique among this vertex’s out-links
    "type": "<string>",       // required: type label
    "tags": ["<string>"],     // optional: any list of tags
    "body": { /* optional JSON */ }
  },
  "options": {
    "op_stack": <bool>         // optional
  }
}
```

**Request (incoming self-request):**

```json
{
  "payload": {
    "in_name": "<string>"      // required: link name to register on this vertex
  },
  "options": { "op_stack": <bool> }
}
```

---

### functions.graph.api.link.update.\<object\_id>

**Function:** LLAPILinkUpdate

**Request:**

```json
{
  "payload": {
    "query_id": "<string>",     // required
    // identify link via either:
    "name": "<string>",         // optional if to+type are provided
    "to": "<string>",           // optional if name provided
    "type": "<string>",         // optional if name provided
    "tags": ["<string>"],       // optional: tags to merge/replace
    "body": { /* optional JSON */ },
    "upsert": <bool>,             // optional, default false
    "replace": <bool>             // optional, default false
  },
  "options": { "op_stack": <bool> }
}
```

**Behavior:**

* If `link` not found and `upsert=true`, falls back to create.
* If `replace=true`, clears existing tag/type indices before reindexing.

---

### functions.graph.api.link.delete.\<object\_id>

**Function:** LLAPILinkDelete

**Request (outgoing):**

```json
{
  "payload": {
    "query_id": "<string>",     // required for initial caller
    "name": "<string>"          // required: out-link name
    // OR if name omitted:
    "to": "<string>",           // required if name not provided
    "type": "<string>"          // required if name not provided
  },
  "options": { "op_stack": <bool> }
}
```

**Request (incoming self-request):**

```json
{
  "payload": {
    "in_name": "<string>"        // required: in-link name
  },
  "options": { "op_stack": <bool> }
}
```

**Behavior:**

* Removes link body, target, type, and all indices.

---

### functions.graph.api.link.read.\<object\_id>

**Function:** LLAPILinkRead

**Request:**

```json
{
  "payload": {
    "query_id": "<string>",     // required
    "name": "<string>",         // required: out-link name
    "details": <bool>             // optional, default false
  },
  "options": { "op_stack": <bool> }
}
```

**Response Data:**

* `body`: link’s JSON body
* if `details=true`, also:

  * `name`, `type`, `from`, `to`
  * `tags`: array of tag strings

---
