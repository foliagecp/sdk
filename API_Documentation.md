# Foliage Graph Store API Documentation

This document provides a comprehensive overview of the Graph Store API, including both Low-Level and High-Level APIs.

## Table of Contents
- [Low-Level Graph API](#low-level-graph-api)
  - [Vertex Operations](#vertex-operations)
  - [Link Operations](#link-operations)
- [High-Level CMDB API](#high-level-cmdb-api)
  - [Type Operations](#type-operations)
  - [Types Link Operations](#types-link-operations)
  - [Object Operations](#object-operations)
  - [Objects Link Operations](#objects-link-operations)
  - [Helper Functions](#helper-functions)

---

## Low-Level Graph API

### Vertex Operations

#### `functions.graph.api.vertex.create`
**Implementation:** `LLAPIVertexCreate` in `ll_crud.go`

Creates a vertex in the graph with an id the function being called with.

**Request:**
```json
{
  "body": {
    "<key>": "<type>"
  }
}
```

**Options:**
```json
{
  "op_stack": true
}
```

**Response:**
```json
{
  "status": "string",
  "details": "string",
  "data": {
    "op_stack": []
  }
}
```

---

#### `functions.graph.api.vertex.update`
**Implementation:** `LLAPIVertexUpdate` in `ll_crud.go`

Updates a vertex in the graph with an id the function being called with. Merges or replaces the old vertex's body with the new one.

**Request:**
```json
{
  "body": {
    "<key>": "<type>"
  },
  "upsert": false,
  "replace": false
}
```

**Options:**
```json
{
  "op_stack": true
}
```

**Response:**
```json
{
  "status": "string",
  "details": "string",
  "data": {
    "op_stack": []
  }
}
```

---

#### `functions.graph.api.vertex.delete`
**Implementation:** `LLAPIVertexDelete` in `ll_crud.go`

Deletes a vertex with an id the function being called with from the graph and deletes all links related to it.

**Request:**
```json
{}
```

**Options:**
```json
{
  "op_stack": true
}
```

**Response:**
```json
{
  "status": "string",
  "details": "string",
  "data": {
    "op_stack": []
  }
}
```

---

#### `functions.graph.api.vertex.read`
**Implementation:** `LLAPIVertexRead` in `ll_crud.go`

Reads and returns vertex's body.

**Request:**
```json
{
  "details": false
}
```

**Options:**
```json
{
  "op_stack": true
}
```

**Response:**
```json
{
  "status": "string",
  "details": "string",
  "data": {
    "body": {},
    "links": {
      "out": {
        "names": ["string"],
        "types": ["string"],
        "ids": ["string"]
      },
      "in": [
        {
          "from": "string",
          "name": "string"
        }
      ]
    },
    "op_stack": []
  }
}
```

---

### Link Operations

#### `functions.graph.api.link.create`
**Implementation:** `LLAPILinkCreate` in `ll_crud.go`

Creates a link.

**Request:**
```json
{
  "to": "string",
  "name": "string",
  "type": "string",
  "tags": ["string"],
  "body": {
    "<key>": "<type>"
  }
}
```

**Internal Request (for in_name):**
```json
{
  "in_name": "string"
}
```

**Options:**
```json
{
  "op_stack": true
}
```

**Response:**
```json
{
  "status": "string",
  "details": "string",
  "data": {
    "op_stack": []
  }
}
```

---

#### `functions.graph.api.link.update`
**Implementation:** `LLAPILinkUpdate` in `ll_crud.go`

Updates a link.

**Request:**
```json
{
  "name": "string",
  "to": "string",
  "type": "string",
  "tags": ["string"],
  "upsert": false,
  "replace": false,
  "body": {
    "<key>": "<type>"
  }
}
```

**Options:**
```json
{
  "op_stack": true
}
```

**Response:**
```json
{
  "status": "string",
  "details": "string",
  "data": {
    "op_stack": []
  }
}
```

---

#### `functions.graph.api.link.delete`
**Implementation:** `LLAPILinkDelete` in `ll_crud.go`

Delete a link.

**Request:**
```json
{
  "name": "string",
  "to": "string",
  "type": "string"
}
```

**Internal Request (for in_name):**
```json
{
  "in_name": "string"
}
```

**Options:**
```json
{
  "op_stack": true
}
```

**Response:**
```json
{
  "status": "string",
  "details": "string",
  "data": {
    "op_stack": []
  }
}
```

---

#### `functions.graph.api.link.read`
**Implementation:** `LLAPILinkRead` in `ll_crud.go`

Reads and returns link's body.

**Request:**
```json
{
  "name": "string",
  "to": "string",
  "type": "string",
  "details": false
}
```

**Options:**
```json
{
  "op_stack": true
}
```

**Response:**
```json
{
  "status": "string",
  "details": "string",
  "data": {
    "body": {},
    "name": "string",
    "type": "string",
    "tags": ["string"],
    "from": "string",
    "to": "string",
    "op_stack": []
  }
}
```

---

## High-Level CMDB API

### Type Operations

#### `functions.cmdb.api.type.create`
**Implementation:** `CreateType` in `hl_crud.go`

Creates a type in the CMDB.

**Request:**
```json
{
  "body": {}
}
```

**Response:**
```json
{
  "status": "string",
  "details": "string",
  "data": {}
}
```

---

#### `functions.cmdb.api.type.update`
**Implementation:** `UpdateType` in `hl_crud.go`

Updates a type in the CMDB.

**Request:**
```json
{
  "upsert": false,
  "replace": false,
  "body": {}
}
```

**Response:**
```json
{
  "status": "string",
  "details": "string",
  "data": {}
}
```

---

#### `functions.cmdb.api.type.delete`
**Implementation:** `DeleteType` in `hl_crud.go`

Deletes a type from the CMDB.

**Request:**
```json
{}
```

**Response:**
```json
{
  "status": "string",
  "details": "string",
  "data": {}
}
```

---

#### `functions.cmdb.api.type.read`
**Implementation:** `ReadType` in `hl_crud.go`

Reads a type from the CMDB.

**Request:**
```json
{}
```

**Response:**
```json
{
  "status": "string",
  "details": "string",
  "data": {
    "body": {},
    "to_types": ["string"],
    "object_ids": ["string"],
    "links": {}
  }
}
```

---

### Types Link Operations

#### `functions.cmdb.api.types.link.create`
**Implementation:** `CreateTypesLink` in `hl_crud.go`

Creates a link between types.

**Request:**
```json
{
  "to": "string",
  "object_type": "string",
  "body": {},
  "tags": ["string"]
}
```

**Response:**
```json
{
  "status": "string",
  "details": "string",
  "data": {}
}
```

---

#### `functions.cmdb.api.types.link.update`
**Implementation:** `UpdateTypesLink` in `hl_crud.go`

Updates a link between types.

**Request:**
```json
{
  "to": "string",
  "body": {},
  "tags": ["string"],
  "upsert": false,
  "replace": false
}
```

**Response:**
```json
{
  "status": "string",
  "details": "string",
  "data": {}
}
```

---

#### `functions.cmdb.api.types.link.delete`
**Implementation:** `DeleteTypesLink` in `hl_crud.go`

Deletes a link between types.

**Request:**
```json
{
  "to": "string"
}
```

**Response:**
```json
{
  "status": "string",
  "details": "string",
  "data": {}
}
```

---

#### `functions.cmdb.api.types.link.read`
**Implementation:** `ReadTypesLink` in `hl_crud.go`

Reads a link between types.

**Request:**
```json
{
  "to": "string"
}
```

**Response:**
```json
{
  "status": "string",
  "details": "string",
  "data": {}
}
```

---

### Object Operations

#### `functions.cmdb.api.object.create`
**Implementation:** `CreateObject` in `hl_crud.go`

Creates an object in the CMDB.

**Request:**
```json
{
  "origin_type": "string",
  "body": {}
}
```

**Response:**
```json
{
  "status": "string",
  "details": "string",
  "data": {}
}
```

---

#### `functions.cmdb.api.object.update`
**Implementation:** `UpdateObject` in `hl_crud.go`

Updates an object in the CMDB.

**Request:**
```json
{
  "origin_type": "string",
  "upsert": false,
  "replace": false,
  "body": {}
}
```

**Response:**
```json
{
  "status": "string",
  "details": "string",
  "data": {}
}
```

---

#### `functions.cmdb.api.object.delete`
**Implementation:** `DeleteObject` in `hl_crud.go`

Deletes an object from the CMDB.

**Request:**
```json
{}
```

**Response:**
```json
{
  "status": "string",
  "details": "string",
  "data": {}
}
```

---

#### `functions.cmdb.api.object.read`
**Implementation:** `ReadObject` in `hl_crud.go`

Reads an object from the CMDB.

**Request:**
```json
{}
```

**Response:**
```json
{
  "status": "string",
  "details": "string",
  "data": {
    "body": {},
    "type": "string",
    "to_objects": ["string"],
    "links": {}
  }
}
```

---

### Objects Link Operations

#### `functions.cmdb.api.objects.link.create`
**Implementation:** `CreateObjectsLink` in `hl_crud.go`

Creates a link between objects.

**Request:**
```json
{
  "to": "string",
  "name": "string",
  "body": {},
  "tags": ["string"]
}
```

**Response:**
```json
{
  "status": "string",
  "details": "string",
  "data": {}
}
```

---

#### `functions.cmdb.api.objects.link.update`
**Implementation:** `UpdateObjectsLink` in `hl_crud.go`

Updates a link between objects.

**Request:**
```json
{
  "to": "string",
  "name": "string",
  "body": {},
  "tags": ["string"],
  "upsert": false,
  "replace": false
}
```

**Response:**
```json
{
  "status": "string",
  "details": "string",
  "data": {}
}
```

---

#### `functions.cmdb.api.objects.link.delete`
**Implementation:** `DeleteObjectsLink` in `hl_crud.go`

Deletes a link between objects.

**Request:**
```json
{
  "to": "string"
}
```

**Response:**
```json
{
  "status": "string",
  "details": "string",
  "data": {}
}
```

---

#### `functions.cmdb.api.objects.link.read`
**Implementation:** `ReadObjectsLink` in `hl_crud.go`

Reads a link between objects.

**Request:**
```json
{
  "to": "string"
}
```

**Response:**
```json
{
  "status": "string",
  "details": "string",
  "data": {
    "from_type": "string",
    "to_type": "string"
  }
}
```

---

### Helper Functions

#### `functions.cmdb.api.delete_object_filtered_out_links`
**Implementation:** `DeleteObjectFilteredOutLinksStatefun` (referenced but not shown in provided files)

Helper function for deleting filtered outbound links from objects.

**Request:**
```json
{
  "link_type": "string",
  "to_object_type": "string"
}
```

---

## Notes

1. **Optional Parameters:** Parameters marked as "optional" in the comments can be omitted from requests.
2. **Default Values:** Boolean parameters typically default to `false` when not specified.
3. **Op Stack:** The `op_stack` option in requests enables operation tracking and can be used for debugging or auditing purposes.
4. **Domain Handling:** Some operations automatically handle domain creation and cross-domain references.
5. **Validation:** Link names must match the pattern `[a-zA-Z0-9\/_$#@%+=-]+`.
6. **Error Handling:** All functions return standardized error responses with status, details, and data fields. 