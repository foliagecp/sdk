# Object CRUD

This document describes the stateful CRUD functions for **Types**, **Objects**, and their **Links** in the Foliage CMDB system, analogous to the Graph CRUD API.

## Overview

Each function is invoked via NATS and composes underlying Graph API calls (`vertex.*` and `link.*`).

**Invocation pattern:**

```sh
nats pub --count=1 -s nats://nats:foliage@nats:4222 signal.<domain>.<function_name> '<JSON payload>'
```

Replies include a `payload` object containing:

* `status`: "ok" | "failed" | "idle"
* `details`: human-readable message
* `data`: JSON object, may include `body`, `links`, and `op_stack` if requested

---

## Functions Index

1. **Type CRUD**

   * [CreateType](#createtype)
   * [UpdateType](#updatetype)
   * [DeleteType](#deletetype)
   * [ReadType](#readtype)
2. **Object CRUD**

   * [CreateObject](#createobject)
   * [UpdateObject](#updateobject)
   * [DeleteObject](#deleteobject)
   * [ReadObject](#readobject)
3. **Type-Link CRUD**

   * [CreateTypesLink](#createtypeslink)
   * [UpdateTypesLink](#updatetypeslink)
   * [DeleteTypesLink](#deletetypeslink)
   * [ReadTypesLink](#readtypeslink)
4. **Object-Link CRUD**

   * [CreateObjectsLink](#createobjectslink)
   * [UpdateObjectsLink](#updateobjectslink)
   * [DeleteObjectsLink](#deleteobjectslink)
   * [ReadObjectsLink](#readobjectslink)

---

## 1. Type CRUD

### <a name="createtype"></a>CreateType

**Function:** CreateType

**Request Payload:**

```json
{
  "body": { /* new type properties */ }
}
```

* `body` (object): JSON document of the new type's attributes.

**Behavior:**

1. Calls `functions.graph.api.vertex.create.<type_id>` with `payload.body`.
2. Calls `functions.graph.api.link.create.<types_hub_id>` to link hub→new type.

**Response:** Pass-through of both operations:

```json
{
  "payload": {
    "status": "ok",
    "details": "",
    "data": { /* link.create data or null */ }
  }
}
```

---

### <a name="updatetype"></a>UpdateType

**Function:** UpdateType

**Request Payload:**

```json
{
  "body": { /* fields to update */ },
  "upsert": <bool>,     // optional, default false
  "replace": <bool>     // optional, default false
}
```

* `body` (object): fields to merge or replace in the type.
* `upsert` (boolean): if `true`, create type when missing.
* `replace` (boolean): if `true`, overwrite existing body; otherwise merge.

**Behavior:**

* If `upsert=true` and vertex missing, redirects to `functions.cmdb.api.type.create`.
* Otherwise, calls `functions.graph.api.vertex.update.<type_id>`.

**Response:**

```json
{
  "payload": {
    "status": "ok|failed|idle",
    "details": "",
    "data": { /* update data or null */ }
  }
}
```

---

### <a name="deletetype"></a>DeleteType

**Function:** DeleteType

**Request Payload:** *None*

**Behavior:**

1. Finds all outgoing `OBJECT_TYPELINK` links from type.
2. For each linked object ID, calls `functions.cmdb.api.object.delete.<object_id>`.
3. Calls `functions.graph.api.vertex.delete.<type_id>` to delete the type vertex.

**Response:** Aggregated delete responses:

```json
{ "payload": { "status": "ok|failed", "details": "" } }
```

---

### <a name="readtype"></a>ReadType

**Function:** ReadType

**Request Payload:** *None*

**Behavior:**

* Calls `functions.graph.api.vertex.read.<type_id>` with `details=true`.
* Verifies it links from the types hub.
* Extracts:

  * `body`: original type properties
  * `to_types`: IDs from outgoing `TO_TYPELINK`
  * `object_ids`: IDs from outgoing `OBJECT_TYPELINK`
  * `links`: raw link metadata

**Response:**

```json
{
  "payload": {
    "status": "ok",
    "details": "",
    "data": {
      "body": { /* properties */ },
      "to_types": ["<type_id>"],
      "object_ids": ["<object_id>"],
      "links": { /* raw links */ }
    }
  }
}
```

---

## 2. Object CRUD

### <a name="createobject"></a>CreateObject

**Function:** CreateObject

**Request Payload:**

```json
{
  "origin_type": "<type_id>",  // required
  "body": { /* new object data */ }
}
```

* `origin_type` (string): ID of the type vertex.
* `body` (object): initial properties of the new object.

**Behavior:**

1. Calls `functions.graph.api.vertex.create.<object_id>`.
2. Creates three forced links:

   * hub→object (`OBJECT_TYPELINK`)
   * object→origin type (`TO_TYPELINK`)
   * origin type→object (`OBJECT_TYPELINK`)

**Response:** Combined:

```json
{ "payload": { "status": "ok", "data": {/* last link data */} } }
```

---

### <a name="updateobject"></a>UpdateObject

**Function:** UpdateObject

**Request Payload:**

```json
{
  "body": { /* fields to update */ },
  "upsert": <bool>,        // optional
  "replace": <bool>,       // optional
  "origin_type": "<type_id>" // required if upsert=true
}
```

* Merges or replaces object payload.
* `upsert=true` redirects to `cmdb.api.object.create` if missing.

**Behavior:**

* Calls `functions.graph.api.vertex.update.<object_id>` with `options.op_stack=true`.
* Executes `executeTriggersFromLLOpStack` for each op in returned stack.

**Response:**

```json
{ "payload": { "status": "ok", "data": {/* updated payload */} } }
```

---

### <a name="deleteobject"></a>DeleteObject

**Function:** DeleteObject

**Request Payload:** *None*

**Behavior:**

* Determines object type via `findObjectType`.
* Calls `functions.graph.api.vertex.delete.<object_id>` with `options.op_stack=true`.
* After deletion, executes cleanup triggers via op\_stack.

**Response:**

```json
{ "payload": { "status": "ok", "data": null } }
```

---

### <a name="readobject"></a>ReadObject

**Function:** ReadObject

**Request Payload:** *None*

**Behavior:**

* Calls `functions.graph.api.vertex.read.<object_id>` with `details=true`.
* If missing, returns idle status.
* Extracts:

  * `body`: object properties
  * `type`: from outgoing `TO_TYPELINK`
  * `to_objects`: other outgoing targets
  * `links`: raw link data
* Validates inbound links from objects hub and type for consistency.

**Response:**

```json
{
  "payload": {
    "status": "ok",
    "data": {
      "body": {...},
      "type": "<type_id>",
      "to_objects": ["<object_id>"],
      "links": {...}
    }
  }
}
```

---

## 3. Type-Link CRUD

### <a name="createtypeslink"></a>CreateTypesLink

**Function:** CreateTypesLink

**Request Payload:**

```json
{
  "to": "<type_id>",           // required
  "object_type": "<string>",   // required: label stored in body
  "body": { /* optional */ },
  "tags": ["<string>"]         // optional array of tags
}
```

* Creates `functions.graph.api.link.create.<type_id>` with:

  * `payload.to`, `name`, `type=TO_TYPELINK`, `body`, `tags`.

**Response:**

```json
{ "payload": { "status": "ok" } }
```

---

### <a name="updatetypeslink"></a>UpdateTypesLink

**Function:** UpdateTypesLink

**Request Payload:**

```json
{
  "to": "<type_id>",
  "body": { /* optional */ },
  "tags": ["<string>"],
  "upsert": <bool>,
  "replace": <bool>
}
```

* Maps to `functions.graph.api.link.update.<type_id>` with proper fields.

**Response:** Pass-through update result.

---

### <a name="deletetypeslink"></a>DeleteTypesLink

**Function:** DeleteTypesLink

**Request Payload:**

```json
{ "to": "<type_id>" }
```

* Calls cleanup via `cmdb.api.delete_object_filtered_out_links` per object.
* Deletes type→type link via `functions.graph.api.link.delete.<type_id>`.

**Response:** Aggregated results.

---

### <a name="readtypeslink"></a>ReadTypesLink

**Function:** ReadTypesLink

**Request Payload:**

```json
{ "to": "<type_id>" }
```

* Calls `functions.graph.api.link.read.<type_id>` with `details=true`.

**Response:** Raw link data from Graph API.

---

## 4. Object-Link CRUD

### <a name="createobjectslink"></a>CreateObjectsLink

**Function:** CreateObjectsLink

**Request Payload:**

```json
{
  "to": "<object_id>",
  "name": "<string>",       // optional, defaults to target
  "body": { /* optional */ },
  "tags": ["<string>"]
}
```

* Resolves reference `type` between objects.
* Calls `functions.graph.api.link.create` with `to`, `name`, `type`, `body`, `tags`, `options.op_stack=true`.

**Response:** Executes triggers on op\_stack, returns create result.

---

### <a name="updateobjectslink"></a>UpdateObjectsLink

**Function:** UpdateObjectsLink

**Request Payload:**

```json
{
  "to": "<object_id>",
  "name": "<string>",   // required if upsert=true
  "body": { /* optional */ },
  "tags": ["<string>"],
  "upsert": <bool>,
  "replace": <bool>
}
```

* Resolves link type and maps to `link.update` with full options.
* Uses `options.op_stack=true` and triggers cleanup.

**Response:** Update result, stripped of op\_stack.

---

### <a name="deleteobjectslink"></a>DeleteObjectsLink

**Function:** DeleteObjectsLink

**Request Payload:**

```json
{ "to": "<object_id>" }
```

* Resolves link type, calls `link.delete` with `options.op_stack=true`.
* Executes triggers from op\_stack after delete.

**Response:** Delete result.

---

### <a name="readobjectslink"></a>ReadObjectsLink

**Function:** ReadObjectsLink

**Request Payload:**

```json
{ "to": "<object_id>" }
```

* Resolves `from_type` and `to_type` via helper.
* Calls `functions.graph.api.link.read.<object_id>` with `details=true`.
* Augments raw data with `from_type` and `to_type` fields.

**Response:**

```json
{
  "payload": {
    "status": "ok",
    "data": {
      /* raw link fields */, 
      "from_type": "<type_id>",
      "to_type": "<type_id>"
    }
  }
}
```
