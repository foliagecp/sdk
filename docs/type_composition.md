# Type Composition in Enhanced `crud1.0`

**Navigation**

- [Type Composition in Enhanced `crud1.0`](#type-composition-in-enhanced-crud10)
  - [Overview](#overview)
  - [Key Concepts](#key-concepts)
  - [Implementation Details](#implementation-details)
  - [API Reference](#api-reference)
    - [Create SuperType-Based Object Link](#create-supertype-based-object-link)
    - [Delete SuperType-Based Object Link](#delete-supertype-based-object-link)
    - [Set SubType Relation](#set-subtype-relation)
    - [Remove SubType Relation](#remove-subtype-relation)
  - [Examples](#examples)
    - [Diamond Composition](#diamond-composition)
      - [State Evolution Table](#state-evolution-table)
      - [Cascade Delete](#cascade-delete)
    - [Parallel Composition](#parallel-composition)
      - [State Evolution Table](#state-evolution-table-1)
      - [Removing a SubType](#removing-a-subtype)
      - [Revoking Permission](#revoking-permission)
  - [NATS CLI Examples](#nats-cli-examples)
  - [Go Client API Examples](#go-client-api-examples)
- [Set SubType](#set-subtype)
- [Remove SubType](#remove-subtype)
- [Create SuperType-Based Object Link](#create-supertype-based-object-link-1)
- [Delete SuperType-Based Object Link](#delete-supertype-based-object-link-1)

---

## Overview

This document explains how hierarchical type composition and supertype-based object linking work in the enhanced `crud1.0`. You will learn:

* How to manage type hierarchies
* How object link permissions derive from supertypes
* How cascade updates handle link consistency
* How to use the NATS API to interact with these features

---

## Key Concepts

* **Type Hierarchy**: Parent–child relationships of types.
* **Link Permissions**: `TypesLinkCreate/TypesLinkDelete` control which type pairs can link objects.
* **SuperType-Based Linking**: `CreateObjectsLinkFromSuperTypes` and `DeleteObjectsLinkFromSuperTypes` automate object links based on hierarchies.
* **Cascade Handlers**: Automatic recalculation or removal of links when hierarchies change.
* **Dynamic Metadata**: `TypeRead` returns `parent_types`, recalculated on demand.

---

## Implementation Details

* **Cache Schema**:

  * `type.<id>.parent_types`: JSON array of direct supertypes.
  * `type.<id>.children_types`: JSON array of direct subtypes.
* **Traversal Helpers**:

  * `gatherTypes4CascadeObjectLinkRefreshStartingFromTypeID(typeId)`
  * `getObjectAllTypesBaseAndParents(objectId)`
* **Cascade Functions**:

  * `PolyTypeCascadeRefresh`
  * `PolyTypeCascadeDelete`
* **Concurrency**: Mutexes guard `children_types`; `lateTriggers` support post-cascade hooks.

---

## API Reference

For each high-level operation, you can invoke via NATS CLI or via the Go client API.

### Create SuperType-Based Object Link

* **NATS Subject**: `functions.cmdb.api.objects.link.supertype.create`
* **Go Client**: `cmdb.ObjectsLinkSuperTypeCreate(from, to, fromClaimType, toClaimType, name, tags, body)`
* **NATS Payload**:

  ```json
  {
    "to":            "<target object ID>",
    "from_super_type":"<superTypeA>",
    "to_super_type":  "<superTypeB>",
    "name":          "<linkName>",
    "body":          { /* optional JSON */ },
    "tags":          [ /* optional tags */ ]
  }
  ```
* **Go Payload Construction**:

  ```go
  payload := easyjson.NewJSONObject()
  payload.SetByPath("to", easyjson.NewJSON(to))
  payload.SetByPath("from_super_type", easyjson.NewJSON(fromClaimType))
  payload.SetByPath("to_super_type", easyjson.NewJSON(toClaimType))
  payload.SetByPath("name", easyjson.NewJSON(linkName))
  payload.SetByPath("body", easyjson.NewJSONObject())
  if len(tags) > 0 {
      payload.SetByPath("tags", easyjson.JSONFromArray(tags))
  }
  err := cmdb.ObjectsLinkSuperTypeCreate(from, to, fromClaimType, toClaimType, linkName, tags, payload)
  ```

### Delete SuperType-Based Object Link

* **NATS Subject**: `functions.cmdb.api.objects.link.supertype.delete`
* **Go Client**: `cmdb.ObjectsLinkSuperTypeDelete(from, to, fromClaimType, toClaimType)`
* **NATS Payload**:

  ```json
  {
    "to":             "<target object ID>",
    "from_super_type":"<superTypeA>",
    "to_super_type":  "<superTypeB>"
  }
  ```
* **Go Payload Construction**:

  ```go
  payload := easyjson.NewJSONObject()
  payload.SetByPath("to", easyjson.NewJSON(to))
  payload.SetByPath("from_super_type", easyjson.NewJSON(fromClaimType))
  payload.SetByPath("to_super_type", easyjson.NewJSON(toClaimType))
  err := cmdb.ObjectsLinkSuperTypeDelete(from, to, fromClaimType, toClaimType)
  ```

### Set SubType Relation

* **NATS Subject**: `functions.cmdb.api.type.subtype.set`
* **Go Client**: `cmdb.TypeSetSubType(parent, child)`
* **NATS Payload**:

  ```json
  { "sub_type": "<childType>" }
  ```
* **Go Call**:

  ```go
  err := cmdb.TypeSetSubType("typeA", "typeC")
  ```

### Remove SubType Relation

* **NATS Subject**: `functions.cmdb.api.type.subtype.remove`
* **Go Client**: `cmdb.TypeRemoveSubType(parent, child)`
* **NATS Payload**:

  ```json
  { "sub_type": "<childType>" }
  ```
* **Go Call**:

  ```go
  err := cmdb.TypeRemoveSubType("typeA", "typeC")
  ```

---

## Examples

### Diamond Composition

```text
      typeA
       /  \
  typeB    typeC
       \  /
      typeD
```

#### State Evolution Table

| Step | Operation                                     | NATS CLI Command                                                                                                                                       | Go Client API                                                                     | Hierarchy Change          | `parent_types`                           | Object Links |
| ---- | --------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------- | ------------------------- | ---------------------------------------- | ------------ |
| 1    | Set `typeB` as subtype of `typeA`             | `nats req request.hub.functions.cmdb.api.type.subtype.set '{"payload":{"sub_type":"typeB"}}'`                                                          | `cmdb.TypeSetSubType("typeA", "typeB")`                                           | B ← A                     | \[`hub/typeA`]                           | none         |
| 2    | Set `typeC` as subtype of `typeA`             | `nats req request.hub.functions.cmdb.api.type.subtype.set '{"payload":{"sub_type":"typeC"}}'`                                                          | `cmdb.TypeSetSubType("typeA", "typeC")`                                           | C ← A                     | \[`hub/typeA`]                           | none         |
| 3    | Set `typeD` as subtype of `typeB`             | `nats req request.hub.functions.cmdb.api.type.subtype.set '{"payload":{"sub_type":"typeD"}}'`                                                          | `cmdb.TypeSetSubType("typeB", "typeD")`                                           | D ← B                     | \[`hub/typeB`]                           | none         |
| 4    | Add additional `typeD` ← `typeC` link         | `nats req request.hub.functions.cmdb.api.type.subtype.set '{"payload":{"sub_type":"typeD"}}'` *(on **`typeC`**)*                                       | `cmdb.TypeSetSubType("typeC", "typeD")`                                           | D ← C                     | \[`hub/typeB`, `hub/typeC`]              | none         |
| 5    | Read `typeD` parents                          | `nats req request.hub.functions.cmdb.api.type.read.typed '{}'`                                                                                         | *N/A*                                                                             | —                         | \[`hub/typeB`, `hub/typeC`, `hub/typeA`] | none         |
| 6    | Create objects `a`,`b`,`c`,`d`                | `nats req request.hub.functions.cmdb.api.objects.create '<payload>'`                                                                                   | `cmdb.ObjectCreate("a", "typeA", ...)`                                            | —                         | —                                        | none         |
| 7    | Create direct link `a` → `b`                  | `nats req request.hub.functions.cmdb.api.objects.link.create '{"name":"linkAB"}'`                                                                      | `cmdb.ObjectsLinkCreate("a","b","linkAB",nil,...)`                                | —                         | —                                        | a–b          |
| 8    | Create supertype-based link `c` → `b` via A→B | `nats req request.hub.functions.cmdb.api.objects.link.supertype.create '{"to":"b","from_super_type":"typeA","to_super_type":"typeB","name":"linkCB"}'` | `cmdb.ObjectsLinkSuperTypeCreate("c","b","typeA","typeB","linkCB",nil,payloadCB)` | verifies C→A & B→B by A→B | —                                        | a–b, c–b     |

#### Cascade Delete

```bash
nats req request.hub.functions.cmdb.api.type.delete.typeA '{}'
```

```go
cmdb.TypeDelete("typeA")
```

* Cascade handler via `PolyTypeCascadeDelete`:

  * Removes `typeA` from B & C `parent_types`.
  * Recomputes D `parent_types` to `[]`.
  * Internally triggers `DeleteObjectsLinkFromSuperTypes(c, b, "typeA", "typeB", "linkCB")`.
* **Result**: only the direct link a–b remains if still allowed.

### Parallel Composition

```text
 typeA → typeC → typeE
 typeB → typeD → typeF
```

#### State Evolution Table

| Step | Operation                                     | NATS CLI Command                                                                                                                                       | Go Client API                                                                                                                                            | Hierarchy Change          | `parent_types`             | Object Links |
| ---- | --------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------- | -------------------------- | ------------ |
| 1    | Create chains A→C→E and B→D→F                 | `nats req request.hub.functions.cmdb.api.type.subtype.set '{"payload":{"sub_type":"typeC"}}'` etc.                                                     | `cmdb.TypeSetSubType("typeA","typeC"); cmdb.TypeSetSubType("typeC","typeE"); cmdb.TypeSetSubType("typeB","typeD"); cmdb.TypeSetSubType("typeD","typeF")` | Chains set                | E:\[C,A], F:\[D,B]         | none         |
| 2    | Allow A→B object links                        | `nats req request.hub.functions.cmdb.api.types.link.create '{"to":"typeB"}'`                                                                           | `cmdb.TypesLinkCreate("typeA","typeB","linkAB",nil)`                                                                                                     | allow A→B                 | —                          | none         |
| 3    | Read `typeE` parents                          | `nats req request.hub.functions.cmdb.api.type.read.typed '{}'`                                                                                         | *N/A*                                                                                                                                                    | —                         | \[`hub/typeC`,`hub/typeA`] | none         |
| 4    | Read `typeF` parents                          | `nats req request.hub.functions.cmdb.api.type.read.typed '{}'`                                                                                         | *N/A*                                                                                                                                                    | —                         | \[`hub/typeD`,`hub/typeB`] | none         |
| 5    | Create objects `e`,`f`                        | `nats req request.hub.functions.cmdb.api.objects.create '<payload>'`                                                                                   | `cmdb.ObjectCreate("e","typeE",...); cmdb.ObjectCreate("f","typeF",...)`                                                                                 | —                         | —                          | none         |
| 6    | Create supertype-based link `e` → `f` via A→B | `nats req request.hub.functions.cmdb.api.objects.link.supertype.create '{"to":"f","from_super_type":"typeA","to_super_type":"typeB","name":"linkEF"}'` | `cmdb.ObjectsLinkSuperTypeCreate("e","f","typeA","typeB","linkEF",nil,payloadEF)`                                                                        | verifies E→(C,A), F→(D,B) | —                          | e–f          |

#### Removing a SubType

```bash
nats req request.hub.functions.cmdb.api.type.subtype.remove '{"payload":{"sub_type":"typeC"}}'
```

```go
cmdb.TypeRemoveSubType("typeA","typeC")
```

* Removes C←A; now C `parent_types`=\[].
* `PolyTypeCascadeDelete` triggers `DeleteObjectsLinkFromSuperTypes(e, f, "typeA", "typeB", "linkEF")`.

#### Revoking Permission

```bash
nats req request.hub.functions.cmdb.api.types.link.delete '{"payload":{"from":"typeA","to":"typeB"}}'
```

```go
cmdb.TypesLinkDelete("typeA","typeB")
```

* Revokes A→B permission.
* Cascade deletes any supertype-based links (e–f, c–b).

---

## NATS CLI Examples

```bash
# Create subtype relationship
nats req request.hub.functions.cmdb.api.type.subtype.set '{"payload":{"sub_type":"typeC"}}'

# Remove subtype relationship
nats req request.hub.functions.cmdb.api.type.subtype.remove '{"payload":{"sub_type":"typeC"}}'

# Create supertype-based object link
nats req request.hub.functions.cmdb.api.objects.link.supertype.create '{"to":"b","from_super_type":"typeA","to_super_type":"typeB","name":"linkCB","body":{},"tags":[]}'

# Delete supertype-based object link
nats req request.hub.functions.cmdb.api.objects.link.supertype.delete '{"to":"b","from_super_type":"typeA","to_super_type":"typeB"}'
```

## Go Client API Examples

```go
// Set and remove subtype
cmdb.TypeSetSubType("typeA", "typeC")
cmdb.TypeRemoveSubType("typeA", "typeC")

// Create and delete supertype-based object link
cmdb.ObjectsLinkSuperTypeCreate("c", "b", "typeA", "typeB", "linkCB", nil, payloadCB)
cmdb.ObjectsLinkSuperTypeDelete("c", "b", "typeA", "typeB")
```

# Set SubType

nats req request.hub.functions.cmdb.api.type.subtype.set '{"payload":{"parent":"typeA","sub\_type":"typeC"}}'

# Remove SubType

nats req request.hub.functions.cmdb.api.type.subtype.remove '{"payload":{"parent":"typeA","sub\_type":"typeC"}}'

# Create SuperType-Based Object Link

nats req request.hub.functions.cmdb.api.objects.link.supertype.create '{"payload": {"objA":"c","objB":"b","superTypeA":"typeA","superTypeB":"typeB","linkName":"linkCB","opts"\:null,"payload":{"cb\_state":"created"}} }'

# Delete SuperType-Based Object Link

nats req request.hub.functions.cmdb.api.objects.link.supertype.delete '{"payload": {"objA":"c","objB":"b","superTypeA":"typeA","superTypeB":"typeB","linkName":"linkCB"} }'
