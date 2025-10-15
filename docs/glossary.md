# Glossary

## Foliage Control Plane

The **Foliage Control Plane** is the infrastructure layer of the Pregel platform, providing a universal control environment for integrated third-party systems. It orchestrates asynchronous communication via signals over a heterofunctional graph and offers platform services such as CRUD operations, triggers, security policies, introspection, and a graph query language (JPGQL).

## Heterofunctional Graph

The **Heterofunctional Graph** is the core data model in Foliage, composed of:

* **Data Level**: A graph-based model storing objects (vertices) and typed, directed links (edges), including metadata and type definitions as first-class vertices.
* **Functional Level**: A mesh of stateful functions that propagate and react to signals along the graph edges, enabling behavior to co-evolve with topology changes.

This unified graph enables both persistent data storage and signal routing, supporting dynamic control plane logic.

## Vertex

A **Vertex** is the fundamental data element in the heterofunctional graph, representing a node that stores a JSON body (`Object Context`). Vertices can be created, read, updated, or deleted via the low-level CRUD LLAPI (LLAPIVertexCreate, LLAPIVertexRead, LLAPIVertexUpdate, LLAPIVertexDelete). Each vertex is uniquely identified by its object ID and can carry arbitrary JSON properties.

## Edge

An **Edge** is a synonym for a Link in the heterofunctional graph, representing a typed, directed connection between two vertices. The term "edge" emphasizes the graph-theoretic aspect of links, while "link" highlights its operational semantics. Edges and links are managed interchangeably via the Graph CRUD LLAPI. Creating or deleting a link automatically registers or unregisters the corresponding incoming link on the target vertex.

## Object

An **Object** is the high-level concept corresponding to a vertex in the Foliage graph. An object encapsulates real-world entities or system components, modeled as vertices with typed attributes and links. The terms "vertex" and "object" are often used interchangeably.

## Object Link

An **Object Link** is an instance of an edge (link) connecting two objects (vertices) at the object-CRUD level. It represents a concrete relationship managed by Object CRUD functions (CreateObjectsLink, UpdateObjectsLink, DeleteObjectsLink, ReadObjectsLink). It represents a concrete relationship between a source object and a target object.

## Type

A **Type** is a special vertex in the graph that defines a schema or category for other objects or links. Type vertices store metadata such as allowed attributes, validation rules, and permissible link types. Types enable governance and introspection of graph topology and payload structures.

## Type Link

A **Type Link** is a schema-level edge between two type vertices that defines permitted relationships in the graph. Managed via Type-Link CRUD functions (CreateTypesLink, UpdateTypesLink, DeleteTypesLink, ReadTypesLink). These definitions enforce topology policies and guide object-link creation. Type links serve as schema-level constraints, guiding the creation of object links and enforcing topology policies.

## Stateful Function

A **Stateful Function** is a function with persistent context, invoked via a NATS topic by publishing a signal message. It is uniquely identified by a **typename**, which serves as its address. Functions execute with three contexts:

* **Object Context**: The vertex data and its attributes/links.
* **Function Context**: The function’s own persisted state for this object identifier.
* **Signal Context**: The payload data carried by the triggering signal.

Concurrent invocations for the same object identifier are serialized; different identifiers execute in parallel. Contexts persist across invocations and restarts.

## Signal

A **Signal** is an event message sent to a stateful function’s NATS topic. A signal is denoted as `S(O, payload)`, where `S` is the function’s typename, `O` is the object identifier, and `payload` carries any invocation data.

## Functional Space

The **Functional Space** is the namespace of all Foliage stateful functions. Any function in this space can invoke any other by publishing the appropriate signal, enabling a loosely coupled, highly cohesive interaction model.

## Functional Graph

The **Functional Graph** combines the heterofunctional data model with the functional space. It enables objects to both store state and participate in dynamic signal propagation, effectively blending a graph database with an event-driven runtime.

## Runtime

A **Runtime** is a binary service (or distributed cluster of services) that hosts and executes a set of Foliage stateful functions. It manages their lifecycle, subscribes to NATS topics, loads contexts from the key-value store, and ensures push-based delivery of signals for immediate execution.

## Foliage Application

A **Foliage Application** consists of one or more stateful functions deployed to a runtime. Applications can be specialized as:

* **Connectors**: Handle bidirectional integration with external systems, ingesting or applying changes to real-world systems and materializing them as graph objects.
* **Adapters**: Transform and enrich connector-generated objects into domain-specific models, orchestrating target system interactions.
* **Main Runtime Services**: Provide core platform capabilities, such as CRUD for graph data, trigger management, the JPGQL query engine, introspection/debug tools, and security enforcement.

## Foliage Processing Language (FPL)

**Foliage Processing Language (FPL)** is an extension to JPGQL that adds:

* **Set Operations**: Unions and intersections of JPGQL query results.
* **Sorting**: Ordering UUID lists ascending or descending.
* **Post-Processing**: Applying arbitrary functions to resulting UUID sets (e.g., resolving bodies).

FPL queries are issued via `functions.graph.api.query.fpl.root` and consist of:

```json
{
  "jpgql_uoi": [ [... JPGQL stages ...] ],
  "sort": "asc" | "dsc",
  "post_processor_func": {
    "name": "<function>",
    "data": { ... }
  }
}
```

### Example Post-Processor

* **functions.graph.api.query.fpl.pp.vbody**: Takes UUIDs, returns their JSON bodies, and optionally sorts by JSON fields.
