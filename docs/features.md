# Foliage Features

## Graph Database & Functional Graph
Foliage utilizes graph vertices for data storage and edges for connectivity between objects. These edges also serve as a means of data storage. Functions can be triggered by incoming signals within the context of vertices and utilize edges to call functions on neighboring vertices.

## Information & Metainformation in One Graph
In Foliage, all object types, link types, type connectivity, functions, and applications are stored in the same data graph. This integration allows for complete linkage between data and metadata, facilitating graph traversal and signal distribution based on data types.

## Distributed Event Bus
Foliage employs an asynchronous event system where all signals are represented as events in various topics within a clusterized event bus. The event bus is persistent and implements an "exactly once" method of signal processing.

## Distributed Async Runtime
An application's runtime is composed of asynchronous functions called on objects. Business logic is defined through the declaration of call chains that implement its various use cases. Functions can be distributed across geographical and logical boundaries, providing flexibility in execution.

## Serverless Stateful Functions
Foliage's stateful functions are distributed across numerous runtimes connected to the common network, eliminating the need for centralized management. Each stateful function instance has its own persistent context, enabling it to store data between calls.

## Persistent Storage
Function contexts are persistent and stored asynchronously in the central core cluster. They can be restored in the event of function or application crashes or relocations. Each function has a dedicated context for each object (graph vertex).

## Graph as Signal Path
Graphs in Foliage serve not only as data models but also as a means to propagate signals from one object to another. Signals can traverse one or many edges, depending on edge types and attributes.

## Edge-Friendly Runtime
Functions can be directly triggered on object controllers, such as BMC, PLC, RPi, etc. Function calls are routed to edge runtimes running on corresponding controllers.

## High Performance
Foliage boasts high performance, capable of handling up to 400,000 function calls per second on a midsize server. Scalability is nearly linear through clusterization.

## XPath-Like Query Language for Graph Traversal and Signal Distribution
Foliage provides a query language for defining graph queries, enabling graph exploration, object discovery, and the definition of traversal routes for signal distribution, among other applications.

## Applications Can Run Simultaneously and Extend Existing Graph Functional Models
Applications can coexist and interact using the same graph data model and communicate through graph edges. This data model reuse enhances code reuse.

## No-Code Function and Application Construction
Foliage allows for low-code/no-code declaration of both functions (via scripts and configuration files) and applications. Applications can be visually designed or configured via declaration of call chains.

## Flexible/Tunable No-Code Graph Observation Web UI Construction
Foliage offers a template-based UI toolkit, enabling the use of graphs as data models to construct interactive user interfaces with graph objects and links.

## Weighted Functional Graphs for Scalar, Vector, and Tensor Signals Propagation, and ML Applications
In Foliage's graph database, links between vertices can be weighted, effectively turning the entire graph into a neural-like network. This feature makes Foliage suitable for MLOps applications and signal propagation involving scalars, vectors, and tensors.
