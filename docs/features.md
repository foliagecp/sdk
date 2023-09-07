# Foliage features

### Graph Database & Functional Graph
Graph vertices used as data storage and edges used for connectivity between objects (and for data storage too). Functions can be fired by incoming signals in a context of vertice, and use edges to call functions on neighbor vertices. 
### Information & metainformation in one graph</b>   
Object types, link types, types connectivity, functions, applications - all the metainformation stored in the same data graph. It allows full linkage between data and metadata, and can be useful for graph traversal and signal distribution based on data types.
### Distributed event bus
All signals are asynchronous events in various topics in the clusterized event bus. The event bus is persistent, and implements “exactly ones” method of signal processing. 
### Distributed async runtime
An application’s runtime consists of a set of asynchronous functions called on objects, and its business logic is specified through the declaration of call chains implementing all its use cases. Functions can be distributed arbitrarily geographically (different computators), logically (different execution runtimes).
### Serverless stateful functions
Functions are distributed over an arbitrary amount of separate runtimes provided by any computing device connected to the common network and do not require any centralized management. Each instance of a stateful function has its own persistent context allowing it to store arbitrary data between calls.
### Persistent storage
Function context is persistent, stored in the central core cluster asynchronous way, and can be restored in case of function or application crash or relocation. Each function has dedicated context for each object (graph vertice)/
### Graph as Signal Path
Graphs are used not only as data models, but as a way to propagate signals from one object to another. Signals can traverse one or many edges, depending to edge type and attributes. 
### Edge-friendly runtime
Functions can be fired directly on object controllers, such as BMC, PLC, RPi, etc. Function calls routed to edge runtime running on corresponding controller.
### High performance
Up to 400.000 function calls per second on a midsize server. Can be scaled almost linear by clusterization.
### XPath-like query language for graph traversal and signal distribution
Query language for defining graph query, an be used for graph exploration, object findings, and for defining graph traversal routes, for signal distribution, for example.
### Applications can run simultaneously over existing graph functional model, can mutate and extend it
Applications can share existing data model presented as graph and can communicate with each other using graph edges. Data model reuse is a powerful mechanism that works together with code reuse.
### No-code functions and applications construction
The logic of both a function and an application can be declared low-code/no-code. For functions, scripts and configuration files. For applications, either visual or configuration declaration of call chains.
### Flexible/tunable no-code graph observation web UI construction 
Template based UI toolkit, allows use graphs as data models, and construct an interactive UI with graph objects and links. 
### Weighted functional graphs for scalar, vector and tensor signals propagation, and ML applications
Links between vertices in the Foliage's graph database can be weighted, turning the whole graph into a neural-like network and making it applicable for MLOps.