# Glossary

## Foliage Control Plane

The **Foliage Control Plane** represents a comprehensive set of tools and functionalities of the platform allowing to implement the listed [features](./features.md). 

## Stateful Functions (statefun)

A **Stateful Function** in Foliage is a specialized type of function that can be invoked via a NATS topic by sending a signal, typically accomplished by publishing a message. These functions are uniquely identified by a `typename`, which serves as both their name and address. Stateful Functions are always invoked with a string identifier, representing an object or entity within the system.

When a Stateful Function is invoked with the same identifier concurrently, its executions are carried out sequentially, ensuring that the function's actions are synchronized. However, when invoked with different identifiers, the executions occur concurrently, allowing for parallel processing.

Each Stateful Function associated with a specific identifier has its dedicated context. This context persists between function calls and restarts, ensuring that the Stateful Function can maintain and manage its state across successive invocations.

_Note: NATS is a messaging system that provides both publish-subscribe and request-reply messaging capabilities._

## Application

A **Foliage Application** is composed of a collection of **Foliage Stateful Functions** running within a single or distributed runtime environment. These functions are organized and orchestrated to achieve specific behaviors or functionalities within the system.

## Adapter

**Foliage's Adapter** refers to a specialized Foliage Application designed with the primary purpose of abstracting the interaction with external software systems or services. This adaptation layer allows other Foliage Applications to seamlessly integrate with it as part of the platform's unified functional graph, facilitating interoperability.

## Signal

A **Foliage Signal** represents the `typename` of a Stateful Function. Sending a signal, denoted as `S`, to an object, represented as `O`, essentially triggers the execution of a Stateful Function identified by the `typename` `S` on the object `O`.

## Stateful Function Typename

The **Stateful Function Typename** is the unique name assigned to a Foliage Stateful Function. It serves as the function's address within the functional space. Each function in a Foliage Application is identified by its typename, enabling precise invocation and addressing.

## Functional Space

**Functional Space** encompasses the collective domain of all Foliage functions, within which each function can invoke any other function. It represents the environment in which Foliage functions interact and collaborate.

## Object

In the context of Foliage, an **Object** refers to a vertex within the system's graph structure. This vertex represents a comprehensive set of information about a real-world object or entity, providing a means to model and interact with real-world entities within the system.

## Link

A **Link** signifies a directed connection or edge within the graph structure of the Foliage system. It serves as the means to establish relationships and connections between two objects or vertices, enabling the representation of complex data dependencies and associations.

## Object Context

**Object Context** denotes the data associated with an object upon which a Stateful Function is invoked. This context can be read or modified on demand, allowing Stateful Functions to access and manipulate the data related to specific objects during their execution.

## Function Context

**Function Context** encompasses the data associated with a Stateful Function, which can be read or modified during its execution. This context persists across multiple invocations and is unique for each object's identifier when the function is called.

## Runtime

A **Runtime** in Foliage is a binary service responsible for providing a set of Stateful Functions. It serves as the execution environment for these functions, ensuring their availability and responsiveness.

## Functional Graph

The **Functional Graph** represents the combination of the Functional Space and a graph database. It empowers objects within the system with functional capabilities, reflecting not only their behavior in the real world but also a desired one.
