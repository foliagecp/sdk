# Thesaurus
## Foliage Stateful Functions (statefun)

A Foliage stateful function is a type of function that can be invoked via a NATS topic by sending a signal (publishing a message). It is identified by a typename, which serves as its address, and it is always invoked with a string identifier representing an object.

When invoked with the same identifier concurrently, the function executes sequentially. However, for different identifiers, the executions occur concurrently.

Each stateful function associated with a specific identifier has its own context. This context persists between function calls and restarts, ensuring that the stateful function maintains its state across invocations.

_Note: NATS is a messaging system that provides publish-subscribe and request-reply functionality._

## Foliage Application

A **Foliage application** consists of a set of **Foliage stateful functions** running within a single or distributed runtime. These functions are organized to achieve specific behavior or functionality.


## Foliage's Adapter

**Foliage's adapter** refers to a Foliage application specifically designed to abstract the interaction with a software system. This adaptation enables other Foliage applications to seamlessly integrate it as part of the platform's unified functional graph.


## Signal

A **Foliage signal** represents the typename of a stateful function. Sending a signal `S` to an object `O` means invoking a stateful function with the typename `S` on object `O`.
