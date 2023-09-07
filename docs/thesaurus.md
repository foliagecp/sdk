# Thesaurus
## Foliage stateful function (statefun)
Foliage stateful function is a function that can be called via NATS topic by passing a signal (publishing a message). It has a typename (its address) and it is always called against a string identifier (object's identifier). Being called on the same id concurrently it always executes sequentially. For different ids it executes concurrently. Each statefun on each ids posesses its own context that persisists between the calls and restarts.

## Foliage application
Set of Foliage stateful functions in a single or distributed runtime organized in a way to achieve some specific behaviour.

## Foliage's adapter
A Foliage application designed to abstract interaction with a software system allowing other Foliage applications to work with it as a part of platform's unified functional graph.

## Signal
Foliage signal is a stateful function's typename. To send a signal `S` to an object `O` means to call a stateful function with the typename `S` on object `O`.