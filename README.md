# Foliage
Foliage is a high-performance application platform for organizing coordinated work and interaction of a complex heterogeneous software and information systems. The platform's technology is based on the theory of heterogeneous functional graphs.

The knowledge about a complex/compound system yet stored separately moves into a single associative space. This allows to have transparent knowledge about the entire system and its behavior as one and inseparable whole; gives an ability into account all the hidden relationships and previously unpredictable features; erases the boundary between the system model and the system itself. 
![Alt text](./docs/pics/FoliageUnification.jpg)

By transferring knowledge from different domain planes into a single space, Foliage endows related system components with transparency, consistency, and unambiguity. Reveals weakly detected dependencies, which could be, for example, only inside the head of a devops engineer or in some script. This allows to transparently evaluate the system as a whole and easily endow it with new links and relationships that were previously difficult to implement due to the functional rigidity of the software part.
![Alt text](./docs/pics/FoliageSingleSpace.jpg)

# Stack
1. Backend
    - Jetstream NATS
    - Key/Value Store NATS
    - WebSocket NATS
    - GoLang
    - JavaScript (V8)
2. Frontend
    - React
    - Typescript/JavaScript
    - WebSocket
3. Common
    - docker
    - docker-compose

[Why NATS?](./docs/technologies_comparison.md)

# How to start
## 1. Run foliage-nats-test-statefun on main server
1. Build: 
```sh
docker-compose build
```
2. Run: 
```sh
docker-compose up -d
```

## 2. Call foliage-nats-test-statefun function
1. Start whatching logs stateful function does:
```sh
docker logs foliage-nats-test-statefun_sf_1 --tail 100 -f
```
2. Get into nats-io container:
```sh
docker exec -it foliage-nats-test-statefun_io_1 sh
```
3. Send message:
```sh
nats pub --count=1 -s nats://nats:foliage@nats:4222 functions.app1.json.master.abc "{\"payload\":{\"foo\":\"bar\"}}"
```

## 3. Stop & clean
```sh
docker-compose down -v
```

## What's next?
1. [Create a stateful function](./docs/deep_dive.md#Create-your-first-foliage-stateful-function)
2. [Find out how to work with the graph](./docs/graph_crud.md)
3. [Foliage's Json Path Graph Query Language](./docs/jpgql.md)
4. [Write your own application](./docs/deep_dive.md#Write-your-own-application)
5. [Measure performance](./docs/performance_measures.md)

# Roadmap
![Roadmap](./docs/pics/Roadmap.jpg)

# References
- [Thesaurus](./docs/thesaurus.md)
- [Conventions](./docs/conventions.md)






