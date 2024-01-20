# Performance Measures

Below are the performance indicators for different types of operations on the Foliage platform. To reproduce these measurements, follow these steps:

1. Push messages to NATS while the application's runtime with stateful functions to measure is down.
2. Start the application's runtime.
3. Use `docker logs` to view the application's runtime logs.

Please note that the measures presented here were not obtained from the fastest server. In practice, performance can increase by up to 3 times, depending on the hardware configuration, especially if NATS is installed natively (not in a Docker container).

## Master function

> JavaScript is disabled

```sh
nats pub --count=100000 -s nats://nats:foliage@nats:4222 functions.tests.basic.master.abc "{\"payload\":{\"foo\":\"bar\"}}"
```
```
Total duration: 17338ns
Function call frequency: 57673Hz
```

## JPGQL_DCRA
### Search at depth=3 by tags

```sh
nats pub --count=100000 -s nats://nats:foliage@nats:4222 functions.graph.api.query.jpgql.dcra.root "{\"payload\":{\"query_id\":\"QUERYID\", \"jpgql_query\":\".type1.*.*[tags('t1') || tags('t4')]\"}}"
```
```
Total duration: 25002ms
Query frequency: 4000Hz
```

## Graph CRUD
### Create object

```sh
nats pub --count=100000 -s nats://nats:foliage@nats:4222 functions.graph.api.vertex.create.root "{\"payload\":{\"query_id\":\"QUERYID\", \"body\":{\"name\":\"root\"}}}"
```
```
Total duration: 4258ms
Query frequency: 23485Hz
```

### Update object

```sh
nats pub --count=100000 -s nats://nats:foliage@nats:4222 functions.graph.api.link.update.root "{\"payload\":{\"query_id\":\"QUERYID\", \"descendant_uuid\":\"a\", \"link_type\": \"type1\", \"link_body\":{\"tags\":[\"t4\"]}}}
```
```
Total duration: 3152ms
Query frequency: 31726Hz
```

### Create link from object `root` to object `a`

```sh
nats pub --count=100000 -s nats://nats:foliage@nats:4222 functions.graph.api.link.create.root "{\"payload\":{\"query_id\":\"QUERYID\", \"descendant_uuid\":\"a\", \"link_type\": \"type1\", \"link_body\":{\"tags\":[\"t1\", \"t2\"]}}}"
```
```
Total duration: 7216ms
Query frequency: 13858Hz
```

### Update link from object `root` to object `a`

```sh
nats pub --count=100000 -s nats://nats:foliage@nats:4222 functions.graph.api.link.update.root "{\"payload\":{\"query_id\":\"QUERYID\", \"descendant_uuid\":\"a\", \"link_type\": \"type1\", \"link_body\":{\"tags\":[\"t4\"]}}}"
```
```
Total duration: 3957ms
Query frequency: 25272Hz
```





