# Perfrormance measures
Below are the performance indicators of the platform for different types of operations. To reproduce:
1. Push messages to NATS while appliction's runtime with stateful functions to measure is down
2. Start the appliction's runtime
3. Do `docker logs` to see appliction's runtime logs

The measures below were not taken from the fastest server. Practice shows that the performance can increase up to 3 times depending on the hardware configuration, especially if NATS is installed natively (not in docker).

## Master function
> JavaScript is disabled

```sh
nats pub --count=100000 -s nats://nats:foliage@nats:4222 functions.app1.json.master.abc "{\"payload\":{\"foo\":\"bar\"}}"
```
```
Total duration: 17338ns
Function call frequency: 57673Hz
```

## JPGQL_DCRA
### Search at depth=3 by tags
```sh
nats pub --count=100000 -s nats://nats:foliage@nats:4222 functions.graph.ll.api.query.jpgql.dcra.root "{\"payload\":{\"query_id\":\"QUERYID\", \"jpgql_query\":\".root_a.*.*[tags('t1') || tags('t4’)]\"}}"
```
```
Total duration: 25002ms
Query frequency: 4000Hz
```

## Graph CRUD
### Create object
```sh
nats pub --count=100000 -s nats://nats:foliage@nats:4222 functions.graph.ll.api.object.create.root "{\"payload\":{\"query_id\":\"QUERYID\", \"body\":{\"name\":\"root\"}}}"
```
```
Total duration: 4258ms
Query frequency: 23485Hz
```

### Update object
```sh
nats pub --count=100000 -s nats://nats:foliage@nats:4222 functions.graph.ll.api.link.update.root "{\"payload\":{\"query_id\":\"QUERYID\", \"descendant_uuid\":\"a\", \"link_type\": \"type1\", \"link_body\":{\"tags\":[\"t4\"]}}}
```
```
Total duration: 3152ms
Query frequency: 31726Hz
```

### Create link from object `root` to object `a`
```sh
nats pub --count=100000 -s nats://nats:foliage@nats:4222 functions.graph.ll.api.link.create.root "{\"payload\":{\"query_id\":\"QUERYID\", \"descendant_uuid\":\"a\", \"link_type\": \"type1\", \"link_body\":{\"tags\":[\"t1\", \"t2\"]}}}"
```
```
Total duration: 7216ms
Query frequency: 13858Hz
```

### Update link from object `root` to object `a`
```sh
nats pub --count=100000 -s nats://nats:foliage@nats:4222 functions.graph.ll.api.link.update.root "{\"payload\":{\"query_id\":\"QUERYID\", \"descendant_uuid\":\"a\", \"link_type\": \"type1\", \"link_body\":{\"tags\":[\"t4\"]}}}"
```
```
Total duration: 3957ms
Query frequency: 25272Hz
```





