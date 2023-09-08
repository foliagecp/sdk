# JSONPath Graph Query Language - JPGQL

JPGQL is a lightweight async-parallel JSON-path like graph query language. Currently it is at the very initial stage of its growth. A lot of functional features are planned to be implemented later.

## Functions
  - [functions.graph.ll.api.query.jpgql.ctra](#Usage-of-JPGQL_CTRA-call-tree-result-aggregation)
  - [functions.graph.ll.api.query.jpgql.dcra](#Usage-of-JPGQL_DCRA-direct-cache-result-aggregation)
## Original jsonpath syntax:
https://support.smartbear.com/alertsite/docs/monitors/api/endpoint/jsonpath.html

## Syntax
```<level_access><link_name>[filter][<level_access><link_name>[filter][<level_access><link_name>[filter]]...]```

`level_access` – one of 2 literals:  
`.` – next level  
`..` – any level (first occurences on all roots only)

`link_name` – text value  
Valid characters: `a-zA-Z_-`

`filter` - value in square bracket   
Within the square brackets filtering expressions resides

### Restrictions:
1. Do not use `$` and `@` within a filter
2. A filter may contain only predifined functions connected via `&&` and `||`
3. Link types valid characters: `a-zA-Z_-`

## Predefined functions
### tags(tag1:string, tag2:string, ...)
Check that a link contains all of the defined tags

## Examples
### Finds all objects from the target one via its output routes that satisfy:

Any links of depth=1:  
`.*`

Any links of depth=1 that contain tag `tag1`:  
`.*[tags('tag1')]`

Link typed as `type1` at depth=1 that contain both tags `tag1` and `tag2`:  
`.a[tags('tag1', 'tag2')]`

Links at any depth that contain both tags `tag1` and `tag2`:  
`..*[tags("tag1", "tag2")]`

Routes which goes through link typed as `type1` at depth=1 and through any link with tags `tag1` and `tag2` any deeper:  
`.type1..*[tags('tag1', "tag2")]`

Routes which goes through links typed as `type1` at depth=4:  
`.*.*.*.a`

> When using `nats pub` double quotes aroung a tag must be screened right due to the nested quotes. Either use single quotes `'tag'` or triple backslash screening `\\\"tag\\\"`.
> 
> For e.g.:   
> ```sh
> nats pub --count=1 -s nats://nats:foliage@nats:4222 functions.graph.ll.api.query.jpgql.dcra.root "{\"payload\":{\"query_id\":\"QUERYID\", \"jpgql_query\":\".type2[tags('t2')]\"}}"
>```

# Small test graph
Small test graph shown on a picture below is created automatically on start in the [basic](./tests/basic.md) test sample:
![Alt text](./pics/TestGraph.jpg)

# Usage of JPGQL_CTRA (call tree result aggregation)
[Description](https://pkg.go.dev/github.com/foliagecp/sdk/embedded/graph/jpgql/#LLAPIQueryJPGQLCallTreeResultAggregation)
1. Subscribe and listen for result:
```nats sub -s nats://nats:foliage@nats:4222 functions.graph.query.QUERYID```
1. Call `functions.graph.ll.api.query.jpgql.ctra.<object_id>`

## JPGQL_CTRA query examples
1. From the `root` at any depth find all objects preceded by link with the type `type5`  

```sh
nats pub --count=1 -s nats://nats:foliage@nats:4222 functions.graph.ll.api.query.jpgql.ctra.root "{\"payload\":{\"query_id\":\"QUERYID\", \"jpgql_query\":\"..type5\"}}"
```
```json
{"result":{"g":true},"status":"ok"}
```

2. From the `root` at any depth find all objects preceded by link which contains both tags `t1` and `t3` 

```sh
nats pub --count=1 -s nats://nats:foliage@nats:4222 functions.graph.ll.api.query.jpgql.ctra.root "{\"payload\":{\"query_id\":\"QUERYID\", \"jpgql_query\":\"..*[tags('t1','t3')]\"}}"
```
```json
{"result":{"b":true,"e":true,"g":true},"status":"ok"}
```

3. Find all `root`'s descendants
   
```sh
nats pub --count=1 -s nats://nats:foliage@nats:4222 functions.graph.ll.api.query.jpgql.ctra.root "{\"payload\":{\"query_id\":\"QUERYID\", \"jpgql_query\":\".*\"}}"
```
```json
{"result":{"a":true,"b":true,"c":true},"status":"ok"}
```

4. Find all `root`'s descendants through links of type `type1` and from them get as the result all descendants through links of type `type3`

```sh
nats pub --count=1 -s nats://nats:foliage@nats:4222 functions.graph.ll.api.query.jpgql.ctra.root "{\"payload\":{\"query_id\":\"QUERYID\", \"jpgql_query\":\".type1.type3\"}}"
```
```json
{"result":{"d":true,"e":true},"status":"ok"}
```

5. From the `root` get all objects at depth=5, where `root`'s depth=0
 
```sh
nats pub --count=1 -s nats://nats:foliage@nats:4222 functions.graph.ll.api.query.jpgql.ctra.root "{\"payload\":{\"query_id\":\"QUERYID\", \"jpgql_query\":\".*.*.*.*.*\"}}"
```
```json
{"result":{"b":true,"d":true,"f":true,"h":true},"status":"ok"}
```

6. Find all `root`'s descendants through links of type `type1` then get all their descendants and from them as the result get all objects preceded by link which contains either tag `t1` or `t4`

```sh
nats pub --count=1 -s nats://nats:foliage@nats:4222 functions.graph.ll.api.query.jpgql.ctra.root "{\"payload\":{\"query_id\":\"QUERYID\", \"jpgql_query\":\".type1.*.*[tags('t1') || tags('t4')]\"}}"
```
```json
{"result":{"b":true,"f":true},"status":"ok"}
```

## JPGQL_CTRA query examples with call on targets
Make `functions.graph.ll.api.query.jpgql.ctra` to call `functions.graph.ll.api.object.debug.print` on each found object. 

Example:  
```sh
nats pub --count=2 -s nats://nats:foliage@nats:4222 functions.graph.ll.api.query.jpgql.ctra.root "{\"payload\":{\"query_id\": \"QUERYID\", \"jpgql_query\":\"..type5\", \"call\":{\"typename\": \"functions.graph.ll.api.object.debug.print\", \"payload\":{}}}}"
```
```
docker logs foliage-nats-test-statefun_sf_1 --tail 100 -f
...
************************* Object's body (id=g):
{}
************************* In links:
g.in.oid_ltp-nil.f.type5
************************* Out links:
g.out.ltp_oid-bdy.type2.d
{"tags":["t5"]}
g.out.ltp_oid-bdy.type2.h
{"tags":[]}
...
```

# Usage of JPGQL_DCRA (direct cache result aggregation)
[Description](https://pkg.go.dev/github.com/foliagecp/sdk/embedded/graph/jpgql/#LLAPIQueryJPGQLDirectCacheResultAggregation)

1. Subscribe and listen for result:
```nats sub -s nats://nats:foliage@nats:4222 functions.graph.query.QUERYID```
1. Call `functions.graph.ll.api.query.jpgql.dcra.<object_id>`:

## JPGQL_DCRA query examples
Same as for `JPGQL_CTRA query examples`

## JPGQL_DCRA query examples with call on targets
Same as for `JPGQL_CTRA query examples with call on targets`

## Comparison with other graph query languages
| Features | AQL | JPGQL |
|----------|:-------------:|:------:|
| Tunable indices | Yes | No |
| Vertex attribute filtering | Yes | No |
| Link attribute filtering | Yes | Types & Tags only |
| Whole path filtering | Yes | No |
| Outbound directional search | Yes | Yes |
| Inbound directional search | Yes | No |
| Query plan | Yes | No |
| JOIN, SORT, GROUP, LIMIT, AGGREGATE, etc. | Yes | No |
| Traverse pruning | Yes | No |
| User-defined functions, traverse algorithm | Functions only | Yes |
| User-defined  | No | Yes |
| Async execution | No | Yes |
| Parallel execution | Yes | Yes |
| Graph traversals in a cluster | Yes | Yes |
