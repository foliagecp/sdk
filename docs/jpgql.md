# JPGQL: JSONPath-Like Lightweight Graph Query Language

JPGQL is a lightweight asynchronous and parallel graph query language, similar to JSONPath. Currently, it is in its initial stages of development, with plans to implement additional functional features in the future.

Read also about JPGQL based extension named Foliage Processing Language ([FPL](./fpl.md)).

## Original jsonpath syntax:
https://support.smartbear.com/alertsite/docs/monitors/api/endpoint/jsonpath.html

## Syntax

The JPGQL syntax follows this pattern:

```<level_access><link_name>[filter][<level_access><link_name>[filter][<level_access><link_name>[filter]]...]```


- `level_access` can be one of two literals:
  - `.` for the next level
  - `..` for any level (first occurrences on all roots only)

- `link_name` is a text value with valid characters: `a-zA-Z0-9/_$#@%+=-`

- `filter` is a value enclosed in square brackets, where filtering expressions reside.


### Restrictions:

1. Do not use `$` and `@` within a filter
2. A filter may contain only predefined functions connected via `&&` and `||`

## Predefined filter functions

### Link filters

#### l:type(type_name:string)

Each out link of a vertex has its type. Desired link type should be named as defined.

#### l:tags(tag1:string, tag2:string, ...)

Desired links should contain all of the defined tags.

#### l:has(key:string, value_type:string, operation:string, target_value:string)

Checks if a link's body contains a field matching the given criteria.

- `key` - field path in the link body, supports dot-notation for nested fields (e.g. `'metrics.bw'`)
- `value_type` - `'numeric'`, `'string'`, or `'bool'`
- `operation` - `'=='`, `'!='`, `'>'`, `'<'`
- `target_value` - value to compare against

Top-level scalar fields are checked via pre-built index (fast). Nested fields fall back to direct body read via `GetByPath` (correct, but slower on large graphs).

**String operators:** `==` exact match, `!=` not equal, `>` value contains target (substring), `<` target contains value (substring).

Example: links where bandwidth > 800:
```
.*[l:has('metrics.bw', 'numeric', '>', '800')]
```

#### l:array:has(key:string, value_type:string, operation:string, target_value:string)

Checks if a link's body contains an array at the given path with at least one element matching the criteria.

Also available as `l:array_has` or `l_array_has`.

- `key` - field path to the array, supports dot-notation (e.g. `'config.protocols'`)
- `value_type` - element type: `'numeric'`, `'string'`, or `'bool'`
- `operation` - `'=='`, `'!='`, `'>'`, `'<'`
- `target_value` - value to compare array elements against

Example: links where protocols array contains `'bgp'`:
```
.*[l:array:has('protocols', 'string', '==', 'bgp')]
```

### Vertex filters

#### v:has(key:string, value_type:string, operation:string, target_value:string)

Checks if the target vertex's body contains a field matching the given criteria.

- `key` - field path in the vertex body, supports dot-notation for nested fields (e.g. `'info.mac'`, `'config.mtu'`)
- `value_type` - `'numeric'`, `'string'`, or `'bool'`
- `operation` - `'=='`, `'!='`, `'>'`, `'<'`
- `target_value` - value to compare against

Top-level scalar fields are checked via pre-built index (fast). Nested fields fall back to direct body read via `GetByPath` (correct, but slower on large graphs).

**String operators:** `==` exact match, `!=` not equal, `>` value contains target (substring), `<` target contains value (substring).

Examples:
```
.*[v:has('val2', 'string', '==', 'Hello World')]
.*[v:has('info.mac', 'string', '==', 'aa:bb:cc:dd:ee:02')]
.*[v:has('config.mtu', 'numeric', '==', '9000')]
```

#### v:array:has(key:string, value_type:string, operation:string, target_value:string)

Checks if the target vertex's body contains an array at the given path with at least one element matching the criteria.

Also available as `v:array_has` or `v_array_has`.

- `key` - field path to the array, supports dot-notation (e.g. `'config.vlans'`)
- `value_type` - element type: `'numeric'`, `'string'`, or `'bool'`
- `operation` - `'=='`, `'!='`, `'>'`, `'<'`
- `target_value` - value to compare array elements against

Examples:
```
.*[v:array:has('tags', 'string', '==', 'prod')]
.*[v:array:has('ports', 'numeric', '>', '3')]
.*[v:array:has('config.vlans', 'numeric', '==', '200')]
```

### Combining filters

Filters can be combined with `&&` (AND) and `||` (OR):

```
.*[l:type('type1') && v:has('info.role', 'string', '==', 'spine')]
.*[v:has('val3', 'bool', '==', 'true') || v:array:has('tags', 'string', '==', 'core')]
.*[l:has('lval2', 'string', '==', 'disk') && l:array:has('protocols', 'string', '==', 'bgp')]
```

### Notes on filter values

- Both single quotes `'value'` and double quotes `"value"` are supported
- Values may contain colons (e.g. MAC addresses `'aa:bb:cc:dd:ee:01'`, URLs, timestamps)
- The colon-based function syntax (`v:has`, `l:type`) and underscore syntax (`v_has`, `l_type`) are interchangeable

## Examples
### Finds all objects from the target one via its output routes that satisfy:

Any links of depth=1:  
`.*`

Any links of depth=1 that contain tag `tag1`:  
`.*[l:tags('tag1')]`

Link typed as `type1` at depth=1 that contain both tags `tag1` and `tag2`:  
`.*[l:type('type1') && l:tags('tag1', 'tag2')]`

Link typed as `type1` at depth=1 that contain both tags `tag1` and `tag2` at depth=2 followed by link with name `name1`:  
`.*[l:type('type1') && l:tags('tag1', 'tag2')].name1`

Full address of a vertex via names `name1` and `name2` and `name3`:  
`.name1.name2.name3`

Links at any depth that contain both tags `tag1` and `tag2`:  
`..*[l:tags('tag1', 'tag2')]`

Routes which goes through link typed as `type1` at depth=1 and through any link with tags `tag1` and `tag2` any deeper:  
`.*[l:type('type1')]..*[l:tags('tag1', 'tag2')]`

Routes which goes through links typed as `type1` at depth=4:  
`.*.*.*.*[l:type('type1')]`

Vertices with nested field `info.role` equal to `spine` at depth=1:  
`.*[v:has('info.role', 'string', '==', 'spine')]`

Vertices whose `tags` array contains `staging` at depth=1:  
`.*[v:array:has('tags', 'string', '==', 'staging')]`

# Small test graph

Small test graph shown on a picture below is created automatically on start in the [basic](./tests/basic.md) sample:
![Alt text](./pics/TestGraph.jpg)

# Usage of JPGQL_CTRA (call tree result aggregation)

JPGQL CTRA is a synchronous request/reply function. Send a NATS request to `request.<domain>.functions.graph.api.query.jpgql.ctra.<object_id>` and receive the result in the reply. The domain prefix in the object ID is optional — if omitted, the default domain is used automatically.

The response contains `data.uuids` (matching vertex IDs) and `data.stats` (query performance statistics). Vertex IDs include the domain prefix (e.g. `hub/g`). The `stats` field is omitted in the examples below for brevity.

## JPGQL_CTRA query examples

> In the examples below the domain is `hub` (default). Adjust to match your deployment.

1. From the `root` at any depth find all objects preceded by link with the type `type5`  

```sh
nats -s nats://nats:foliage@nats:4222 req request.hub.functions.graph.api.query.jpgql.ctra.root "$(jq -n '
{
    "payload": {
        "query": "..*[l:type(\"type5\")]"
    }
}
')" | jq '.'
```
```json
{"data":{"uuids":{"hub/g":true},"stats":{...}},"status":"ok"}
```

2. From the `root` at any depth find all objects preceded by link which contains both tags `t1` and `t3` 

```sh
nats -s nats://nats:foliage@nats:4222 req request.hub.functions.graph.api.query.jpgql.ctra.root "$(jq -n '
{
    "payload": {
        "query": "..*[l:tags(\"t1\",\"t3\")]"
    }
}
')" | jq '.'
```
```json
{"data":{"uuids":{"hub/b":true,"hub/e":true,"hub/g":true},"stats":{...}},"status":"ok"}
```

3. Find all `root`'s descendants
   
```sh
nats -s nats://nats:foliage@nats:4222 req request.hub.functions.graph.api.query.jpgql.ctra.root "$(jq -n '
{
    "payload": {
        "query": ".*"
    }
}
')" | jq '.'
```
```json
{"data":{"uuids":{"hub/a":true,"hub/b":true,"hub/c":true},"stats":{...}},"status":"ok"}
```

4. Find all `root`'s descendants through links of type `type1` and from them get as the result all descendants through links of type `type3`

```sh
nats -s nats://nats:foliage@nats:4222 req request.hub.functions.graph.api.query.jpgql.ctra.root "$(jq -n '
{
    "payload": {
        "query": ".*[l:type(\"type1\")].*[l:type(\"type3\")]"
    }
}
')" | jq '.'
```
```json
{"data":{"uuids":{"hub/d":true,"hub/e":true},"stats":{...}},"status":"ok"}
```

5. From the `root` get all objects at depth=5, where `root`'s depth=0
 
```sh
nats -s nats://nats:foliage@nats:4222 req request.hub.functions.graph.api.query.jpgql.ctra.root "$(jq -n '
{
    "payload": {
        "query": ".*.*.*.*.*"
    }
}
')" | jq '.'
```
```json
{"data":{"uuids":{"hub/b":true,"hub/d":true,"hub/f":true,"hub/h":true},"stats":{...}},"status":"ok"}
```

6. Find all `root`'s descendants through links of type `type1` then get all their descendants and from them as the result get all objects preceded by link which contains either tag `t1` or `t4`

```sh
nats -s nats://nats:foliage@nats:4222 req request.hub.functions.graph.api.query.jpgql.ctra.root "$(jq -n '
{
    "payload": {
        "query": ".*[l:type(\"type1\")].*.*[l:tags(\"t1\") || l:tags(\"t4\")]"
    }
}
')" | jq '.'
```
```json
{"data":{"uuids":{"hub/b":true,"hub/f":true},"stats":{...}},"status":"ok"}
```

7. Access vertex `e` through one of its names from `root` vertex: `2c`, `2d`, `2b`, `2e`:

```sh
nats -s nats://nats:foliage@nats:4222 req request.hub.functions.graph.api.query.jpgql.ctra.root "$(jq -n '
{
    "payload": {
        "query": ".2c.2d.2b.2e"
    }
}
')" | jq '.'
```
```json
{"data":{"uuids":{"hub/e":true},"stats":{...}},"status":"ok"}
```

## Comparison with other graph query languages

| Features                                   | JPGQL (Foliage)                         | AQL (ArangoDB)  | DQL (Dgraph)   | Gremlin (Amazon Neptune) | Cypher (Neo4j) |
|--------------------------------------------|:---------------------------------------:|:---------------:|:--------------:|:------------------------:|:--------------:|
| Vertex attribute filtering                 | **Yes** (scalar + nested + arrays)      | Yes             | Yes            | Yes                      | Yes            |
| Link attribute filtering                   | **Yes** (types, tags, body + arrays)    | Yes             | No             | Yes                      | Yes            |
| Tunable indices                            | **Top-level scalars** (auto-indexed)    | Yes             | No             | Yes                      | Yes            |
| Whole path filtering                       | **No**                                  | Yes             | No             | Yes                      | Yes            |
| Outbound directional search                | **Yes**                                 | Yes             | Yes            | Yes                      | Yes            |
| Inbound directional search                 | **No**                                  | Yes             | No             | Yes                      | Yes            |
| Query plan                                 | **No**                                  | Yes             | Yes            | Yes                      | Yes            |
| JOIN, SORT, GROUP, LIMIT, AGGREGATE, etc.  | **No**                                  | Yes             | Yes            | Yes                      | Yes            |
| Traverse pruning                           | **No**                                  | Yes             | Yes            | Yes                      | Yes            |
| User-defined functions, traverse algorithm | **Yes**                                 | Functions only  | Functions only | Functions only           | Functions only |
| Async-parallel vertex traverse             | **Yes**                                 | No              | Yes            | Yes                      | No             |
| Graph traversals in a cluster              | **Yes**                                 | Yes             | Yes            | Yes                      | Yes            |
