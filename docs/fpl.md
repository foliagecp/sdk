# Foliage Processing Language (FPL)

Foliage Processing Language (FPL) is a query language of the Foliage core that extends the capabilities of the JPGQL query language as follows:

## Enhancements

- Ability to perform unions and intersections of queries
- Ability to sort the search results (list of resulting UUIDs) in ascending or descending order
- Ability to apply post-processing functions to resulting UUIDs

---

## JPGQL Filtering Functions

All filtering functions include a prefix that specifies whether the function refers to a link (`l:` / `l_`) or a vertex (`v:` / `v_`).

### Link Filters

- `l:type('<link should have type name>')` - filter by link type
- `l:tags('<tag1>', '<tag2>', ...)` - filter by link tags (all must match)
- `l:has('<key>', '<value_type>', '<operation>', '<value>')` - filter by link body field
- `l:array:has('<key>', '<value_type>', '<operation>', '<value>')` - filter by element in link body array

### Vertex Filters

- `v:has('<key>', '<value_type>', '<operation>', '<value>')` - filter by vertex body field
- `v:array:has('<key>', '<value_type>', '<operation>', '<value>')` - filter by element in vertex body array

### Filter parameters

| Parameter | Description | Values |
|-----------|-------------|--------|
| `key` | Field path in body, supports dot-notation for nested access | `'hostname'`, `'info.mac'`, `'config.vlans'` |
| `value_type` | Data type of the value (or array element type for `array:has`) | `'numeric'`, `'string'`, `'bool'` |
| `operation` | Comparison operator | `'=='`, `'!='`, `'>'`, `'<'` |
| `value` | Target value to compare against | `'spine'`, `'9000'`, `'aa:bb:cc'` |

**String operators:** `==` exact match, `!=` not equal, `>` value contains target (substring), `<` target contains value (substring).

**Indexing:** top-level scalar body fields are auto-indexed for fast lookup. Nested fields and arrays use direct body read (fallback) which is correct but slower on large graphs.

For full JPGQL syntax reference see [jpgql.md](./jpgql.md).

---

## FPL + JPGQL

FPL query syntax is based on JSON structure and can be described by the following schema:

```json
{
  "jpgql_uoi": [
    [
      {
        "jpgql": "<JPGQL query string>",
        "from_uuid": "<UUID of starting vertex>"
      }
    ]
  ],
  "sort": "asc" | "dsc",
  "post_processor_func": {
    "name": "<Function name>",
    "data": {
      ...
    }
  }
}
```

---

## Existing Post-Processing Function in FPL

Currently, the core of Foliage supports the following post-processing function:

**Function:** `functions.graph.api.query.fpl.pp.vbody`

This function takes UUIDs from FPL and returns their bodies in an array. It can also sort the result by multiple JSON fields.

### Function Input Schema

```json
{
  "uuids": ["<uuid_1>", "<uuid_2>", "..."],
  "data": {
    "sort_by_field": [
      "<field_name_1>[:asc|:dsc]",
      "<field_name_2>[:asc|:dsc]"
    ]
  }
}
```

---

## FPL Query Examples

FPL is a synchronous request/reply function. Send a NATS request to `request.<domain>.functions.graph.api.query.fpl.<object_id>`. The domain prefix in the object ID is optional — if omitted, the default domain is used automatically.

```bash
nats -s nats://nats:foliage@nats:4222 req request.hub.functions.graph.api.query.fpl.root "$(jq -n '
{
    "payload": {
        "jpgql_uoi": [
            [
                {
                    "jpgql": ".*[v:has(\"hostname\",\"string\",\">\",\"cluster\")]",
                    "from_uuid": "pak1/scala_hlm_hw-server"
                }
            ],
            [
                {
                    "jpgql": ".*[v:has(\"hostname\",\"string\",\">\",\"bc\")]",
                    "from_uuid": "pak1/scala_hlm_hw-server"
                }
            ]
        ],
        "sort": "dsc"
    }
}
')" | jq '.'
```

```bash
nats -s nats://nats:foliage@nats:4222 req request.hub.functions.graph.api.query.fpl.root "$(jq -n '
{
    "payload": {
        "jpgql_uoi": [
            [
                {
                    "jpgql": ".*[v:has(\"hostname\",\"string\",\">\",\"cluster\")]",
                    "from_uuid": "pak1/scala_hlm_hw-server"
                }
            ],
            [
                {
                    "jpgql": ".*[v:has(\"hostname\",\"string\",\">\",\"bc\")]",
                    "from_uuid": "pak1/scala_hlm_hw-server"
                }
            ]
        ],
        "post_processor_func": {
            "name": "functions.graph.api.query.fpl.pp.vbody",
            "data": {
                "sort_by_field": [
                    "body.hostname:asc"
                ]
            }
        }
    }
}
')" | jq '.'
```
