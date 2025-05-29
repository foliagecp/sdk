# Foliage Processing Language (FPL)

Foliage Processing Language (FPL) is a query language of the Foliage core that extends the capabilities of the JPGQL query language as follows:

## Enhancements

- Ability to perform unions and intersections of queries
- Ability to sort the search results (list of resulting UUIDs) in ascending or descending order
- Ability to apply post-processing functions to resulting UUIDs

---

## Updates to JPGQL

### Syntax Changes

Compared to the previous version, JPGQL syntax has undergone a slight modification. All filtering functions must now include a prefix that specifies whether the function refers to a link (`l:` / `l_`) or a vertex (`v:` / `v_`). For example:

**Old Syntax:**
```txt
.*[type('__object')]
```

**New Syntax:**
```txt
.*[l:type('__object')]
```
or
```txt
.*[l_type('__object')]
```

---

### New Filtering Functions

#### Existing Link Filtering Functions

- `l:type('<link should have type name>')`
- `l:tags('<link should have tag1 name>', '<and link should have tag2 name>', '<and link should have tag3 name>',...)`

#### New Filtering Functions for Links and Vertices by Indexed `body` Fields

- `l:has('<body's first level key name>', '<key value type: numeric|string|bool>', '<compare op: <, >, ==, !=>', '<value>')`
- `v:has('<body's first level key name>', '<key value type: numeric|string|bool>', '<compare op: <, >, ==, !=>', '<value>')`

---

## Examples

### For CLI

```bash
./foliage-cli gwalk to pak1/scala_hlm_hw-server && ./foliage-cli gwalk query ".*[l:type('__object')]"
./foliage-cli gwalk to pak1/scala_hlm_hw-server && ./foliage-cli gwalk query ".*[l:type('__object')].*[l:type('scala_hlm_hw_disk')]"
./foliage-cli gwalk to pak1/scala_hlm_hw-server && ./foliage-cli gwalk query ".*[v:has('hostname','string','>','cluster')]"
./foliage-cli gwalk to pak1/scala_hlm_hw-server && ./foliage-cli gwalk query ".*[l:type('__object') && v:has('hostname','string','>','bc')]"
```

### For NATS

```bash
nats -s nats://nats:foliage@nats:4222 req request.pak1.functions.graph.api.query.jpgql.ctra.pak1/scala_hlm_hw-server '{"payload":{"query":".*[l:type('__object')]"}}'
nats -s nats://nats:foliage@nats:4222 req request.pak1.functions.graph.api.query.jpgql.ctra.pak1/scala_hlm_hw-server '{"payload":{"query":".*[l:type('__object')].*[l:type('scala_hlm_hw_disk')]"}}'
nats -s nats://nats:foliage@nats:4222 req request.pak1.functions.graph.api.query.jpgql.ctra.pak1/scala_hlm_hw-server '{"payload":{"query":".*[v:has('hostname','string','>','cluster')]"}}'
nats -s nats://nats:foliage@nats:4222 req request.pak1.functions.graph.api.query.jpgql.ctra.pak1/scala_hlm_hw-server '{"payload":{"query":".*[l:type('__object') && v:has('hostname','string','>','bc')]"}}'
```

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

```bash
nats -s nats://nats:foliage@nats:4222 req request.pak1.functions.graph.api.query.fpl.root "$(jq -n '
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
nats -s nats://nats:foliage@nats:4222 req request.pak1.functions.graph.api.query.fpl.root "$(jq -n '
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