# How the cache stores the graph

The in-memory cache is the primary store: everything a stateful function reads
comes from it, and NATS KV is where it is written through to and reloaded from.
How it holds the graph is chosen by one setting.

## The setting

```sh
CACHE_MODE=records   # default
```

| value | what it does |
|---|---|
| `tree` | every cache key is its own node in a tree — what the cache did before records existed, unchanged |
| `records` | **default.** a graph vertex is one compact record: its body, its links in buckets, and the (type, target) table |
| `zstd` | records, with buckets and bodies compressed once they go cold |
| `zstd-dict` | as `zstd`, against a dictionary trained on the graph's own data |

`CACHE_MODE` is a *preset*: it decides what the individual settings default to,
and an individual setting named in the environment still wins. A deployment
already passing `CACHE_TIERING` or `CACHE_RECORD_COMPRESSION` keeps behaving
exactly as it did.

Setting `CACHE_MODE` to nothing is the same as not setting it — it is not a
choice of the tree. To get the tree, say so: `CACHE_MODE=tree`, or
`CACHE_TIERING=off`. Either restores the old behaviour exactly, without a
rebuild.

## What records cost and save

Measured on a customer graph of 10 820 vertices and 61 376 links:

| | tree | records | + zstd | + dictionary |
|---|---|---|---|---|
| memory | 137.8 MB | 39.5 MB (3.5×) | 36.2 MB | 28.1 MB (4.9×) |
| reading a link | 325 ns | **217 ns** | 196 ns | 213 ns |
| reading a body again | 2458 ns | **2172 ns** | 2089 ns | 2237 ns |
| reading a body the first time | 2184 ns | 9408 ns | 9166 ns | 10270 ns |
| filling the graph | 1.0 s | 0.9 s | 1.1 s | 1.9 s |

The first read of a body is the one loss, and it is not the cache's to fix:
parsing these bodies costs 8582 ns and copying an already-parsed one 1556, so
almost all of that number is `easyjson`. The tree does not read a body faster —
it pays the same parse once, at write time, and then holds the result forever.
Those held results are what its 137.8 MB are.

Compression is off by default. It is worth another 1.4× of memory but doubles
the time to fill the graph, which is the wrong trade to make on behalf of
somebody who has not asked for it.

## What reading does to memory

Reading grows a record — a body read is kept parsed — but not without bound and
not without giving it back.

- A parse is kept only from the **second** read of a body. A sweep over the
  whole graph reads each body once and never returns, so it leaves nothing
  behind: 39.5 MB before, 39.7 MB after.
- Repeated reading reaches a ceiling of one parse per vertex — 82.0 MB on this
  graph — and stops there. That ceiling is below what the tree occupies at rest,
  so records never need more memory than the tree at any moment.
- Two maintenance passes after the reads stop (about two seconds), it is 39.5 MB
  again. What stays parsed is the working set, not the graph.

## What is not there: a memory budget

The cache does not enforce a memory ceiling. It has no regulator that watches
the heap and gives memory back under pressure, and no tier to evict a vertex
to — those belong to a fuller design that was deliberately not built.

So a graph larger than the machine will still exhaust it. Records move that
wall out by 3.5× to 4.9×; they do not build a new one. Size the machine for the
graph.

What the cache does give back, it gives back on its own schedule: buckets that
have gone cold are compressed, bodies nobody asked for are released, and both
happen in the maintenance pass about once a second.

## What it publishes

Alongside the existing `cache_values` and `cache_sweep_*`:

| metric | |
|---|---|
| `cache_mode` | one series per mode, 1 on the one in use |
| `cache_record_vertices` | vertices kept as records |
| `cache_record_bytes` | what those records hold |
| `cache_record_buckets` | buckets across all three directories |
| `cache_record_buckets_compressed` | of those, held compressed |
| `cache_record_buckets_decoded` | of those, left decoded by a write and awaiting compaction |
| `cache_record_parsed_bodies` | bodies held parsed for readers |
| `cache_record_dictionary_version` | how many dictionaries have been installed |
| `cache_record_dictionary_ratio_trained` / `_now` | compression ratio when trained, and on the latest sample |
| `cache_record_dictionary_retrains` | how many times it has been rebuilt |

There are no histograms of reading a key. A link read takes 217 ns and
observing a Prometheus histogram costs a large fraction of that, so the
measurement would be a third of the thing measured. The two rates worth
watching are derivatives of state anyway: buckets appear only by splitting, so
the split rate is `rate(cache_record_buckets)`, and reads decompressing buckets
show as `cache_record_buckets_compressed` falling between passes.

## Checking a change did not slow anything down

```sh
scripts/run-perf-tests.sh --gate
```

Runs the embedded scenarios under both modes and requires records to stay
within 80 % of the tree's throughput on each. It compares the modes against
each other inside one run rather than against a stored baseline, because
absolute numbers belong to a machine and a ratio does not.

Read the `spread` column before believing a `ratio`: it says how much each mode
varied against itself, and these scenarios swing between 1.2× and 6× on an idle
laptop. The gate catches gross regressions and says plainly when it cannot see
smaller ones.
