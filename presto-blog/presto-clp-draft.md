# Petabyte-Scale Schema-Less Data, Columnar by Design: The CLP Connector for Presto Native

A Parquet table is easiest to optimize when its writer knows the schema. Many analytics platforms, however, receive semi-structured records only after producer applications have defined—and changed—their structure.

Service logs are only one example. Traces, audit records, and AI coding-agent session histories mix tool calls, structured state, and natural-language output; all evolve with independently deployed software. Producers add fields, change nesting, or emit different types at the same path, yet rarely maintain the downstream analytical table. Its owner cannot predict every useful field. Storing each record as one JSON string preserves it, but gives up field-level columnar execution: reading requested fields without decoding complete records.

This is more than ordinary table-schema evolution. The platform cannot pause every producer for a coordinated migration. It must preserve the long tail immediately, keep common fields convenient, and defer path-selection decisions until query time. Logs provide the production evidence in this article; the storage problem is broader.

[CLP from YScope](https://github.com/y-scope/clp) takes a different route. A CLP archive—an independently searchable storage unit—discovers the structures it contains and columnizes every observed field without a global schema or promotion list. The Presto connector then maps SQL expressions to those columns and pushes filtering and projection into CLP, preserving query-time schema-less access with columnar execution. At Uber, this design serves workloads producing approximately 25 PB of raw logs per month.

## Why Not Just Use Iceberg Variant?

Where engine support exists, [Iceberg v3](https://iceberg.apache.org/spec/#variant-type) can also store schema-less data in a Variant column. It may be the right choice when interoperability matters most or frequently queried paths are predictable. Understanding the comparison requires separating three layers:

- [Trino `VARIANT`](https://trino.io/docs/current/functions/variant.html) gives SQL a dynamic value type and operations.
- Iceberg v3 gives that value a place in an interoperable table.
- Parquet defines its binary representation and optional [Variant shredding](https://parquet.apache.org/docs/file-format/types/variantshredding/).

An unshredded Variant is a **native binary document column**: each row contains one complete dynamic value. Types, offsets, and field metadata make a known path directly navigable without parsing text. Its inner fields, however, remain physically co-located and lack independent typed-column statistics.

Shredding promotes writer-selected paths such as `request.id` into typed columns. Those paths gain independent encoding, statistics, projection, and data skipping—the ability to reject data from metadata. Promoting every path is rarely free: a per-file union of heterogeneous paths and types becomes wide and mostly missing. Parquet stores missingness compactly, but every promoted path still adds schema and column overhead. The Variant remainder preserves the dynamic long tail while writers accelerate selected fields.

That choice is physical and file-local. Changing existing files requires rewriting them; a catalog change cannot create inner columns retroactively. Put simply:

> Iceberg can store schema-less data; CLP removes the need to predict which paths deserve columnar storage.

With one stable schema, CLP has no field-columnization advantage over typed Iceberg/Parquet: both can store every field as a column. CLP's advantage grows when many schemas recur outside the table owner's control and paths or types are unpredictable. Its logtype encoding remains a separate advantage when text contains repeated patterns.

## CLP Needs No Shredding Mode

A [Parquet file](https://parquet.apache.org/docs/file-format/) uses one physical schema across all its row groups. Combining many event schemas produces a wide sparse union; separating them produces many small files and repeats metadata.

CLP instead holds many **encoded record tables (ERTs)** in one archive. Each compact table contains records with the same keys and value types. During ingestion, CLP builds an archive-level schema tree, stores each key-and-type schema once, groups matching records, and gives each schema node an encoded column. At query time, the tree routes a path only to ERTs that contain it.

This is why CLP needs no shredding mode. There is no unshredded remainder or writer-maintained promotion list: every observed path is already columnized inside its ERT. One logical path may map to several columns when schemas or types differ, without forcing every record into one global sparse table.

Grouping also helps compression: each ERT column holds values with the same structural role and type, giving specialized encoders homogeneous input. This is one reason CLP can outperform general-purpose Parquet on heterogeneous logs; separating variables from recurring text is another.

CLP also finds repeated structure inside log strings. For example:

~~~text
Request 123 failed after 45 ms
Request 987 failed after 12 ms
                ↓
Request <int> failed after <int> ms
~~~

The shared text is a **logtype**; the placeholders are explanatory, not literal on-disk syntax. CLP records request IDs and durations as integer variables using type-appropriate encodings. Parquet shredding separates document fields; CLP also separates changing values from recurring text inside them. This is syntactic pattern extraction, not language understanding. The [μSlope paper](https://www.usenix.org/system/files/osdi24-wang-rui.pdf) describes the design and reports broader compression results. A direct Parquet comparison still requires the same corpus, codec, and shredding policy.

By workload:

- **One stable schema:** both provide columnar fields; CLP can still exploit logtypes.
- **Many recurring, uncontrolled schemas:** CLP columnizes each shape without a global union or promotion list.
- **Almost one schema per event:** CLP coalesces small ERTs to control metadata overhead. Neither format can amortize a record schema across many events, so their structural layouts converge. CLP may still find repeated logtypes inside string values.

An optional **convenience schema** exposes selected path-and-type bindings as SQL columns after archives are written. Changing it does not rewrite CLP data; it remaps logical columns to existing ERT columns. Unexposed path/type combinations remain available through schema-less access.

> **One logical document, many native columns.** The convenience schema is defined after ingestion over already-columnized storage.

## From SQL Expression to Encoded Column

The current [PrestoDB type system](https://prestodb.io/docs/current/language/types.html) has `JSON`, fixed-shape `ROW`, and homogeneous `ARRAY(T)`, but no first-class `VARIANT`. None naturally represents a path that may return a scalar, object, or heterogeneous array from one event to the next.

The public [CLP connector plugin for Presto](https://github.com/y-scope/clp-plugin-presto-connector) therefore provides typed accessors such as `CLP_GET_STRING(path)` and `CLP_GET_BIGINT(path)`. `CLP_GET_JSON_STRING()` reconstructs a complete event. Typed `CLP_WILDCARD_*_COLUMN()` functions search values across fields that the query does not name.

~~~sql
SELECT
    timestamp,
    CLP_GET_STRING('request.id') AS request_id,
    CLP_GET_BIGINT('response.status') AS status,
    CLP_GET_JSON_STRING() AS event_json
FROM clp.logs
WHERE timestamp >= current_timestamp - INTERVAL '1' HOUR
  AND CLP_GET_BIGINT('response.status') >= 500
  AND CLP_WILDCARD_STRING_COLUMN() LIKE 'timeout%'
ORDER BY timestamp DESC
LIMIT 100;
~~~

These functions look like user-defined functions, but their Java bodies are plan markers, not row-at-a-time JSON evaluators. The [connector rewriter](https://github.com/y-scope/clp-plugin-presto-connector/blob/main/presto-connector/src/main/java/com/facebook/presto/plugin/clp/optimization/ClpUdfRewriter.java) recognizes constant paths and supported predicates. It then performs **pushdown**: moving eligible filters and column selection into the storage reader instead of processing reconstructed rows in Presto. [Pushdown is connector-specific](https://trino.io/docs/current/optimizer/pushdown.html), so unsupported expressions still run in the engine.

The execution path is:

> **Query-plan resolution → projection/predicate pushdown → split-local schema resolution → native encoded execution**

A [split](https://prestodb.io/docs/current/overview/concepts.html#split) is Presto's unit of parallel scan. At planning time, the connector retains the requested paths and filters. On each split, it matches those paths against the archive's schema tree, selects the relevant ERT columns, and scans their encoded values.

Iceberg/Parquet and CLP use similar execution phases; they bind to different storage:

| Phase | Iceberg/Parquet Variant | CLP |
|---|---|---|
| Plan | Resolve Variant paths and filters | Retain CLP paths, types, and operations |
| Pushdown | Pass supported subfields and filters to the file reader | Pass them to the CLP scan |
| Bind | Use a shredded column if present; otherwise navigate Variant values | Match the split's schema tree to existing ERT columns |
| Scan | Read shredded columns or pages containing unshredded values | Search and project encoded ERT columns |

Today, whole-event access regardless of runtime type returns one JSON string. If PrestoDB gains Variant across planning, serialization, and native execution, the connector could expose the same event as one `event VARIANT` column without changing CLP storage. A constant expression such as `event['request']['id']` could follow the same pushdown path; selecting the complete `event` would require full reconstruction.

CLP's wildcard operations remain complementary. Current functions search values across unknown fields. Future key discovery could match a pattern against the archive schema tree before scanning records, then project matching `{path, value}` pairs. An unshredded Variant generally must inspect per-value object metadata to discover arbitrary keys; CLP can first narrow candidates using archive-level structure. Key discovery and wildcard projection are future directions, not current public APIs.

## A Native Storage Engine Behind a Presto Catalog

[Presto Native, documented as Presto C++](https://prestodb.io/docs/current/presto-cpp.html), replaces Java workers with native workers while retaining the Java coordinator. In [Presto's data-source model](https://prestodb.io/docs/current/overview/concepts.html), a connector adapts a source to catalogs, schemas, tables, and splits. CLP keeps those concepts familiar: a metadata provider discovers archives, an optional convenience schema exposes common fields, and archive splits provide parallelism. Split-local structure remains authoritative for the long tail, so the catalog need not union every observed field.

~~~mermaid
flowchart TB
    client["SQL client"]
    coordinator["Java coordinator<br/>Plan queries · create splits"]
    metadata["CLP metadata provider<br/>Archives · bounds · statistics"]
    workers["Presto Native workers"]
    plugin["CLP connector and shared library"]
    archives["CLP archives<br/>Schema trees · ERT columns · logtypes"]

    client --> coordinator
    coordinator <--> metadata
    coordinator --> workers
    workers --> plugin
    plugin --> archives
    plugin --> workers
    workers --> coordinator
    coordinator --> client
~~~

The Java coordinator plans queries and serializes connector-specific scan information. Each native worker loads the connector plugin, which links CLP as a shared library running in the worker process. CLP scans encoded columns and returns columnar batches. This avoids a separate service call, a per-record language bridge, and raw-event materialization before search. The boundary does not require CLP to remain implemented in one language.

Building this external connector exposed missing pieces in Presto Native's plugin path. [PR #27653](https://github.com/prestodb/presto/pull/27653) enabled connector factories from dynamically loaded native code; [PR #27652](https://github.com/prestodb/presto/pull/27652) routed connector-specific serialized values to native workers.

YScope engineers led the connector implementation. IBM engineers and the Presto community supported infrastructure and review; YScope and Uber engineers extended and applied the architecture in production.

> CLP was not rewritten for Presto Native; Presto Native gained a path to load CLP as an independently developed connector.

## Avoid Work at Every Layer

At petabyte scale, performance comes from eliminating work before decoding values:

1. **Metadata planning** uses time bounds, indexed fields, and statistics to avoid irrelevant archives.
2. **Schema resolution** removes ERTs and columns that cannot contain the requested path or type; dictionaries can reject some string predicates.
3. **Encoded execution** scans the remaining columns and reconstructs only requested values or complete matching events.

> Metadata determines what not to read; CLP makes what remains inexpensive to search.

CLP is laid out for object storage: its header, schema tree, dictionaries, and indexes appear up front, after which the reader advances sequentially without backward seeks. At Uber, each Presto Native worker is a compute process running the CLP connector in-process. The connector opens an object-storage byte stream; CLP's streaming archive reader interprets and searches it as bytes arrive, without staging or caching it in full. Compression reduces transfer and scan work.

Uber also uses planning-time **early termination** for eligible newest-first `ORDER BY timestamp DESC LIMIT N` queries. Given append-only logs and trustworthy time bounds, planning stops creating older splits once they cannot change the result. This can eliminate entire archive ranges before object-store reads begin. It differs from pruning: pruning proves data cannot match, while early termination proves it cannot affect the ordered limited result. This optimization is not current open-source connector behavior.

## What Other Presto Native Connector Authors Can Reuse

As one of Presto Native's first independently developed pluggable connectors, CLP offers a short playbook:

1. Define the tables, types, splits, and operations the connector exposes.
2. Divide coordinator planning/serialization from worker registration/deserialization/scanning.
3. Package both components against compatible Presto and native-engine versions.
4. Test ordinary SQL, schema-less access, pushdown, missing fields, and coordinator-to-worker serialization end to end.

Treat serialized plan objects—called connector handles—as a versioned protocol: tests on each side cannot detect every cross-boundary mismatch. The [connector documentation](https://docs.yscope.com/presto/connector/clp.html) covers packages, configuration, metadata, and split filtering; the main CLP project provides Docker Compose and Kubernetes setups.

## Open Source, Production Scale, and What Comes Next

The open-source connector works end to end today, including typed schema-less access, filter and projection pushdown, wildcard value search, whole-event JSON, and pluggable metadata filtering.

At Uber, onboarded workloads produce approximately 25 PB of raw logs per month, compressed to roughly 750–850 TB depending on workload mix—a 29–33× reduction. Five 48-core Presto Native workers serve recent and older data directly from object storage. Advanced metadata planning, early termination, and billion-file metadata scale are Uber extensions, not current public features.

Future PrestoDB Variant support can improve the SQL surface without changing CLP storage. Schema-tree and pattern metadata may also support key discovery, structural summaries, or candidate narrowing before deeper analysis.

The larger lesson is simple: schema-less data does not require row-blob execution. CLP columnizes observed structure during ingestion; Presto resolves requested paths and pushes them into native encoded execution.

CLP gives Presto one logical document, many native columns, and a query plan that knows how to reach them.
