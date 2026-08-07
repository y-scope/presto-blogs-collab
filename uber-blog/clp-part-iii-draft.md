# From 25 PB of Logs to Native SQL: Modernizing Logging at Uber with CLP (Part III)

**Document status:** Working publication draft

Every month, a growing set of Uber’s machine learning, streaming, and data platforms produces approximately 25 PB of raw logs. CLP compresses this volume to approximately 750–850 TB—a 29–33× reduction—while keeping the data directly searchable.

Our warm and cold log workload is served by only five Presto-native workers, each with 48 CPU cores and typically using 5–10% of its CPU capacity. These workers search CLP-S archives directly from Uber’s internal object storage, with no local-disk staging or archive cache. Query latency already provides a responsive user experience, so we deliberately pace object-storage access to use the smallest practical share of that common infrastructure rather than maximizing throughput for its own sake.

Achieving this footprint required more than compression. The logs range from natural-language messages to deeply nested JSON; their schemas evolve continuously, and one field may contain several types. Two effects make the system efficient: metadata planning avoids unnecessary work, and CLP-S executes the remaining search quickly against its encoded representation. This article describes how Uber and YScope brought both capabilities into Uber’s SQL-centric query ecosystem.

## From Spark Logs to a Spectrum of Platform Data

This is the third installment in our series on modernizing Uber’s logging infrastructure with CLP. In [Part I](https://www.uber.com/en-GB/blog/reducing-logging-cost-by-two-orders-of-magnitude-using-clp/), we described how streaming CLP compression reduced Spark logging costs, mitigated SSD write amplification, and allowed us to retain more logs. [Part II](https://www.uber.com/gb/en/blog/modernizing-logging-with-clp-ii/) expanded that work into an end-to-end system for ingesting, retaining, viewing, and programmatically analyzing logs.

Since then, Uber and YScope have expanded CLP across major platform families:

- Machine learning and AI platforms, including Michelangelo and Ray
- Streaming platforms, including Flink and Kafka
- Data platforms, including Spark and Hudi

Together, Uber and YScope developed language-specific logging appenders and native bindings that enrich existing logs with correlation, platform, application, job, host, and container identifiers. Existing pattern configurations are translated into JSON fields automatically, adding operational context without requiring teams to rewrite their logging statements or abandon the natural-language messages they use for debugging.

This work also blurred the traditional boundary between structured and unstructured logs. A message-oriented log is dominated by natural language with embedded variables: “Task 42 failed on host abc.” A JSON event is dominated by request data, dimensions, and application state, yet it often includes natural-language context. Enriched application logs sit between the two. CLP-S, the structured component of CLP, handles this full spectrum. Its underlying ideas are described in the [OSDI ’24 μSlope paper](https://www.usenix.org/conference/osdi24/presentation/wang-rui), jointly published by YScope, the University of Toronto, and Uber’s Observability, Pinot, and Storage and Search teams.

Two properties make this data challenging. It is **semi-structured**: fields can be discovered, but no predefined schema describes every event. It is also **polymorphic**: the same path may contain an integer in one event, a string in another, and an object in a third. This is not necessarily a data-quality failure. Independently deployed services evolve, languages represent values differently, and historical records coexist with newer versions.

At logging scale, schema evolution is not an occasional migration event. It is a continuous property of the data.

## When SQL Meets Schemaless Logs

Presto normally expects columns and their types to be available during query planning. Schemaless logs violate this contract: fields appear and disappear, nested structures change, one field may have several types, and names may contain special characters, differ only by case, or conflict with SQL keywords.

Manually maintaining a complete schema is impractical. No central team can anticipate every field emitted by independently deployed applications. Choosing one canonical type for a polymorphic field either loses information or breaks when another type appears. Rebuilding tables whenever an application adds a field would turn routine deployments into data-platform migrations.

Storing each event as one JSON string is technically schemaless, but too costly for this workload. Queries would repeatedly read and parse entire objects, discover types at runtime, materialize unused fields, and discard most decoded data after filtering. This gives up efficient projection, compressed execution, and opportunities to eliminate irrelevant data before scanning.

SQL also lacks natural syntax for common log operations: searching every value without knowing the field names, searching only keys, searching keys and values together, or applying grep-like wildcard semantics. With polymorphic fields, engineers may need one physical type—or all observed types—without losing their distinctions.

Yet SQL remained strategically important. Uber already has observability interfaces, platform query builders, programmatic clients, and mature routing built around Presto SQL. QueryRunner parses, governs, routes, and fails over queries without requiring every client to understand the storage tier. Presto also supplies the aggregation, joins, and analytical composition users expect.

The challenge, therefore, was not simply to make Presto accept JSON. It was to preserve Uber’s SQL ecosystem while allowing CLP-S—not a generic record-at-a-time JSON parser—to interpret and search the underlying logs.

## One Query Entry Point, Multiple Storage Tiers

Uber’s log-query architecture separates the user-facing query experience from the physical storage tier.

```text
Observability UI       Platform Query Builder       Programmatic Clients
         \                       |                         /
                           QueryRunner
               SQL parsing, policy, routing, cluster
                     selection, and failover
                              |
                +-------------+-------------+
                |                           |
          Pinot: hot logs           Presto-native + CLP
          indexed access              warm/cold logs
                |                           |
          streaming data       Metastore + CLP-S archives
                                in object storage
```

**Figure 1: High-level log query architecture. Clients submit SQL through shared query infrastructure, which routes requests to Pinot for hot data or Presto-native and CLP for warm and cold data.**

Pinot provides indexed, random access for hot logs. Presto-native and CLP-S serve warm and cold logs from Uber’s internal object storage. Workers stream and search the archives directly, without copying them to local data disks or depending on an archive cache. Users access both paths through familiar tools rather than selecting storage engines themselves.

“Warm and cold” does not mean that users must wait for a log file to close. Active log streams are published incrementally, making their latest data queryable through Presto before the producer closes the file. Depending on the workload and publication policy, new events typically become searchable within approximately 30 seconds to a few minutes—sufficient for current users of this tier.

The publication policy balances three competing goals. Larger chunks generally improve compression and reduce object-storage API cost, while smaller, more frequent publications improve freshness. We tune the cadence to keep debugging responsive without sacrificing the compression ratio and low operating cost that make long retention practical.

The Pinot hot tier also reuses CLP-S ideas and implementation components. Uber and YScope adapted its dynamic schema discovery, custom tokenization, and grep-like search algorithms to Pinot-native structures and Lucene indexes optimized for low-latency random access. Those details are outside this article’s scope and may be discussed through a future venue.

## Bringing Native Schemaless Data Access to Presto

YScope engineers, working alongside Uber engineers, developed the Presto-native CLP connector and integrated it into Uber’s production environment. The connector combines CLP-S’s schema discovery and compressed search with Presto’s planning and distributed execution.

CLP-S builds a schema tree containing the field paths and types observed across a dataset without imposing one structure on every event. The connector uses this tree to resolve dynamic paths. Users can project a discovered field directly, with absent fields returning `NULL`, or retrieve a complete object as JSON. Production does not currently materialize complete dynamic objects using Presto’s `ROW` data type; supporting `ROW` for these objects is a possible future extension.

Polymorphism needs explicit semantics. If `status` occurs as both an integer and a string, silently coercing both variants would alter their meaning. Typed UDFs let users choose: `CLP_GET_INT('$.status')` addresses the integer variant, while `CLP_GET_STRING('$.status')` addresses the string variant. The same expression can project in `SELECT`, filter in `WHERE`, or do both. The connector RFC also defines `CLP_GET_FLOAT(...)`, `CLP_GET_BOOL(...)`, and `CLP_GET_JSON_STRING()`. Path-based access preserves case-sensitive names and keys containing symbols that are awkward to express as SQL identifiers.

These UDFs are the natural carrier for schemaless query mechanics in SQL, so “projection UDF” and “search UDF” are not strict categories. Depending on its context, one expression may describe projection, filtering, or both. The family also expresses value-only, key-only, combined key/value, and message searches with grep-like wildcard and regular-expression behavior. For example, `CLP_WILDCARD_COLUMN()` searches across dynamically discovered fields without requiring users to enumerate every path.

The following simplified query uses names from the connector RFC. The production interface supports additional operations.

```sql
SELECT
    timestamp,
    application_id,
    CLP_GET_STRING('$.event.request_id') AS request_id,
    CLP_GET_INT('$.event.status_code') AS status_code
FROM clp.platform_logs
WHERE timestamp >= current_timestamp - INTERVAL '30' DAY
  AND application_id = 'example-application'
  AND CLP_GET_INT('$.event.status_code') >= 500
  AND CLP_WILDCARD_COLUMN() LIKE '%OutOfMemory%'
ORDER BY timestamp DESC
LIMIT 100;
```

Here, the same `CLP_GET_INT(...)` expression drives projection and filtering, while `CLP_WILDCARD_COLUMN()` searches without enumerating every nested path.

The functions are not record-at-a-time UDFs layered on JSON deserialization. During planning, the connector recognizes supported expressions, rewrites the query, and pushes field access, projection, and filtering into CLP-S. Time and top-level metadata constraints can be applied before archives are read.

As compressed bytes arrive from object storage, CLP-S begins searching the encoded representation directly; it does not first reconstruct raw JSON or text and then scan it. The schema tree, log-type dictionaries, and statistics identify encoded regions that could contain matches. CLP-S selectively decodes or decompresses those candidate regions—often a small fraction of the archive—and materializes only the matching events and projected values needed by Presto.

SQL supplies the composition model; CLP-S supplies the schemaless semantics and physical execution. This division makes logs native to Uber’s SQL experience without sacrificing the performance and cost advantages of the storage format.

The collaboration also closed an infrastructure gap in Presto-native: dynamically loaded, out-of-band C++ connectors were not then a production path outside test infrastructure. YScope contributors ChenXing Yang and Jack Luo led key upstream changes for native plugin loading and connector serialization. IBM engineers, including Deepak Majeti, contributed infrastructure work and reviews; Meta engineers and the Presto Foundation provided design feedback and helped drive the upstream effort.

## Avoid Work, Then Execute the Rest Efficiently

Performance comes from two complementary effects. Metadata planning minimizes the archives, splits, and encoded regions that reach execution. CLP-S then searches the remaining encoded data at high speed, selectively decoding only candidate regions. Avoidance reduces the amount of work; native CLP-S execution makes the unavoidable work inexpensive.

The metastore is an active part of query planning. It tracks archive locations, time bounds, record counts, selected top-level dimensions, available fields, and file-level statistics. Filters on an application, host, job, correlation identifier, or time range can eliminate irrelevant archives before Presto schedules worker splits.

The connector does not depend on one metastore product. Uber’s current implementation uses a relational database and manages metadata for billions of files with a comparatively small footprint. Another implementation can work if it supports evolving top-level fields, efficient file lookup, and preferably incremental result streaming. Strong transactions are useful, although eventual consistency can also work under a well-defined correctness contract.

This coarse-grained pruning answers: which archives cannot contain a match? A second, independent optimization handles a common log-query shape:

```sql
ORDER BY timestamp DESC
LIMIT N
```

Logs are append-only and predominantly partitioned by time. Each archive is timestamp-ordered, making the dataset a collection of overlapping, sorted chunks. A request for the 100 latest events usually does not need every older chunk that could contain a match.

Presto does not guarantee split execution order, and a distributed query normally continues until its scheduled work completes. Even after finding enough recent results, a conventional plan may scan and sort older archives.

For eligible queries, the metadata layer terminates early during split planning. It streams candidate archives in timestamp-aware order while tracking their time boundaries and, where available, exact match counts or correctness-preserving upper bounds. Once the planner can prove that older archives cannot alter the global Top-N result, it stops requesting metadata and generating splits. If the proof is unavailable, it safely selects more archives or falls back to a complete scan.

The optimizations are independent but composable. A query can combine top-level filters—such as application, host, job, or time—with `ORDER BY timestamp DESC LIMIT N`. The metastore first prunes archives that cannot satisfy those filters. Early termination then streams the surviving metadata in timestamp order and stops once older candidates cannot change the filtered Top-N result.

```text
SQL predicates
      |
Metadata pruning
(remove impossible files)
      |
Timestamp-aware split planning
(stop after sufficient Top-N coverage)
      |
CLP-native predicate and projection pushdown
      |
Streaming search over encoded data
(selective decode of candidate regions)
      |
Small result returned to Presto
```

**Figure 2: Query-work elimination. Metadata reduces the candidate set before CLP-S searches and projects directly from compressed archives.**

Together, these techniques let workers search compressed archives as data arrives from object storage, without a full secondary index for every field, local-disk staging, or an archive cache. Metadata planning proves what does not need to be read; CLP-S rapidly searches and selectively decodes the remaining encoded regions. The small production footprint depends on both.

## Production Impact and Open Source

The platforms currently integrated with CLP generate approximately 25 PB of raw logs per month. Their compressed footprint fluctuates between 750 and 850 TB, corresponding to 29–33× compression and approximately 97% less storage.

Five Presto-native workers, each with 48 CPU cores, serve the current warm and cold query workload directly from object storage. Typical CPU utilization is 5–10%, and latency is already sufficient for a responsive user experience. We deliberately pace access through object-storage API limits, using the smallest practical share of this common service rather than pursuing throughput that would not materially improve the experience.

### Representative Query Performance

The following profile uses the representative 30-day query above, including application and status filters, wildcard search, and `ORDER BY timestamp DESC LIMIT 100`.

| Metric | Production result | What it demonstrates |
| --- | ---: | --- |
| Data freshness | Approximately **30 seconds to a few minutes** | Open log streams become queryable before their files close, with sufficient freshness for current users |
| End-to-end latency | P50 **[TBD] s**, P95 **[TBD] s** | User-visible performance under production object-storage API limits |
| CLP-S encoded-search throughput | **[TBD] compressed GB/s per worker** | Execution speed after planning selects the remaining archives |
| Metadata pruning | **[TBD]** candidate archives reduced to **[TBD]** (**[TBD]%** eliminated) | Work avoided before split scheduling |
| Planning-time early termination | **[TBD]** splits scheduled versus **[TBD]** without early termination | Older matching archives safely skipped for Top-N |
| Compressed data read | **[TBD] GB** from object storage across a **[TBD] TB/PB** logical raw query scope | Small physical read volume after compression, pruning, and early termination |
| Object-storage API requests | **[TBD]** requests versus **[TBD]** without planning and pushdown | Consumption avoided on the shared object-storage service |

The core integration is available through the open-source [CLP Presto connector](https://github.com/y-scope/clp-plugin-presto-connector) and [CLP repository](https://github.com/y-scope/clp), where users can compress JSON logs and query them with Presto. The connector includes schema discovery, polymorphic type handling, metadata filtering, and split-provider interfaces.

Uber and YScope’s production implementation adds metastore, planning, and optimization capabilities developed for Uber’s environment. We intend to open-source more components where practical. YScope partners with double-digit-petabyte monthly log volumes are also deploying the connector, helping validate the design beyond Uber.

## Conclusion

The central result of the Uber–YScope collaboration is not simply that Presto can query CLP-compressed logs. It is that logs can retain their natural, evolving structures while gaining the projection, search, optimization, and composition expected from an analytical system.

Language-specific appenders add context without requiring application teams to rewrite existing logging configurations. CLP-S handles the continuum from natural-language messages to nested JSON. Presto connects that data to Uber’s query ecosystem. Metadata planning avoids unnecessary work, while CLP-S executes the remaining search quickly against encoded data. Schemaless no longer has to mean opaque, and petabyte scale does not require a petabyte-scale query cluster.

Directly searchable archives also create possibilities beyond centralized Presto execution. Suitable, authorized investigations could eventually run on an engineer’s laptop or another edge environment, subject to access control and resource governance. Deeper work on Pinot, Presto-native, edge execution, summaries, and semantic search remains outside scope and may be shared through future venues.
