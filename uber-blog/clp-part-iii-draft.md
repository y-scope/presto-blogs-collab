# From 25 PB of Logs to Native SQL: Modernizing Logging at Uber with CLP (Part III)

**Document status:** Working publication draft

Every month, a growing set of Uber’s machine learning, streaming, and data platforms produces approximately 25 PB of raw logs. CLP compresses this volume to approximately 750–850 TB—a 29–33× reduction—while keeping it directly searchable. This is monthly ingest volume, not the amount scanned by an individual query.

Only five 48-core Presto Native workers serve the current warm and cold workload, typically at 5–10% CPU utilization. They search fresh KV-IR files and immutable CLP archives directly from Uber’s internal object storage, without local-disk staging or an archive cache. Because latency already supports a responsive user experience, we deliberately pace object-storage access to consume the smallest practical share of that common infrastructure instead of maximizing throughput for its own sake.

At this scale, however, compression alone does not explain the footprint. The logs range from natural-language messages to deeply nested JSON; their schemas evolve continuously, and one field may contain several types. Metadata planning eliminates unnecessary work, while CLP executes the remaining search against its encoded representation. This article explains how Uber and YScope brought those capabilities into Uber’s SQL-centric query ecosystem.

## From Spark Logs to a Spectrum of Platform Data

This is the third installment in our series on modernizing Uber’s logging infrastructure with CLP. In [Part I](https://www.uber.com/en-GB/blog/reducing-logging-cost-by-two-orders-of-magnitude-using-clp/), we described how streaming CLP compression reduced Spark logging costs, mitigated SSD write amplification, and allowed us to retain more logs. [Part II](https://www.uber.com/gb/en/blog/modernizing-logging-with-clp-ii/) expanded that work into an end-to-end system for ingesting, retaining, viewing, and programmatically analyzing logs.

Since then, Uber and YScope have expanded CLP across major platform families:

- Machine learning and AI platforms, including Michelangelo and Ray
- Streaming platforms, including Flink and Kafka
- Data platforms, including Spark and Hudi

Together, Uber and YScope developed language-specific logging appenders and native bindings that enrich existing logs with correlation, platform, application, job, host, and container identifiers. Existing pattern configurations are translated into JSON fields automatically, adding operational context without requiring teams to rewrite their logging statements or abandon the natural-language messages they use for debugging.

The expansion also revealed a continuum rather than a clean boundary between structured and unstructured logs. A message-oriented log is dominated by natural language with embedded variables: “Task 42 failed on host abc.” A JSON event is dominated by request data, dimensions, and application state, yet often includes natural-language context. Enriched application logs sit between the two, and CLP handles the full spectrum. The underlying ideas are described in the [OSDI ’24 μSlope paper](https://www.usenix.org/conference/osdi24/presentation/wang-rui), jointly published by YScope, the University of Toronto, and Uber’s Observability, Pinot, and Storage and Search teams.

Two properties make this data challenging. It is **semi-structured**: fields are discoverable, but no predefined schema describes every event. It is also **polymorphic**: the same path may contain an integer in one event, a string in another, and an object in a third. That is not necessarily a data-quality failure. Independently deployed services evolve, languages represent values differently, and historical records coexist with newer versions.

At logging scale, schema evolution is not an occasional migration event. It is a continuous property of the data.

## When SQL Meets Schema-Less Logs

Presto normally expects column names and types to be available during query planning. Schema-less logs break that contract: fields appear and disappear, nested structures change, one path may have several types, and names may contain special characters, differ only by case, or conflict with SQL keywords.

Manually maintaining a complete schema is impractical. No central team can anticipate every field emitted by independently deployed applications. Choosing one canonical type for a polymorphic path either loses information or breaks when another type appears. Rebuilding tables whenever an application adds a field would turn routine deployments into data-platform migrations.

Storing each event as one JSON string preserves flexibility, but is too costly for this workload. Queries would repeatedly read and parse complete objects, discover types at runtime, materialize unused fields, and discard most decoded data after filtering. That sacrifices efficient projection, encoded execution, and opportunities to eliminate irrelevant data before scanning.

Log investigation also requires operations SQL does not natively express: searching values without knowing their field names, discovering keys, relating matched keys to values, and applying grep-like wildcard semantics. For a polymorphic path, engineers may need one observed type—or several—without losing their distinctions.

Still, SQL remained strategically important. Uber already has observability interfaces, platform query builders, programmatic clients, and mature routing built around Presto. QueryRunner parses, governs, routes, and fails over queries without requiring every client to understand the storage tier. Presto also supplies the aggregation, joins, and analytical composition users expect.

The goal, therefore, was not simply to make Presto accept JSON. It was to preserve Uber’s SQL ecosystem while allowing CLP—not a generic record-at-a-time JSON parser—to interpret and search the logs.

## One Query Entry Point, Multiple Storage Tiers

Uber’s log-query architecture separates the user experience from the physical storage tier.

```text
Observability UI       Platform Query Builder       Programmatic Clients
         \                       |                         /
                           QueryRunner
               SQL parsing, policy, routing, cluster
                     selection, and failover
                              |
                +-------------+-------------+
                |                           |
          Pinot: hot logs           Presto Native + CLP
          indexed access              warm/cold logs
                |                           |
          streaming data          Metadata-coordinated
                                  mixed split planning
                                      /         \
                              KV-IR files    immutable CLP archives
                                      \         /
                                      object storage
```

**Figure 1: High-level log query architecture. Shared infrastructure routes SQL to Pinot for hot data or to Presto Native and CLP for warm and cold data.**

Pinot provides indexed, random access for hot logs. Presto Native and CLP serve warm and cold logs from Uber’s internal object storage. Each worker supplies compute and hosts the connector in-process. The open-source connector can execute one query over a mix of KV-IR and archive splits; for archive splits, CLP’s streaming reader searches bytes as they arrive without local staging or caching. Users keep the same tools instead of choosing storage engines themselves.

“Warm and cold” does not mean waiting for archive creation. [KV-IR](https://docs.yscope.com/clp/main/dev-docs/design-kv-ir-streams/index.html) advances the streaming CLP file introduced in Part I, extending its low-buffering design to semi-structured logs. Logs first land in searchable KV-IR files. Asynchronous jobs later aggregate them into immutable CLP archives for longer-term storage and search.

A query can span any mix of KV-IR files and archives needed for its time range. Where the formats overlap during aggregation, Uber’s metadata and query engine prioritize the archived copy and exclude the incorporated KV-IR input. Both formats can contribute to one result, but each event appears only once. New events generally become searchable within approximately 30 seconds to a few minutes.

The Pinot hot tier reuses CLP ideas, and tiering remains flexible. Data that no longer justifies hot indexing can be offboarded to CLP while remaining searchable. If repeated low-latency access becomes necessary again, selected data can be rehydrated into Pinot. For workloads that need both properties immediately, CLP’s low incremental storage cost also makes selective dual ingest practical.

## Bringing Native Schema-Less Data Access to Presto

YScope and Uber engineers developed and productionized the Presto Native CLP connector, combining CLP’s schema discovery and encoded search with Presto’s distributed execution.

Each CLP archive—also the basis of a Presto split—contains an archive-local schema tree and many schema-homogeneous encoded tables. Records with the same keys and value types share a table, and every schema node becomes an encoded column within it. This intrinsic columnization avoids a dataset-wide sparse union and the need to choose fields before ingestion. The same logical path can bind to several columns when schemas or types differ across archives.

An optional convenience schema exposes selected path-and-type bindings as ordinary SQL columns after archives are written. Changing it remaps logical names to existing encoded columns without rewriting data; paths outside that view remain available through schema-less accessors. CLP also separates recurring text patterns from typed variables inside strings, preserving the message-oriented compression described in Parts I and II.

Polymorphism requires explicit semantics. If `status` occurs as both an integer and a string, coercion could alter its meaning. Public typed accessors let users choose: `CLP_GET_BIGINT('status')` addresses the integer variant, while `CLP_GET_STRING('status')` addresses the string variant. Either expression can project, filter, or do both. `CLP_GET_JSON_STRING()` reconstructs a complete event, while typed `CLP_WILDCARD_*_COLUMN()` functions search values across fields the query does not enumerate.

Wildcard value search is distinct from discovering keys or projecting matching `{path, value}` pairs. Schema-tree-backed key discovery and general wildcard projection are future directions, not current public APIs. Uber production supports additional log-search operations with separate syntax. For a deeper look at CLP’s storage model, connector, and the Variant/shredding comparison, see the companion Presto article: **[PRESTO_BLOG_URL](PRESTO_BLOG_URL)**.

The following simplified query uses verified public function names. Uber’s production interface supports additional operations.

```sql
SELECT
    timestamp,
    application_id,
    CLP_GET_STRING('event.request_id') AS request_id,
    CLP_GET_BIGINT('event.status_code') AS status_code
FROM clp.platform_logs
WHERE timestamp >= current_timestamp - INTERVAL '30' DAY
  AND application_id = 'example-application'
  AND CLP_GET_BIGINT('event.status_code') >= 500
  AND CLP_WILDCARD_STRING_COLUMN() LIKE 'OutOfMemory%'
ORDER BY timestamp DESC
LIMIT 100;
```

The same `CLP_GET_BIGINT(...)` expression drives projection and filtering, while `CLP_WILDCARD_STRING_COLUMN()` searches string values without enumerating every nested path.

The functions are not record-at-a-time UDFs layered on JSON deserialization. Their Java bodies act as planner markers. The execution path is:

> **SQL expression → connector rewrite and pushdown → split-local schema resolution → encoded CLP execution**

During planning, the connector retains constant paths, requested types, and supported predicates in the scan. At each split, CLP resolves paths against split-local structure; for an archive split, that maps the paths to existing encoded columns. Unsupported residual expressions remain in Presto, while time and top-level metadata constraints can be applied before data is read.

For archive splits, CLP searches the encoded representation as compressed bytes arrive, without first reconstructing raw JSON or text. Schema, dictionary, logtype, and statistical information narrows the tables and columns that may match. CLP minimizes unrelated decompression and scanning, then materializes the projected values or matching events. A complete event requires more reconstruction than one bound path.

SQL supplies the composition model; CLP supplies the schema-less semantics and physical execution. This division makes logs native to Uber’s SQL experience without sacrificing the performance and cost advantages of the storage format.

The collaboration also helped close gaps in Presto Native’s out-of-band connector path. YScope contributors ChenXing Yang and Jack Luo led key upstream changes for plugin loading and connector serialization; IBM engineers and the broader Presto community contributed infrastructure, review, and design feedback.

## Avoid Work, Then Execute the Rest Efficiently

Metadata planning minimizes the files and splits that reach execution; split-local schema resolution then narrows the encoded tables and columns before CLP searches what remains.

The metastore is an active part of query planning. It tracks source locations, time bounds, record counts, selected top-level dimensions, available fields, and file-level statistics. Filters on an application, host, job, correlation identifier, or time range can eliminate irrelevant files before Presto schedules worker splits.

Uber’s relational implementation manages billions of file records. The connector remains metastore-agnostic; another provider can work if it supports evolving indexed fields, efficient lookup, and incremental results under a defined consistency contract.

This coarse-grained pruning answers: which files cannot contain a match? A second, independent optimization handles a common log-query shape:

```sql
ORDER BY timestamp DESC
LIMIT N
```

Logs are append-only and predominantly partitioned by time. Candidate files carry time bounds, and archives are internally timestamp-ordered. A request for the 100 latest events usually does not need every older file that could contain a match.

Presto does not guarantee split execution order, and a distributed query normally continues until its scheduled work completes. Even after finding enough recent results, a conventional plan may scan and sort older files.

For eligible queries—append-only logs, a finite limit, trustworthy time bounds, time-aware enumeration, and compatible filters—the metadata layer can terminate early during split planning. It streams candidate-file metadata in time-aware order while tracking boundaries and, where available, exact match counts or correctness-preserving upper bounds. Once the planner proves that older files cannot alter the global Top-N result, it stops requesting metadata and generating splits. If that proof is unavailable, it selects more files or falls back to the complete candidate set. Presto still schedules the selected splits normally; the optimization changes which splits exist, not their execution order.

The optimizations are independent but composable. A query can combine top-level filters—such as application, host, job, or time—with `ORDER BY timestamp DESC LIMIT N`. The metastore first prunes files that cannot satisfy those filters. Early termination then processes the surviving metadata in timestamp order and stops once older candidates cannot change the filtered Top-N result.

```text
SQL predicates
      |
Metadata pruning
(remove impossible files)
      |
Planning-time early termination
(stop when older candidates cannot change Top-N)
      |
Split-local schema and column resolution
      |
Streaming encoded CLP search
(selective reconstruction; no local staging/cache)
      |
Small result returned to Presto
```

**Figure 2: Query-work elimination. Metadata reduces the candidate set before CLP searches and projects from the selected encoded files.**

CLP’s archive layout supports this flow. The header, archive-local schema tree, dictionaries, and indexes appear up front; after initialization, the reader advances sequentially without backward seeks. The connector can therefore begin searching as bytes arrive rather than staging the complete archive. Compression reduces transferred bytes and scan work, although opening the stream and fetching encoded regions still have costs.

Together, these techniques let workers search encoded files without a secondary index for every field, local-disk staging, or an archive cache. Metadata planning proves what need not be read; CLP minimizes decompression and scanning for the remaining encoded columns. The small production footprint depends on both.

## Production Impact and Open Source

For currently onboarded workloads, the platforms integrated with CLP generate approximately 25 PB of raw logs per month. Their compressed footprint fluctuates between 750 and 850 TB, corresponding to 29–33× compression and approximately 97% less storage.

Five 48-core Presto Native workers serve the current warm and cold workload directly from object storage. Typical CPU utilization is 5–10%, and latency already supports a responsive user experience. We deliberately pace object-storage access, consuming the smallest practical share of this common service rather than pursuing throughput that would not materially improve the experience.

Searchable KV-IR files make new events available in approximately 30 seconds to a few minutes, while asynchronous aggregation produces immutable archives for longer-term access.

The Kafka-fed hot tier can experience rare loss during extreme overloads; the warm/cold path does not share this peak-loss mode. Such bursts often come from logs printed in loops. Their repetition raises CLP’s compression ratio, so raw volume can spike without a proportional rise in object-storage traffic, while the storage path retains additional throughput headroom.

The open-source [CLP Presto connector](https://github.com/y-scope/clp-plugin-presto-connector) and [CLP repository](https://github.com/y-scope/clp) provide end-to-end JSON-log compression and Presto querying. Public capabilities include mixed KV-IR/archive queries, typed exact-path access, expression rewriting, filter and projection pushdown, typed wildcard value search, complete-event JSON, and pluggable metadata and split filtering.

Uber and YScope’s production implementation adds the advanced metastore, billion-file metadata scale, planning-time early termination, routing, reliability, and operational controls developed for Uber’s environment. We intend to open-source reusable components where practical, without committing to scope or timing.

**Future direction:** CLP archive metadata—including schemas, log-format patterns, and frequencies—could preload LLMs with compact structural context, narrow candidates for semantic search, and summarize recurring behavior without processing every repeated event independently.

## Conclusion

The central result of the Uber–YScope collaboration is not simply that Presto can query CLP-compressed logs. It is that logs can retain their natural, evolving structure while gaining the projection, search, optimization, and composition expected from an analytical system.

Language-specific appenders add context without requiring application teams to rewrite existing logging configurations. CLP preserves and columnizes the spectrum from natural-language messages to nested JSON. Presto connects that data to Uber’s query ecosystem. Metadata planning avoids unnecessary work, while CLP searches the remaining encoded data. Schema-less no longer has to mean opaque, and petabyte scale does not require a petabyte-scale query cluster.

By preserving evolving structure, eliminating work early, and searching encoded data in place, Uber made 25 PB per month of logs accessible through one SQL experience—served by five lightly utilized workers.
