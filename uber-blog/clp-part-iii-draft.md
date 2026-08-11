# From 25 PB of Logs to Native SQL: Modernizing Logging at Uber with CLP (Part III)

**Document status:** Working publication draft

Every month, Uber’s currently onboarded machine-learning, AI, streaming, and data platforms produce approximately 25 PB of raw logs. CLP compresses this volume to approximately 750–850 TB—a 29–33× reduction—while keeping it directly searchable. This is monthly ingest volume, not the amount read by an individual query.

Only five 48-core Presto Native workers serve recent and older CLP data from Uber’s internal object storage, typically at 5–10% CPU utilization. There is no local archive cache or disk-staging tier. Compression is essential, but it does not by itself explain how a small cluster supports an evolving corpus at this scale.

The answer spans the log lifecycle. Uber and YScope built an ingestion path that accepts independently evolving producers without coordinating one global schema. They developed a CLP connector that brings those logs into Uber’s existing Presto ecosystem without first expanding them into row-oriented JSON. Finally, metadata planning and CLP’s encoded execution eliminate enough work that most of the corpus never reaches a worker. This article follows that path from producer to SQL result.

## Evolving Logs Without a Global Schema

This is the third installment in our series on modernizing Uber’s logging infrastructure with CLP. [Part I](https://www.uber.com/en-GB/blog/reducing-logging-cost-by-two-orders-of-magnitude-using-clp/) described how streaming CLP compression reduced Spark logging costs, mitigated SSD write amplification, and enabled longer retention. [Part II](https://www.uber.com/gb/en/blog/modernizing-logging-with-clp-ii/) expanded that work into an end-to-end system for ingesting, retaining, viewing, and programmatically analyzing logs.

Since then, Uber and YScope have expanded CLP across representative platform families:

- Machine-learning and AI platforms, including Michelangelo and Ray
- Streaming platforms, including Flink and Kafka
- Data platforms, including Spark and Hudi

These platforms do not emit one stable record type. A message-oriented event may be dominated by natural language with embedded variables: “Task 42 failed on host abc.” A JSON event may contain request data, application state, and nested dimensions while still carrying a human-readable error. Historical and current versions coexist, and the same path may be an integer in one event, a string in another, and an object in a third.

In other words, the data is **semi-structured**—its fields can be discovered, but no predefined schema describes every event—and **polymorphic**—one logical path can have several types. At logging scale, schema evolution is not an occasional migration. It is a continuous property of data controlled by independently deployed producers.

Uber and YScope developed language-specific appenders and native bindings that preserve existing log messages while adding correlation, platform, application, job, host, container, and related identifiers. Existing pattern configurations can become structured fields without requiring every team to rewrite its logging statements. The underlying storage ideas are described in the [OSDI ’24 μSlope paper](https://www.usenix.org/conference/osdi24/presentation/wang-rui), jointly published by YScope, the University of Toronto, and Uber’s Observability, Pinot, and Storage and Search teams.

This producer boundary matters. A centralized table schema would make ordinary application changes into data-platform migrations. Storing every event as a single JSON value avoids that coordination, but a query system that repeatedly parses complete documents, discovers types at runtime, and materializes unused fields gives up much of the compression and selective execution that made the data affordable to retain.

## Ingestion Without Producer Coordination

The ingestion design separates two concerns that operate at different cadences: making new events searchable and creating the denser representation used for long-term retention.

A log collector runs on every host and monitors logs produced by its containers. The collector enriches each event with operational context and incrementally compresses it into an appendable [KV-IR](https://docs.yscope.com/clp/main/dev-docs/design-kv-ir-streams/index.html) file. KV-IR advances the streaming CLP file introduced in Part I, extending its low-buffering design to dynamically structured logs. The active file is already compressed as it grows; it preserves each event’s keys, value types, and recurring text structure without waiting for a dataset-wide schema.

On a configurable timeout, the collector uploads the current extent of the KV-IR file without closing the stream. It can continue appending new records to the same file; later uploads publish the newly appended data. Each update also produces a metadata event on Kafka. A metastore consumes those updates and makes the growing files discoverable to the query path. New events generally become searchable in approximately 30 seconds to a few minutes.

Asynchronous aggregation later combines the already compressed KV-IR inputs into immutable CLP archives. Aggregation does not begin compression; it transforms the streaming representation into a denser layout with the metadata and access pattern needed for long-term search. This lets the write path remain responsive. The aggregation cadence balances freshness, compression density, and object-storage request cost: smaller, more frequent publications improve freshness; larger aggregation units generally improve compression and reduce per-object overhead.

```text
Containers on every host
          |
host-local log collector
(monitor + enrich + appendable streaming KV-IR compression)
          |
          +-- configurable upload without closing the file
          |                 |
          |                 +-> Uber internal object storage: growing KV-IR file
          |                                      |
          |                                      +-> recent search
          |                                      |
          |                                      +-> asynchronous aggregation
          |                                                   |
          |                                                   +-> immutable CLP archive
          |
          +-- metadata update -> Kafka -> metastore
```

**Figure 1: Per-host ingestion lifecycle. A collector incrementally compresses an appendable KV-IR stream while periodically publishing data and metadata; denser archive construction remains asynchronous.**

The simplicity is in the contract, not in pretending every producer is identical. Producers can continue emitting different shapes. The ingestion layer preserves those differences, adds stable operational dimensions where available, and moves expensive archive construction off the freshness-critical path. The same storage model therefore accommodates natural-language logs, enriched application events, and deeply nested JSON.

## One SQL Entry Point Across Recent and Archived Data

Engineers investigating a failure want SQL projection, filtering, aggregation, ordering, and joins. They should not need to know which file representation contains a given time range or which backend is best suited to the request.

Uber’s observability interfaces, platform query builders, and programmatic clients already converge on a Presto-based query ecosystem. QueryRunner and related services parse and govern queries, select a cluster, route work, and provide failover. For CLP data, the metastore identifies relevant recent KV-IR files and immutable archives, and Presto Native executes the resulting splits. The metadata backend is deliberately a black box at this layer; users and connector authors depend on its contract rather than its internal implementation.

A CLP query can contain any mix of KV-IR and archive splits required for its time range. Archive aggregation temporarily creates overlapping coverage. Uber’s metadata gives the archive precedence and excludes only the KV-IR inputs incorporated into it, so recent files and archives can contribute to one result without returning an event twice. Users keep one SQL-oriented experience while the platform resolves the physical representation.

That experience is valuable only if the connector preserves the storage model underneath it. A naive compatibility layer could reconstruct every event as JSON and then let Presto evaluate expressions row by row. At Uber’s scale, that would expand encoded data before filtering and throw away much of CLP’s structural locality. The connector instead carries supported SQL intent into CLP.

## From Polymorphic Logs to Native SQL

Presto normally expects column names and result types during planning. CLP archives do not impose one dataset-wide table. Each immutable archive contains an archive-local schema tree and many schema-homogeneous encoded tables. Records with the same keys and value types share a table, and every observed schema node becomes an encoded column within it. One logical path can therefore bind to several columns across tables or archives without creating one globally sparse table.

CLP also finds structure inside natural-language values. It separates recurring log text from typed variables, improving compression and allowing search to operate on the encoded representation. An optional convenience schema can expose selected path-and-type bindings as ordinary SQL columns after ingestion; changing that view does not rewrite the underlying archive.

For paths outside that view, typed accessors give Presto a stable result type without erasing polymorphism. If `status` appears as both an integer and a string, `CLP_GET_BIGINT('status')` and `CLP_GET_STRING('status')` address the two variants explicitly. `CLP_GET_JSON_STRING()` reconstructs a complete event when needed. Typed `CLP_WILDCARD_*_COLUMN()` functions search values across fields even when the query does not know the key.

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

The functions look like UDFs, but their Java bodies are planner markers rather than record-at-a-time JSON evaluators. The connector recognizes supported constant paths, requested types, predicates, and projections, then retains them in the scan:

> **SQL expression → connector rewrite and pushdown → split-local schema resolution → encoded CLP execution**

At each split—the unit of data assigned to a Presto worker—the connector resolves paths against that file’s structure. For an archive split, CLP maps them to the relevant encoded tables and columns. Unsupported residual expressions remain in Presto. Selecting a complete event requires more reconstruction than projecting one bound path, but both use the same query surface.

The connector runs in-process inside each Presto Native worker and links to CLP’s native reader. For archive splits, it opens an object-storage stream and CLP interprets and searches compressed bytes as they arrive, without reconstructing raw JSON first or staging the complete archive on local disk. SQL supplies the composition model; CLP supplies the semi-structured semantics and encoded execution.

For a deeper treatment of CLP’s storage model, the connector implementation, and its relationship to Variant and shredding, see the companion Presto article: **[PRESTO_BLOG_URL](PRESTO_BLOG_URL)**.

## Search by Eliminating Work

At this volume, a query should read only the small fraction of the corpus that could affect its result. The production architecture reduces work at three levels: files, splits, and encoded data.

First, Uber’s metastore participates in query planning. It tracks source locations, formats, time bounds, record counts, selected top-level dimensions, available paths, and file-level statistics. Filters on time, application, host, job, or correlation identifiers can eliminate files before Presto creates worker splits or opens object-storage streams. Uber’s implementation manages metadata for billions of files; the public connector remains provider-agnostic.

Second, eligible newest-first queries can terminate split planning early:

```sql
ORDER BY timestamp DESC
LIMIT N
```

Pruning proves that a file cannot match a predicate. Early termination proves something different: an older candidate might match, but it cannot change the requested Top-N. Correctness depends on append-only data, trustworthy file time bounds and record counts, time-aware metadata enumeration, a finite limit, and filters compatible with the proof. The planner stops producing older splits only after it has established that they cannot change the result. Otherwise, it safely plans more data or falls back to the complete candidate set. This optimization does not depend on Presto executing splits in order.

Finally, split-local schema resolution narrows the encoded tables and columns inside each selected archive. CLP’s front-loaded schema, dictionary, logtype, and index metadata lets its reader begin interpreting the archive as bytes arrive and then advance without backward seeks. Supported filters and projections run against the encoded representation; only requested values or matching events are reconstructed.

```text
SQL predicates and Top-N
          |
file-level metadata pruning
          |
correctness-preserving split early termination
          |
split-local table and column resolution
          |
streaming encoded CLP search
          |
small result returned to Presto
```

**Figure 2: Search work is removed from coarse to fine granularity before CLP materializes results.**

This is what distinguishes the connector from a syntax translator. It does not make every query cheap, and broad searches can still read substantial data. It does, however, preserve opportunities to avoid object opens, transfers, decompression, parsing, and materialization at the layer best equipped to make each decision.

## Production Impact, Open Source, and Collaboration

For currently onboarded workloads, CLP reduces approximately 25 PB of raw monthly logs to 750–850 TB—roughly 29–33× compression and about 97% less storage. Five 48-core Presto Native workers serve recent and older data directly from Uber’s internal object storage, typically at 5–10% CPU utilization. Because latency already supports a responsive user experience, Uber deliberately paces object-storage access to consume the smallest practical share of that shared infrastructure rather than maximizing throughput for its own sake.

The open-source [CLP Presto connector](https://github.com/y-scope/clp-plugin-presto-connector) and [CLP repository](https://github.com/y-scope/clp) provide end-to-end JSON-log compression and Presto querying. Public capabilities include mixed KV-IR/archive queries, typed exact-path access, expression rewriting, filter and projection pushdown, typed wildcard value search across unknown keys, complete-event JSON, and pluggable metadata and split filtering.

Uber’s production deployment adds its advanced metastore, billion-file metadata scale, planning-time early termination, routing, reliability, and operational controls. These distinctions matter: the article should show what is available to open-source users without presenting every Uber-specific extension as a public connector feature.

YScope engineers led the connector implementation. IBM engineers and the Presto community supported the native connector infrastructure and review; YScope and Uber engineers extended and applied the architecture in production. The collaboration’s architectural result is not simply that Presto can read another file format. It is that storage-specific execution can fit behind a common SQL interface without forcing the storage model to become generic.

## Conclusion

Uber’s logging platforms had three connected problems. Independently evolving producers could not coordinate one complete table schema. Engineers still needed familiar SQL semantics for diagnosis and analysis. At production volume, the query path had to avoid expanding or scanning most of the retained data.

The resulting system addresses those problems as one lifecycle. Appenders and KV-IR preserve evolving structure and compress it incrementally on the freshness-critical ingestion path. Asynchronous aggregation transforms those streams into denser, immutable CLP archives. The Presto Native connector maps SQL intent to split-local structure and CLP’s encoded execution, while metadata pruning and early termination prevent unnecessary files from reaching workers.

That combination—not compression, SQL compatibility, or cluster size in isolation—is the transferable lesson. Semi-structured logs can remain faithful to their producers, become part of an analytical SQL ecosystem, and still be served economically at production scale.
