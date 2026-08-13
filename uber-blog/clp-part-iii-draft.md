# Bringing Native SQL to Petabyte-Scale Semi-Structured Logs: Inside Uber’s CLP and Presto Infrastructure (Part III)

At Uber, the [Compressed Log Processor (CLP) platform](https://github.com/y-scope/clp) now handles logs from machine learning and AI platforms such as [Ray](https://www.ray.io/) and [Michelangelo](https://www.uber.com/us/en/blog/michelangelo-machine-learning-platform/), streaming platforms such as [Flink](https://flink.apache.org/) and [Kafka](https://kafka.apache.org/), and data platforms such as [Spark](https://spark.apache.org/). Together, these workloads produce 25 PB of raw semi-structured logs each month—and that volume is growing. Yet five 48-core [Presto Native](https://prestodb.io/docs/current/presto-cpp.html) workers—Presto's C++ worker implementation—serve hundreds of users each week and thousands of queries per day while reading compressed data directly from object storage without a cache. They use 5–10% of CPU and memory, with low network and I/O utilization. How can such a small query cluster provide SQL access at that scale?

This is the third blog in Uber's CLP series. In [“Reducing Logging Cost by Two Orders of Magnitude Using CLP”](https://www.uber.com/en-GB/blog/reducing-logging-cost-by-two-orders-of-magnitude-using-clp/), we showed the compression and write-path savings for Spark logs and pointed toward familiar SQL through Presto. In [“Modernizing Logging at Uber with CLP (Part II)”](https://www.uber.com/gb/en/blog/modernizing-logging-with-clp-ii/), we described production ingestion, retention, and viewing. As adoption grew, SQL access naturally became the next step: engineers needed to select evolving fields, count recurring patterns, and analyze events across files and time ranges for on-call diagnosis, experimentation, and broader analytics. Here, we follow data end to end: CLP compresses arriving semi-structured data into an appendable stream searchable as it grows, asynchronously consolidates it into an immutable columnar format, and exposes both through Presto.

Compression alone does not explain how five workers provide SQL access at this scale. The system keeps arriving data compressed and queryable, executes supported SQL operations directly in CLP, and uses metadata and encoded structure to limit what each query schedules, reads, and reconstructs.

## Logs Without a Global Schema

At Uber, thousands of applications and the platforms hosting them evolve independently, often with multiple versions running at once. Their log fields, nesting, and value types change with them. An event may combine natural-language text with embedded variables, typed fields, and nested JSON. Across events, the same path may hold an integer, string, or object. We call this data **semi-structured** because each event has discoverable structure but the dataset does not share one predefined schema. It is **polymorphic** because one path can hold several value types.

We cannot require one global table schema without turning routine application changes into coordinated migrations. Storing each event as an untyped JSON string avoids those migrations, but every query must parse the string and rediscover its fields and types. CLP instead records structure as events arrive without requiring one schema for the entire dataset.

The [second blog in Uber's CLP series](https://www.uber.com/gb/en/blog/modernizing-logging-with-clp-ii/) describes the production ingestion path behind these records: it preserves existing messages while adding evolving structured context. The [μSlope paper](https://www.usenix.org/conference/osdi24/presentation/wang-rui), presented at OSDI '24 by [YScope](https://yscope.com/), the University of Toronto, and Uber, explains why CLP's storage model fits this data: storing shared structure once and grouping records by shape supports high compression and search without fully decompressing every event.

> **CLP's semantics-preserving compression:** Here, *semantics* means queryable structure—field paths, value types, record shapes, and inferred log format patterns—not natural-language meaning. For natural-language fields, CLP infers a **logtype** that preserves recurring static text and marks changing values with typed labels that may also be named. CLP encodes the variable values separately. Keeping this information explicit helps CLP compress similar values together and rule out irrelevant data before reconstructing events. Later, we explore how the same compact representation could support natural-language semantic search and pattern analysis for agents.

## Compressed and Queryable from Arrival through Archival

A query may span newly emitted events and older consolidated data. We keep both queryable through two compressed storage forms. Host log collectors use CLP to compress events into appendable [**key-value intermediate representation (KV-IR)**](https://docs.yscope.com/clp/main/dev-docs/design-kv-ir-streams/index.html) files that remain streaming-searchable as they grow. Later, our aggregation service selects groups of closed KV-IR files, and workers asynchronously consolidate each group into an immutable, columnar CLP archive. Queries continue using KV-IR while the archive is built.

KV-IR carries structure and values in the same compressed stream. When a new path or type appears, KV-IR writes a definition update before the first event that uses it. Each snapshot is self-contained and searchable through streaming reads. Configurable triggers upload a snapshot of the current KV-IR contents to object storage without closing the local stream. As more events arrive, later uploads replace that object with a newer snapshot. Once our metastore processes the accompanying metadata update, queries can find the new events, typically within 30 seconds to a few minutes of being emitted.

CLP can build archives directly from encoded KV-IR. As it streams each input, CLP merges that file's schema tree into an archive-wide tree of nested paths and value types called the **Merged Schema Tree (MST)**. Each event's schema identifies an internal set of homogeneous columns for its record shape, called an **Encoded Record Table (ERT)**. CLP appends the event as a row in that ERT. Consolidation therefore mainly merges schema trees and transposes encoded rows into ERT columns instead of reconstructing, reparsing, and re-indexing original events.

When a consolidation group contains files with similar structure and text, CLP can deduplicate recurring structure, logtypes, and values across them. Keeping files often queried together in one archive also reduces the number of archives a query must open.

![CLP storage flow from application containers through KV-IR to consolidated archives](figures/clp-storage-flow.svg)

**Figure 1: Host log collectors upload searchable KV-IR snapshots. The metastore tracks their metadata and selects closed files for asynchronous consolidation into immutable, columnar CLP archives.**

During the handoff, the same events may temporarily exist in source KV-IR and an archive. Our metastore hides this overlap by selecting one physical representation for each source file: an archive takes priority over KV-IR, and the newest archive wins if more than one contains it. A query can therefore span both storage forms without returning duplicate events.

## Native Semi-Structured Querying with Presto

At query time, engineers do not need to know whether an event is stored in KV-IR or an archive. They use either Lucene-style log searches or SQL directly. We normalize and validate both inputs before sending SQL to Presto, where existing access controls and tooling apply.

Presto must know the type of every SQL expression while planning a query. A path in CLP, however, may be absent or hold different types across events. To bridge that mismatch, an optional Presto table schema can expose common path-and-type pairs, such as `timestamp` and `event.request_id`, as ordinary typed columns without rewriting CLP data.

For example, this query returns the latest 100 events with a server-error status code under a variable path and an out-of-memory message in any string field. Its projections also show two ways to read a known request ID:

```sql
SELECT
    timestamp, -- direct access through the optional table schema
    event.request_id, -- direct access through the optional table schema
    CLP_GET_STRING('event.request_id'), -- same field through an exact-path function
    CLP_GET_JSON_STRING() AS event -- complete event without declaring its schema
FROM clp.platform_logs
WHERE timestamp >= current_timestamp - INTERVAL '30' DAY
  AND CLP_GET_BIGINT('event.*.status_code') >= 500 -- wildcard path, integer value
  AND CLP_WILDCARD_STRING_COLUMN() LIKE 'OutOfMemory%' -- string value in any field
ORDER BY timestamp DESC
LIMIT 100;
```

With or without the optional schema, the [open-source CLP connector](https://github.com/y-scope/clp-plugin-presto-connector) provides the [typed functions](https://docs.yscope.com/presto/connector/clp.html) shown above for exact paths, wildcard path segments, and value searches across all fields. These functions make the value type explicit because Presto must resolve expression types during planning. This is a constraint of Presto's current SQL type system, not a limitation of CLP's storage or search. CLP can already infer the type from predicate context during execution. Future [Variant](https://parquet.apache.org/docs/file-format/types/variantencoding/) support in Presto could expose these dynamic values through more natural field access instead of separate type-specific functions. The preceding **[PrestoDB CLP blog](PRESTO_BLOG_URL)** explores that possibility in detail.

**Native** applies at two levels. At the SQL syntax level, familiar fields remain ordinary columns. Evolving paths use targeted expressions. During planning, a connector optimizer rewrites CLP function calls into column references backed by field requests. At the execution level, the connector invokes CLP within each Presto Native worker process and passes those requests and filters to CLP's query execution. It avoids generic row materialization and per-event JSON serialization and deserialization for `JSON_EXTRACT_SCALAR()`, which would weaken vectorized columnar execution. CLP instead searches its semantics-preserving representation and reconstructs only requested results. [YScope's August 2025 blog](https://blog.yscope.com/querying-clp-compressed-logs-with-prestosql-71dff29a0b82) explains the rewriting mechanics.

## Exploiting Data Knowledge at Every Layer

The five-worker deployment is possible because three layers reduce how much data the system must consider.

**1. Prune files before scheduling.** Our metastore compares query filters with per-file metadata such as event-time bounds, application, host, job, and correlation ID. A file that cannot match never becomes a **split**—a file-processing task assigned to one worker—and is never opened. This file-pruning step is metastore-agnostic.

**2. Avoid scheduling older files for latest-N queries.** Most log searches ask for only the latest `N` matches with `ORDER BY timestamp DESC LIMIT N`. Presto normally scans every file left after pruning, then globally orders the matches to return the latest `N`. At Uber, we added a planning step that considers files newest first and stops scheduling older files once they cannot change the latest `N`. Unlike pruning, this step may skip files containing matches because those matches are determined to be too old to appear in the result. Overlapping file time ranges introduce correctness constraints and an optimization tradeoff between guaranteed and approximate modes, which the callout explains for interested readers.

> **Approximate mode.** The metastore receives pruned files in descending `max_timestamp` order and accumulates matching-event counts. When the count first reaches `N`, it sets a watermark at the lowest `min_timestamp` selected so far and returns immediately. This can schedule fewer files and reduce latency, but an omitted file overlapping the watermark may contain events that belong in the true latest `N`.
>
> **Guaranteed mode.** To preserve the true latest `N`, the metastore includes every file whose `max_timestamp` overlaps the watermark and stops at the first file below it. With exact per-file counts, later candidates cannot change the result. If any predicate lacks an exact count, guaranteed mode returns every file remaining after pruning.
>
> Both modes run before split creation, so skipped files are never scheduled or opened. If the metastore streams ordered candidates, planning can also terminate candidate retrieval early instead of materializing the full file list. The same logic runs in reverse for `ORDER BY timestamp ASC LIMIT N`. It can also account for filters such as application or host when the metastore knows how many matching events each file contains.

**3. Narrow each selected CLP scan.** The connector pushes supported filters and projections down into CLP. Within an archive, CLP uses its Merged Schema Tree (MST) to find relevant Encoded Record Tables (ERTs) and columns. Logtype and value dictionaries then rule out text patterns and values that cannot match. CLP reads the relevant encoded streams and reconstructs only matching results.

**Combined impact.** Across the production queries we measured, these three layers reduce the number of log events examined by over 200,000× on average compared with time-range filtering alone. The difference comes from files eliminated by metadata, older files excluded by latest-N planning, and events ruled out by CLP inside selected files.

When a planned query exceeds configured thresholds for file count, event count, or data volume, our query layer offers a non-blocking suggestion to narrow the timestamp range or add filters. This small nudge can prevent substantial work without blocking users who need the complete query.

## Production Results

Across workloads now using CLP, roughly 25 PB of raw logs each month is reduced to 750–850 TB, about 97% less storage.[^compression-comparison]

The five-node cluster, with one 48-core Presto Native worker per node, serves hundreds of users each week and thousands of queries per day directly from object storage. Average response time is roughly two seconds, and 99% of queries finish within 26 seconds.

The workers typically use 5–10% of CPU and memory, with low network and I/O utilization. We apply conservative rate limits to object-storage reads to control parallelism and preserve bandwidth for other workloads. Even under these limits, queries meet our latency needs, leaving headroom as demand or scope grows.

A few engineers maintain the entire system as part of their broader roles rather than as their primary responsibility.

## What Comes Next

With the production path in place, CLP's metadata and compressed structure can guide more than today's queries. We are pursuing smarter data placement and better tools for agents exploring unfamiliar data.

### Smarter Data Placement and Consolidation

**Intelligent tiering.** For suitable workloads, CLP could serve as the default queryable tier, reserving faster indexed storage for tables or time ranges whose access and latency needs justify it. CLP would retain the longer history and restore selected data to the index if demand rises.

**Grouping similar data during consolidation.** Schema, logtype, and source metadata plus query history could guide how closed KV-IR files are grouped into archives. Similar files improve deduplication, while files queried together reduce the number of archives a query opens. Time range, retention, freshness, and archive-size limits still bound each group.

### Beyond Guesswork: CLP Context for Agents

More than half of production CLP queries arrive through a [Model Context Protocol (MCP)](https://modelcontextprotocol.io/docs/getting-started/intro) server from AI agents performing root cause analysis, behavior analytics, and exploratory debugging. With unfamiliar logs, agents may not know which fields exist or which terms will find relevant events. They guess paths and keywords: narrow searches miss events, while broad searches return too much raw data.

CLP already preserves observed paths, types, and deduplicated log format patterns in compressed form. We are exploring interfaces that expose this context before agents retrieve raw events.

> ### From unfamiliar logs to three recurring failure patterns
>
> An agent reviewing [Claude Code](https://docs.anthropic.com/en/docs/claude-code/getting-started) transcripts asks: **“Which native connector load failures recur, and on which branches?”**
>
> First, `SCHEMA()` returns the observed sub-schema tree under `message`. It reveals nested paths and their types, including paths that have appeared with multiple types:
>
> ```text
> SCHEMA('message')
> └── content[*].content
>     ├── STRING
>     └── <other observed types>
> ```
>
> After identifying the string-valued message path, the agent searches it by meaning and groups matching events by format:
>
> ```sql
> SELECT
>     gitBranch AS branch, -- where the failure occurred
>     PATTERN('message.content[*].content') AS failure_pattern, -- recurring format
>     COUNT(*) AS occurrences
> FROM clp.claude_code_session_transcripts
> WHERE SEMANTIC(
>     'message.content[*].content',
>     'native connector load failure'
> ) -- match the idea across different wording
> GROUP BY
>     gitBranch,
>     PATTERN('message.content[*].content')
> ORDER BY occurrences DESC;
> ```
>
> `SEMANTIC()` finds differently worded messages expressing the same idea. `PATTERN()` replaces changing details with named variables so equivalent failures share one pattern.
>
> | branch | failure pattern | occurrences |
> | --- | --- | ---: |
> | `feature/dynamic-connector` | Failed to load connector library **`%library_path%`**: undefined symbol **`%symbol%`** | 47 |
> | `feature/dynamic-connector` | Connector factory **`%factory_name%`** was not registered | 19 |
> | `main` | Could not open shared object file **`%file_path%`** | 6 |
>
> **Result:** The agent sees three ranked failure patterns instead of 72 raw events and retrieves individual examples only when needed.

We are refining and evaluating these interfaces. Operating on CLP's compact structure before reconstructing events can narrow searches and supply useful context at petabyte scale.

## Open Source and Collaboration

[CLP](https://github.com/y-scope/clp) provides open-source semi-structured storage and search. The open-source [CLP Presto connector](https://github.com/y-scope/clp-plugin-presto-connector) exposes KV-IR and archives through Presto. It supports typed exact and wildcard paths, typed value search across fields, complete-event reconstruction, and filter and projection pushdown into CLP.

At Uber, Observability integrated the connector with the metastore and query layer. The metastore manages billions of file records, and the query layer handles routing, reliability, and operational controls. YScope worked with Uber's Presto team on internal infrastructure and Observability on planning, including Top-N file skipping.

YScope led Presto Native's CLP connector, with design input from IBM's, Meta's, and Uber's Presto teams. YScope and IBM's team also worked with the community on Presto Native's first end-to-end pluggable connector path, simplifying deployment and maintenance by keeping connectors independent of its source tree.

## Conclusion

This article is the third in Uber's CLP blog series, documenting nearly five years of collaborative work to build the CLP platform at Uber for compressing, storing, and querying application and platform logs. The [first blog](https://www.uber.com/en-GB/blog/reducing-logging-cost-by-two-orders-of-magnitude-using-clp/) showed how CLP made longer retention affordable. The [second blog](https://www.uber.com/gb/en/blog/modernizing-logging-with-clp-ii/) showed how Uber kept compressed logs fresh and easy to browse. This third blog adds native SQL across arriving KV-IR and consolidated archives. Our metastore narrows the files Presto schedules, and CLP narrows each selected scan. Together, these layers explain how five workers support queries over workloads producing 25 PB of raw logs per month.

The broader lesson is that a common SQL interface does not require one physical storage model. SQL becomes practical when the platform preserves structure from ingestion through consolidation and carries query intent into CLP-native execution. Presto handles SQL planning and distributed execution. CLP keeps arriving KV-IR and consolidated archives compressed and searchable. Together, they provide an Uber-wide query service with room to grow.

At petabyte scale, performance depends on avoiding unnecessary work and executing the remainder efficiently. Metadata-driven planning automatically eliminates much of that work, while CLP searches what remains directly in its semantics-preserving compressed representation. If a query remains too broad, the query layer gives users a gentle nudge to narrow it without blocking execution. Looking ahead, query and discovery interfaces can expose CLP's stored structure and log patterns, laying the foundation for agent-driven root-cause analysis, behavior analytics, and exploratory debugging.

[^compression-comparison]: The [first blog in the series](https://www.uber.com/en-GB/blog/reducing-logging-cost-by-two-orders-of-magnitude-using-clp/) reported 169× compression for Spark messages alone. Today's broader workload mix includes records enriched with hostnames, tags, and trace IDs (UUIDs). It averages roughly 30× compression, still about 97% less storage. High-cardinality trace IDs give compressors little repetition to exploit.
