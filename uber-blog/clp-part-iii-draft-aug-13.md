# Bringing Native SQL to Petabyte-Scale Semi-Structured Logs: Inside Uber’s CLP and Presto Infrastructure (Part III)

At Uber, the [Compressed Log Processor (CLP) platform](https://github.com/y-scope/clp) now handles logs from machine learning and AI platforms such as [Ray](https://www.ray.io/) and [Michelangelo](https://www.uber.com/us/en/blog/michelangelo-machine-learning-platform/), streaming platforms such as [Flink](https://flink.apache.org/) and [Kafka](https://kafka.apache.org/), and data platforms such as [Spark](https://spark.apache.org/). Together, these workloads produce 25 PB of raw semi-structured logs each month—and that volume is growing. Yet five 48-core [Presto Native](https://prestodb.io/docs/current/presto-cpp.html) workers—PrestoDB's C++ workers built on Meta's [Velox](https://velox-lib.io/) execution engine—serve queries over the resulting compressed corpus, streaming only selected files from object storage rather than preloading complete archives into a dedicated cache. How can such a small query cluster provide SQL access at that scale?

This is the third blog in Uber's CLP series. In [“Reducing Logging Cost by Two Orders of Magnitude Using CLP”](https://www.uber.com/en-GB/blog/reducing-logging-cost-by-two-orders-of-magnitude-using-clp/), we showed the compression and write-path savings for Spark logs and pointed toward familiar SQL through Presto. In [“Modernizing Logging at Uber with CLP (Part II)”](https://www.uber.com/gb/en/blog/modernizing-logging-with-clp-ii/), we described production ingestion, retention, and viewing. As adoption grew, SQL access naturally became the next step: engineers needed to select evolving fields, count recurring patterns, and analyze events across files and time ranges for on-call diagnosis, experimentation, and broader analytics. Here, we follow data end to end: CLP compresses arriving semi-structured data into an appendable stream searchable as it grows, asynchronously consolidates it into an immutable columnar format, and exposes both through Presto.

Compression alone does not explain how five workers provide SQL access at this scale. The system keeps arriving data compressed and queryable, executes supported SQL operations directly in CLP, and uses metadata and encoded structure to limit what each query schedules, reads, and reconstructs.

## Logs Without a Global Schema

At Uber, thousands of applications and the platforms hosting them evolve independently, often with multiple versions running at once. Their log fields, nesting, and value types change with them. An event may combine natural-language text with embedded variables, typed fields, and nested JSON. Across events, the same path may hold an integer, string, or object. We call this data **semi-structured** because each event has discoverable structure but the dataset does not share one predefined schema. It is **polymorphic** because one path can hold several value types.

We cannot require one global table schema without turning routine application changes into coordinated migrations. Storing each event as an untyped JSON string avoids those migrations, but every query must parse the string and rediscover its fields and types. CLP instead records structure as events arrive without requiring one schema for the entire dataset.

The [second blog in Uber's CLP series](https://www.uber.com/gb/en/blog/modernizing-logging-with-clp-ii/) describes the production ingestion path behind these records: it preserves existing messages while adding evolving structured context. The [μSlope paper](https://www.usenix.org/conference/osdi24/presentation/wang-rui), presented at OSDI '24 by [YScope](https://yscope.com/), the University of Toronto, and Uber, explains why CLP's storage model fits this data. By storing shared structure once and grouping records by shape, CLP supports high compression and search without fully decompressing every event.

> **CLP's semantic-preserving compression** makes data smaller while retaining queryable meaning. CLP preserves paths and types, discovers which fields occur together, and infers logtypes that separate recurring static text from variable values, preserve their types, and optionally label their roles. These are real semantics within semi-structured data, even though they are not a complete interpretation of natural language. CLP uses them to compress related values together and rule out irrelevant data before reconstructing events. The same preserved signals can support semantic search, pattern analysis, and agent context.

## Compressed and Queryable from Arrival through Archival

A query may span newly emitted events and older consolidated data. We keep both queryable through two compressed storage forms. Host log collectors use CLP to compress events into appendable [**key-value intermediate representation (KV-IR)**](https://docs.yscope.com/clp/main/dev-docs/design-kv-ir-streams/index.html) files that remain streaming-searchable as they grow. Later, our aggregation service selects groups of closed KV-IR files, and workers asynchronously consolidate each group into an immutable, columnar CLP archive. Queries continue using KV-IR while the archive is built.

KV-IR carries structure and values in the same compressed stream. When a new path or type appears, KV-IR writes a definition update before the first event that uses it. Each snapshot is self-contained and searchable through streaming reads. KV-IR is prefix-decodable: CLP can search every complete event available in the stream even when the tail is incomplete. Configurable triggers upload a snapshot of the current KV-IR contents to object storage without closing the local stream. As more events arrive, later uploads atomically replace that object with a newer snapshot. Once our metastore processes the accompanying metadata update, queries can find the new events, typically within 30 seconds to a few minutes of being emitted.

CLP can build archives directly from encoded KV-IR. As it streams each input, CLP merges that file's schema tree into an archive-wide tree of nested paths and value types called the **Merged Schema Tree (MST)**.

Think of an archive as a collection of compact internal tables rather than one wide table. CLP calls each internal table an **Encoded Record Table (ERT)**. Each ERT is a column group for records sharing the same paths and value types, so absent fields do not create null entries. CLP appends encoded values directly to the matching ERT columns instead of reconstructing and reparsing the original events. It also separates recurring static text from typed variables, storing each inferred format as a **logtype**.

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

With or without the optional schema, the [open-source CLP connector](https://github.com/y-scope/clp-plugin-presto-connector) provides the [typed functions](https://docs.yscope.com/presto/connector/clp.html) shown above for exact paths, wildcard path segments, and value searches across all fields. These functions make the value type explicit because Presto must resolve expression types during planning, even though CLP can infer the type from predicate context during execution. That requirement comes from Presto's current SQL type system, not from CLP's storage or search.

At the table and file layers, [Iceberg v3 Variant](https://iceberg.apache.org/spec/#semi-structured-types) and [Parquet Variant](https://parquet.apache.org/docs/file-format/types/variantencoding/) provide a standardized, interoperable representation for semi-structured values. Parquet stores the complete dynamic value, while optional [shredding](https://parquet.apache.org/docs/file-format/types/variantshredding/) materializes selected paths as typed columns. CLP starts from a different physical model: it discovers and encodes every observed path and type as data arrives, then columnizes records with the same shape together in immutable archives. Across both storage forms, CLP separates recurring text patterns from typed variable values. This supports log-specific compression and wildcard path or value searches without selecting hot paths before data is written. Presto does not yet support Variant end to end, but future support could give CLP paths more natural SQL access without replacing CLP's physical storage model. The preceding **[PrestoDB CLP blog](PRESTO_BLOG_URL)** compares the two complementary models in detail.

Here, **native** applies at two levels. At the SQL interface, common paths appear as ordinary typed columns and evolving paths use typed CLP functions. In both cases, field access does not require first materializing and decoding each complete event as JSON. During execution, the connector turns the typed functions into field requests and passes them, together with supported filters, directly to CLP inside each Presto Native worker. CLP evaluates those requests against its compressed representation and reconstructs only the requested results. [YScope's August 2025 blog](https://blog.yscope.com/querying-clp-compressed-logs-with-prestosql-71dff29a0b82) explains the optimizer rewriting in detail.

## Exploiting Data Knowledge at Every Layer

The five-worker deployment is possible because three layers reduce how much data the system must consider.

**1. Prune files before scheduling.** Our metastore compares query filters with per-file metadata such as event-time bounds, application, host, job, and correlation ID. A file that cannot match never becomes a **split**—a file-processing task assigned to one worker—and is never opened. The connector is agnostic to its metadata provider. Uber's provider performs query-aware file planning, including these filters and the latest-N optimization below.

**2. Avoid scheduling older files for latest-N queries.** Most log searches ask for only the latest `N` matches with `ORDER BY timestamp DESC LIMIT N`. Without latest-N-aware file selection, Presto workers must process every split remaining after ordinary pruning before distributed Top-N can determine the latest `N`. At Uber, we added a planning step that considers files newest first and can stop selecting files before every older candidate becomes a split. Unlike pruning, a file skipped by this step may still contain matches. Determining where to stop is more nuanced than simply reaching `N` matching events because file time ranges can overlap. The callout explains the guaranteed and approximate modes for readers who want the details.

> **Approximate mode.** The metastore receives pruned files in descending `max_timestamp` order and accumulates matching-event counts. When the count first reaches `N`, it sets a watermark at the lowest `min_timestamp` selected so far and returns immediately. This can schedule fewer files and reduce latency, but an omitted file overlapping the watermark may contain events that belong in the true latest `N`.
>
> **Guaranteed mode.** With candidate files ordered by accurate bounds for the query's timestamp field, the metastore includes every file whose `max_timestamp` overlaps the watermark and stops at the first file below it. Exact matching-event counts must account for every remaining predicate. If any predicate lacks an exact count, guaranteed mode returns every file remaining after pruning.
>
> Both modes run before split creation, so skipped files are never scheduled or opened. If the metastore streams ordered candidates, planning can also terminate candidate retrieval early instead of materializing the full file list. The same logic runs in reverse for `ORDER BY timestamp ASC LIMIT N`. It can also account for filters such as application or host when the metastore knows how many matching events each file contains.

**3. Narrow each selected CLP scan.** The connector pushes supported filters and projections down into CLP. Within an archive, CLP uses its Merged Schema Tree (MST) to find relevant Encoded Record Tables (ERTs) and columns. Logtype and value dictionaries then rule out text patterns and values that cannot match. CLP reads the relevant encoded streams and reconstructs only matching results.

**Combined impact.** Across a 30-day measurement window on our production cluster, the aggregate event count covered by time-range filtering alone was more than 200,000× the count examined after these three layers.[^work-reduction] The difference comes from files eliminated by metadata, older files excluded by latest-N planning, and events ruled out by CLP inside selected files.

When a planned query exceeds configured thresholds for file count, event count, or data volume, our query layer offers a non-blocking suggestion to narrow the timestamp range or add filters. This small nudge can prevent substantial work without blocking users who need the complete query.

## Production Results

Across workloads now using CLP, approximately 25 PB of logical uncompressed log data per month occupies 750–850 TB in object storage, a 29–33× reduction in physical storage—about 97%.[^compression-comparison]

Over the same 30-day measurement window, the five-node cluster—with one 48-core Presto Native worker per node—served hundreds of users per week and thousands of successful queries per day directly from object storage. Among successful queries, mean response time was roughly two seconds and p99 was 26 seconds.

CLP's storage model is designed for unusually high compression while remaining directly searchable. Operating the production query service introduces a different set of tradeoffs. We balance interactive query latency, efficient resource use, limited impact on shared object storage, and operational simplicity rather than pursuing the lowest possible latency or maximum scan throughput at any cost. Conservative read-rate limits control parallelism and preserve bandwidth for other workloads. Over a longer production operating period, workers have typically used 5–10% of their CPU and memory capacity under the current query mix and object-storage read limits. Network and I/O utilization has likewise remained well below available capacity. This leaves compute headroom as demand grows and as we expand the system's scope, including the storage and agent-focused query work described later in this article.

Operational overhead also remains low: a few engineers maintain the system alongside their broader platform responsibilities.

## What Comes Next

With the production path in place, we are excited to build on the metadata and compressed structure CLP already preserves. Two directions stand out: using that knowledge to organize data more intelligently and exposing it to agents so they can explore unfamiliar data with less guesswork.

### Smarter Data Placement and Consolidation

**Intelligent tiering.** The same balance guides future tiering. For suitable workloads, CLP could remain the default queryable tier, while a faster indexed tier is provisioned for tables or time ranges whose access patterns and latency needs justify it. CLP would retain the longer history and could repopulate the index if demand rises.

**Grouping similar data during consolidation.** The same schema, logtype, source, and query-history metadata could guide how closed KV-IR files are grouped into archives. Similar files improve deduplication, while files queried together reduce the number of archives a query opens. Time range, retention, freshness, and archive-size limits still bound each group.

### Beyond Guesswork: CLP Context for Agents

Production traffic trends over a longer period show that AI agents issuing queries through a [Model Context Protocol (MCP)](https://modelcontextprotocol.io/docs/getting-started/intro) server now account for more than half of successful CLP queries by count. These agents use CLP for root cause analysis, behavior analytics, and exploratory debugging. When the logs are unfamiliar, however, agents may not know which fields exist or which terms will find relevant events. A narrow guess can miss events, while a broad one can return too much raw data.

CLP already preserves observed paths, types, and deduplicated log format patterns in compressed form. Because each pattern remains connected to its field, typed variables, and source events, the underlying CLP mechanisms can count patterns within a filtered query directly over the original data in its losslessly compressed representation. Building on those existing mechanisms, we are refining interfaces that let agents inspect available structure, search messages by semantic meaning, and group matches by recurring format before retrieving raw events. The following example illustrates how these interfaces could work together.

> ### From unfamiliar logs to three recurring failure patterns
>
> An agent reviewing coding-agent session logs asks: **“Which native connector load failures recur, and on which branches?”**
>
> Coding-agent sessions can contain polymorphic, nested message content. Rather than guess where the relevant text is stored, the agent could first use `SCHEMA()` to inspect the observed paths and types under `message`:
>
> ```text
> SCHEMA('message')
> └── content[*].content
>     ├── STRING
>     └── <other observed types>
> ```
>
> After identifying the string-valued message path, the agent could use `SEMANTIC()` to find load failures expressed in different words. `PATTERN()` would replace changing details with named variables so equivalent failures could be grouped together:
>
> ```sql
> SELECT
>     git_branch AS branch, -- where the failure occurred
>     PATTERN('message.content[*].content') AS failure_pattern, -- recurring format
>     COUNT(*) AS occurrences
> FROM clp.coding_agent_sessions
> WHERE SEMANTIC(
>     'message.content[*].content',
>     'native connector load failure'
> ) -- match the idea across different wording
> GROUP BY
>     git_branch,
>     PATTERN('message.content[*].content')
> ORDER BY occurrences DESC;
> ```
>
> **Illustrative output:**
>
> | branch | failure pattern | occurrences |
> | --- | --- | ---: |
> | `feature/dynamic-connector` | Failed to load connector library **`%library_path%`**: undefined symbol **`%symbol%`** | 47 |
> | `feature/dynamic-connector` | Connector factory **`%factory_name%`** was not registered | 19 |
> | `main` | Could not open shared object file **`%file_path%`** | 6 |
>
> **In this example:** The agent sees three ranked failure patterns instead of 72 raw events and retrieves individual examples only when needed.

We are refining and evaluating these interfaces for both Uber's workloads and broader CLP use cases. They build on CLP's existing storage and search foundation, using its compact structure to narrow searches and supply useful context before reconstructing individual events at petabyte scale.

## Open Source and Collaboration

[CLP](https://github.com/y-scope/clp) provides open-source semi-structured storage and search. The open-source [CLP Presto connector](https://github.com/y-scope/clp-plugin-presto-connector) exposes KV-IR and archives through Presto. It supports typed exact and wildcard paths, typed value search across fields, complete-event reconstruction, and filter and projection pushdown into CLP.

At Uber, Observability integrated the connector with the metastore and query layer. The metastore manages billions of file records, and the query layer handles routing, reliability, and operational controls. YScope worked with Uber's Presto team on internal infrastructure and Observability on planning, including Top-N file skipping.

YScope led development of the CLP connector for Presto Native, with design input from Presto teams at Uber and IBM, as well as Meta's Presto and Velox teams. In parallel, YScope implemented and submitted the upstream changes for [runtime connector registration](https://github.com/prestodb/presto/pull/27653) and [connector-specific coordinator-to-worker serialization](https://github.com/prestodb/presto/pull/27652), with review from IBM's Presto team. These changes made CLP the first end-to-end pluggable connector for Presto Native. Keeping native connectors independent of the Presto Native source tree shortens development and release cycles while simplifying deployment and maintenance.

## Conclusion

This is the third article in Uber's CLP series. Together, the three articles document nearly five years of collaboration to build the CLP platform for compressing, storing, and querying Uber's application and platform logs. The [first blog](https://www.uber.com/en-GB/blog/reducing-logging-cost-by-two-orders-of-magnitude-using-clp/) showed how CLP made longer retention affordable. The [second blog](https://www.uber.com/gb/en/blog/modernizing-logging-with-clp-ii/) showed how Uber kept compressed logs fresh and easy to browse. This third blog adds native SQL across arriving KV-IR and consolidated archives. Our metastore narrows the files Presto schedules, and CLP narrows each selected scan. Together, these layers explain how five workers support queries over workloads producing 25 PB of raw logs per month.

The broader lesson is that a common SQL interface does not require one physical storage model. SQL becomes practical when the platform preserves structure from ingestion through consolidation and carries query intent into CLP-native execution. Presto handles SQL planning and distributed execution. CLP keeps arriving KV-IR and consolidated archives compressed and searchable. Together, they provide an Uber-wide query service for workloads onboarded to CLP, with room to grow.

At petabyte scale, performance depends on avoiding unnecessary work and executing the remainder efficiently. Metadata-driven planning automatically eliminates much of that work, while CLP searches what remains directly in its semantic-preserving compressed representation. If a query remains too broad, the query layer gives users a gentle nudge to narrow it without blocking execution. Looking ahead, query and discovery interfaces can expose CLP's stored structure and log patterns, laying the foundation for agent-driven root-cause analysis, behavior analytics, and exploratory debugging.

[^compression-comparison]: The [first blog in the series](https://www.uber.com/en-GB/blog/reducing-logging-cost-by-two-orders-of-magnitude-using-clp/) reported 169× compression for Spark messages alone. Today's broader workload mix includes records enriched with hostnames, tags, and trace IDs (UUIDs). It averages roughly 30× compression, still about 97% less storage. High-cardinality trace IDs give compressors little repetition to exploit.

[^work-reduction]: We divided the total number of events covered by each successful query's time range, summed across the measurement window, by the total number examined after metadata pruning, guaranteed latest-N file selection where applicable, and CLP filtering for the same queries.
