# Bringing Native SQL to Petabyte-Scale Semi-Structured Logs: Inside Uber’s CLP and Presto Infrastructure (Part III)

At Uber, the [CLP platform](https://github.com/y-scope/clp) now handles semi-structured logs from machine learning and AI platforms such as [Ray](https://www.ray.io/) and [Michelangelo](https://www.uber.com/us/en/blog/michelangelo-machine-learning-platform/), streaming platforms such as [Flink](https://flink.apache.org/) and [Kafka](https://kafka.apache.org/), and data analytics platforms such as [Spark](https://spark.apache.org/). Together, these workloads produce about 25 PB of logs per month before compression. Yet just five 48-core [Presto Native](https://prestodb.io/docs/current/presto-cpp.html) workers serve hundreds of users each week and execute thousands of successful queries each day. How can such a small cluster make interactive SQL practical at that scale?

This is the third blog in Uber's CLP series. [“Reducing Logging Cost by Two Orders of Magnitude Using CLP”](https://www.uber.com/en-GB/blog/reducing-logging-cost-by-two-orders-of-magnitude-using-clp/) showed how streaming CLP compression reduced Spark-log storage and SSD writes, and identified familiar SQL access through Presto as a next step. [“Modernizing Logging at Uber with CLP (Part II)”](https://www.uber.com/gb/en/blog/modernizing-logging-with-clp-ii/) described production ingestion, retention, and viewing. Broader adoption increased both what engineers wanted to do with the data and how much data each analysis could span. A question that once involved a few log files could now cover many hosts, application versions, and time ranges. Whether diagnosing an incident, evaluating an experiment, or exploring system behavior, engineers often begin with a symptom or question rather than an exact field or message. They need to find relevant events, inspect fields even as those fields evolve, group and count recurring patterns, and compare behavior across the selected data. They may also need to join logs with related datasets. Uber's mature query ecosystem around Presto, with established governance, access control, and tooling, was the natural place to provide these capabilities.

Supporting those analytical needs at this scale and efficiency required more than exposing CLP through SQL. We had to make semi-structured data feel natural to query while handling evolving structures and conflicting field types, keeping new events searchable, and improving storage efficiency over time. Simple time-range pruning also could not eliminate enough work, so we built multiple layers that reduce the data considered before and during CLP scans. This article follows that design from storage through query execution, then shows how the same CLP metadata could support future evidence-based agent analysis.

## Organizing Evolving Logs for Columnar Compression and Search

The first requirement is a physical layout that achieves high compression for evolving events while supporting efficient field-level search. Uber's logging platform cannot impose one stable schema because it does not control each event's structure. Even within one application, events may contain arbitrary combinations of fields, nested structures, and value types. Separate release schedules and multiple active versions add further variation. Users still expect to query any field efficiently. Three common storage models show why satisfying these requirements together is difficult.

**Documents.** A document representation, such as JSON, preserves a complete event as its fields and types change. That flexibility is useful, but in a document-only layout, filtering requires inspecting each document, locating the requested path if it exists, checking its type, retrieving the value, and then testing it. Repeating this work for every document makes field-level search slow.

**Typed columns.** When a column is defined, its value type is fixed. A columnar format such as Parquet stores a column's values one after another, so a filter can scan them directly without navigating a document for every row. This makes filtering and compression efficient. The tradeoff is that the writer must choose the columns and their types before writing the file. If `status_code` is an integer in one event and a string in another, one typed column cannot preserve both original types, so the writer must use a common representation or separate columns.

**Hybrid layouts.** A common optimization retains stable, frequently queried paths as typed columns while placing the evolving long tail in a document. Those paths and types must be chosen before writing each file and kept aligned with changes from log-producing applications. Unknown paths and values whose types no longer match remain in the document.

Uber instead stores these semi-structured logs in the CLP format. Its physical layout combines the flexibility of complete documents with columnar compression and search efficiency. CLP losslessly preserves the path of each source log file, every event's structure and values, and the original order of events within that file. It requires neither a global schema nor a predefined list of typed fields.

Think of a CLP archive as a collection of small internal tables. CLP puts events with the same structure into the same table: they have the same nested field paths, and each field has the same value type. Each field is stored as a column of encoded values. For natural-language fields, CLP goes a step further by separating the static text shared by similar messages into patterns, called **logtypes**, and storing the changing values as typed variables. Both are encoded in columnar form. An archive map tells CLP which tables and columns contain a requested path and type.

This layout fits Uber's workload because every path and type observed within an archive receives columnar storage without coordinating schema changes or choosing important fields in advance. Grouping events with the same structure stores their shared schema once and often places similar logs and values together. That locality gives compression more repetition to exploit. Logtype and variable dictionaries further deduplicate recurring text and values.

The same organization narrows search. The archive map limits an exact or wildcard path to tables and columns that contain it. Dictionary metadata then rules out text patterns and values that cannot match. CLP returns requested fields directly and reconstructs a complete event only when the query asks for it. The [μSlope paper from OSDI '24](https://www.usenix.org/conference/osdi24/presentation/wang-rui) describes the formal layout.

This archive layout organizes evolving logs for both compression and search. New events, however, must remain searchable before they are ready for archive consolidation.

## Compressed and Queryable from Arrival through Archival

Incident diagnosis, rollout validation, and active-job monitoring all depend on recent data. Uber therefore keeps two CLP storage forms queryable: an appendable representation for arriving data and archives for consolidated data.

[**Key-value intermediate representation (KV-IR)**](https://docs.yscope.com/clp/main/dev-docs/design-kv-ir-streams/index.html) **preserves data freshness:** appendable files keep arriving data compressed and searchable without waiting for an archive. **Archives improve long-term storage and search efficiency:** asynchronous consolidation combines closed files for better compression and faster columnar search.

![CLP storage flow from application containers through KV-IR to consolidated archives](figures/clp-storage-flow.svg)

**Figure 1: New logs remain compressed and queryable in KV-IR within minutes. Closed files are later consolidated into columnar archives for better compression and faster search, while the metastore lets queries span both forms without duplicates.**

Host log collectors write arriving events to KV-IR, which carries structure and values in the same compressed stream. A path-and-type definition appears before the first event that uses it, making each uploaded snapshot self-contained and searchable. Configurable triggers upload updated snapshots without closing the local stream. Once our metastore processes the accompanying metadata, new events become queryable, typically within 30 seconds to a few minutes of being emitted.

After KV-IR files close, they remain queryable while our aggregation service waits until it can form a suitable consolidation group. It assigns that group to a consolidation worker, which builds an archive asynchronously. Consolidating multiple files lets CLP deduplicate recurring structure, text, and values across file boundaries. Queries keep reading the KV-IR inputs until the archive is ready, so consolidation does not delay access to the data. The work consumes background CPU and I/O but stays off the ingestion and query critical paths.

Consolidation is efficient because CLP reuses encoded KV-IR instead of reconstructing and reparsing the original events. As it streams the inputs, CLP merges their structure maps and appends encoded values directly to matching internal columns. It also deduplicates recurring logtypes and values across files. This extends the physical organization described above across a larger body of data, improving compression and search without moving that work onto the ingestion or query critical paths.

Until source KV-IR files are purged, both forms may contain the same events. To prevent duplicate query results, our metastore selects one representation for each logical log file. If that file appears in both KV-IR and an archive, the archive takes priority. If it appears in multiple archives, the newest archive wins.

## Native Semi-Structured Querying with Presto

Adding SQL syntax alone would be straightforward. The harder requirement is native field access without reconstructing complete documents row by row. Here, **native** means preserving CLP's storage model: the Java coordinator plans each query and asks the connector's metadata layer for splits, while the dynamically loaded connector inside each Presto Native worker converts typed field requests and supported filters into CLP operations over compressed data. Presto Native is PrestoDB's C++ worker implementation, built on Meta's [Velox](https://velox-lib.io/) execution engine.

Engineers do not need to know whether an event is stored in KV-IR or an archive. They submit either Lucene-style field-and-keyword searches or SQL directly. We normalize and validate both inputs before sending SQL to Presto, where existing access controls and tooling apply.

Presto must know the type of every SQL expression during planning, while a CLP path may be absent or hold different types across events. An optional Presto table schema can expose common path-and-type pairs, such as `timestamp` and `event.request_id`, as ordinary columns without rewriting CLP data. Any path can also be accessed through typed functions.

The following query shows both access forms, wildcard path and value searches, and complete-event reconstruction:

```sql
SELECT
    timestamp, -- optional-schema column
    event.request_id, -- optional-schema string column
    CLP_GET_STRING('event.request_id'), -- same path through a typed function
    CLP_GET_JSON_STRING() AS event -- complete event without a declared schema
FROM clp.platform_logs
WHERE timestamp >= current_timestamp - INTERVAL '30' DAY
  AND CLP_GET_BIGINT('event.*.status_code') >= 500 -- wildcard path with integer values
  AND CLP_WILDCARD_STRING_COLUMN() LIKE 'OutOfMemory%' -- string in any field
ORDER BY timestamp DESC
LIMIT 100;
```

The [open-source CLP connector](https://github.com/y-scope/clp-plugin-presto-connector) provides these [typed functions](https://docs.yscope.com/presto/connector/clp.html) for exact paths, wildcard path segments, and value searches across fields. Their names declare a return type because Presto needs it during planning. CLP can infer the type from a filter during execution, and exposing that inference through the connector remains on our todo list. Optional columns and typed functions use the same CLP-native execution path.

PrestoDB does not yet support Variant end to end, so the connector currently uses typed functions for dynamic paths. The preceding **[PrestoDB CLP blog](PRESTO_BLOG_URL)** compares the storage models, while [YScope's August 2025 blog](https://blog.yscope.com/querying-clp-compressed-logs-with-prestosql-71dff29a0b82) explains the optimizer rewriting retained by the current connector.

## How Five Workers Serve the Workload: Avoiding Work at Every Layer

Typed SQL explains how engineers use the system. Avoiding work explains why five workers can serve it. A query returning only 100 rows can still be expensive if Presto first schedules every file in a 30-day range and examines every candidate event.

Across a 30-day measurement window on our production cluster, the aggregate event count covered by time-range filtering alone was more than 200,000× the count examined after three layers of work reduction.[^work-reduction] The achievement is not scanning petabytes quickly. It is preventing the source volume from becoming the query volume.

**1. Prune files before scheduling.** Our metastore compares query filters with per-file metadata such as event-time bounds, application, host, job, and correlation ID. A file that cannot match never becomes a **split** (a file-processing task assigned to one worker) and is never opened. Uber's metadata provider performs this query-aware file planning, including the latest-N optimization below.

**2. Avoid scheduling older files for latest-N queries.** Most log searches ask for only the latest `N` matches with `ORDER BY timestamp DESC LIMIT N`. Without latest-N-aware file selection, Presto workers must process every split remaining after ordinary pruning before distributed Top-N can determine the latest `N`.

At Uber, we added a planning step that considers files newest first and can stop selecting files before every older candidate becomes a split. Unlike pruning, a file skipped by this step may still contain matches. Overlapping file time ranges make the stopping rule more complicated than simply counting `N` matches. The callout explains the guaranteed and approximate modes for readers who want the details.

> Guaranteed mode preserves exact latest-N results. Users may explicitly select approximate mode when they prefer lower latency and accept that some qualifying events may be omitted.
>
> **Approximate mode.** The metastore receives pruned files in descending `max_timestamp` order and accumulates matching-event counts. When the count first reaches `N`, it sets a watermark at the lowest `min_timestamp` selected so far and returns immediately. This can schedule fewer files and reduce latency, but an omitted file overlapping the watermark may contain events that belong in the true latest `N`.
>
> **Guaranteed mode.** With candidate files ordered by accurate bounds for the query's timestamp field, the metastore includes every file whose `max_timestamp` overlaps the watermark and stops at the first file below it. Exact matching-event counts must account for every remaining filter. If any filter lacks an exact count, guaranteed mode returns every file remaining after pruning.
>
> Both modes run before split creation, so skipped files are never scheduled or opened. If the metastore streams ordered candidates, planning can also terminate candidate retrieval early instead of materializing the full file list. The same logic runs in reverse for `ORDER BY timestamp ASC LIMIT N`. It can also account for filters such as application or host when the metastore knows how many matching events each file contains.

**3. Narrow each selected CLP scan.** The connector pushes supported filters and projections down into CLP. Within an archive, its structure map finds relevant shape-specific tables and columns. Logtype and value dictionaries then rule out text patterns and values that cannot match. CLP reads the relevant encoded streams and reconstructs only matching results.

When a planned query exceeds configured thresholds for file count, event count, or data volume, our query layer suggests a narrower timestamp range or additional filters. This small nudge can prevent substantial work. Users can still run the original query when they need the complete result.

## Production Results

Across the workloads introduced above, approximately 25 PB of logical uncompressed log data per month occupies 750–850 terabytes (TB) in object storage, a 29–33× reduction in physical storage, or about 97%.[^compression-comparison]

During the same 30-day measurement window, five nodes, each running one 48-core Presto Native worker, served hundreds of users per week and thousands of successful queries per day. Workers streamed selected data directly from object storage without relying on a cache. Among successful queries, mean response time was roughly two seconds and the 99th-percentile (p99) response time was 26 seconds.

The query service prioritizes interactive latency, resource efficiency, operational simplicity, and low impact on shared object storage over maximum scan throughput. Conservative read-rate limits control parallelism and preserve bandwidth. Under this operating model and the current query mix, workers have typically used 5–10% of their CPU and memory over a longer production period, with network and I/O also well below capacity. This leaves headroom for growing demand and future work. Operational overhead remains low: a few engineers maintain the system alongside their broader platform responsibilities.

## What Comes Next

With the production path in place, we are excited to build on the same metadata and compressed structure that reduce query work today. Two directions stand out: using that knowledge to organize data more intelligently and exposing it to agents so they can explore unfamiliar data with less guesswork.

### Smarter Data Placement and Consolidation

**Intelligent tiering.** The same balance guides future tiering. For suitable workloads, CLP could remain the default queryable tier, while a faster indexed tier could be provisioned for tables or time ranges whose access patterns and latency needs justify it. CLP would retain the longer history and could repopulate the index if demand rises.

**Grouping similar data during consolidation.** The same schema, logtype, source, and query-history metadata could guide how closed KV-IR files are grouped into archives. Grouping similar files can improve deduplication, while grouping files queried together can reduce the number of archives a query opens. Time range, retention, freshness, and archive-size limits would still bound each group.

### Beyond Guesswork: CLP Context for Agents

In production, AI agents issue more than half of CLP queries for root cause analysis, behavior analytics, and exploratory debugging. SQL works well once an agent knows what to ask. With a fixed-schema table, the available columns and types are known in advance. Evolving semi-structured logs have no equivalent stable list because the fields and types present depend on which events the query selects.

Independent application changes and concurrently running versions make this harder: the same information may appear under different paths or with different types across those events. An agent that guesses the wrong path or type can miss relevant evidence before keyword matching even begins. Natural-language messages add another challenge because the same problem may be described in different words. Without better context, agents often combine broad guesses such as `message LIKE '%failed%' OR message LIKE '%exception%' OR message LIKE '%error%'`. These searches may still miss relevant events or bury them in irrelevant results.

**CLP's stored metadata already contains much of this missing context. Before retrieving raw events, an agent could inspect a compact view of the filtered data: which fields occur, their value types, and recurring message formats. With that context, it could decide what to search and find relevant message formats by semantic meaning rather than exact keywords. It could then request only the supporting evidence it needs, preserving its limited context window for reasoning.** Schema trees record fields and types, while deduplicated logtypes record recurring formats. Because CLP stores this metadata with the compressed data, the workflow would not require a separate event-level index. A **field path** identifies a location inside a nested event. For example, `message.content[*].content` selects the `content` field from every element of the `message.content` array. We are refining three SQL functions as one way to expose these capabilities:

- `SCHEMA()` would list the fields and types below a chosen path within the filtered events.
- `PATTERN()` would expose the stored logtype for a chosen text field, allowing SQL to group and count recurring formats.
- `SEMANTIC()` would match a plain-language description against stored logtypes, either at query time or through an embedding generated once for each logtype.

> ### Example: From keyword guesses to recurring failure patterns
>
> Suppose an agent reviewing coding-agent session logs asks: **“Which native connector load failures recur?”**
>
> `SCHEMA('message')` shows which fields and types occur below `message`. The agent combines that schema with its knowledge of the session format and identifies `message.content[*].content` as the relevant text field. `SEMANTIC()` selects logtypes related to connector load failures without guessing their exact wording. `PATTERN()` returns those recurring formats for SQL to group and count:
>
> ```sql
> SELECT
>     PATTERN('message.content[*].content') AS failure_pattern,
>     COUNT(*) AS occurrences
> FROM clp.coding_agent_sessions
> WHERE SEMANTIC(
>     'message.content[*].content',
>     'native connector load failure'
> )
> GROUP BY PATTERN('message.content[*].content');
> ```
>
> **Illustrative output:**
>
> | failure pattern | occurrences |
> | --- | ---: |
> | Failed to load connector library **`%library_path%`**: undefined symbol **`%symbol%`** | 47 |
> | Connector factory **`%factory_name%`** was not registered | 19 |
> | Could not open shared object file **`%file_path%`** | 6 |
>
> The agent first sees three recurring failure patterns instead of 72 raw events, then chooses which exact evidence to inspect.

When a query spans multiple files, CLP can merge their schema trees and deduplicate identical logtypes. The agent would perform this discovery over compact metadata instead of reconstructing raw events. Because each metadata entry remains linked to matching events and typed variables, the agent could later ground its final results in the exact events it retrieves. This discovery work scales with the number of distinct structures and message formats rather than the number of raw events, making the workflow practical over petabyte-scale data.

## Open Source and Collaboration

[CLP](https://github.com/y-scope/clp) and the [CLP Presto connector](https://github.com/y-scope/clp-plugin-presto-connector) are open source. The connector exposes KV-IR and archives through Presto, remains agnostic to its metadata provider, and supports typed path access, complete-event reconstruction, and filter and projection pushdown into CLP. The [μSlope paper](https://www.usenix.org/conference/osdi24/presentation/wang-rui), produced by [YScope](https://yscope.com/), the University of Toronto, and Uber, documents the storage foundation.

At Uber, the Observability team integrated the connector with its metastore and query layer and operates the production service. YScope worked with Uber's Presto team on connector infrastructure and with Observability on production planning, including latest-N file skipping.

YScope led connector development with design input from the Presto teams at Uber and IBM and from Meta's Presto and Velox teams. YScope also implemented and submitted the upstream changes for [runtime connector registration](https://github.com/prestodb/presto/pull/27653) and [connector-specific coordinator-to-worker serialization](https://github.com/prestodb/presto/pull/27652), reviewed by IBM's Presto team. These changes made CLP the first end-to-end pluggable connector for Presto Native and let native connectors evolve outside the Presto Native source tree.

## Conclusion

Adding SQL syntax was not the hard part. The challenge was keeping evolving logs fresh, compact, and queryable while touching only a small fraction of petabyte-scale data.

Together, the three articles in Uber's CLP series document nearly five years of collaboration to build the CLP platform for compressing, storing, and querying Uber's application and platform logs. The [first blog](https://www.uber.com/en-GB/blog/reducing-logging-cost-by-two-orders-of-magnitude-using-clp/) showed how CLP made longer retention affordable. The [second blog](https://www.uber.com/gb/en/blog/modernizing-logging-with-clp-ii/) showed how Uber kept compressed logs fresh and easy to browse. This third blog adds native SQL across arriving KV-IR and consolidated archives.

Our metastore narrows the files Presto schedules, and CLP narrows each selected scan. Together, these layers explain how five workers support queries over workloads producing 25 PB of log data per month before compression.

The broader lesson is that a common SQL interface does not require one physical storage model. SQL becomes practical when the platform preserves structure from ingestion through consolidation and carries query intent into CLP-native execution. Presto handles SQL planning and distributed execution, while CLP keeps arriving KV-IR and consolidated archives compressed and searchable. The result is a query service available across Uber for workloads onboarded to CLP, with room to grow.

At petabyte scale, performance depends on avoiding unnecessary work and executing the remainder efficiently. Metadata-driven planning automatically eliminates much of that work, while CLP searches what remains directly in its native compressed representation. If a query remains too broad, the query layer gives users a gentle nudge to narrow it without blocking execution. Looking ahead, query and discovery interfaces can expose CLP's stored structure and log patterns, laying the foundation for agent-driven root-cause analysis, behavior analytics, and exploratory debugging.

[^compression-comparison]: The [first blog in the series](https://www.uber.com/en-GB/blog/reducing-logging-cost-by-two-orders-of-magnitude-using-clp/) reported 169× compression for Spark messages alone. Today's broader workload mix includes records enriched with hostnames, tags, and trace IDs (UUIDs). It averages roughly 30× compression, still about 97% less storage. High-cardinality trace IDs give compressors little repetition to exploit.

[^work-reduction]: We divided the total number of events covered by each successful query's time range, summed across the measurement window, by the total number examined after metadata pruning, guaranteed latest-N file selection where applicable, and CLP filtering for the same queries.
