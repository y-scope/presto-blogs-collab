# Bringing Native SQL to Petabyte-Scale Semi-Structured Logs: Inside Uber’s CLP and Presto Infrastructure (Part III)

At Uber, the [CLP platform](https://github.com/y-scope/clp) handles semi-structured logs from machine learning and AI platforms such as [Ray](https://www.ray.io/) and [Michelangelo](https://www.uber.com/us/en/blog/michelangelo-machine-learning-platform/), streaming platforms such as [Flink](https://flink.apache.org/) and [Kafka](https://kafka.apache.org/), and data analytics platforms such as [Spark](https://spark.apache.org/). Together, these workloads produce about 25 PB of logs per month before compression. Yet just five 48-core [Presto Native](https://prestodb.io/docs/current/presto-cpp.html) workers serve hundreds of users each week and execute thousands of successful queries each day. How can such a small cluster make interactive SQL practical at that scale?

This is the third blog in Uber’s CLP series. [“Reducing Logging Cost by Two Orders of Magnitude Using CLP”](https://www.uber.com/en-GB/blog/reducing-logging-cost-by-two-orders-of-magnitude-using-clp/) showed how streaming CLP compression reduced Spark-log storage and SSD writes, and identified familiar SQL access through Presto as a next step. [“Modernizing Logging at Uber with CLP (Part II)”](https://www.uber.com/gb/en/blog/modernizing-logging-with-clp-ii/) described production ingestion, retention, and viewing.

As CLP adoption expanded, investigations began to span more hosts, application versions, and time ranges. Engineers often start with a symptom or question rather than an exact field or message. They need to discover evolving fields and types, find and count recurring patterns, compare behavior across selected data, and perhaps join logs with related datasets. Uber’s mature query ecosystem around Presto, with established governance, access control, and tooling, was the natural place to provide these capabilities.

Making this practical required more than a SQL interface. The storage path had to preserve changing paths and conflicting types, keep arriving events compressed and searchable, and consolidate closed data for better compression and faster search without interrupting access. At query time, simple time-range pruning was not enough. We use metastore metadata to remove files that cannot match, skip older files that may match but cannot change a latest-result query, and use CLP’s stored structure, text patterns, and values to narrow each selected scan. Across a 30-day production window, successful queries’ time ranges covered more than 200,000 times as many events as the complete query path ultimately examined. This article follows that design from storage through query execution, reports its production results, and shows how the same metadata could later give agents compact context before they retrieve exact evidence.

## Organizing Evolving Logs for Columnar Compression and Search

The first requirement is organizing evolving events in storage for both high compression and efficient field-level search. Uber's logging platform cannot control the structure of every event. Even within one application, events may contain different fields, nested objects, and value types. Independent release schedules and multiple versions running at once add more variation, yet users still expect every field to remain searchable. A **field path** identifies a location inside an event. For example, `event.request_id` identifies `request_id` inside `event`. A single file format can support multiple layouts, so the table compares how data is arranged rather than naming file formats.

| Layout | Handling changing fields and types | Field search | Main tradeoff |
|---|---|---|---|
| One document per event, such as a JSON object | Complete event preserved without a fixed schema | Document navigation unless paths are separately indexed | Extra query work or a separate index |
| One typed column per predefined field | Field paths and types chosen before each file is written | Values for the requested field are stored together and scanned directly | New fields and type changes require schema decisions |
| Document plus selected typed columns | Full event remains available, with chosen paths stored in columns | Direct scans for chosen paths and document navigation for the rest | Chosen paths and types must be maintained |
| CLP | Observed paths and types organized within each archive | Only relevant encoded columns, text patterns, and values are scanned | Specialized storage and query integration |

Uber stores these logs in CLP, which losslessly preserves the path of each source file, the nested structure and values of every event, and event order within each file. Rather than keeping each event as a single document, CLP organizes every observed path and type for columnar access within each archive. It requires neither a global schema nor a predefined list of important fields.

Think of a CLP archive as a collection of small internal tables. Events with the same nested fields and the same type for each field share a table. Within that table, each field becomes a column containing encoded values of one type. For free-form text such as log messages, CLP identifies the static text shared by similar messages and stores it as a pattern called a **logtype**. Changing parts such as library paths, IDs, and counts are encoded separately while preserving their types. An archive-local schema tree records which tables and columns contain each field path and type.

Because CLP derives each archive's layout from observed data, new fields and type changes are incorporated during compression rather than through a global schema update. Events in the same internal table have the same field paths and types, so CLP stores their shared schema once rather than repeating it with every event. Values from the same field and type are stored together, improving compression, while dictionaries avoid repeating logtypes and values.

The same organization speeds up search. For an exact or wildcard field path, the schema tree selects only tables and columns that can contain a matching path with the requested type. Dictionary metadata then rules out text patterns and values that cannot match. CLP returns requested fields directly and reconstructs a complete event only when requested. The [μSlope paper from OSDI '24](https://www.usenix.org/conference/osdi24/presentation/wang-rui) describes the formal layout.

Archives therefore provide high compression and selective search for consolidated data. The next requirement is keeping new events searchable before they are consolidated into an archive.

## Compressed and Queryable from Arrival through Archival

Recent logs must remain searchable for incident diagnosis, rollout validation, and active-job monitoring. Building an immutable archive for each small batch of container logs would create tiny objects, while waiting for a larger batch would delay search. Uber avoids this tradeoff by writing arriving events to appendable, compressed [**key-value intermediate representation (KV-IR)**](https://docs.yscope.com/clp/main/dev-docs/design-kv-ir-streams/index.html) and asynchronously consolidating closed files into immutable CLP archives.

![CLP storage flow from application containers through KV-IR to consolidated archives](figures/clp-storage-flow.svg)

**Figure 1: KV-IR snapshots keep arriving data compressed and searchable. Closed files are consolidated asynchronously into columnar archives, and queries span both forms without duplicates.**

Configurable triggers upload snapshots without closing the local stream. After the metastore processes an update, new events are typically queryable within 30 seconds to a few minutes.

The surprising part is that this fresh-data path does not force archive construction to parse and encode the same events again. KV-IR remains directly searchable as it grows, and consolidation reuses its interpreted and encoded representation.

> ### KV-IR makes an intermediate representation searchable as it grows
>
> A compiler typically transforms source into an intermediate representation (IR) before producing optimized output. CLP similarly transforms incoming events into a structured, encoded form before building columnar archives. KV-IR makes that intermediate form incremental: it serializes the representation as events arrive and keeps every complete readable prefix searchable.
>
> **The parsing work on any one host is modest, but repeating it centrally across Uber's full log volume would make consolidation far more expensive.** Host log collectors perform that work as events arrive and together form a massively parallel parsing system distributed across Uber's fleet. Each collector interprets event nesting, assigns fine-grained value types, and separates natural-language text into logtypes and typed variables.
>
> KV-IR then writes evolving schema-tree references alongside encoded values rather than streaming self-contained documents. Conceptually, each event is already a row ready for its matching internal table. Archive construction can route those values into columns without parsing the original event again.
>
> Each new schema-tree node appears before the first event that refers to it, so every complete readable prefix contains all the structure needed to interpret its events. Although *intermediate* describes KV-IR's position in the data flow, KV-IR is compressed and appendable, and its published snapshots are durable, directly searchable production data.

Closed KV-IR files remain queryable while the aggregation service groups them and a worker builds an archive in the background. Because the data is already interpreted and encoded, consolidation merges schema trees, appends values to archive columns, and deduplicates structure, logtypes, and values across files instead of reparsing the originals. This cross-file organization improves compression and enables faster columnar search. Consolidation uses background CPU and I/O but adds no synchronous work to ingestion or queries.

Until source KV-IR files are purged, the two forms overlap. The metastore chooses one representation per logical file. An archive takes priority over KV-IR, and the newest archive wins if several contain it. The connector exposes one duplicate-free logical table.

## Native Semi-Structured Querying with Presto

To engineers, recent KV-IR and consolidated archives look like one Presto table. They query it with SQL and do not need to know where an event is stored. Uber also accepts familiar Lucene-style searches and translates them into the same SQL path, preserving existing access controls and query tools.

Underneath this familiar interface, Presto plans each query. Inside [Presto Native](https://prestodb.io/docs/current/presto-cpp.html) C++ workers, the connector pushes supported field reads and filters into CLP. CLP searches its compressed representation directly instead of reconstructing every event as a document.

Frequently used fields, such as `timestamp` and `event.request_id`, appear as ordinary typed columns. The [open-source connector](https://github.com/y-scope/clp-plugin-presto-connector) exposes other supported fields through [typed SQL functions named `CLP_*`](https://docs.yscope.com/presto/connector/clp.html). The function name gives Presto a stable return type even when a field is absent or has another type in some events. These functions support exact paths, wildcard path segments, and typed value searches across fields. `CLP_GET_JSON_STRING()` reconstructs the complete event only when requested.

The main access forms fit in one query:

```sql
SELECT
    timestamp, -- ordinary table column
    event.request_id, -- ordinary string column
    CLP_GET_STRING('event.request_id'), -- same stored path through a typed function
    CLP_GET_JSON_STRING() AS event -- complete event without a declared schema
FROM clp.platform_logs
WHERE timestamp >= current_timestamp - INTERVAL '30' DAY
  AND CLP_GET_BIGINT('event.*.status_code') >= 500 -- wildcard path with integer values
  AND CLP_WILDCARD_STRING_COLUMN() LIKE 'OutOfMemory%' -- string in any field
ORDER BY timestamp DESC
LIMIT 100;
```

All these access forms use the same CLP-native execution path. CLP uses its schema tree, internal tables, encoded columns, and dictionaries to narrow the search and reconstruct only what the query requests. Exposing a path as an ordinary column does not create another copy of the data. That is the complete user-facing model.

The connector makes this experience simple by handling substantial complexity behind the interface. That simplicity reflects substantial engineering by YScope, the Presto team, and the broader PrestoDB community in both the connector and Presto Native.

Readers who want the implementation story can continue with the companion **[PrestoDB blog, “The Pluggable CLP Connector for Presto Native: Specialized Storage for Semi-Structured Data”](PRESTO_BLOG_URL)**. Written for connector developers and Presto maintainers, it covers Java planning, native plugin loading, filter pushdown, packaging, deployment, testing, and the relationship between CLP and future PrestoDB `VARIANT` support.

[YScope's August 2025 blog](https://blog.yscope.com/querying-clp-compressed-logs-with-prestosql-71dff29a0b82) provides a deeper walkthrough of the optimizer rewrite used by the connector.

## How Five Workers Serve the Workload: Avoiding Work at Every Layer

Typed SQL explains how engineers use the system. Avoiding work explains how five workers can serve it. Returning 100 rows can still be expensive if Presto schedules every file in a 30-day range and examines every event. Uber narrows that work at three progressively finer levels: files that cannot match, older files that latest-N planning can avoid scheduling, and irrelevant structures inside each selected CLP file.

**1. Prune files that cannot match.** Semi-structured logs carry useful dimensions such as application, host, job, and correlation ID alongside event-time bounds. Our metastore compares query filters with this file-level metadata. If a file cannot match, it never becomes a Presto split and no worker opens it. This familiar optimization is effective, but it still keeps every file that might contain even one match.

**2. Avoid scheduling older files for latest-N queries.** Most log searches ask only for the latest `N` matches with `ORDER BY timestamp DESC LIMIT N`. Ordinary pruning keeps every file that may match, even when its matches are too old to enter the result. Presto would normally read every scheduled split before identifying the latest `N`. Uber's metastore instead considers the remaining files newest first and, when conditions allow, stops before unnecessary older files become splits.

This is not another form of pruning. A skipped file may contain valid matches. Deciding that those matches cannot change the answer requires reasoning across file time bounds and matching-event counts. The obvious stopping rule is subtly wrong. The callout explains the trap and the guaranteed and approximate modes built around it.

> ### Why stopping after N matches can be wrong
>
> The tempting implementation is to order pruned files by `max_timestamp`, add their matching-event counts until the total reaches `N`, and stop. An unselected file may overlap the time bounds of selected files and contain events newer than some already counted results.
>
> **Reaching `N` establishes a watermark. It does not yet make every older file irrelevant.**
>
> **Approximate mode.** Users explicitly choose this mode when lower latency matters more than an exact latest-N result. The metastore receives pruned files in descending `max_timestamp` order and accumulates the available counts for supported filters. When the count first reaches `N`, it sets a watermark at the lowest `min_timestamp` selected so far and returns immediately. This schedules fewer files, but an omitted file overlapping the watermark may contain events that belong in the true latest `N`. The tradeoff can be worthwhile for rapid exploration.
>
> **Guaranteed mode.** This mode preserves the exact latest-N result. With accurate bounds for the query's timestamp field, the metastore uses the same watermark but includes every file whose `max_timestamp` is at or above it. It stops only when the next file is strictly older than the watermark. Counts must be exact for every remaining filter. If any filter cannot be counted exactly, guaranteed mode returns every file left after pruning.
>
> Both modes run before split creation, so skipped files are never scheduled or opened. The same logic runs in reverse for `ORDER BY timestamp ASC LIMIT N`. If the metastore streams ordered candidates, it can also stop retrieving file metadata early.

**3. Narrow each selected CLP scan.** This is where CLP's physical organization becomes a query advantage. A selected file may still contain many events, fields, and recurring messages. The connector passes supported filters and requested fields directly to CLP. Its schema tree maps exact and wildcard paths to relevant internal tables and type-specific columns. Logtype and value dictionaries rule out text patterns and encoded values that cannot match. CLP reads the remaining encoded streams, and Presto evaluates any expressions that could not be pushed down.

Each layer starts where the previous one stops. File metadata excludes files that cannot match while retaining every file that might. Guaranteed latest-N selection then avoids scheduling older files that may match but cannot change the result. The selected files can still contain many unrelated structures and values, so CLP metadata narrows the remaining scan to relevant tables, columns, patterns, and encoded values. The work therefore narrows from the query's time range, to the files that matter, to the encoded data that can satisfy its predicates.

Across a 30-day production window, the successful queries' time ranges covered **more than 200,000 times as many events** as the system ultimately examined after all three layers.[^work-reduction] The achievement is not scanning petabytes quickly. It is avoiding almost all of that work.

Some queries request far more data than the user actually needs. Their time range may be unnecessarily wide, or they may omit an available metadata filter. During planning, Uber's query layer detects queries that exceed configured thresholds and suggests these simple refinements. One small edit often brings the query back into the interactive range. This is a guardrail, not a rejection. When the full scope is intentional, users can proceed with the original query. The system executes it efficiently, but genuinely large queries still take time.

## Results in Production

Each month, the workloads described above produce approximately **25 PB** of log data before compression. Their CLP footprint is **750–850 TB**, about **97% less physical storage**.[^compression-comparison]

In the same 30-day production window, **five nodes**, each running one 48-core Presto Native worker, served **hundreds of users per week** and completed **thousands of successful queries per day**. Workers streamed selected data directly from object storage without relying on a cache. Successful queries had a mean response time of roughly **two seconds**, and **99% completed within 26 seconds**.

Uber's CLP query deployment balances interactive query performance with a small footprint on shared infrastructure. Rather than maximize scan throughput, we deliberately throttle our object-storage read rate and parallelism to avoid disrupting other workloads while still meeting users' latency needs. Over a longer production period, workers typically used **5–10% of their CPU and memory**, with network and I/O also lightly loaded. The remaining capacity provides headroom as demand and scope grow. A few engineers maintain the system alongside their broader platform responsibilities.

## What Comes Next

With the production path in place, we are excited to build on the same metadata and compressed structure that reduce query work today. Two directions stand out: using that knowledge to organize data more intelligently and exposing it to agents so they can explore unfamiliar data with less guesswork.

### Better Storage Decisions from Existing Metadata

The decision points and metadata already exist, making both directions incremental policy improvements with potentially substantial storage and query-efficiency returns.

**Demand-driven hot indexing.** Because CLP preserves the original events losslessly and remains queryable, it could serve as the default durable tier. Access patterns, latency needs, and retention policy would determine which tables or time ranges also receive a copy in a hot index. If demand returns, CLP could repopulate the index with the exact events, reducing hot storage without sacrificing history.

**Metadata- and domain-guided consolidation.** Schema, logtypes, sources, query history, and platform knowledge could guide which files share an archive. Structural similarity improves deduplication and compression. Keeping data queried together, such as Spark driver and executor logs, reduces archive opens. Time range, retention, freshness, and archive-size limits still bound each group.

### Beyond Guesswork: CLP Context for Agents

AI agents already issue more than half of CLP queries at Uber for root cause analysis, behavior analytics, and exploratory debugging. That production use exposes the next bottleneck: SQL works once a query is precise, but an agent must first discover which paths, types, and message formats occur in the events selected by its filters.

With evolving semi-structured logs, guessing the wrong path or type can exclude relevant evidence before text search begins. Broad keyword guesses such as `message LIKE '%failed%' OR message LIKE '%exception%' OR message LIKE '%error%'` may still miss unfamiliar wording or bury useful evidence in irrelevant results.

**The goal is simple: inspect compact context, generate targeted SQL, and retrieve exact evidence.** CLP could show an agent the fields, types, and recurring message formats in the filtered data, then search those formats by meaning. The agent would preserve more of its limited context window for reasoning and ground its final result in exact source events.

Recall that CLP's schema trees record paths and types, while logtypes capture recurring message formats. Both are stored with the compressed data and remain linked to matching events, so this workflow would not require a separate event-level index. We are refining three SQL functions to expose these capabilities:

- `SCHEMA()` would list the fields and value types below a chosen path in the filtered data.
- `PATTERN()` would expose stored logtypes so SQL can group and count recurring formats.
- `SEMANTIC()` would match a plain-language description against stored logtypes, either directly or through an embedding generated once per logtype.

The functions could be used independently or combined.

> ### Example: From keyword guesses to recurring failure patterns
>
> The following syntax is illustrative. These functions remain under development.
>
> Suppose an agent reviewing coding-agent session logs asks: **“Which native connector load failures recur?”**
>
> `SCHEMA('message')` identifies `message.content[*].content` as the relevant text field. The agent then asks SQL to group semantically related logtypes:
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
> Instead of placing 72 near-duplicate events in its context, the agent first sees three recurring formats and their counts, then retrieves the exact events it needs as evidence.

Across selected files, CLP can merge schema trees and deduplicate identical logtypes before presenting this context. The context therefore grows with distinct structures and message formats rather than raw-event count. Every metadata entry remains linked to matching events and typed variables, allowing an agent to move from a compact overview to exact supporting evidence. This is what makes metadata-first agent analysis practical over petabyte-scale data.

## From Open Source to Production: Collaboration Across Teams and Companies

This production system grew from nearly five years of sustained collaboration across teams and companies. The public [CLP](https://github.com/y-scope/clp) and [CLP Presto connector](https://github.com/y-scope/clp-plugin-presto-connector) projects provided shared storage and query foundations. Production at Uber required additional planning, metastore and query-layer integration, and operational support.

YScope led the CLP connector's design and implementation and developed the production planning path, including latest-N file selection, in close collaboration with Uber's Presto, Observability, and Data Storage teams. Uber's Presto team worked with YScope on connector infrastructure. Observability shaped production requirements and workflows and operates the query service. Data Storage collaborated on the metastore and object-storage paths used for planning, consolidation, and query execution. IBM's Presto team and Meta's Presto and Velox teams also participated in design discussions, helping us test assumptions and refine the architecture.

The collaboration also produced reusable changes upstream. Supporting native connectors outside the Presto Native source tree required [runtime connector registration](https://github.com/prestodb/presto/pull/27653) and [connector-specific coordinator-to-worker serialization](https://github.com/prestodb/presto/pull/27652). YScope implemented and submitted both changes, IBM's Presto team reviewed them, and the broader PrestoDB community provided the open review and integration path. These changes made the CLP connector Presto Native's first end-to-end pluggable connector and created infrastructure that future connectors can reuse.

## Conclusion

Adding SQL syntax was not the hard part. The challenge was keeping evolving logs fresh, compact, and queryable while preventing most petabyte-scale data from becoming query work. Appendable KV-IR keeps arriving events searchable, consolidated CLP archives improve compression and search, the metastore avoids unnecessary files, and CLP narrows each selected scan. Across the 30-day production window, successful queries’ time ranges covered more than 200,000 times as many events as the full query path examined.[^work-reduction] That is how five workers serve hundreds of weekly users and thousands of successful daily queries across workloads producing 25 PB of logs per month before compression.

Across this three-blog series, nearly five years of collaboration have taken CLP from [streaming compression and affordable retention](https://www.uber.com/en-GB/blog/reducing-logging-cost-by-two-orders-of-magnitude-using-clp/), through [production ingestion and viewing](https://www.uber.com/gb/en/blog/modernizing-logging-with-clp-ii/), to native SQL over arriving KV-IR and consolidated archives. The broader lesson is that a common SQL interface does not require one physical storage model. Presto brings CLP data into Uber’s established query ecosystem and handles planning and distributed execution; CLP preserves evolving structure and searches its compressed representation directly.

That preserved structure also creates the path forward. The metadata that avoids query work today can guide storage decisions and give agents compact context about unfamiliar data before they retrieve events. Keeping structure with the compressed data lets the platform reuse it throughout the lifecycle: to improve compression, plan queries, narrow searches, and ground future agent answers in exact evidence.

[^compression-comparison]: The first blog reported 169× compression for Spark messages alone. Today's figure is an aggregate across a broader set of workloads whose records include operational fields such as hostnames, tags, and high-cardinality trace IDs (UUIDs). These nearly unique values give the compressor little repetition to exploit. Even so, storing 25 PB in 750–850 TB corresponds to about 29–33× compression, or roughly 97% less storage.

[^work-reduction]: We divided the aggregate number of events covered by successful queries' time ranges across the 30-day window by the aggregate number those queries examined after metadata pruning, guaranteed latest-N file selection where applicable, and CLP filtering.
