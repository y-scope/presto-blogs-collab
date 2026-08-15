# Bringing Native SQL to Petabyte-Scale Semi-Structured Logs: Inside Uber’s CLP and Presto Infrastructure (Part III)

At Uber, the [CLP platform](https://github.com/y-scope/clp) now handles semi-structured logs from machine learning and AI platforms such as [Ray](https://www.ray.io/) and [Michelangelo](https://www.uber.com/us/en/blog/michelangelo-machine-learning-platform/), streaming platforms such as [Flink](https://flink.apache.org/) and [Kafka](https://kafka.apache.org/), and data analytics platforms such as [Spark](https://spark.apache.org/). Together, these workloads produce about 25 PB of logs per month before compression. Yet just five 48-core [Presto Native](https://prestodb.io/docs/current/presto-cpp.html) workers serve hundreds of users each week and execute thousands of successful queries each day. How can such a small cluster make interactive SQL practical at that scale?

This is the third blog in Uber's CLP series. [“Reducing Logging Cost by Two Orders of Magnitude Using CLP”](https://www.uber.com/en-GB/blog/reducing-logging-cost-by-two-orders-of-magnitude-using-clp/) showed how streaming CLP compression reduced Spark-log storage and SSD writes, and identified familiar SQL access through Presto as a next step. [“Modernizing Logging at Uber with CLP (Part II)”](https://www.uber.com/gb/en/blog/modernizing-logging-with-clp-ii/) described production ingestion, retention, and viewing.

Broader adoption increased both what engineers wanted to do with the data and how much data each analysis could span. A question that once involved a few log files could now cover many hosts, application versions, and time ranges. Whether diagnosing an incident, evaluating an experiment, or exploring system behavior, engineers often begin with a symptom or question rather than an exact field or message. They need to find relevant events, inspect fields even as those fields evolve, group and count recurring patterns, and compare behavior across the selected data. They may also need to join logs with related datasets. Uber's mature query ecosystem around Presto, with established governance, access control, and tooling, was the natural place to provide these capabilities.

Meeting those analytical needs with only five workers required more than adding a SQL interface to CLP. We had to make semi-structured data feel natural to query in Presto while preserving evolving event structures and conflicting field types. We also had to keep new events searchable and improve storage efficiency over time. Time-range pruning alone could not eliminate enough work. We prune files using selective metadata, skip older files that may contain matches but cannot affect exact latest-result queries, and use CLP's structural, text-pattern, and value metadata to narrow each selected scan. These layers are cumulative, not interchangeable. Losing any one would materially slow queries. Losing two would make interactive latency impractical, while relying on time-range filtering alone would make this five-worker deployment infeasible. Across the 30-day production window, the full query path examined over 200,000× less event data than time-range filtering alone. This article follows that design from storage through query execution, then shows how the same CLP metadata could support future evidence-based agent analysis.

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

> ### KV-IR turns a compiler-like intermediate representation into a searchable stream
>
> A compiler's intermediate representation typically sits between source and optimized output. KV-IR occupies a similar position between incoming events and columnar CLP archives, but it goes further: the representation itself is serialized incrementally as data arrives. CLP has already interpreted each event's nesting, assigned fine-grained value types, and separated natural-language text into logtypes and typed variables. Rather than streaming self-contained documents, KV-IR writes schema-tree references and encoded values, conceptually forming a row ready for a matching internal table.
>
> New schema-tree nodes precede the first event that references them. Every readable stream prefix is therefore structurally complete and directly searchable. KV-IR is not temporary internal state. It is compressed, appendable, durable production storage that archive consolidation can consume directly.

Closed KV-IR files remain queryable while the aggregation service groups them and a worker builds an archive in the background. Because the data is already interpreted and encoded, consolidation merges schema trees, appends values to archive columns, and deduplicates structure, logtypes, and values across files instead of reparsing the originals. This cross-file organization improves compression and enables faster columnar search. Consolidation uses background CPU and I/O but adds no synchronous work to ingestion or queries.

Until source KV-IR files are purged, the two forms overlap. The metastore chooses one representation per logical file. An archive takes priority over KV-IR, and the newest archive wins if several contain it. The connector exposes one duplicate-free logical table.

## Native Semi-Structured Querying with Presto

Adding SQL syntax alone would be straightforward. The harder requirement is native field access without reconstructing complete documents row by row. Here, **native** means preserving CLP's storage model. Presto's Java coordinator plans each query and asks the connector for splits, the connector-defined units of work assigned to workers. A split identifies one logical file in KV-IR or a set of logical files within an archive. Inside each Presto Native worker, the dynamically loaded connector converts typed field requests and supported filters into CLP operations over compressed data. Presto Native is PrestoDB's C++ worker implementation, built on Meta's [Velox](https://velox-lib.io/) execution engine.

Engineers do not need to know whether an event is stored in KV-IR or an archive. They submit either Lucene-style field-and-keyword searches or SQL directly. We translate Lucene-style input into SQL, then validate and submit every query to Presto, where existing access controls and tooling apply.

Presto must know the type of every SQL expression during planning, while a CLP path may be absent or hold different types across events. An optional Presto table schema can expose common path-and-type pairs, such as `timestamp` and `event.request_id`, as ordinary columns without rewriting CLP data. Paths whose values map to supported SQL types can also be accessed through typed functions. `CLP_GET_JSON_STRING()` reconstructs the complete event when a query needs the full structure.

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

The [open-source CLP connector](https://github.com/y-scope/clp-plugin-presto-connector) provides these [typed functions](https://docs.yscope.com/presto/connector/clp.html) for exact paths, wildcard path segments, and value searches across fields. Their names declare a return type because Presto needs it during planning. CLP can infer a field's type from a filter at execution time, but the connector does not yet expose that capability. Queries over undeclared paths must therefore choose a typed function. Optional columns and typed functions use the same CLP-native execution path.

At the time of writing, PrestoDB does not yet support the `VARIANT` type. Because a `VARIANT` value can hold different runtime types, future support could provide the native dynamic paths SQL operator interfaces while retaining the capability to integrate with CLP-native execution. Until then, we provide first-class support for these capabilities through the `CLP_*` UDFs exposed by the connector. The preceding **[PrestoDB blog, “The Pluggable CLP Connector for Presto Native: Specialized Storage for Semi-Structured Data”](PRESTO_BLOG_URL)** compares the Variant vs CLP storage model and provides a first hand reference for the pluggable Presto native connector design, while [YScope's August 2025 blog](https://blog.yscope.com/querying-clp-compressed-logs-with-prestosql-71dff29a0b82) explains the optimizer rewrite that the connector still uses.

## How Five Workers Serve the Workload: Avoiding Work at Every Layer

Typed SQL explains how engineers use the system. Avoiding work explains why five workers can serve it. A query returning only 100 rows can still be expensive if Presto first schedules every file in a 30-day range and examines every candidate event.

In aggregate across a 30-day production window, query time ranges covered more than 200,000 times as many events as the system ultimately examined after all three work-reduction layers.[^work-reduction] The achievement is not scanning petabytes quickly. It is preventing the source volume from becoming the query volume.

**1. Prune files before scheduling.** Our metastore compares query filters with per-file metadata such as event-time bounds, application, host, job, and correlation ID. A file that cannot match is excluded before Presto creates splits, so workers never open it. This query-aware file planning also includes the latest-N optimization below.

**2. Avoid scheduling older files for latest-N queries.** Most log searches ask for only the latest `N` matches with `ORDER BY timestamp DESC LIMIT N`. If file selection ignores that limit, workers must process every split left after pruning before Presto can identify the latest `N` matches.

At Uber, we added a planning step that considers files newest first and can stop selecting files before every older candidate becomes a split. Unlike pruning, a file skipped by this step may still contain matches. Overlapping file time ranges make the stopping rule more complicated than simply counting `N` matches. The callout explains the guaranteed and approximate modes for readers who want the details.

> ### How latest-N file selection balances correctness and speed
>
> Guaranteed mode preserves exact latest-N results. Users may explicitly select approximate mode when they prefer lower latency and accept that some qualifying events may be omitted.
>
> **Approximate mode.** The metastore receives pruned files in descending `max_timestamp` order and accumulates the available per-file event counts for applicable file-level filters. When the count first reaches `N`, it sets a watermark at the lowest `min_timestamp` selected so far and returns immediately. This can schedule fewer files and reduce latency, but an omitted file overlapping the watermark may contain events that belong in the true latest `N`.
>
> **Guaranteed mode.** With candidate files ordered by accurate bounds for the query's timestamp field, the metastore includes every file whose `max_timestamp` overlaps the watermark and stops at the first file below it. Exact matching-event counts must account for every remaining filter. If any filter lacks an exact count, guaranteed mode returns every file remaining after pruning.
>
> Both modes run before split creation, so skipped files are never scheduled or opened. If the metastore streams ordered candidates, planning can also terminate candidate retrieval early instead of materializing the full file list. The same logic runs in reverse for `ORDER BY timestamp ASC LIMIT N`. It can also account for filters such as application or host when the metastore knows how many matching events each file contains.

**3. Narrow each selected CLP scan.** The connector passes supported filters and requested fields directly to CLP. Within an archive, its schema tree finds only the tables and columns that could contain matches. Logtype and value dictionaries then rule out text patterns and values that cannot match. CLP reads the relevant encoded streams and materializes rows satisfying those filters. Presto evaluates any remaining expressions.

When a planned query exceeds configured thresholds for file count, event count, or data volume, our query layer suggests a narrower timestamp range or additional filters. This small nudge can prevent substantial work. Users can still run the original query when they need the complete result.

## Production Results

Each month, approximately 25 PB of logical uncompressed log data from the workloads introduced above is stored as 750–850 terabytes (TB) of CLP compressed data, equivalent to about 97% less physical storage.

During the same 30-day measurement window, five nodes, each running one 48-core Presto Native worker, served hundreds of users per week and thousands of successful queries per day. Workers streamed selected data directly from object storage without relying on a cache. Among successful queries, mean response time was roughly two seconds and the 99th-percentile (p99) response time was 26 seconds.

The query service prioritizes interactive latency, resource efficiency, operational simplicity, and low impact on shared object storage over maximum scan throughput. Conservative read-rate limits control parallelism and preserve bandwidth. Under this operating model and the current query mix, workers have typically used 5–10% of their CPU and memory over a longer production period, with network and I/O also well below capacity. This leaves headroom for growing demand and future work. Operational overhead remains low: a few engineers maintain the system alongside their broader platform responsibilities.

## What Comes Next

With the production path in place, we are excited to build on the same metadata and compressed structure that reduce query work today. Two directions stand out: using that knowledge to organize data more intelligently and exposing it to agents so they can explore unfamiliar data with less guesswork.

### Smarter Data Placement and Consolidation

**Intelligent tiering.** Future tiering would balance query latency against storage cost. For suitable workloads, CLP could remain the default queryable tier, while a faster indexed tier could be provisioned for tables or time ranges whose access patterns and latency needs justify it. CLP would retain the longer history and could repopulate the index if demand rises.

**Grouping similar data during consolidation.** The same schema, logtype, source, and query-history metadata could guide how closed KV-IR files are grouped into archives. Grouping similar files can improve deduplication, while grouping files queried together can reduce the number of archives a query opens. Time range, retention, freshness, and archive-size limits would still bound each group.

### Beyond Guesswork: CLP Context for Agents

AI agents already issue more than half of the queries on this platform for root cause analysis, behavior analytics and exploratory debugging. That production usage exposes the next challenge: helping agents decide what to ask. SQL works well once the relevant paths and types are known. With a fixed-schema table, the available columns and types are known in advance. Evolving semi-structured logs have no equivalent stable list because the fields and types present depend on which events the query selects.

Independent application changes and concurrently running versions make this harder: the same information may appear under different paths or with different types across those events. An agent that guesses the wrong path or type can miss relevant evidence before keyword matching even begins. Natural-language messages add another challenge because the same problem may be described in different words. Without better context, agents often combine broad guesses such as `message LIKE '%failed%' OR message LIKE '%exception%' OR message LIKE '%error%'`. These searches may still miss relevant events or bury them in irrelevant results.

**CLP's stored metadata already contains much of this missing context. Before retrieving raw events, an agent could inspect a compact view of the filtered data: which fields occur, their value types, and recurring message formats. With that context, it could decide what to search and find relevant message formats by semantic meaning rather than exact keywords. It could then request only the supporting evidence it needs. For Uber, that could mean fewer trial-and-error queries, more agent context available for reasoning, and faster investigations grounded in supporting data.** Schema trees record fields and types, while deduplicated logtypes record recurring formats. Because CLP stores this metadata with the compressed data, the workflow would not require a separate event-level index. For example, the field path `message.content[*].content` selects the `content` field from every element of the `message.content` array. Today, agents use the production SQL interface described above. We are refining three additional SQL functions as one way to expose these capabilities:

- `SCHEMA()` would list the fields and types below a chosen path within the filtered events.
- `PATTERN()` would expose the stored logtype for a chosen text field, allowing SQL to group and count recurring formats.
- `SEMANTIC()` would match a plain-language description against stored logtypes, either at query time or through an embedding generated once for each logtype.

> ### Example: From keyword guesses to recurring failure patterns
>
> The following syntax is illustrative. These functions are under development.
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

Across selected inputs, CLP can merge schema trees and deduplicate identical logtypes. The agent would perform this discovery over compact metadata instead of reconstructing raw events. Because each metadata entry remains linked to matching events and typed variables, the agent could later ground its final results in the exact events it retrieves. The agent-facing context would grow with the number of distinct structures and message formats which are bound within an application rather than the number of unbounded raw events, making the workflow practical over petabyte-scale data.

## Open Source and Collaboration

The foundation for both the deployed system and these future directions is open source: [CLP](https://github.com/y-scope/clp) and the [CLP Presto connector](https://github.com/y-scope/clp-plugin-presto-connector). The connector exposes KV-IR and archives through Presto, remains agnostic to its metadata provider, and supports typed path access, complete-event reconstruction, and passing supported filters and requested fields directly to CLP. The [μSlope paper](https://www.usenix.org/conference/osdi24/presentation/wang-rui), produced by [YScope](https://yscope.com/), the University of Toronto, and Uber, documents the storage foundation.

At Uber, the Observability team integrated the connector with its metastore and query layer and operates the production service. YScope worked with Uber's Presto team on connector infrastructure and with Observability on production planning, including latest-N file skipping.

YScope led Presto CLP connector development with design input from the Presto teams at Uber and IBM and from Meta's Presto and Velox teams. YScope also implemented and submitted two upstream changes to PrestoDB: [runtime connector registration](https://github.com/prestodb/presto/pull/27653) and [connector-specific coordinator-to-worker serialization](https://github.com/prestodb/presto/pull/27652). IBM's Presto team reviewed both changes. Together, they made CLP the first end-to-end pluggable connector for Presto Native and let native connectors evolve outside the Presto Native source tree.

## Conclusion

Adding SQL syntax was not the hard part. The challenge was keeping evolving logs fresh, compact, and queryable while touching only a small fraction of petabyte-scale data.

Together, the three blogs in Uber's CLP series document nearly five years of collaboration to build the CLP platform for compressing, storing, and querying Uber's application and platform logs. The [first blog](https://www.uber.com/en-GB/blog/reducing-logging-cost-by-two-orders-of-magnitude-using-clp/) showed how CLP made longer retention affordable. The [second blog](https://www.uber.com/gb/en/blog/modernizing-logging-with-clp-ii/) showed how Uber kept compressed logs fresh and easy to browse. This third blog adds native SQL across arriving KV-IR and consolidated archives.

Our metastore narrows the files Presto schedules, and CLP narrows each selected scan. Together, these layers explain how five workers support queries over workloads producing 25 PB of log data per month before compression.

The broader lesson is that a common SQL interface does not require one physical storage model. SQL becomes practical when the platform preserves structure from ingestion through consolidation and carries query intent into CLP-native execution. Presto handles SQL planning and distributed execution, while CLP keeps arriving KV-IR and consolidated archives compressed and searchable. The result is a query service available across Uber for workloads onboarded to CLP, with room to grow.

At petabyte scale, performance depends on avoiding unnecessary work and executing the remainder efficiently. Metadata-driven planning automatically eliminates much of that work, while CLP searches what remains directly in its native compressed representation. If a query remains too broad, the query layer gives users a gentle nudge to narrow it without blocking execution. Looking ahead, CLP's stored structure and log patterns could give agents compact context before they retrieve events, laying the foundation for agent-driven root-cause analysis, behavior analytics, and exploratory debugging.

[^work-reduction]: We divided the total number of events covered by each successful query's time range, summed across the measurement window, by the total number examined after metadata pruning, guaranteed latest-N file selection where applicable, and CLP filtering for the same queries.
