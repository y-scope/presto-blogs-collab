# Bringing Native SQL to Petabyte-Scale Semi-Structured Logs: Inside Uber’s CLP and Presto Infrastructure (Part III)

- **Type:** Production architecture case study
- **Length:** Aim for roughly 3,500 words. Prefer a clear, skimmable story over a rigid cap.
- **Audience:** Presto, observability, storage, data-platform, streaming, machine-learning, and artificial-intelligence engineers
- **Central question:** How does Uber keep evolving logs compressed and queryable as they arrive, expose them through SQL, and use five Presto Native workers to serve hundreds of weekly users and thousands of daily queries over workloads producing roughly 25 petabytes (PB) per month before compression?

## Story

Uber makes workloads producing 25 PB per month before compression searchable within minutes and queryable through typed SQL. Five Presto Native workers serve hundreds of weekly users and thousands of successful daily queries. The workers do not scan petabytes quickly. Presto, Uber's metastore, the connector, and CLP avoid work at every layer, so workers examine only a small fraction of the source data. Follow the structure preserved during compression from production to query:

> Applications and platform workloads → searchable CLP key-value intermediate representation (KV-IR) → consolidated columnar CLP archives → one Presto query path → progressively narrower execution

Make the series payoff explicit in the introduction: Part I established compression and pointed toward familiar SQL through Presto, Part II made compressed ingestion and viewing operational, and Part III delivers the native SQL path across fresh and consolidated data.

### Three Reader Takeaways

1. **Native SQL for evolving semi-structured logs.** The Presto CLP connector exposes polymorphic paths through typed SQL while preserving CLP's native storage and execution. It is neither a conversion into one fixed physical table nor a syntax-only SQL adapter that materializes every event first.
2. **Compressed and queryable throughout ingestion.** Appendable KV-IR snapshots make arriving data searchable after their metadata reaches the metastore, typically within 30 seconds to a few minutes. Asynchronous consolidation later creates columnar CLP archives. Queries span both forms without duplicate events, and the onboarded workloads achieve roughly 30× aggregate compression, or about 97% less storage.
3. **A small query cluster through layered work reduction.** Metastore pruning, latest-N file skipping, and CLP's archive-local metadata prevent most irrelevant files, encoded streams, and events from being examined. This explains how five Presto Native workers serve hundreds of weekly users and thousands of daily queries with roughly two-second average and 26-second 99th-percentile (p99) response times. It does not mean each query scans petabytes.

### Forward-Looking Payoff

The work does more than add SQL syntax to CLP. Typed access, stored schema metadata, deduplicated log format patterns, and production MCP usage create a foundation for agent-oriented discovery: inspect observed structure with `SCHEMA(<path>)`, expose patterns with `PATTERN(<path>)`, and search by meaning with `SEMANTIC(<path>, <natural-language query>)`. These interfaces remain under development and evaluation.

The application-defined-schema problem is the foundation for all three takeaways, not a separate fourth takeaway: the logging platform cannot coordinate one fixed schema across independently deployed applications and versions.

The article reaches them in production-system order: schema foundation → compressed ingestion and consolidation → native SQL integration → layered search reduction and measured impact → future storage and agent workflows.

## Prior Foundations

- First article in Uber's CLP series, [“Reducing Logging Cost by Two Orders of Magnitude Using CLP”](https://www.uber.com/en-GB/blog/reducing-logging-cost-by-two-orders-of-magnitude-using-clp/): compression and write-path savings for Spark logs.
- Second article, [“Modernizing Logging at Uber with CLP (Part II)”](https://www.uber.com/gb/en/blog/modernizing-logging-with-clp-ii/): appender adoption, operational enrichment, and timed publication of active streams.
- [YScope's August 2025 blog](https://blog.yscope.com/querying-clp-compressed-logs-with-prestosql-71dff29a0b82): original motivation and query mechanics.
- Earlier PrestoDB blog, **[PRESTO_BLOG_URL](PRESTO_BLOG_URL)**: current pluggable connector, CLP versus Variant, and connector-author lessons.
- This article: Uber's production data flow, Presto query path, production evidence, and future direction.

Introduce Merged Schema Tree, Encoded Record Table, and logtype once through plain-language descriptions. Later sections should use “schema tree,” “column groups,” and “recurring text patterns” unless the formal term is necessary.

## Capability Boundary

- **Current open source:** CLP KV-IR and archives, mixed-form queries, typed exact and wildcard paths, typed value search without a known path, complete-event JSON, filters and projections pushed into CLP, and pluggable metadata and split filtering.
- **Uber production:** Collection and enrichment, metadata publication, archive orchestration, mixed-form precedence, a metastore that manages billions of file records and performs latest-N file skipping, plus a query layer that handles routing, reliability, and operational controls.
- **Further development:** Metadata-guided tiering and archive grouping build on deployed storage workflows. Schema-tree inspection, pattern discovery, and semantic search build on production MCP traffic and existing CLP metadata. Agents can summarize the resulting evidence without a separate storage-level summary operator. These more advanced workflows still need development and evaluation.

## 1. Querying Semi-Structured Logs at Scale

- **Goal:** Make both hard problems obvious to a broad technical reader: structure changes without coordination, and query-time parsing or broad scanning is too expensive at this volume.
- **Why:** The logging platform cannot require every application version to coordinate one table schema, and five workers cannot rediscover structure in every candidate event.
- **Important points:**
  - Establish the production contrast in one progression: 25 PB per month before compression and continuously changing structure, the schema and query-scale challenges, then five Presto Native workers serving hundreds of weekly users and thousands of successful daily queries by avoiding work at every layer. Link Presto Native in the opening, but defer its C++ and Velox definition to the native SQL section so it does not interrupt the story. Keep utilization measurements for Production Results.
  - Move the Ray, Michelangelo, Flink, Kafka, and Spark roster to Production Results so the opening can establish the challenge before naming platforms.
  - Use a rolling-deployment example before defining semi-structured and polymorphic data: simultaneous application versions can place `status_code` at different paths or use different value types.
  - The opening already states the global-schema and opaque-JSON tradeoff. Do not repeat it word for word. Explain what the concrete example adds: this is more than ordinary schema evolution because multiple versions coexist, so the platform cannot simply migrate from one schema to the next. It must accept each shape immediately, retain every observed type for SQL, and avoid reparsing every candidate event.
  - State the three simultaneous requirements: fresh access, compact retention, and structured execution without a global schema.
  - Treat natural-language messages, structured envelopes, and evolving JSON as one semi-structured continuum rather than rigid categories.
  - Keep the schema foundation brief. Order the section as evolving structure, the global-schema and opaque-JSON tradeoff, and CLP's preserved structure, then move to the production data flow.
  - End with an optional **CLP's semantic-preserving compression** callout. Keep it independent and limited to one concept rather than recapping the article. Define *semantic* immediately as queryable structure, relationships, and recurring patterns already present in the data, not complete natural-language understanding. Name the preserved signals: nested paths, value types, which fields occur together, and recurring text patterns. Use two structured events containing `service`, `host`, and nested library-load messages. Show that CLP preserves their shared record shape and field types while encoding the messages as one inferred pattern with searchable typed variables. Present descriptive variable labels in bold and underline for readability, while stating explicitly that the labels are illustrative rather than literal stored syntax. Do not repeat the freshness, consolidation, query-scale, or agent sections as bullets. Skipping the callout must still leave a natural transition into the production data flow.
- **Takeaway:** Preserve application-defined structure without turning application deployments into data-platform migrations.

## 2. Compressed and Queryable from Arrival through Archival

- **Goal:** Explain why freshness matters, what archive consolidation improves, and what consolidation costs.
- **Why:** Incident diagnosis, rollout validation, and active-job monitoring need the newest events. Appendable KV-IR keeps them compressed and queryable, while asynchronous consolidation improves compression and search without blocking ingestion or queries.
- **Important points:**
  - Begin with two explicitly labeled roles that cannot be left for readers to infer. **KV-IR preserves data freshness:** appendable files keep arriving events compressed and searchable without waiting for an archive. **Archives improve long-term storage and search efficiency:** asynchronous consolidation delivers a higher compression ratio and faster columnar search.
  - State why freshness is operationally important before introducing the two forms: incident diagnosis, rollout validation, and active-job monitoring cannot wait for archive construction.
  - Expand key-value intermediate representation (KV-IR) at first use.
  - Define KV-IR directly for readers who have not read the earlier blogs: an appendable compressed stream containing structural updates and encoded values.
  - Explain the structural guarantee in plain language: a path-and-type update appears before the first event that uses it, keeping each uploaded snapshot self-contained and searchable. Omit incomplete-tail behavior from this article.
  - Host log collectors use CLP to compress events into appendable KV-IR that remains searchable as it grows. Configurable triggers upload snapshots without closing the local stream. Omit atomic-replacement mechanics from this article.
  - Once Uber's metastore processes an upload's metadata update, queries can find the new events in roughly 30 seconds to a few minutes.
  - Omit the historical move from log-appender compression to host-collector compression; it is not required for the query story.
  - Uber orchestrates centralized aggregation. CLP supplies the archive writer and format.
  - The aggregation service selects groups of closed KV-IR files. Background consolidation workers use CLP to turn each group asynchronously into one immutable archive while queries continue using KV-IR. Distinguish these workers from the five Presto Native query workers. State the freshness guarantee directly: archive construction never blocks access to the data.
  - Explain that closed KV-IR files remain queryable while the aggregation service waits for a suitable consolidation opportunity. Combining multiple files creates more opportunities to deduplicate recurring structure, text, and values across file boundaries.
  - State the cost honestly. Consolidation is not resource-free, but it stays off the ingestion and query critical paths. It consumes background CPU and I/O and creates temporary storage overlap. The consolidation itself is efficient because CLP merges schema trees and appends encoded values directly to matching ERTs instead of reconstructing and reparsing events.
  - Explain the temporary overlap after consolidation and before source KV-IR deletion. For each logical log file, the metastore gives an archive priority over KV-IR and chooses the newest archive if more than one contains it. This lets one query span both storage forms without duplicate events.
  - Present direct KV-IR-to-archive consolidation as a reusable CLP capability. Do not imply that CLP archives can only be built from KV-IR.
  - Explain why this path is efficient: CLP streams each encoded KV-IR input and merges its schema tree into the archive's Merged Schema Tree (MST) without reconstructing and reparsing original events. The MST records every observed path and value type and maps them to the columns that store their values.
  - Use a paragraph break before the Encoded Record Table (ERT) mental model. An archive contains compact internal column groups rather than one wide table. Each ERT stores records with one compatible set of paths and types, so absent fields do not create null entries. State the relationship explicitly: the MST tells CLP what exists and where to find it, while the ERTs hold records of compatible shapes in homogeneous columns.
  - Define logtype before the following grouping paragraph uses it: CLP separates recurring static text from typed variables, storing each inferred format as a logtype.
  - After defining MSTs, ERTs, and logtypes, connect them explicitly to the opening claim: archives compress repeated information across files and direct searches to relevant columns and encoded text instead of scanning the entire archive.
  - Leave similarity-aware and query-aware grouping policies, archive-open reduction, and agent context to the future-work section.
- **Figure:** Place the fixed-layout SVG in `figures/clp-storage-flow.svg` immediately after the opening two-form explanation, before the MST, ERT, and logtype details. Place application containers above the host log collector. Align the CLP KV-IR snapshot and immutable CLP archive in a right-hand object-storage column. Place asynchronous consolidation between those storage states, with the metastore below the collector. Route the KV-IR read into consolidation's right edge and the remaining consolidation flows through its bottom edge. Use only horizontal, vertical, and right-angle arrows. Label the archive-write arrow “Build immutable archive.” Distinguish the metastore's selection of closed KV-IR files from the consolidation worker's physical read. Use purple for observability services and green for files in object storage, with a two-item legend across the upper right. Make the caption carry the payoff for skimmers: new logs are searchable within minutes, closed files later gain better compression and faster search, and the metastore prevents duplicates across the two forms.
- **Takeaway:** Data stays compressed and queryable as it arrives and after consolidation.

## 3. Native Semi-Structured Querying with Presto

- **Goal:** Make Presto a co-equal part of the architecture and follow one query into a native CLP scan.
- **Why:** Uber's tools and programmatic clients already depend on Presto access control, planning, routing, aggregation, and SQL tooling.
- **Important points:**
  - Bridge from Part II's individual-log viewing to broader analysis across files and time ranges: exploration, experimentation, analytics, and production diagnosis.
  - Place that bridge in the introduction as the user motivation for SQL. Begin this section with the harder native-execution requirement: SQL syntax must not become row-by-row JSON extraction. Then explain the two user-facing query interfaces without repeating the motivation.
  - Users may submit field-and-keyword log searches (Lucene-style) or write SQL directly. Uber normalizes and validates both forms before sending SQL to Presto.
  - Organize the field interface by how precisely a query can name a field, rather than presenting a function inventory:
    1. An optional Presto table schema exposes frequently used path-and-type pairs as ordinary typed columns without rewriting CLP data.
    2. Typed functions access known exact paths with or without an optional schema, including paths already exposed as columns.
    3. A wildcard expression can replace a variable segment within a path.
    4. Typed wildcard functions search compatible values when the path is completely unknown.
    5. Complete-event JSON remains available as a projection.
  - Use one SQL example showing an ordinary column, an exact path, a wildcard path, any-path typed value search, complete-event JSON, and Top-N.
  - Introduce that example in user terms before showing SQL: the latest 100 events with a server error under a variable path and an out-of-memory message in any string field, while projecting a known request ID through both access styles.
  - Label each access pattern with a short same-line SQL comment. Show `event.request_id` twice intentionally: direct access through the optional table schema and access to the same field through an exact-path function. Do not repeat those explanations below the query or introduce optimizer terminology there.
  - After the example, state the capability boundary once: typed SQL functions make value types explicit because Presto must resolve expression types during planning, while CLP can already infer the type from filter context during execution. Make clear that this is a constraint of Presto's current SQL type system, not a CLP storage or search limitation. Use a paragraph break to acknowledge Iceberg v3 Variant at the table layer and Parquet Variant at the file layer as a standardized, interoperable representation for semi-structured values, linking each concept to its official specification. Explain the complementary physical models without framing either as deficient: Parquet stores the complete dynamic value and optional shredding materializes selected paths as typed columns; CLP discovers and encodes every observed path and type as data arrives, then columnizes records with the same shape together in immutable archives. Across both CLP storage forms, recurring text patterns are separated from typed variable values. Connect that distinction to log-specific compression and wildcard path or value searches without choosing in advance which fields deserve columns. Time-qualify PrestoDB's lack of end-to-end Variant support as of Q3 2026 and link the upstream Iceberg v3 tracking issue. Future support could give CLP paths more natural SQL access without replacing CLP's physical storage model. Refer the detailed comparison to the preceding PrestoDB CLP blog.
  - Define “native” near the beginning of the section as preserving CLP's storage model instead of extracting values from reconstructed JSON, with scans executing through CLP inside Presto Native workers. State the coordinator/worker boundary once: the Java coordinator plans the query and asks the connector's metadata layer for splits, while each native worker uses the dynamically loaded connector to execute its assigned scan directly against compressed data. Later, state concisely that ordinary columns and typed functions share this CLP-native execution path. Attach the reference to YScope's earlier blog directly to the optimizer-rewriting detail.
  - Do not add a second connector-architecture figure or query-path callout. The following section explains the planning and execution layers without duplicating them here.
  - Define `split` in Section 4, where file pruning first requires the term. Introduce latest-N file skipping there as well, while reserving Top-N for Presto's distributed operator.
- **Takeaway:** Presto supplies the common relational system while the connector preserves CLP-native execution.

## 4. How Five Workers Serve the Workload: Avoiding Work at Every Layer

- **Goal:** Explain the five-worker result through concrete work elimination.
- **Why:** Compression alone does not prevent broad object reads, decompression, or materialization.
- **Important points:**
  - Tie the section directly to the opening question. A query returning 100 rows can still be expensive if Presto schedules every file in a 30-day range and examines every candidate event.
  - State the combined result before the detailed layers: across a 30-day production window, time-range filtering alone covered more than 200,000 times as many events as the three work-reduction layers examined. Phrase the achievement as preventing source volume from becoming query volume.
  - Present three skimmable layers with numbered bold lead-ins, without using a Markdown list that would be interrupted by the callout:
    1. Prune files that cannot match before scheduling. State that the connector is agnostic to its metadata provider, while Uber's provider performs query-aware file planning, including these filters and the latest-N optimization that follows.
    2. For eligible latest-N queries, avoid scheduling every older candidate. Explain first that without latest-N-aware file selection, Presto workers must process every split remaining after ordinary pruning before distributed Top-N can determine the latest `N`. Introduce newest-first file selection as a specialized planning step added at Uber, not a common metastore capability. Keep the main setup mode-neutral: a skipped file may still contain matches, and overlapping file ranges make the stopping point more nuanced than simply reaching `N` matching events. Leave time bounds, exact counts, the watermark, and the guaranteed-versus-approximate distinction to the expert callout.
    3. Apply filter and projection pushdown to each selected CLP scan. Within archives, restate the formal names without editorial narration: the Merged Schema Tree (MST) identifies relevant Encoded Record Tables (ERTs) and columns, while logtype and value dictionaries narrow the encoded streams further.
- Explain the broad-query guardrail operationally rather than calling a query “broad” without definition. When a planned query exceeds configured thresholds for file count, event count, or data volume, the query layer offers a non-blocking suggestion to narrow the timestamp range or add filters. Emphasize that this small nudge can prevent substantial work without blocking users who need the complete query. Do not invent unpublished numeric threshold values.
- Mention Uber's query-aware metadata provider in the first work-reduction layer. Reserve the connector's metadata-provider agnosticism for the open-source section.
- Keep the main latest-N explanation mode-neutral and distinguish it from pruning: a skipped older file may still contain matches. Do not claim in the main text that none can enter the requested result, since the callout covers both guaranteed and approximate modes.
- Motivate the optional callout by noting that file-range overlap creates correctness nuances and optimization opportunities. Present guaranteed and approximate as parallel modes; describe the fallback only within guaranteed mode.
- Put the correctness details in an optional Markdown blockquote immediately after the latest-N point. Open by stating that guaranteed mode preserves exact latest-N results and that approximate mode is an explicit user-selected latency-versus-completeness tradeoff. Do not add a redundant title inside the callout. Use parallel bold lead-ins and present **Approximate mode** before **Guaranteed mode**.
- Explain approximate mode as the simpler baseline: retrieve pruned files by descending `max_timestamp`, accumulate matching-event counts, set the watermark to the minimum selected `min_timestamp` when the count first reaches `N`, and return immediately. An omitted file overlapping the watermark may contain events that belong in the true latest `N`. Do not imply a correctness guarantee.
- Explain guaranteed mode as the correctness refinement. Require candidate files to be ordered by accurate bounds for the same timestamp field used by the query. Include every candidate whose `max_timestamp` overlaps the watermark and stop at the first candidate below it. Exact matching-event counts must account for every remaining filter. If any filter lacks an exact count, return every file left after pruning.
- Keep approximate mode an independent latency-versus-completeness choice, never a fallback for missing exact counts.
- Briefly note that the same logic runs in reverse for ascending timestamp queries. Explain the filter combination concretely: latest-N planning can also account for filters such as application or host when the metastore knows how many matching events each file contains. Do not imply that those fields become the sort key.
- Explain the additional benefit of a metastore that streams ordered candidates: planning can terminate candidate retrieval early instead of materializing the complete file list before discarding older files.
- State that the algorithm runs before split creation and preserve the production-versus-open-source boundary. Omit metadata-maintenance and range-quality commentary from the draft callout.
- Define the aggregate ratio in a footnote: sum the time-range-covered event counts across the query population, then divide by the summed event counts after metadata pruning, guaranteed latest-N file selection where applicable, and CLP filtering. Do not call this an average of per-query ratios. Attribute the reduction to all three layers, not pruning or latest-N planning alone. Retain the exact measured value only in the publication-evidence checklist.
- **Visualization:** Do not add a separate planning/execution figure. The three bold lead-ins provide the overview, and the optional latest-N callout carries correctness details.
- **Takeaway:** Each layer avoids work it can determine is unnecessary.

## 5. Production Results

- **Goal:** Validate the design as a serving system, not only a compression result.
- **Important points:**
  - Establish the breadth here rather than in the opening. Name representative onboarded workloads: Ray and Michelangelo for machine learning and AI, Flink and Kafka for streaming, and Spark for data platforms. Link each project or platform to its official site or introduction.
  - Define the storage boundaries explicitly: approximately 25 PB of logical uncompressed log data per month occupies 750–850 terabytes (TB) in object storage, a 29–33× reduction in physical storage, or about 97%.
  - Put the 169× comparison in a footnote rather than the main narrative. Explain that the first article measured Spark messages alone, while today's broader, enriched workload mix averages roughly 30× compression, still about 97% less storage. High-cardinality trace IDs (UUIDs) offer little repetition for compression. Convert the Markdown footnote to the publisher's native footnote component if necessary.
  - A five-node cluster of 48-core Presto Native workers serves the CLP SQL workload.
  - Scope the query metrics to the same 30-day measurement window used for the work-reduction result: hundreds of users per week and thousands of successful queries per day, with roughly two-second mean response time and 26-second p99 among successful queries.
  - Distinguish format-level and service-level objectives. CLP's storage model deliberately achieves unusually high compression while retaining direct search. Operating the production query service requires balancing interactive query latency, efficient resource use, limited impact on shared object storage, and operational simplicity rather than pursuing the lowest possible latency or maximum scan throughput at any cost. Uber applies conservative read-rate limits to control parallelism and preserve bandwidth for other workloads.
  - Scope resource utilization separately because it comes from a longer production operating period. Under the current query mix and read-rate limits, workers typically use 5–10% of their central processing unit (CPU) and memory capacity. State separately, without attaching an invented percentage, that network and input/output (I/O) utilization has remained well below available capacity. Present this specifically as compute headroom for growing demand and the broader storage and agent-focused query work described later in the article.
  - Frame staffing as operational leverage: overhead remains low, and a few engineers maintain the system alongside their broader platform responsibilities.
  - Direct object-storage reads using streaming access, without relying on a cache.
- **Takeaway:** Application flexibility, freshness, compression, SQL, and a five-worker Presto cluster work as one system.

## 6. What Comes Next

- **Goal:** Transition from production results to two development directions: smarter storage organization, followed by agent-assisted data exploration beyond today's guess-and-check process. Use an optimistic, shared narrative voice that conveys continuity.
- **Why:** Separating the two ideas avoids treating agent interaction as part of storage planning and gives the longer agent direction appropriate emphasis.
- **Important points:**
  - **Smarter data placement and consolidation:** Group the first two short directions under this subheading.
  - **Intelligent tiering:** Connect this direction to the same balance established in Production Results. Scope the model to suitable workloads. Make CLP the default queryable storage tier and provision a faster indexed tier for tables or time ranges whose access patterns and latency needs justify it. Use how often each table is queried, which fields and time ranges those queries access, and retention requirements to decide whether it needs an index and how long data should remain there. If demand rises, repopulate the index from selected CLP data.
  - **Grouping similar data during consolidation:** Connect this direction to the same metadata theme. Use schema, logtype, source, and query-history metadata to group closed KV-IR files into archives. Similar files improve deduplication, while files queried together reduce the number of archives a query opens. Retain time-range, retention, freshness, and archive-size limits.
  - **Beyond Guesswork: CLP Context for Agents:** Present MCP adoption as a longer-term production traffic trend, separate from the 30-day performance sample: agent queries through the MCP server now account for more than half of successful CLP queries by count. Establish today's problem with a tiny concrete example. An agent unfamiliar with the data may combine `message LIKE '%failed%' OR message LIKE '%exception%' OR message LIKE '%error%'` in one query. Narrow guesses can miss unfamiliar wording. Broad guesses can return too many raw events, and one common term can bury important matches in irrelevant results.
  - Distinguish the existing foundation from the interfaces being refined. CLP already preserves observed paths, types, and deduplicated log format patterns in compressed form. Present the proposed interfaces as independent tools that may be used separately or together: `SCHEMA(<path>)` would expose stored path-and-type information, `SEMANTIC(<path>, <description>)` could search relevant patterns and values by meaning before retrieving raw events, and `PATTERN(<path>)` would expose logtypes already deduplicated for compression. Bridge explicitly into the example with one useful combination: inspect where text is stored, search for a concept without its exact wording, and group the matches by recurring format. Explain the feasibility concretely: CLP keeps each pattern linked to its field, typed values, and source events, allowing this grouping to run over the original losslessly compressed data without reconstructing every event or maintaining a separate pattern store.
  - Put the detailed workflow in one optional, independently shareable callout titled “From guessed keywords to three recurring failure patterns.” Ask the simpler question, “Which native connector load failures recur?” Include the role of all three interfaces, compact SQL, an illustrative result table, and the 72-events-to-three-patterns payoff.
  - Keep the example vendor-neutral with a `clp.coding_agent_sessions` table. Explain `SCHEMA('message')` in prose rather than displaying a tree. Label the SQL explicitly as an illustration of an interface under development. Apply `SEMANTIC(...)` to the discovered text path and group matches by `PATTERN(...)` only. Use named variables such as `%library_path%` in the output. After the callout, note that ordinary SQL can further break known patterns down by branch, version, service, or another useful dimension.
  - Define each operator before its syntax appears. Explain that coding-agent sessions can contain polymorphic, nested message content, so `SCHEMA()` reveals the observed paths and types instead of requiring the agent to guess where text is stored. Before the SQL, explain that `SEMANTIC()` matches the requested idea despite different wording and `PATTERN()` replaces changing details with named variables so equivalent formats can be grouped. Do not repeat those definitions after the SQL.
  - Make the prose before and after the callout flow without it. Close by framing CLP's deployed storage and search as the foundation for these interfaces. State that they are being refined and evaluated for both Uber's workloads and broader CLP use cases, and that they can operate on CLP's compact representation before reconstructing individual events.
  - Do not apply one blanket maturity label. State that these directions build on deployed capabilities while their more advanced forms remain under development and evaluation.
- **Takeaway:** Metadata can guide storage organization and, separately, help agents discover and retrieve relevant evidence without guessing exact paths or terms.
- **Weighting:** Use tiering → consolidation → querying. Keep the first two to one substantive paragraph each, then end with the longer agent direction and the section's only SQL example.
- **Length safeguard:** Avoid explaining keyword limitations or the three proposed interfaces more than once. Let the query and illustrative result table carry the detailed agent example.

## 7. Open Source, Collaboration, and Conclusion

- List current open-source capabilities, including metadata-provider agnosticism, Uber production extensions, and further-development areas once.
- Link the μSlope paper here and attribute its storage foundation to YScope, the University of Toronto, and Uber.
- Give individual developers a concrete next step: run the connector's Docker Compose integration stack against sample KV-IR and archive data, inspect the native boundary, and adapt its metadata-provider or worker-plugin pattern.
- Credit YScope for leading development of the CLP connector for Presto Native, with design input from Presto teams at Uber and IBM, as well as Meta's Presto and Velox teams.
- Present the infrastructure work with precise roles: YScope implemented and submitted the upstream changes for runtime connector registration and connector-specific coordinator-to-worker serialization, and IBM's Presto team reviewed them. Link both merged pull requests. State the concrete outcome: CLP became the first end-to-end pluggable connector for Presto Native. Connect source-tree independence to shorter connector development and release cycles plus simpler deployment and maintenance.
- Credit Uber's Observability team for integrating the connector with Uber's internal metastore and existing query layer and for operating the resulting production service.
- Credit YScope and Uber's Presto team for internal connector infrastructure, and YScope and Uber Observability for production planning, including latest-N file skipping.
- Open the conclusion with the real accomplishment: adding SQL syntax was not the hard part. The challenge was keeping evolving logs fresh, compact, and queryable while touching only a small fraction of petabyte-scale data. Then explicitly frame this as the third article and state that the three-part series documents nearly five years of collaborative work to build the CLP platform at Uber for compression, storage, and query. Recap the series in three short steps: the first blog made longer retention affordable, the second kept compressed logs fresh and easy to browse, and this blog adds native SQL across KV-IR and archives. Summarize layered work reduction, efficient CLP execution, distinct Presto and CLP responsibilities, and production headroom. Describe it as a query service available across Uber for workloads onboarded to CLP, not as covering every Uber log source. End by connecting today's gentle query guidance to a foundation for agent-driven root-cause analysis, behavior analytics, and exploratory debugging. Do not repeat the latest-N mechanics.
- End on the memorable thesis that, at petabyte scale, the most effective optimization is often work the system never schedules or reads. Connect structure preserved from ingestion through execution to today's efficiency and tomorrow's agent workflows.

> Common SQL and distributed execution can coexist with specialized semi-structured storage and native execution.

## Guardrails

- Scope 25 PB/month to logical uncompressed data from currently onboarded workloads, never one query or physical bytes read.
- Keep raw volume, compressed footprint, and bytes read distinct.
- Keep schemas archive- or split-local, not dataset-global.
- Attribute Uber services, Presto, the connector, and CLP's native storage and search engine precisely.
- Keep Uber's metastore implementation a black box.
- Do not repeat the optimizer walkthrough, benchmarks, Variant comparison, or connector-author guide.
- Distinguish current open source, Uber production, and further-development areas at first mention.
- Do not describe metadata-guided archive grouping, automated tiering, selective reload, or the proposed agent operations as complete production or open-source workflows.
- Match the earlier series' engineer-to-engineer voice: identify the actor, action, and result; use “we” for Uber design choices; and prefer concrete verbs over abstract umbrella phrases.
- Keep genuine technical terms such as KV-IR, split, and Top-N, and define each through the work it performs. Introduce MST and ERT once for technical readers, then prefer their plain-language descriptions.
- Do not use em dashes. Use periods, commas, parentheses, or colons instead. Keep en dashes in numeric ranges.
- Replace `PRESTO_BLOG_URL` before publication.
- Finish with a complete narrative review and `git diff --check`.
