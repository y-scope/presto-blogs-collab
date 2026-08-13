# Bringing Native SQL to Petabyte-Scale Semi-Structured Logs: Inside Uber’s CLP and Presto Infrastructure (Part III)

- **Type:** Production architecture case study
- **Length:** 3,000-word hard cap, including final evidence
- **Audience:** Presto, observability, storage, data-platform, streaming, machine-learning, and artificial-intelligence engineers
- **Central question:** How does Uber keep evolving logs compressed and queryable as they arrive, expose them through SQL, and serve data from workloads producing roughly 25 petabytes (PB) per month with five Presto Native workers?

## Story

CLP's compression is necessary but does not explain how five Presto Native workers serve the current workload. Presto, Uber's metastore, the connector, and CLP each avoid different work as the story follows data from production to query:

> Applications and platform workloads → searchable CLP key-value intermediate representation (KV-IR) → consolidated columnar CLP archives → one Presto query path → progressively narrower execution

Make the series payoff explicit in the introduction: Part I established compression and pointed toward familiar SQL through Presto, Part II made compressed ingestion and viewing operational, and Part III delivers the native SQL path across fresh and consolidated data.

### Three Reader Takeaways

1. **Native SQL for evolving semi-structured logs.** The Presto CLP connector exposes polymorphic paths through typed SQL while preserving CLP's native storage and execution. It is neither a conversion into one fixed physical table nor a syntax-only SQL adapter that materializes every event first.
2. **Compressed and queryable throughout ingestion.** Appendable KV-IR snapshots make arriving data searchable after their metadata reaches the metastore, typically within 30 seconds to a few minutes. Asynchronous consolidation later creates columnar CLP archives. Queries span both forms without duplicate events, and the onboarded workloads achieve roughly 30× aggregate compression, or about 97% less storage.
3. **A small query cluster through layered work reduction.** Metastore pruning, latest-N file skipping, and CLP's archive-local metadata prevent most irrelevant files, encoded streams, and events from being examined. This—not a claim that each query scans petabytes—explains how five Presto Native workers serve hundreds of weekly users and thousands of daily queries with approximately two-second average and 26-second p99 response times.

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
- **Uber production:** Collection and enrichment, metadata publication, archive orchestration, mixed-form precedence, a metastore that manages billions of file records and performs latest-first limit (Top-N) file skipping, plus a query layer that handles routing, reliability, and operational controls.
- **Further development:** Metadata-guided tiering and archive grouping build on deployed storage workflows. Schema-tree inspection, pattern discovery, and semantic search build on production MCP traffic and existing CLP metadata. Agents can summarize the resulting evidence without a separate storage-level summary operator. These more advanced workflows still need development and evaluation.

## 1. Logs Without a Global Schema

- **Goal:** Establish why schema evolution is continuous rather than an occasional migration.
- **Why:** The logging platform cannot require every application and deployed version to coordinate one complete table schema.
- **Important points:**
  - Name representative onboarded workloads in the opening: Ray and Michelangelo for machine learning and AI, Flink and Kafka for streaming, and Spark as a data platform. Link each project or platform to its official site or introduction.
  - Use message and nested-JSON examples before defining semi-structured and polymorphic data.
  - Follow with the storage tradeoff: a global schema requires coordinated migrations, while an opaque JSON string forces every query to rediscover structure.
  - Refer to the second article in the series for appender and enrichment mechanics.
  - Repeat only the resulting storage problem: original messages, typed fields, nested objects, and evolving enrichment coexist.
  - Treat natural-language messages, structured envelopes, and evolving JSON as one semi-structured continuum rather than rigid categories.
  - Keep the schema foundation brief. Order the section as evolving structure → global-schema and opaque-JSON tradeoff → prior production context and μSlope foundation, then move to the production data flow.
  - Link the μSlope paper and attribute it inline to YScope, the University of Toronto, and Uber without listing individual teams here. Explain why it matters: storing shared structure once and grouping records by shape supports high compression and search without full event decompression.
  - End with the optional **CLP's semantics-preserving compression** callout. Define *semantics* as queryable structure—field paths, value types, record shapes, and inferred log format patterns—not natural-language meaning. For natural-language fields, explain that CLP infers logtypes that preserve recurring static text and mark changing values with typed labels that may also be named, while encoding the variable values separately. Explain that CLP keeps this information explicit for compression and search, then foreshadow semantic search and pattern analysis for agents. Skipping the callout must not interrupt the transition into the production data flow.
- **Takeaway:** Preserve application-defined structure without turning application deployments into data-platform migrations.

## 2. Compressed and Queryable from Arrival through Archival

- **Goal:** Explain why queries can span newly emitted events and older consolidated data without waiting for one storage phase to finish.
- **Why:** CLP keeps both storage forms compressed and queryable, then uses consolidation to improve compression and search.
- **Important points:**
  - Begin from the query requirement, not the ingestion pipeline: one query may span data still arriving and data already consolidated.
  - Expand key-value intermediate representation (KV-IR) at first use.
  - Define KV-IR directly for readers who have not read the earlier blogs: an appendable compressed stream containing structural updates and encoded values.
  - Explain the structural guarantee in plain language: a path-and-type update appears before the first event that uses it, keeping the file self-describing and searchable as data arrives.
  - Host log collectors use CLP to compress events into appendable KV-IR that remains streaming-searchable as it grows. Configurable triggers upload a snapshot of the current contents to object storage without closing the local stream; later uploads replace the same object with a newer snapshot.
  - Once Uber's metastore processes an upload's metadata update, queries can find the new events in roughly 30 seconds to a few minutes.
  - Omit the historical move from log-appender compression to host-collector compression; it is not required for the query story.
  - Uber orchestrates centralized aggregation. CLP supplies the archive writer and format.
  - The aggregation service selects groups of closed KV-IR files. Workers use CLP to consolidate each group asynchronously into one immutable archive while queries continue using KV-IR.
  - Explain the temporary overlap after consolidation and before source KV-IR deletion. For each source file, the metastore gives an archive priority over KV-IR and chooses the newest archive if more than one contains it. This lets one query span both storage forms without duplicate events.
  - Present direct KV-IR-to-archive consolidation as a reusable CLP capability. Do not imply that CLP archives can only be built from KV-IR.
  - Explain why this path is efficient: CLP streams each encoded KV-IR input, merges its schema tree into the archive's Merged Schema Tree (MST), and transposes encoded event rows into the Encoded Record Table (ERT) columns for their shapes instead of reconstructing, reparsing, and re-indexing original events.
  - Introduce each acronym through its physical role. The MST is the archive-wide tree of nested paths and value types. Each ERT is an internal set of homogeneous columns for one record shape, avoiding a wide table filled with nulls. Typed variables occupy separate columns.
  - Explain the mechanism rather than size alone: when a consolidation group contains similar files, CLP can deduplicate more recurring structure, logtypes, and values across them. Do not imply that today's grouping policy already selects files by similarity.
  - Grouping files queried together can reduce the number of archives a query must open. Leave grouping constraints and policy tradeoffs to the future-work section.
- **Figure:** Use the fixed-layout SVG in `figures/clp-storage-flow.svg`. Place application containers above the host log collector. Align the CLP KV-IR snapshot and immutable CLP archive in a right-hand object-storage column. Place asynchronous consolidation between those storage states, with the metastore below the collector. Route the KV-IR read into consolidation's right edge and the remaining consolidation flows through its bottom edge. Use only horizontal, vertical, and right-angle arrows. Label the archive-write arrow “Build immutable archive.” Distinguish the metastore's selection of closed KV-IR files from the consolidation worker's physical read. Use purple for observability services and green for files in object storage, with a two-item legend across the upper right.
- **Takeaway:** Data stays compressed and queryable as it arrives and after consolidation.

## 3. Native Semi-Structured Querying with Presto

- **Goal:** Make Presto a co-equal part of the architecture and follow one query into a native CLP scan.
- **Why:** Uber's tools and programmatic clients already depend on Presto access control, planning, routing, aggregation, and SQL tooling.
- **Important points:**
  - Bridge from Part II's individual-log viewing to broader analysis across files and time ranges: exploration, experimentation, analytics, and production diagnosis.
  - Place that bridge in the introduction as the user motivation for SQL. Begin this section directly with the two query interfaces to avoid repeating it.
  - Users may submit Lucene-style searches or write SQL directly. Uber normalizes and validates both forms before sending SQL to Presto.
  - Organize the field interface by how precisely a query can name a field, rather than presenting a function inventory:
    1. An optional Presto table schema exposes frequently used path-and-type pairs as ordinary typed columns without rewriting CLP data.
    2. Typed functions access known exact paths with or without an optional schema, including paths already exposed as columns.
    3. A wildcard expression can replace a variable segment within a path.
    4. Typed wildcard functions search compatible values when the path is completely unknown.
    5. Complete-event JSON remains available as a projection.
  - Use one SQL example showing an ordinary column, an exact path, a wildcard path, any-path typed value search, complete-event JSON, and Top-N.
  - Introduce that example in user terms before showing SQL: the latest 100 events with a server error under a variable path and an out-of-memory message in any string field, while projecting a known request ID through both access styles.
  - Label each access pattern with a short same-line SQL comment. Show `event.request_id` twice intentionally: direct access through the optional table schema and access to the same field through an exact-path function. Do not repeat those explanations below the query or introduce optimizer terminology there.
  - After the example, state the capability boundary once: typed SQL functions make value types explicit because Presto must resolve expression types during planning, while CLP can already infer the type from predicate context during execution. Make clear that this is a constraint of Presto's current SQL type system, not a CLP storage or search limitation. Immediately connect the preceding PrestoDB CLP blog: future Variant support could expose CLP's dynamic values through more natural field access instead of separate type-specific functions. Refer detailed Variant and shredding discussion to that blog.
  - Define “native” in two senses: the SQL syntax feels familiar, and the connector invokes CLP within each Presto Native worker process. Briefly explain the bridge: during planning, a connector optimizer rewrites CLP function calls into column references backed by field requests; during execution, those requests and supported filters pass to CLP. This avoids generic row materialization and per-event JSON serialization and deserialization for functions such as `JSON_EXTRACT_SCALAR()`, which would weaken vectorized columnar execution. CLP reconstructs only requested results. Attach the reference to YScope's earlier blog directly to this explanation of the rewriting mechanics.
  - Reuse “semantics-preserving representation” once here to connect direct native execution to the archive model; do not redefine the term.
  - Do not add a second connector-architecture figure or query-path callout. The following section explains the planning and execution layers without duplicating them here.
  - Define `split` in Section 4, where file pruning first requires the term. Introduce Top-N file skipping there as well.
- **Takeaway:** Presto supplies the common relational system while the connector preserves CLP-native execution.

## 4. Exploiting Data Knowledge at Every Layer

- **Goal:** Explain the five-worker result through concrete work elimination.
- **Why:** Compression alone does not prevent broad object reads, decompression, or materialization.
- **Important points:**
  - Tie the section directly to the opening question: the five-worker result comes from the following three layers.
  - Present three skimmable layers with numbered bold lead-ins, without using a Markdown list that would be interrupted by the callout:
    1. Prune files that cannot match before scheduling. Briefly state that this file-pruning step is metastore-agnostic.
    2. For eligible latest-N queries, avoid scheduling older files whose matches cannot change the result. Explain first that Presto normally scans every file left after pruning and globally orders the matches. Introduce newest-first file selection as a specialized planning step added at Uber, not a common metastore capability. Leave time bounds, exact counts, and the watermark to the expert callout.
    3. Apply filter and projection pushdown to each selected CLP scan. Within archives, restate the formal names without editorial narration: the Merged Schema Tree (MST) identifies relevant Encoded Record Tables (ERTs) and columns, while logtype and value dictionaries narrow the encoded streams further.
- Explain the broad-query guardrail operationally rather than calling a query “broad” without definition. When a planned query exceeds configured thresholds for file count, event count, or data volume, the query layer offers a non-blocking suggestion to narrow the timestamp range or add filters. Emphasize that this small nudge can prevent substantial work without blocking users who need the complete query. Do not invent unpublished numeric threshold values.
- Mention metadata-provider agnosticism only in the first layer; do not repeat it in the open-source section.
- Keep the main latest-N explanation to its key distinction from pruning: a skipped older file may contain matches, but none can enter the requested result.
- Motivate the optional callout by noting that file-range overlap creates correctness nuances and optimization opportunities. Present guaranteed and approximate as parallel modes; describe the fallback only within guaranteed mode.
- Put the correctness details in an optional Markdown blockquote immediately after the latest-N point. The preceding sentence already introduces its purpose, so do not add a redundant title inside the callout. Use parallel bold lead-ins and present **Approximate mode** before **Guaranteed mode**.
- Explain approximate mode as the simpler baseline: retrieve pruned files by descending `max_timestamp`, accumulate matching-event counts, set the watermark to the minimum selected `min_timestamp` when the count first reaches `N`, and return immediately. An omitted file overlapping the watermark may contain events that belong in the true latest `N`; do not imply a correctness guarantee.
- Explain guaranteed mode as the correctness refinement: include every candidate whose `max_timestamp` overlaps the watermark and stop at the first candidate below it. Exact per-file counts ensure later candidates cannot change the result. If any predicate lacks an exact count, return every file left after pruning.
- Keep approximate mode an independent latency-versus-completeness choice, never a fallback for missing exact counts.
- Briefly note that the same logic runs in reverse for ascending timestamp queries. Explain the filter combination concretely: latest-N planning can also account for filters such as application or host when the metastore knows how many matching events each file contains. Do not imply that those fields become the sort key.
- Explain the additional benefit of a metastore that streams ordered candidates: planning can terminate candidate retrieval early instead of materializing the complete file list before discarding older files.
- State that the algorithm runs before split creation and preserve the production-versus-open-source boundary. Omit metadata-maintenance and range-quality commentary from the draft callout.
- After the third layer, state the combined result: across the measured production query population, the three layers reduce the number of log events examined by over 200,000× on average compared with time-range filtering alone. Attribute the reduction to all three layers, not pruning or latest-N planning alone. Retain the exact measured value only in the publication-evidence checklist.
- **Visualization:** Do not add a separate planning/execution figure. The three bold lead-ins provide the overview, and the optional latest-N callout carries correctness details.
- **Takeaway:** Each layer avoids work it can determine is unnecessary.

## 5. Production Results

- **Goal:** Validate the design as a serving system, not only a compression result.
- **Important points:**
  - Approximately 25 PB/month raw and 750–850 terabytes (TB) per month compressed.
  - Put the 169× comparison in a footnote rather than the main narrative. Explain that the first article measured Spark messages alone, while today's broader, enriched workload mix averages roughly 30× compression, still about 97% less storage. High-cardinality trace IDs (UUIDs) offer little repetition for compression. Convert the Markdown footnote to the publisher's native footnote component if necessary.
  - A five-node cluster of 48-core Presto Native workers serves the CLP SQL workload.
  - Current production scale: hundreds of users per week and thousands of queries per day, with approximately two-second average response time and 99% of queries finishing within 26 seconds.
  - Under the current workload, workers typically use 5–10% of their central processing unit (CPU) and memory capacity. Network and input/output (I/O) utilization also remain low.
  - Uber applies conservative rate limits to object-storage reads to control parallelism and preserve bandwidth for other workloads. Even under these limits, queries meet the required latency, leaving room to serve more queries or onboard more workloads.
  - State the scoped maintenance result without repeating the component inventory: a few engineers maintain the entire system as part of their broader roles rather than as their primary responsibility.
  - Direct object-storage reads using streaming access, without relying on a cache.
- **Takeaway:** Application flexibility, freshness, compression, SQL, and a five-worker Presto cluster work as one system.

## 6. What Comes Next

- **Goal:** Transition from production results to two development directions: smarter storage organization, followed by agent-assisted data exploration beyond today's guess-and-check process.
- **Why:** Separating the two ideas avoids treating agent interaction as part of storage planning and gives the longer agent direction appropriate emphasis.
- **Important points:**
  - **Smarter data placement and consolidation:** Group the first two short directions under this subheading.
  - **Intelligent tiering:** Scope the model to suitable workloads. Make CLP the default queryable storage tier and reserve faster indexed storage for tables or time ranges whose access patterns and latency needs justify it. Use how often each table is queried, which fields and time ranges those queries access, and retention requirements to decide whether it needs an index and how long data should remain there. If demand rises, quickly restore selected tables or time ranges from CLP to the index.
  - **Grouping similar data during consolidation:** Use schema, logtype, and source metadata plus query history to group closed KV-IR files into archives. Similar files improve deduplication, while files queried together reduce the number of archives a query opens. Retain time-range, retention, freshness, and archive-size limits.
  - **Beyond Guesswork: CLP Context for Agents:** Use production MCP traffic and unfamiliar-log workflows to establish the problem in one short paragraph. Agents may not know which fields exist or which wording will find relevant events, so they guess paths and keyword lists. Narrow queries miss events; broad queries return too much raw data.
  - Explain once that CLP already preserves observed paths, types, and deduplicated log format patterns in compressed form. Do not define the proposed operators again in the surrounding prose.
  - Put the detailed workflow in one optional, independently shareable callout titled “From unfamiliar logs to three recurring failure patterns.” It should contain the diagnostic question, all three operator roles, formatted SQL with short inline comments, an illustrative result table, and the 72-events-to-three-patterns payoff.
  - Use `SCHEMA('message')` to reveal the nested `message.content[*].content` path. Apply `SEMANTIC(...)` to that path and group the matches by both `PATTERN(...)` and `gitBranch`. Use named variables such as `%library_path%` in the resulting patterns instead of generic placeholders.
  - Define each operator through its behavior inside the callout: `SCHEMA()` reveals observed paths and types, `SEMANTIC()` matches the requested idea despite different wording, and `PATTERN()` groups messages with the same format while replacing changing details with named variables.
  - Make the prose before and after the callout flow without it. Close with the native cost model and maturity boundary: the interfaces are being refined and evaluated, and can operate on CLP's compact representation before reconstructing individual events.
  - Do not apply one blanket maturity label. State that these directions build on deployed capabilities while their more advanced forms remain under development and evaluation.
- **Takeaway:** Metadata can guide storage organization and, separately, help agents discover and retrieve relevant evidence without guessing exact paths or terms.
- **Weighting:** Use tiering → consolidation → querying. Keep the first two to one substantive paragraph each, then end with the longer agent direction and the section's only SQL example.
- **Length safeguard:** Avoid explaining keyword limitations or the three proposed interfaces more than once. Let the query and illustrative result table carry the detailed agent example.

## 7. Open Source, Collaboration, and Conclusion

- List current open-source capabilities, Uber production extensions, and further-development areas once.
- Frame the Presto Native CLP connector implementation as YScope-led work with IBM's and Uber's Presto teams and the broader Presto community on native connector infrastructure and review, and with Meta's Presto team on design discussions.
- Credit Uber's Observability team for integrating the connector with Uber's internal metastore and existing query layer.
- Credit YScope and Uber Observability engineers for jointly developing the production-specific planning, including Top-N file skipping.
- Keep the conclusion compact. Explicitly frame this article as the third blog documenting nearly five years of collaborative work to build the CLP platform at Uber for compression, storage, and query, then recap the series in three short steps: the first blog made longer retention affordable, the second kept compressed logs fresh and easy to browse, and this blog adds native SQL across KV-IR and archives. Summarize layered work reduction, efficient CLP execution, distinct Presto and CLP responsibilities, and production headroom. End by connecting today's gentle query guidance to a foundation for agent-driven root-cause analysis, behavior analytics, and exploratory debugging. Do not repeat the Top-N mechanics.
- End on the memorable thesis that, at petabyte scale, the most effective optimization is often work the system never schedules or reads. Connect structure preserved from ingestion through execution to today's efficiency and tomorrow's agent workflows.

> Common SQL and distributed execution can coexist with specialized semi-structured storage and native execution.

## Guardrails

- Scope 25 PB/month to currently onboarded workloads, never one query.
- Keep raw volume, compressed footprint, and bytes read distinct.
- Keep schemas archive- or split-local, not dataset-global.
- Attribute Uber services, Presto, the connector, and CLP's native storage and search engine precisely.
- Keep Uber's metastore implementation a black box.
- Do not repeat the optimizer walkthrough, benchmarks, Variant comparison, or connector-author guide.
- Distinguish current open source, Uber production, and further-development areas at first mention.
- Do not describe metadata-guided archive grouping, automated tiering, selective reload, or the proposed agent operations as complete production or open-source workflows.
- Match the earlier series' engineer-to-engineer voice: identify the actor, action, and result; use “we” for Uber design choices; and prefer concrete verbs over abstract umbrella phrases.
- Keep genuine technical terms such as KV-IR, split, and Top-N, and define each through the work it performs. Introduce MST and ERT once for technical readers, then prefer their plain-language descriptions.
- Replace `PRESTO_BLOG_URL` before publication.
- Finish with a complete narrative review and `git diff --check`.
