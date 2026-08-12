# Making Petabyte-Scale Semi-Structured Logs SQL-Native: Inside Uber’s CLP and Presto Infrastructure (Part III)

- **Type:** Production architecture case study
- **Length:** 3,000-word hard cap, including final evidence
- **Audience:** Presto, observability, storage, data-platform, streaming, machine-learning, and artificial-intelligence engineers
- **Central question:** How can five Presto Native workers provide SQL access to data from workloads producing roughly 25 petabytes (PB) of raw logs per month?

## Story

CLP's compression is necessary but does not explain how five Presto Native workers serve the current workload. Presto, Uber's metastore, the connector, and CLP each avoid different work as the story follows data from production to query:

> Applications and platform workloads → searchable CLP key-value intermediate representation (KV-IR) → consolidated columnar CLP archives → one Presto query path → progressively narrower execution

Readers should understand:

- Why application-defined log structure requires semi-structured storage.
- How CLP keeps newly arriving and consolidated data compressed and queryable.
- What Presto contributes beyond being a SQL endpoint.
- How Uber's metastore, the connector, and CLP's native storage and search engine eliminate different work.
- How SQL semantics lay the groundwork for an artificial-intelligence-friendly interface and more intelligent data placement.

## Prior Foundations

- First article in Uber's CLP series, [“Reducing Logging Cost by Two Orders of Magnitude Using CLP”](https://www.uber.com/en-GB/blog/reducing-logging-cost-by-two-orders-of-magnitude-using-clp/): compression and write-path savings for Spark logs.
- Second article, [“Modernizing Logging at Uber with CLP (Part II)”](https://www.uber.com/gb/en/blog/modernizing-logging-with-clp-ii/): appender adoption, operational enrichment, and timed publication of active streams.
- [YScope's August 2025 blog](https://blog.yscope.com/querying-clp-compressed-logs-with-prestosql-71dff29a0b82): original motivation and query mechanics.
- Earlier PrestoDB blog, **[PRESTO_BLOG_URL](PRESTO_BLOG_URL)**: current pluggable connector, CLP versus Variant, and connector-author lessons.
- This article: Uber's production data flow, Presto query path, production evidence, and future direction.

Introduce Merged Schema Tree, Encoded Record Table, and logtype once through plain-language descriptions. Later sections should use “path-and-type map,” “column groups,” and “recurring text patterns” unless the formal term is necessary.

## Capability Boundary

- **Current open source:** CLP KV-IR and archives, mixed-form queries, typed exact and wildcard paths, typed value search without a known path, complete-event JSON, filters and projections pushed into CLP, and pluggable metadata and split filtering.
- **Uber production:** Collection and enrichment, metadata publication, archive orchestration, mixed-form precedence, a metastore that manages billions of file records and performs latest-first limit (Top-N) file skipping, plus a query layer that handles routing, reliability, and operational controls.
- **Further development:** Metadata-guided tiering and archive grouping build on deployed storage workflows. Pattern discovery, semantic search, and summaries build on production MCP traffic and existing CLP metadata. Their more advanced forms still need development and evaluation.

## 1. When Applications Define the Log Structure

- **Goal:** Establish why schema evolution is continuous rather than an occasional migration.
- **Why:** The logging platform cannot require every application and deployed version to coordinate one complete table schema.
- **Important points:**
  - Use message and nested-JSON examples before defining semi-structured and polymorphic data.
  - Refer to the second article in the series for appender and enrichment mechanics.
  - Repeat only the resulting storage problem: original messages, typed fields, nested objects, and evolving enrichment coexist.
  - Treat natural-language messages, structured envelopes, and evolving JSON as one semi-structured continuum rather than rigid categories.
  - Keep the schema foundation brief. Contrast structural preservation with whole-document parsing, then move to the production data flow.
  - Link the μSlope paper and attribute it inline to YScope, the University of Toronto, and Uber without listing individual teams here.
- **Takeaway:** Preserve application-defined structure without turning application deployments into data-platform migrations.

## 2. Compressed and Queryable as Data Arrives and After Consolidation

- **Goal:** Explain why queries can span newly emitted events and older consolidated data without waiting for one storage phase to finish.
- **Why:** CLP keeps both stages compressed and queryable, then uses consolidation to improve compression and search.
- **Important points:**
  - Begin from the query requirement, not the ingestion pipeline: one query may span data still arriving and data already consolidated.
  - Expand key-value intermediate representation (KV-IR) at first use.
  - Define KV-IR directly for readers who have not read the earlier blogs: an appendable compressed stream containing structural updates and encoded values.
  - Explain the structural guarantee in plain language: a path-and-type update appears before the first event that uses it, keeping the file self-describing and searchable as data arrives.
  - Host log collectors stream events into KV-IR. Configurable triggers upload current contents without closing the local stream; later uploads replace the same object with a longer snapshot.
  - Once Uber's metastore processes an upload's metadata update, queries can find the new events in roughly 30 seconds to a few minutes.
  - Omit the historical move from log-appender compression to host-collector compression; it is not required for the query story.
  - Uber orchestrates centralized aggregation. CLP supplies the archive writer and format.
  - The aggregation service selects closed KV-IR files for consolidation. A worker uses CLP to combine each selected group into one immutable archive.
  - Consolidation runs asynchronously. Queries continue using KV-IR while the archive is built.
  - Lead with consolidation efficiency: CLP processes encoded KV-IR directly instead of reconstructing and reparsing original events.
  - Introduce the plain-language mental model before the acronyms: CLP merges path-and-type metadata into an archive-wide map called the Merged Schema Tree (MST), then appends each record to the columnar group for its shape, called an Encoded Record Table (ERT).
  - Explain the physical benefit in familiar terms: separate record-shape groups avoid filling one wide table with nulls, and typed variables occupy separate columns.
  - Define logtypes as recurring static text patterns stored separately from typed variables.
  - Explain the mechanism rather than size alone: when an archive combines similar files, CLP can deduplicate more recurring structure, logtypes, and values across them.
  - Grouping files queried together can reduce archive opens. Balance this and compression against object size, request cost, query latency, and parallelism.
- **Figure:** Use the fixed-layout SVG in `figures/clp-storage-flow.svg`. Place application containers above the host log collector and the latest KV-IR snapshot directly to the collector's right. On the next row, place Uber's metastore, asynchronous consolidation, and the consolidated columnar archive horizontally. Label each arrow. Distinguish the metastore's selection of closed KV-IR files from the consolidation worker's physical read of those files. Consolidation produces both the archive and its metadata.
- **Takeaway:** Data stays compressed and queryable as it arrives and after consolidation.

## 3. Native Semi-Structured Querying with Presto

- **Goal:** Make Presto a co-equal part of the architecture and follow one query into a native CLP scan.
- **Why:** Uber's tools and programmatic clients already depend on Presto access control, planning, routing, aggregation, and SQL tooling.
- **Important points:**
  - Bridge from Part II's individual-log viewing to broader analysis across files and time ranges: exploration, experimentation, analytics, and production diagnosis.
  - Users may submit Lucene-style searches or write SQL directly. Uber normalizes and validates both forms before sending SQL to Presto.
  - Organize the field interface by how precisely a query can name a field, rather than presenting a function inventory:
    1. An optional table schema binds frequently used path-and-type pairs to ordinary typed columns without rewriting CLP data.
    2. Typed functions access known exact paths with or without an optional schema, including paths already exposed as columns.
    3. A wildcard expression can replace a variable segment within a path.
    4. Typed wildcard functions search compatible values when the path is completely unknown.
    5. Complete-event JSON remains available as a projection.
  - State the capability boundary once: CLP can infer value types from a query, while the current Presto connector still requires typed SQL functions. Describe exposing inference through the connector as being on our to-do list.
  - Use one SQL example showing an ordinary column, an exact path, a wildcard path, any-path typed value search, complete-event JSON, and Top-N.
  - Label each access pattern with a short same-line SQL comment. Do not repeat those explanations below the query or introduce optimizer terminology there.
  - Keep Variant to one contextual sentence. Refer detailed Variant and shredding discussion to the earlier PrestoDB blog.
  - Define “native” in two senses: the SQL interface feels familiar, and execution reaches CLP's encoded storage directly. These are not wrappers around `JSON_EXTRACT_SCALAR()` and do not first pass every event through a serializer/deserializer (SerDe) to create a generic row. CLP reconstructs only requested results. Refer rewriting mechanics to YScope's earlier blog.
  - End by summarizing both halves of native semi-structured querying: a familiar SQL interface and direct CLP execution. Transition to the need to reduce the files and encoded values each query touches at Uber's scale.
  - Define `split` in Section 4, where file pruning first requires the term. Introduce Top-N file skipping there as well.
- **Takeaway:** Presto supplies the common relational system while the connector preserves CLP-native execution.

## 4. Exploiting Data Knowledge at Every Layer

- **Goal:** Explain the five-worker result through concrete work elimination.
- **Why:** Compression alone does not prevent broad object reads, decompression, or materialization.
- **Important points:**
  - Present three skimmable layers with numbered bold lead-ins, without using a Markdown list that would be interrupted by the callout:
    1. Prune files that cannot match before scheduling.
    2. For eligible latest-N queries, avoid scheduling older files whose matches cannot change the result.
    3. Inside each selected archive, use the path-and-type map plus logtype and value dictionaries to narrow the encoded scan.
- Explain the broad-query guardrail: during planning, Uber detects queries expected to cover many files and events and suggests narrower time or metadata filters. This lightweight prompt often leads users to add constraints they already know, substantially reducing the scan. Users can still proceed when they need the complete result.
- State why metadata-provider agnosticism matters: each deployment can plug in an implementation suited to its catalog and storage system.
- Keep the main latest-N explanation to its key distinction from pruning: a skipped older file may contain matches, but none can enter the requested result.
- Motivate the optional callout by noting that correctness becomes subtle when file time ranges overlap. Tell interested readers that the callout explains the watermark and safe fallback.
- Put correctness details in an optional Markdown blockquote immediately after the latest-N point. Use a descriptive title suitable for conversion to the publisher's native callout component.
- In that optional expert callout, keep only the correctness invariant. Stream pruned candidates by descending `max_timestamp`, accumulate exact matches, lock the watermark to the minimum selected `min_timestamp` when the count first reaches `N`, include overlapping candidates at or above the watermark, and stop at the first candidate below it.
- State guaranteed-mode fallback compactly: whenever any remaining predicate lacks an exact per-file count, return every file left after pruning.
- Describe approximate mode as an explicit latency-versus-completeness choice. Estimated matching counts may choose the cutoff. Presto still sorts selected events, but skipped files may contain events that belong in the true latest `N`; do not imply a correctness guarantee.
- State that the algorithm runs before split creation and preserve the production-versus-open-source boundary. Omit metadata-maintenance and range-quality commentary from the draft callout.
- **Visualization:** Do not add a separate planning/execution figure. The three bold lead-ins provide the skimmable structure, and the optional latest-N callout carries the correctness details.
- **Takeaway:** Each layer avoids work it can determine is unnecessary.

## 5. Production Results

- **Goal:** Validate the design as a serving system, not only a compression result.
- **Important points:**
  - Approximately 25 PB/month raw and 750–850 terabytes (TB) per month compressed.
  - Put the 169× comparison in a footnote rather than the main narrative. Explain that the first article measured Spark messages alone, while today's 29–33× figure spans broader workloads enriched with operational fields. Trace IDs (UUIDs) are effectively unique and offer little repetition for compression. Convert the Markdown footnote to the publisher's native footnote component if necessary.
  - A five-node cluster of 48-core Presto Native workers serves the CLP SQL workload.
  - Current production scale: hundreds of users per week and thousands of queries per day, with approximately two-second average response time and 26-second 99th-percentile (p99) response time.
  - Workers typically use only 5–10% of their central processing unit (CPU) capacity.
  - Uber also rate-limits object-storage reads to reduce parallelism and preserve bandwidth for other workloads. Queries still meet the required latency, leaving room to serve more queries or onboard more workloads.
  - Direct object-storage reads without local staging or cache.
  - Add a two-series or two-panel Figure 2 showing weekly active users and daily query volume from May through the approved publication cutoff.
  - Confirm that the user, query-volume, and response-time figures describe the same production query population and measurement window. Add a representative pruning measurement after its definition is approved.
  - Define operational scope before making a maintenance-effort claim.
- **Takeaway:** Application flexibility, freshness, compression, SQL, and a five-worker Presto cluster work as one system.

## 6. What Comes Next

- **Goal:** Transition from production results to two development directions: smarter storage organization, followed by agent-assisted data exploration beyond today's guess-and-check process.
- **Why:** Separating the two ideas avoids treating agent interaction as part of storage planning and gives the longer agent direction appropriate emphasis.
- **Important points:**
  - **Smarter data placement and consolidation:** Group the first two short directions under this subheading.
  - **Intelligent tiering:** Make CLP the default queryable storage tier and use a hot index as optional acceleration. Use how often each table is queried, which fields and time ranges those queries access, and retention requirements to decide whether it needs a hot index and how long data should remain there. If query demand rises, quickly restore selected tables or time ranges from CLP to the hot index.
  - **Grouping similar data during consolidation:** Use CLP's schema and logtype metadata, each file's source, and query history to decide which closed KV-IR files belong in the same archive. Similar inputs improve cross-file deduplication. Frequently co-accessed data reduces archives opened. Retain time, retention, freshness, and archive-size limits.
  - **Beyond guesswork: putting CLP metadata to work for agents:** Give this direction its own subheading and greater narrative weight. Frame SQL access as solving execution but not discovery, then map natural-language or semantic input to typed SQL, metadata predicates, and storage operations.
  - Let the enthusiasm come from a concrete arc: production MCP usage exposes two gaps, keyword guessing establishes today's limitation, and CLP's existing metadata makes better workflows achievable. Use the Claude Code query to make it tangible.
  - Separate the two gaps: agents need structure and patterns to write precise queries, or semantic search and summaries to explore directly without retrieving every raw event.
  - Describe today's behavior concretely: agents guess keywords and hope one surfaces relevant events. Narrow terms miss data; broad terms return too many events.
  - **Search-and-discovery workflow:** Scope with known filters, then place the paths, types, distinct logtypes, and their counts from the remaining data into the agent's context. The agent should know the filtered data's structure, message patterns, and frequencies before writing the next query. Retrieve individual events only as evidence.
  - **Intent search:** When an agent knows the concept but not its exact wording or path, `SEMANTIC(...)` filters by meaning.
  - Make the reduction problem explicit for non-specialists: logs may repeat the same message thousands of times while changing only timestamps, IDs, paths, or other variables. Returning every event consumes bandwidth and agent context while hiding the distinct failure forms.
  - **Query discovery:** `PATTERN(...)` answers “Which distinct message forms did the search find, and how often?” by grouping messages that differ only in variable values.
  - **Richer output:** If the pattern list remains long, `SUMMARY(...)` explains that smaller set while preserving counts, representative events, and links to exact evidence. The agent uses this result to choose its next query.
  - Present the Claude Code query as one the agent generates after receiving that structural context. Use a `claude_code_session_transcripts` table and real transcript structure. Prune top-level `type`, apply `SEMANTIC(...)` to tool-result text under `message.content[*].content`, and group by both `PATTERN(...)` on that content and `gitBranch`. Keep the search phrase fictional. Add short inline comments for the surprising transcript mapping and future operators.
  - Introduce the query with its diagnostic question: which connector-load failures recur, and are they concentrated on particular branches? Explain before the SQL that keyword search may miss differently worded failures, raw-event retrieval hides the common cases, `SEMANTIC(...)` finds the concept, `PATTERN(...)` collapses matches into logtypes, and `gitBranch` supplies the correlation dimension.
  - State the practical gain: semantic search identifies relevant tool output without exact keywords, `PATTERN(...)` maps the matches to CLP's deduplicated logtypes, and `gitBranch` shows where each recurring failure form occurred. The result is a ranked table rather than every matching raw event.
  - Follow the SQL with a compact, clearly illustrative result table. Show several variable-bearing messages collapsing into one logtype, preserve the branch dimension and frequency, and explain how the agent uses the reduced result before fetching individual events.
  - Package the question, operator roles, SQL, output table, and interpretation as one titled callout: “Which failures recur, and on which branches?” It must be optional in the article flow and self-contained enough to share as a screenshot. Repeating the brief operator definitions inside this callout is intentional.
  - Explain only the transcript structure needed by the query: `message.content` may be text or typed blocks, including tool results stored in `user` records.
  - Mention, without repeating the full query, that `SUMMARY(PATTERN(...))` could return one synopsis of recurring matching failures, with representative evidence and source links.
  - Explain the native cost model once: CLP can operate over filtered paths, deduplicated logtypes, dictionary values, and stored counts, then fetch individual events only when an agent needs examples or source evidence.
  - State that these ideas still need refinement and evaluation.
  - Do not apply one blanket maturity label. State that these directions build on deployed capabilities while their more advanced forms remain under development and evaluation.
- **Takeaway:** Metadata can guide storage organization and, separately, help agents discover and retrieve relevant evidence without guessing exact paths or terms.
- **Weighting:** Use tiering → consolidation → querying. Keep the first two to one substantive paragraph each, then end with the longer agent direction and the section's only SQL example.
- **Length safeguard:** Avoid explaining keyword limitations or the three future operators more than once. Let the query and illustrative result table carry the detailed agent example.

## 7. Open Source, Collaboration, and Conclusion

- List current open-source capabilities, Uber production extensions, and further-development areas once.
- Credit YScope for connector leadership.
- Credit IBM engineers and the Presto community for native connector infrastructure and review.
- Credit YScope and Uber engineers for production extension and application.
- Keep the conclusion compact: layered work reduction, distinct Presto and CLP responsibilities, production headroom, then one forward-looking sentence. Do not repeat the data flow or Top-N mechanics.
- End on the proven system, followed by one forward-looking sentence that separates two agent workflows: expose structure and pattern counts for precise query generation, or search by meaning and summarize matching evidence directly.

> Common SQL and distributed execution can coexist with specialized semi-structured storage and native execution.

## Evidence Needed

Treat the remaining word budget as headroom, not a target. Use it only for required production evidence or an explanation readers cannot reasonably infer; otherwise prefer the shorter version. Do not trim the outlook section merely to preserve the current draft length.

- Weekly active users and measurement window.
- Daily query volume and aggregation method.
- Average and p99 latency for one defined query population.
- Representative timestamp-only versus richer-metadata pruning comparison.
- Operational staffing scope.
- Model Context Protocol (MCP) traffic share, denominator, measurement window, and server definition.

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
