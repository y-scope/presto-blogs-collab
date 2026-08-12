# Fresh Logs, Dense Archives, Native Semi-Structured Querying with Presto: CLP at Uber (Part III)

- **Type:** Production architecture case study
- **Length:** 2,300–2,600 words
- **Audience:** Presto, observability, storage, data-platform, streaming, machine-learning, and artificial-intelligence engineers
- **Central question:** How can five Presto Native workers provide SQL access to data from workloads producing roughly 25 petabytes (PB) of raw logs per month?

## Story

CLP's compression is necessary but does not explain how five Presto Native workers serve the current workload. The complete answer follows the data lifecycle:

> Independent producers → fresh CLP key-value intermediate representation (KV-IR) → denser CLP archives → one Presto query path → progressively narrower execution

Readers should understand:

- Why producer-controlled schemas require semi-structured storage.
- How CLP preserves both freshness and long-term density.
- What Presto contributes beyond being a SQL endpoint.
- How Uber's metastore, the connector, and the CLP reader eliminate different work.
- How SQL semantics lay the groundwork for an artificial-intelligence-friendly interface and more intelligent data placement.

## Prior Foundations

- [Uber CLP Part II](https://www.uber.com/gb/en/blog/modernizing-logging-with-clp-ii/): appender adoption, operational enrichment, and timed publication of active streams.
- [YScope's August 2025 post](https://blog.yscope.com/querying-clp-compressed-logs-with-prestosql-71dff29a0b82): original motivation and query mechanics.
- Earlier PrestoDB blog post, **[PRESTO_BLOG_URL](PRESTO_BLOG_URL)**: current pluggable connector, CLP versus Variant, and connector-author lessons.
- This article: Uber's production lifecycle, Presto query path, production evidence, and future direction.

Repeat only enough Merged Schema Tree, Encoded Record Table, log-template (CLP logtype), and accessor material to explain the production scan.

## Capability Boundary

- **Current open source:** CLP KV-IR and archives, mixed-form queries, typed paths, filters and projections pushed into CLP, typed wildcard value search across unknown keys, complete-event JSON, and pluggable metadata and split filtering.
- **Uber production:** Collection and enrichment, metadata publication, archive orchestration, mixed-form precedence, advanced metastore planning, proof-based newest-first limit (Top-N) planning, routing, and operational controls.
- **Future:** Natural-language and semantic query input, filtered semantic output, intelligent consolidation, selective offboarding, hot-storage rehydration, and dual ingestion.

## 1. When Producers Control the Log Schema

- **Goal:** Establish why schema evolution is continuous rather than an occasional migration.
- **Why:** Independently deployed producers cannot coordinate one complete table schema.
- **Important points:**
  - Use message and nested-JSON examples before defining semi-structured and polymorphic data.
  - Refer to Part II for appender and enrichment mechanics.
  - Repeat only the resulting storage problem: original messages, structured fields, and evolving enrichment coexist.
  - Treat natural-language messages, structured envelopes, and evolving JSON as one semi-structured continuum rather than rigid categories.
  - Keep the schema foundation brief. Contrast structural preservation with whole-document parsing, then move to the production lifecycle.
  - Link the μSlope paper.
- **Takeaway:** Preserve producer-controlled structure without turning application deployments into data-platform migrations.

## 2. Fresh Logs: Searchable Before Archiving

- **Goal:** Show that high compression does not require sacrificing freshness.
- **Why:** A dense archive amortizes its layout over a large input volume; accumulating that input can take too long for incident-response freshness, even across hosts.
- **Important points:**
  - Expand key-value intermediate representation (KV-IR) at first use.
  - YScope's CLP project supplies KV-IR and its encoder, reader, and search capabilities.
  - Uber supplies collection, enrichment, publication, metadata integration, and object storage.
  - KV-IR extends Part I's message-oriented intermediate representation to key-value events.
  - KV-IR streams μSlope's structural model: schema-tree updates and per-event references into the evolving tree accompany encoded values.
  - Schema-tree updates precede the events that reference them, keeping the tree fully materialized for every readable stream prefix.
  - Unlike the Part II design, CLP compression now runs in the host-local collector rather than the log appender.
  - If a producer container exits, the collector can still compress and publish logs already emitted by that container.
  - Host collectors append compressed KV-IR locally without closing the active stream.
  - Summarize rather than re-explain Part II's publication design: configurable timers publish the current KV-IR contents and metadata while the local stream remains open.
  - Fresh data becomes discoverable in roughly 30 seconds to a few minutes.
- **Takeaway:** Fresh logs are already compressed, structured, and searchable before archive construction.

## 3. Dense Archives: Asynchronous Columnar Consolidation

- **Goal:** Explain what archive construction adds after streaming compression.
- **Why:** Aggregation reorganizes already compressed streams for greater density and selective long-term search.
- **Important points:**
  - Uber orchestrates centralized aggregation. CLP supplies the archive writer and format.
  - The aggregation service selects a group of finalized KV-IR files and creates a consolidation job.
  - An aggregation worker picks up the job and invokes CLP to produce one immutable archive.
  - Merge each KV-IR file's schema tree into the archive-local Merged Schema Tree (MST), the inventory of observed paths and types.
  - Route each encoded record to an Encoded Record Table (ERT) matching its key-and-type shape; do not reparse raw events.
  - Explain that ERTs are archive-owned internal tables, not SQL tables. Homogeneous columns avoid null-padding unrelated shapes.
  - Define logtypes as recurring static text patterns stored separately from typed variables.
  - Explain the mechanism rather than size alone: when grouped inputs are similar, CLP can reuse more schemas, log templates, and values across files.
  - Balance compression and search locality against object size, request cost, query latency, and parallelism.
  - Archive creation stays off the freshness-critical path.
- **Figure:** Keep the lifecycle simple: producer → collector → fresh KV-IR → asynchronous consolidation → dense archive. Show only metadata from the collector and consolidator to Uber's metastore.
- **Takeaway:** KV-IR provides freshness. Archives add denser columnar organization without beginning compression from scratch.

## 4. Native Semi-Structured Querying with Presto

- **Goal:** Make Presto a co-equal part of the architecture and follow one query into a native CLP scan.
- **Why:** Uber's tools and programmatic clients already depend on Presto governance, planning, routing, joins, aggregation, and composition.
- **Important points:**
  - Users may submit Lucene-style search plus pipe-based projection/aggregation or write SQL directly; Uber converts the former to SQL and validates both before they enter the same path.
  - Use that normalization to bridge log-oriented workflows into the broader Presto ecosystem.
  - The connector analyzes SQL to map top-level-key predicates to Uber's metastore, recognize early-termination shapes, and carry typed-field, wildcard, and log-template predicates to CLP.
  - Uber's metastore narrows candidate KV-IR and archive files. Presto schedules splits, CLP executes supported scans, and Presto performs the remaining computation.
  - Treat this as a preview; Section 5 explains the planning and execution layers.
  - One query may contain both forms. Archive precedence prevents duplicates during overlap.
  - Define native semi-structured querying as carrying typed paths, predicates, and projections into split-local encoded execution instead of reconstructing every event for generic JSON extraction.
  - Explain the SQL surface: an optional convenience schema exposes common paths as ordinary columns; typed accessors handle polymorphic paths; wildcard functions search values across unknown keys.
  - Refer to MST, ERT, and logtype definitions from Section 3 rather than defining them again.
  - Use one SQL example with typed paths, wildcard value search, filters, and Top-N.
  - State that native Variant operators can replace exact-path accessor syntax once Presto supports them end to end; efficient unknown-key search and physical execution remain CLP-specific.
  - Refer detailed Variant-layer, physical-scan, and connector-boundary discussion to the earlier PrestoDB blog post.
  - The connector adds typed SQL access to CLP. Presto supplies the remaining relational semantics.
  - State the consequence of rewriting and pushdown without repeating the optimizer walkthrough: supported intent reaches the split-local native scan, while unsupported expressions remain in Presto.
  - Native workers invoke CLP against compressed object-storage streams without staging complete archives.
- **Takeaway:** Presto supplies the common relational system while the connector preserves CLP-native execution.

## 5. Exploiting Data Knowledge at Every Layer

- **Goal:** Explain the five-worker result through concrete work elimination.
- **Why:** Compression alone does not prevent broad object reads, decompression, or materialization.
- **Important points:**
  1. Uber's metastore removes impossible files before split creation.
  2. At Uber, we added proof-based Top-N planning that uses file time bounds and exact matching-event counts to exclude older files before Presto creates splits, but only when they cannot change the result.
  3. Split-local MST metadata resolves and prunes paths; logtype and variable dictionaries eliminate nonmatches before the reader advances to relevant ERTs and columns.
  4. CLP applies supported filters and projections before reconstructing results.
- Keep pruning distinct from early termination. Include exact matching counts, the timestamp watermark, overlap handling, and safe fallback.
- Label Top-N planning as Uber production behavior, not a current open-source capability.
- **Figure:** Use a compact left-to-right pipeline. Keep pruning and Top-N proof evaluation as separate steps inside metastore planning. Show bounds, counts, and overlap in the proof step, and state the complete fallback in the caption. Inside CLP execution, show MST path resolution and dictionary pruning before forward advancement to relevant ERTs.
- **Takeaway:** Each layer avoids the work it can prove unnecessary.

## 6. Production Results

- **Goal:** Validate the design as a serving system, not only a compression result.
- **Important points:**
  - Approximately 25 PB/month raw and 750–850 terabytes (TB) per month compressed.
  - Explain why Part I's 169× Spark result is not directly comparable: the current representation carries the same Spark logs with operational enrichment, including high-cardinality universally unique identifier (UUID) trace IDs that contain little compressible repetition.
  - Five 48-core workers typically operating at 5–10% central processing unit (CPU) utilization.
  - Direct object-storage reads without local staging or cache.
  - Add users, query volume, average latency, 99th-percentile (p99) latency, and pruning measurements only after definitions are approved.
  - Define operational scope before making a maintenance-effort claim.
- **Takeaway:** Producer flexibility, freshness, density, SQL, and a five-worker Presto cluster work as one system.

## 7. What Comes Next: Letting Metadata Drive the Data Lifecycle

- **Goal:** Connect today's SQL boundary to three future projects.
- **Why:** CLP metadata can become an active input to query interaction and archive organization, while operational metadata can extend the same principle to data placement.
- **Important points:**
  - **Intelligent tiering:** State that CLP is not intended to replace fully indexed systems serving hundreds of queries per second. Use access, latency, and policy metadata to shorten indexed retention, keep colder data queryable in CLP, and rehydrate selected data into a hot index.
  - **Intelligent consolidation:** Use CLP structure, logtypes, values, source, and query locality to group compatible KV-IR streams while respecting operational boundaries.
  - **Laying the groundwork for agent search and discovery without guesswork:** Natural-language or semantic input mapped to typed SQL, metadata predicates, and storage operations.
  - **Intent search:** When an agent knows the concept but not its exact wording or path, `SEMANTIC(...)` filters by meaning.
  - **Query discovery:** Return all recurring patterns under exact filters, or one evidence-backed summary, so an agent can decide what to query next.
  - **Richer output:** Filtered summaries, recurring patterns, structural changes, representative events, and links to exact evidence.
  - Make the example immediately recognizable as Claude Code: use a `claude_code_session_transcripts` table and real transcript structure. Prune top-level `type`, then apply `SEMANTIC(...)` and grouped `PATTERN(...)` to tool-result text under `message.content[*].content`. Keep the search phrase fictional.
  - Explain that `message.content` is polymorphic—string or typed-block array—and tool-result blocks carry `content`, `is_error`, and `tool_use_id`.
  - Mention, without repeating the full query, that `SUMMARY(PATTERN(...))` could return one synopsis of recurring matching failures, with representative evidence and source links.
  - Explain the native cost model: CLP can operate first on compact paths, deduplicated logtypes, dictionary values, and counts, then retrieve individual events only as evidence. This structural reduction is what could make semantic discovery and summarization practical at petabyte scale.
  - Treat all benefits as hypotheses requiring evaluation.
  - State explicitly that every item above is future work.
- **Takeaway:** Metadata embedded within CLP and operational metadata maintained above it can become active inputs to placement, consolidation, and retrieval.
- **Weighting:** Use tiering → consolidation → querying. Keep the first two to one substantive paragraph each, then end with the longer agent direction and the section's only SQL example.

## 8. Open Source, Collaboration, and Conclusion

- List current open-source capabilities, Uber production extensions, and future work once.
- Credit YScope for connector leadership.
- Credit IBM engineers and the Presto community for native connector infrastructure and review.
- Credit YScope and Uber engineers for production extension and application.
- End on the proven system, not the future roadmap:

> Common SQL and distributed execution can coexist with specialized semi-structured storage and native execution.

## Evidence Needed

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
- Attribute Uber services, Presto, the connector, and CLP readers precisely.
- Keep Uber's metastore implementation a black box.
- Do not repeat the optimizer walkthrough, benchmarks, Variant comparison, or connector-author guide.
- Distinguish current open source, Uber production, and future work at first mention.
- Do not describe future artificial-intelligence, consolidation, offboarding, rehydration, or dual-ingestion work as deployed.
- Replace `PRESTO_BLOG_URL` before publication.
- Finish with a complete narrative review and `git diff --check`.
