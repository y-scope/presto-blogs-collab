# CLP Part III Knowledge

- **Status:** Synchronized source of truth for the Uber Engineering draft and publication outline
- **Updated:** August 19, 2026
- **Working title:** *Bringing Native SQL to Petabyte-Scale Semi-Structured Logs: Inside Uber's CLP and Presto Infrastructure (Part III)*
- **Publication target:** Uber Engineering Blog
- **Purpose:** Preserve approved facts, technical mechanisms, capability boundaries, attribution, editorial constraints, and remaining publication work behind the concise article

This document is intentionally more detailed than the article. The draft should remain production-led and accessible to Uber Engineering readers. Deeper storage-format, Variant/shredding, and connector-development details belong in the earlier PrestoDB blog post, assumed to publish first at **[PRESTO_BLOG_URL](PRESTO_BLOG_URL)**.

---

## 1. Executive Summary

- Currently onboarded machine-learning/AI, streaming, and data-platform workloads generate approximately **25 PB of raw logs per month**.
- CLP reduces that monthly volume to approximately **750–850 TB**:
  - Roughly **29–33× compression**
  - Approximately **97% less stored volume**
- Five 48-core Presto Native workers serve recent and older data through searchable KV-IR files and immutable CLP archives directly from Uber's internal object storage.
- Five 48-core Presto Native workers typically operate at **5–10% CPU utilization**.
- Over a 30-day production window, the cluster served **hundreds of weekly active users (WAU)** and completed **thousands of successful queries per day**. Successful queries had a mean response time of roughly **two seconds**, and **99% completed within 26 seconds**.
- **A few engineers** maintain the system alongside their broader platform responsibilities.
- Agents already issue **more than half of CLP queries** at Uber.
- In aggregate across that window, successful queries' time ranges covered **more than 200,000 times as many events** as the system ultimately examined.
- These figures are signed off and published in the draft. They are no longer proposed metrics awaiting approval.
- The objective is responsive user latency with minimal use of shared infrastructure, not maximum throughput.
- Uber intentionally paces object-storage API use. Low CPU does not imply a need to increase scan throughput.
- No individual query scans the complete monthly ingest volume.
- The system supports a continuum from natural-language messages to enriched and native JSON events whose schemas and types evolve continuously.
- Fresh logs first land in searchable, streaming-compressed KV-IR files and are asynchronously aggregated into immutable CLP archives.
- A log collector runs on every host, monitors container logs, enriches each event, and incrementally compresses it into an appendable KV-IR file.
- Compression has moved out of the in-process log appender and into the host-local collector. The collector can finish compressing and publishing already-emitted logs after a producer container exits.
- The active KV-IR file is already compressed as it grows. Archive aggregation is not where compression begins.
- On a configurable publication interval, the collector uploads the current file extent without closing the local stream. Later publications overwrite the same object-storage path with a longer snapshot.
- When the active stream reaches a configured size or age, the collector closes that KV-IR file and starts a new file at a new object path.
- Each file update produces a metadata event on Kafka, which feeds Uber's metastore for query discovery.
- A single query can span a mixed set of KV-IR and archive splits.
- Where KV-IR and archive coverage overlaps, Uber prioritizes the archived copy and excludes the incorporated KV-IR input, so both formats can contribute to a query without returning duplicate events.
- CLP archives contain archive-local schema trees and schema-homogeneous encoded record tables. There is no authoritative dataset-global schema tree.
- An optional convenience schema is a late-bound logical view over columns that already exist in CLP storage.
- Public typed accessors and wildcard value-search functions are planner markers. The connector rewrites supported expressions and pushes their intent into split-local CLP execution.
- The execution chain is:

> **SQL expression → connector rewrite and pushdown → split-local schema resolution → encoded CLP execution**

- Scale comes from two complementary effects:
  - Metadata avoids files and splits before execution.
  - CLP minimizes scanning, decompression, and reconstruction for the selected encoded data.
- Uber additionally uses planning-time early termination for eligible newest-first limited queries, in the two modes the article names: an opt-in **approximate** mode that stops at the first watermark and accepts a possible miss, and a **guaranteed** mode that preserves the exact latest-N result. Selected splits still use Presto's normal scheduler.
- Current OSS already supports end-to-end Presto querying, including mixed KV-IR/archive queries, typed exact-path access, filter/projection pushdown, typed wildcard value search, complete-event JSON, and pluggable metadata/split filtering.
- Uber production adds advanced metadata planning, billion-file scale, early termination, routing, reliability, and operational controls.

### Central thesis

> Logs can retain their evolving structure and still receive the projection, search, optimization, and composition expected from an analytical system.

### Production resolution

> By preserving evolving structure, eliminating work early, and searching encoded data in place, Uber made 25 PB per month of logs accessible through one SQL experience, served by five lightly utilized workers.

---

## 2. Publication Narrative and Boundaries

The article answers one end-to-end question:

> How can five Presto Native workers provide SQL access to data from workloads producing roughly 25 PB of raw logs per month?

The causal answer is:

1. Uber's independently deployed producers emit natural-language messages, enriched events, and polymorphic JSON that cannot share one complete global schema.
2. Compression now runs in per-host collectors rather than producer log appenders. The collectors preserve evolving structure, enrich events, and incrementally compress them into appendable KV-IR even if the emitting container exits. Periodic publications overwrite each active object with a longer snapshot, while size- or time-based rotation starts a new object. Kafka metadata updates make the published extents discoverable without closing the active local stream.
3. Uber preserves one SQL entry point. The connector analyzes SQL to map top-level-key predicates to Uber's metastore, recognize early-termination shapes, and carry typed-field, wildcard, and log-template predicates to CLP. Uber's metastore narrows candidate KV-IR/archive inputs, CLP executes supported scans, and Presto performs the remaining computation.
4. Current OSS can query a mix of both CLP formats. Uber removes overlapping KV-IR inputs once their archived copy is authoritative.
5. CLP columnizes observed structure within archive-local, schema-homogeneous tables.
6. Connector rewriting carries SQL intent into split-local storage resolution and encoded execution instead of forcing row-oriented JSON materialization.
7. Metadata pruning and planning-time early termination avoid work before workers read object storage.
8. Ingestion/storage separation, compression, work avoidance, and encoded execution together explain the production architecture and its use of five Presto Native workers.
9. The typed SQL boundary lays groundwork for one forward-looking direction: exposing CLP's existing metadata (event structure, recurring formats, and their frequencies) through SQL so agents can answer questions without reading events, and extending that idea past logs to any telemetry record. Intelligent tiering and similarity-aware archive grouping were cut for length and are no longer article content.

### Intended audience

- Uber infrastructure, observability, storage, data-platform, streaming, and machine-learning engineers
- Engineering leaders evaluating cost, reliability, operability, and adoption at scale
- Presto and CLP users interested in production architecture rather than connector internals

### Article shape

- Opening and production question
- Producer-controlled schemas
- Fresh KV-IR and denser archive lifecycle, plus one figure
- One Presto query across both storage forms, including one SQL example
- Multi-layer work elimination (carried in prose in the current draft, with no second figure)
- Production results
- One forward-looking direction: CLP metadata as agent context ("What Comes Next: Beyond Guesswork for Agents"), closing on wide events beyond logs
- Open-source and collaboration boundary
- Conclusion that restates the production result and then closes on the forward-looking metadata direction
- Current draft target: approximately 2,600 words before final evidence
- Hard maximum: 3,000 words
- Treat the remaining budget as headroom, not a target. Use it only for required production evidence or genuine comprehension gaps. Otherwise prefer the shorter version. Length pressure did force one cut: the planned "Better Storage Decisions from Existing Metadata" subsection (intelligent tiering and similarity-aware archive grouping) was removed entirely, leaving one forward-looking direction rather than two.

### Story boundary

- Production architecture case study, not a complete connector guide
- Explain the per-host collector-to-KV-IR-to-archive lifecycle, but treat Uber's metastore implementation as a black box rather than turning the article into a complete ingestion-service reference
- No full Iceberg Variant or Parquet shredding comparison
- No complete CLP archive-format derivation
- No formal early-termination proof
- No large benchmark dashboard
- No exhaustive API reference
- No detailed Pinot/Lucene implementation
- Keep the forward-looking material to one bounded section on metadata as agent context. Intelligent tiering and similarity-aware archive grouping were cut for length and are no longer article content. The conclusion restates the production result and then deliberately closes on the forward-looking metadata direction.

---

## 3. Series Continuity and Workload

### Part I

- [Reducing Logging Cost by Two Orders of Magnitude Using CLP](https://www.uber.com/en-GB/blog/reducing-logging-cost-by-two-orders-of-magnitude-using-clp/)
- Introduced streaming CLP compression for Spark.
- Reduced logging cost, mitigated SSD write amplification, and enabled longer retention.

### Part II

- [Modernizing Logging at Uber with CLP (Part II)](https://www.uber.com/en-GB/blog/modernizing-logging-with-clp-ii/)
- Expanded the work into end-to-end ingestion, retention, viewing, and programmatic analysis.
- Described language-specific appenders, native bindings, configuration-based adoption, and operational enrichment.
- Described the publication timer that exposes an active stream before it closes. Part III should reference rather than reintroduce that mechanism.

### Part III

- Expands from Spark into representative platform families:
  - ML/AI: Michelangelo and Ray
  - Streaming: Flink and Kafka
  - Data platforms: Spark and Hudi
- Moves CLP compression from the producer's log appender into the host-local collector, decoupling compression and publication from the container lifetime.
- Do not imply that every Uber service or thousands of services are already onboarded.
- Scope the 25 PB/month figure to currently onboarded workloads.

### Appenders and native bindings

Uber and YScope jointly developed language-specific appenders and bindings that:

- Preserve existing natural-language service messages.
- Translate existing pattern configurations into structured fields.
- Add correlation, platform, application, job, host, container, and related context.
- Apply similar operational enrichment to events that already arrive in a structured form.
- Require only small logging-configuration changes rather than broad rewrites of application logging statements.

The adoption lesson is important: structure is added at the platform boundary rather than through application-wide migrations. Natural-language messages, enriched records, and evolving JSON form one semi-structured continuum rather than separate storage classes.

**Part III use:** Reference Part II for these mechanics. Retain only the resulting semi-structured storage contract and the enrichment details required to explain the current compression ratio.

The current architecture changes one important responsibility: appenders no longer perform CLP compression. The host-local collector owns compression and publication, so already-emitted logs can still be processed if their producer container exits.

### The data spectrum

- **Message-oriented logs:** meaning primarily lives in natural language with embedded variables.
- **Enriched application logs:** retain the message while adding structured operational context.
- **Native JSON events:** meaning primarily lives in request, state, input, and outcome fields, but may still contain natural-language descriptions.

JSON is an encoding syntax, not a stable data contract.

### Definitions

- **Semi-structured:** fields and nesting are discoverable, but one centrally enforced schema does not describe every event.
- **Polymorphic:** one logical path may contain different types across events, producers, versions, or time.

Example:

```json
{"status": 500}
{"status": "FAILED"}
{"status": {"code": 500}}
```

This is not necessarily a data-quality failure. Independent services evolve, languages differ, and historical and current events coexist.

Research reference: [μSlope: High Compression and Fast Search on Semi-Structured Logs, OSDI '24](https://www.usenix.org/conference/osdi24/presentation/wang-rui). Attribute the work to YScope, the University of Toronto, and Uber's Observability, Pinot, and Storage and Search teams.

---

## 4. Ingestion and Storage Lifecycle

### Per-host collection and publication

**Part III use:** Summarize these mechanics as configurable timers, similar to Part II, publishing active KV-IR data and metadata before rotation. Retain the details below as implementation background.

- A log collector runs on every host and monitors logs from containers on that host.
- The collector enriches events before incrementally compressing them into KV-IR.
- Compression runs in the collector rather than the producer's log appender, allowing the collector to finish processing and publishing already-emitted logs after the producer container exits.
- KV-IR is compressed incrementally and appendable: the collector keeps the active compressed file open as new records arrive.
- KV-IR extends the IR stream introduced in Part I from message-oriented logs to key-value events.
- It applies CLP's semi-structured storage model incrementally, as described in the μSlope paper: schema-tree node insertions and per-event references into the evolving tree are serialized alongside encoded values.
- Schema-tree updates precede the events that reference them. A consumer therefore has a fully materialized schema tree for every event in the stream prefix it has read.
- During archive construction, CLP merges each KV-IR file's schema tree into the archive-local tree and appends each encoded record to the ERT matching its key-and-type shape. It does not reparse raw events to rediscover their structure.
- On a configurable publication interval, the collector uploads the current file extent without closing the active local stream.
- Subsequent publications overwrite the same object-storage path with a complete, longer snapshot containing the newly appended records.
- When the active stream reaches a configured size or age, the collector rotates it, closes the current KV-IR file, and starts a new file at a new object path.
- Every publication and rotation also emits a metadata update to a Kafka topic.
- Uber's metastore consumes these updates so the query path can discover the growing KV-IR files.
- The article treats Uber's metastore as a black box. Pinot implementation details are irrelevant to this ingestion explanation.

### KV-IR

- [KV-IR](https://docs.yscope.com/clp/main/dev-docs/design-kv-ir-streams/index.html) is the streaming-compressed key-value internal-representation format for dynamically structured logs.
- Present KV-IR as an extension of the IR stream introduced in Part I, from message-oriented logs to semi-structured key-value events.
- KV-IR makes CLP's structural design, described in the μSlope paper, streamable: it serializes schema-tree updates and represents each event's schema through references to that evolving tree alongside encoded values.
- The schema tree remains fully materialized for the current readable prefix as the KV-IR stream grows. Completeness is not deferred until the stream ends.
- The immutable archive persists the resulting ERTs in denser physically columnar form with richer search metadata.
- KV-IR buffers less data than the archive format and is suitable for low-latency and resource-constrained ingestion.
- Fresh KV-IR files are searchable. Users do not wait for archive creation.

### Archive aggregation

- The asynchronous aggregation service selects a group of closed KV-IR files and creates a consolidation job.
- An aggregation worker picks up the job and invokes CLP to consolidate the group into one denser immutable archive.
- Aggregation improves long-term compression density and layout. It does not mark the beginning of compression.
- Archives provide the longer-term compression and search representation.
- Current freshness through the KV-IR path is approximately 30 seconds to a few minutes.

### Mixed-source queries and overlap handling

- Current OSS supports a single query over a heterogeneous split set containing KV-IR files and CLP archives.
- One time range may require files from both formats.
- Aggregation creates temporary coverage overlap.
- Uber's metastore records the relationship between incorporated KV-IR inputs and their archived representation.
- For overlapping coverage, the archived copy is authoritative and the incorporated KV-IR input is excluded.
- Nonoverlapping KV-IR and archive files may both contribute to the same query.
- Each event appears once. The system does not choose one format for the entire query.

### Publication tradeoffs

- More frequent publication improves freshness but increases object-store requests.
- Grouping structurally and textually similar inputs enables cross-file reuse of schemas, log templates, and values.
- Aggregation policy must balance compression and search locality against object size, object-storage request cost, query latency, and available parallelism.
- The chosen cadence balances freshness, compression, and shared object-storage use.

---

## 5. Query Architecture and Tier Management

The current Part III article treats Uber's metadata implementation as a black-box **metastore** and does not explain Pinot or broader tier-management internals. The details below are retained only as background and capability guardrails. The tier-lifecycle and similarity-aware-consolidation material in this section was cut from the article entirely for length and is no longer article content.

### User-facing architecture

- Observability UI
- Platform query builders
- Programmatic clients
- Uber's SQL query and routing layer, including QueryRunner and related infrastructure
- Some users submit Lucene-style search followed by pipe stages for projection and aggregation.
- Other users submit SQL directly.
- Uber converts the Lucene-plus-pipe form to SQL and validates both forms before they enter the same query path, connecting both interfaces to the wider Presto ecosystem.

Use QueryRunner attribution carefully. It participates in parsing, governance, routing, cluster selection, and failover, but responsibilities may span related services.

### Routed serving paths

- **Uber's metastore:** discovers relevant KV-IR and archive inputs from metadata updates. Its internal implementation is outside the article.
- **Presto Native with CLP:** searches the selected recent KV-IR and immutable-archive data in Uber's internal object storage.
- Users retain one SQL-oriented experience rather than choosing a storage engine directly.

### Worker, connector, and reader ownership

- A Presto Native worker supplies compute.
- The CLP connector runs in-process inside the worker.
- The connector plans/dispatches the appropriate reader for KV-IR or archive splits.
- For archive splits, the connector opens the object-storage byte stream and CLP's streaming archive reader interprets and searches it as bytes arrive.
- No local archive staging or cache exists in the described Uber query path.

### Future flexible tier lifecycle

These are future Uber projects, not deployed production behavior or automatic current-OSS policies:

- Data that no longer justifies Pinot indexing could be offboarded to CLP object-storage access while remaining searchable.
- Selected data could later be **rehydrated** into Pinot if repeated low-latency access again justifies hot indexing.
- Selected streams could be dual-ingested when indexed random access and durable CLP access are both required.
- CLP's low incremental storage cost could make selective dual ingestion practical.

### Future similarity-aware consolidation

- A future consolidator could use schema, log-template, value, source, and query-pattern similarity when deciding which KV-IR streams to place in the same archive.
- Co-locating similar data may improve dictionary reuse, compression, archive metadata, and search locality. These are hypotheses requiring evaluation.
- Placement must still respect time boundaries, retention and deletion policies, access control, freshness, and archive-size targets.
- Do not present similarity as the only grouping rule or claim these benefits are deployed.

### Peak-volume resilience

- The hot/indexed path is fed through Kafka and can experience rare log loss during extreme overloads.
- The CLP-backed object-storage path does not share this peak-loss mode.
- Extreme log bursts often come from loop-generated repetition.
- Increased repetition raises CLP's compression ratio.
- Therefore, a large spike in logical/raw volume need not create a proportional rise in compressed object-storage traffic.
- The object-storage path retains additional throughput headroom.

Keep these claims scoped to Uber's described deployment. Do not present them as universal Kafka or object-storage properties, and do not claim unlimited capacity.

### Pinot scope

- Pinot also reuses CLP ideas and implementation components.
- Details of schema discovery, tokenization, Lucene indexing, and random-access search remain out of scope.

---

## 6. Why SQL Needs Storage-Aware Schema-Less Access

### Presto's normal planning contract

Presto expects relation shape, column names, and expression types during planning. Evolving logs violate that expectation:

- Fields appear and disappear.
- Nesting changes.
- One path may hold multiple types.
- Keys may contain case distinctions, symbols, spaces, or SQL keywords.

### Why a manually maintained schema fails

- No central team can predict every producer's fields.
- Routine application changes become table migrations.
- A canonical type can lose meaning or fail on a later variant.
- Historical and current structures coexist.

### Why one JSON string is insufficient

- Repeated complete-document parsing
- Runtime type discovery
- Materialization of unused fields
- Weaker field-level projection and filtering
- Loss of CLP's encoded execution advantages

The article should not claim that every binary dynamic-document format has these costs. The detailed Variant/shredding comparison belongs in the earlier PrestoDB blog post.

### Why retain SQL

- Existing clients and interfaces
- Governance, routing, and failover
- Aggregation, joins, ordering, and limits
- Familiar analytical composition

The goal is not merely to make Presto accept JSON. It is to carry log-query intent through SQL planning into CLP execution.

The current typed CLP accessors bridge PrestoDB's type system. Once Presto supports Variant operators end to end, the connector could expose the same paths through standard Variant syntax without changing CLP storage or its physical scan. Refer detailed Variant-layer, physical-scan, and connector-boundary discussion to the earlier PrestoDB blog post.

---

## 7. CLP Archive Storage Model

### Structural scope

- A CLP archive is an independently searchable storage unit and typically a Presto split.
- Each archive contains an archive-level schema tree describing the paths and types present in that archive.
- The authoritative structure is archive/split-local, not dataset-global.
- Different archives may contain different paths, nesting, and type variants.

### Schema-homogeneous encoded tables

- Records with the same keys and value types share an encoded record table (ERT).
- Each schema node becomes an encoded column within that ERT.
- One archive contains many ERTs behind one archive-local schema tree.
- This avoids one dataset-wide sparse union and does not require a promotion list chosen before ingestion.
- One logical path may map to multiple encoded columns across tables within one archive or across archives, including typed variants.

### Intrinsic columnization

- Every observed schema node is columnized within its schema-homogeneous table.
- This is intrinsic to ingestion, not a later shredding operation.
- Safe claim: CLP minimizes unrelated decompression and scanning.
- Unsafe claim: CLP never fetches or decompresses any unrelated byte.
- Physical compression-stream packaging can affect byte-level reads even when logical column selection is exact.

### Log templates (CLP logtypes) and variable encoding

CLP also finds recurring structure inside string values:

```text
Request 123 failed after 45 ms
Request 987 failed after 12 ms
                ↓
Request <int> failed after <int> ms
```

The placeholders are explanatory rather than literal on-disk syntax. CLP stores recurring text and typed variables separately using appropriate encodings.

### Convenience schema

- Optional selected path-and-type bindings appear as ordinary SQL columns.
- The convenience schema can be changed after archives are written.
- Changing it does not rewrite or re-encode CLP data.
- It is a late-bound logical view over ERT columns that already exist.
- Paths outside the view remain accessible through schema-less functions.

Preferred formulation:

> The convenience schema is late-bound projection over already-columnized storage.

---

## 8. Current SQL Surface and Capability Boundaries

### Verified current OSS exact-path accessors

- `CLP_GET_BIGINT(path)`
- `CLP_GET_DOUBLE(path)`
- `CLP_GET_STRING(path)`
- `CLP_GET_BOOL(path)`
- `CLP_GET_STRING_ARRAY(path)`
- `CLP_GET_JSON_STRING()` for complete-event reconstruction

Examples in public tests use constant dot-separated paths such as `user_id` and `city.Name`. Do not publish `$`-prefixed JSONPath syntax unless verified for the release being discussed.

### Verified current OSS wildcard value search

- `CLP_WILDCARD_STRING_COLUMN()`
- `CLP_WILDCARD_INT_COLUMN()`
- `CLP_WILDCARD_FLOAT_COLUMN()`
- `CLP_WILDCARD_BOOL_COLUMN()`

These expressions search typed **values across fields whose names the query does not enumerate**.

Keep separate:

- **Current OSS:** typed exact-path access, wildcard path segments such as `CLP_GET_BIGINT('event.*.status_code')`, and typed wildcard value search across fields
- **Future:** key discovery and general `{path, value}` projection
- **Uber production:** additional log-search operations whose public names and exact semantics must be labeled separately if mentioned

Do not use or imply unverified public functions such as `CLP_GET_INT`, `CLP_GET_FLOAT`, or untyped `CLP_WILDCARD_COLUMN()`.

### Example

```sql
SELECT
    timestamp,
    application_id,
    CLP_GET_STRING('event.request_id') AS request_id,
    CLP_GET_BIGINT('event.status_code') AS status_code
FROM clp.platform_logs
WHERE timestamp >= current_timestamp - INTERVAL '30' DAY
  AND application_id = 'example-application'
  AND CLP_GET_BIGINT('event.*.status_code') >= 500 -- wildcard path segment
  AND CLP_WILDCARD_STRING_COLUMN() LIKE 'OutOfMemory%'
ORDER BY timestamp DESC
LIMIT 100;
```

Use only release-verified wildcard patterns. Public tests cover equality, numeric predicates, and one-sided `LIKE` patterns. Do not assume two-sided `%value%` pushdown without verification.

### Whole-event access

- `CLP_GET_JSON_STRING()` reconstructs the complete event.
- Complete-event selection requires more work than projecting one bound path.
- It does not produce one dynamically shaped Presto `ROW` containing every observed field.

---

## 9. Rewriting, Pushdown, and Encoded Execution

### Planner-marker functions

- CLP functions look like UDFs at the SQL surface.
- Their Java bodies are plan markers, not row-at-a-time JSON evaluators.
- The connector recognizes supported constant paths, requested types, projections, and predicates.
- It carries that intent in connector scan handles.
- Unsupported residual expressions remain in Presto.
- Partial pushdown must remain correctness preserving. It cannot drop unsafe branches of an `OR` predicate.

### Execution chain

> **SQL expression → connector rewrite and pushdown → split-local schema resolution → encoded CLP execution**

1. Presto parses and type-checks the expression.
2. Connector rewriting retains constant paths, requested types, and supported operations.
3. Pushdown moves compatible filtering and projection into the CLP scan.
4. At each split, CLP resolves paths against split-local structure.
5. For an archive split, logtype and variable dictionaries eliminate nonmatching encoded values before payload scanning.
6. The reader advances to the relevant ERTs and encoded columns.
7. CLP searches/projects encoded data and reconstructs only requested values or matching events.
8. Presto evaluates unsupported residual work and performs downstream relational operators.

### Ownership chain

- Presto Native worker: compute process
- In-process connector: Presto-to-CLP integration and split dispatch
- Native CLP scan path: storage-specific interpretation, search, and projection

Avoid phrasing that makes Presto itself the storage layer or makes the connector a separate remote service.

Memorable formulation:

> SQL supplies the composition model. CLP supplies the schema-less semantics and encoded execution.

---

## 10. Object-Storage Access

### Archive layout

- Header, archive-local schema tree, dictionaries, and indexes appear up front.
- The reader uses logtype and variable dictionaries to prune nonmatches before advancing to relevant ERT data.
- After initialization, the reader advances forward sequentially without backward seeks.
- The connector can begin archive search as bytes arrive rather than staging the complete object.
- Compression reduces transfer and scan work.
- Object-store stream setup, requests, and encoded-region fetches still have costs.

### Uber production path

- KV-IR and archive files reside in internal object storage.
- No local data-disk staging.
- No local archive/data cache in the described query path.
- Object-storage API pacing is deliberate governance of shared infrastructure.
- The target is sufficient latency with minimal shared-resource consumption.
- Additional CPU or unpaced throughput would not materially improve the intended user experience.

### Claim boundary

Safe:

- Search begins as bytes arrive.
- Reader uses forward sequential access without backward seeks.
- CLP minimizes unrelated decompression and scanning.

Avoid:

- Zero request or stream-setup cost
- Perfect byte isolation
- Unlimited object-store capacity
- Claims that worker CPU is the current bottleneck

---

## 11. Metadata, Pruning, and Early Termination

### Uber's metastore role

Uber's metastore is part of query planning. It may track:

- Source URI and format
- KV-IR-to-archive incorporation/precedence state
- Minimum and maximum timestamp
- Record count
- Selected top-level dimensions
- Available paths/types
- Useful file-level statistics
- Lifecycle and retention state

Uber's relational implementation manages metadata for billions of file records. Detailed schema and resource sizing are not required in the article.

The connector is metastore-agnostic. A provider should support evolving indexed fields, efficient lookup, incremental/streaming results, and a defined consistency model. Uber's metastore streams ordered candidates, so planning also stops retrieving file metadata early. The article states this as a fact about Uber's production system, not as a requirement imposed on other providers.

### Metadata pruning

Question:

> Could this file contain any matching event?

- Predicate-driven
- Removes files that cannot match
- Independent of ordering and limits
- Applies before split scheduling and object-store reads
- Can operate over a mixed KV-IR/archive candidate set

### Planning-time early termination

Question:

> Could this older candidate still change the ordered limited result?

Eligible shape:

```sql
ORDER BY timestamp DESC
LIMIT N
```

The same logic runs in reverse for `ORDER BY timestamp ASC LIMIT N`.

Important prerequisites:

- Append-only logs
- Finite limit
- Recognized timestamp ordering
- Trustworthy file time bounds
- Time-aware enumeration
- Compatible filters and correctness-preserving counts/bounds

High-level behavior (guaranteed mode):

1. Apply ordinary pruning.
2. Process surviving candidate-file metadata in timestamp order.
3. Track counts/bounds and a result watermark.
4. Include overlapping time ranges that may affect global Top-N.
5. Stop generating older splits only when correctness is proven.
6. If proof is unavailable, select more files or the complete pruned set.
7. Let Presto schedule all selected splits normally.

The article names two modes:

- **Guaranteed mode:** preserves the exact latest-N result. It accumulates exact matching-event counts in descending `max_timestamp` order until they reach `N`, sets a watermark at the lowest selected `min_timestamp`, and then includes every remaining file whose `max_timestamp` is at or above the watermark. If the available counts cannot account for every remaining filter exactly, it falls back to the complete pruned set.
- **Approximate mode:** an explicit opt-in shortcut for users who value lower latency over an exact latest-N result. It returns as soon as the accumulated count first reaches `N`, so an omitted file overlapping the watermark may hold events that belong in the true latest `N`.

Early termination changes which splits exist, not their execution order. It is an Uber production extension, not current public connector behavior.

### Distinction

- **Pruning:** file cannot match the predicate.
- **Early termination:** file may match, but cannot change the newest-first limited result.

### Planning-time query guardrail

- During planning, Uber's query layer flags queries whose scope exceeds configured thresholds and suggests narrower time ranges or metadata filters.
- The flag is advisory. The original query still runs when its full scope is intentional, and genuinely large queries still take time.
- This is Uber query-layer production behavior, not connector, metastore, or current OSS behavior.

Preferred formulation:

> The system first proves which files need not become splits, then CLP makes the selected encoded data inexpensive to search.

---

## 12. Current OSS, Uber Production, and Future Directions

### Current OSS

- End-to-end Presto access
- Mixed KV-IR/archive queries
- Typed exact-path access
- Function rewriting
- Filter and projection pushdown
- Typed wildcard value search across fields
- Complete-event JSON
- Pluggable metadata, split, and split-filter providers
- Read-only, append-oriented workflow

Do not describe the open-source connector as a preview or merely an architectural reference. It is usable end to end.

### Uber production

- Approximately 25 PB/month raw onboarded logs
- Approximately 750–850 TB/month compressed
- Five 48-core workers typically operating at 5–10% CPU utilization
- Approximately 30-second-to-few-minute freshness through KV-IR
- Direct object-storage access without local staging/cache
- Metadata for billions of file records
- Advanced metadata filtering and planning
- Mixed-format precedence/de-duplication policy
- Planning-time early termination
- Query routing, reliability, and operational controls
- More than half of production CLP queries issued by agents
- More than 200,000x aggregate work reduction over the 30-day window, measured as events covered by successful queries' time ranges divided by events examined
- Peak-volume resilience behavior described in Section 5

Uber and YScope intend to contribute reusable components where practical, without promising scope or timing.

### Future directions

- Schema-tree-backed key discovery
- General wildcard key/value projection
- More natural Variant-based SQL ergonomics if PrestoDB gains end-to-end Variant support
- Recurring-format enumeration with counts (`PATTERN()`)
- Selecting formats by meaning rather than exact wording (`SEMANTIC()`)
- Naming the typed variables lifted out of message text, so a `job_id` inside a message means the same thing as the `job_id` a query already filters on
- Extending the same compression-materialized metadata past logs to any telemetry record, including the wide event that fuses logs, traces, and metrics into one record
- Compact LLM context preloading from archive metadata

Future directions are architectural possibilities, not product commitments.

Article scope as shipped: the draft carries exactly one forward-looking direction (agent context) and exactly three proposed functions, `SCHEMA()`, `PATTERN()`, and `SEMANTIC()`. There is no `SUMMARY()` operator. The planned "Better Storage Decisions from Existing Metadata" subsection covering intelligent tiering and similarity-aware archive grouping was cut for length, so similarity- and policy-aware archive consolidation, Pinot offboarding, selective rehydration, and selective dual ingestion are internal project directions only and are no longer article content. The worked example generalizes the three functions to a trace and explicitly not to a metric, because metrics carry no free text to pattern-match.

---

## 13. Production Evidence and Metric Discipline

### Approved figures

- Raw volume: approximately 25 PB/month for onboarded workloads
- Compressed volume: approximately 750–850 TB/month
- Compression: approximately 29–33×
- Stored-volume reduction: approximately 97%
- Query workers: five
- Worker cores: 48 each
- Worker CPU utilization: typically 5–10%
- Freshness: approximately 30 seconds to a few minutes
- Storage path: internal object storage
- No local archive cache or disk staging

### Why the compression ratio differs from Part I

- Part I reported 169× compression for its Spark-log workload.
- The same Spark workload is represented in the current system, but its stored records now include operational enrichment.
- Enrichment includes operational dimensions such as hostnames and tags, as well as high-cardinality trace IDs that are often represented as UUIDs.
- Random-looking, high-entropy identifiers contain little repetition for a compressor to exploit.
- Do not present the newer ratio as a regression or compare the two values without explaining the richer current representation.
- A 29–33× reduction corresponds to roughly 97% less data than the raw representation.

### Additional approved figures (signed off and shipped in the draft)

- Weekly active users: hundreds of weekly active users (WAU) over the 30-day production window
- Daily query volume: thousands of successful queries per day over the same window
- Response time: roughly two seconds mean for successful queries, with 99% completing within 26 seconds
- Aggregate work reduction: successful queries' time ranges covered more than 200,000 times as many events as the system examined after metadata pruning, guaranteed latest-N file selection where applicable, and CLP filtering
- Operations: a few engineers maintain the system alongside their broader platform responsibilities
- Agent share: agents issue more than half of CLP queries at Uber. The published claim is about agents rather than about MCP-originated traffic specifically, so do not restate it as an MCP share

These approximate phrases are approved and already published, in these exact forms: "hundreds of weekly active users (WAU)", "thousands of successful queries per day", "a few engineers maintain the system", and agents issue "more than half of CLP queries". Keep the published wording rather than reverting to looser variants such as "hundreds of users" or "a few people".

### Resource philosophy

- Do not present low CPU as an undersized cluster or optimization failure.
- Object-storage requests are deliberately paced.
- Responsive UX is the success criterion.
- Maximum throughput is not the goal.

### Burst behavior

- Rare hot/indexed-path losses occur only under extreme overload conditions in the described deployment.
- CLP-backed object-storage retention does not share that failure mode.
- Loop-generated bursts contain unusually high repetition.
- Compression ratio can rise during those bursts.
- Distinguish logical raw volume from compressed object-storage traffic.
- Do not claim every spike compresses well. Scope the mechanism to the common loop/repetition pattern.

### Benchmark discipline

The incomplete performance table has been removed. Add at most one representative query only if every value is available from the same query definition:

- Candidate files before pruning
- Files after pruning
- Splits after early termination
- Compressed physical bytes read
- Object-storage requests
- End-to-end latency

Always distinguish:

- Monthly raw ingest volume
- Logical raw scope represented by a query
- Compressed physical bytes read
- Files/splits scheduled

---

## 14. Collaboration and Attribution

### Uber and YScope

- Nearly five years of sustained engineering collaboration across teams and companies
- Joint appender and platform integration work
- Joint production architecture, validation, and operations
- YScope led the open-source connector implementation
- Uber supplied production requirements, data diversity, infrastructure integration, scale, and operational validation across three teams: Presto worked with YScope on connector infrastructure, Observability shaped production requirements and workflows and operates the query service, and Data Storage collaborated on the metastore and object-storage paths used for planning, consolidation, and query execution

### Presto Native infrastructure

- The connector helped complete important gaps in the out-of-band Presto Native connector path.
- YScope contributors ChenXing Yang and Jack Luo led key upstream changes for dynamic plugin loading and connector serialization.
- IBM's Presto team reviewed the upstream changes, and the broader PrestoDB community provided the open review and integration path.
- Meta's Presto and Velox teams joined IBM's Presto team in design discussions, helping YScope and Uber test assumptions and refine the architecture.
- Do not claim YScope created all plugin infrastructure.
- Use **the first end-to-end pluggable connector for Presto Native**. The chronology is verified and the claim ships unhedged. Do not soften it to "one of the first."
- Both drafts now state the claim unhedged. The companion Presto draft says CLP was built as "the first end-to-end plugin across that boundary."

Relevant upstream changes:

- [PR #27653](https://github.com/prestodb/presto/pull/27653): connector-factory registration from dynamically loaded native code
- [PR #27652](https://github.com/prestodb/presto/pull/27652): connector-specific serialized values routed to native deserialization

Keep detailed packaging, ABI, handle serialization, and factory-map mechanics in the earlier PrestoDB blog post.

---

## 15. Metadata as Agent Context: SCHEMA, PATTERN, and SEMANTIC

Unifying future-work theme: the metadata CLP already materializes while compressing can become an active input to retrieval and to the context an agent holds before it queries. The article carries exactly one forward-looking direction (agent context), and it applies to any telemetry CLP stores, not only logs.

Intelligent-tiering boundary (background only, cut from the article):

- The planned "Better Storage Decisions from Existing Metadata" subsection, covering intelligent tiering and similarity-aware archive grouping, was cut entirely for length. It is not article content.
- CLP is not intended to replace a fully indexed system optimized to serve hundreds of queries per second.
- A future lifecycle can shorten indexed retention while keeping colder data queryable in CLP.
- Access frequency, latency demand, and policy can trigger selective rehydration into a hot index.
- Keep this available for questions and follow-up posts. Do not reintroduce it into the draft.

### Bounded public direction

Shipped article content, in the section "What Comes Next: Beyond Guesswork for Agents":

- The section opens with the signed-off statistic that agents already issue more than half of CLP queries at Uber.
- Exactly three functions are proposed, in conditional tense: `SCHEMA()`, `PATTERN()`, and `SEMANTIC()`. No `SUMMARY()` operator exists.
- The worked example generalizes to a trace, and explicitly not to a metric, because metrics carry no free text to pattern-match.
- The closing paragraph extends the idea past logs to any telemetry, naming the wide event (logs, traces, and metrics fused into one record), grounded in CLP materializing a schema as metadata during compression.

Safe article wording:

> **Future direction:** Agents already issue more than half of CLP queries at Uber. While compressing, CLP extracts the structure of each event, the recurring formats behind similar messages, and the variables pulled out of them. None of it is a separate index, so it costs nothing extra to keep and cannot drift out of sync. Three proposed SQL functions could expose it: the tree of paths and types in a filtered slice, the recurring formats behind matching events, and a way to select those formats by meaning rather than exact wording.

The interface direction covers both sides of a query:

- **Current limitation:** Agents often guess search terms and may receive no matches or an unmanageably large raw result set.
- **Input:** Natural-language or semantic intent could be translated into typed SQL, metadata predicates, and storage-aware operations.
- **Intent search:** If an agent knows the concept it wants but not the exact wording or path, semantic filtering can retrieve relevant events.
- **Output:** Recurring formats with their counts, the path-and-type tree of a filtered slice, and representative events could be returned with links to exact source evidence.
- **Query discovery:** Enumerating formats and their counts could give an agent compact context for choosing its next query. No summarization operator is proposed. The reduction comes from reading a few formats instead of every matching event.
- Claude Code's lightweight session registry is under `~/.claude/sessions/*.json`. Registry keys include `name`, `status`, `cwd`, `kind`, `startedAt`, and `version`.
- Full transcripts are under `~/.claude/projects/<encoded-project-path>/<session-id>.jsonl`. Subagent transcripts may appear under `<session-id>/subagents/*.jsonl`.
- Transcript records use top-level `type` values such as `user`, `assistant`, and `system`. Text lives under `message.content`, which may be a string or an array of typed blocks. Observed block types include `text`, `thinking`, `tool_use`, and `tool_result`. Tool-result blocks carry `content`, `is_error`, and `tool_use_id`.
- **Illustrative syntax only:** the shipped worked example starts with `SCHEMA('message')` to locate the text at `message.content[*].content`, then applies `SEMANTIC(...)` with grouped `PATTERN(...)` and `COUNT(*)` over that path to find and count recurring connector-load failures. It uses no top-level `type` filter. Keep values fictional.
- **Native reduction rationale:** CLP-S already stores deduplicated logtypes, variable dictionaries, frequencies, MST path mappings, and ERT columns. A future `PATTERN()` can enumerate that metadata without reconstructing every event. A future `SEMANTIC()` can compare intent with a computed embedding per distinct format rather than one per message, so its cost tracks the number of formats. A future `SCHEMA()` can return the path-and-type tree for a filtered slice straight from the MST, which is how the draft's worked example locates its text field before grouping by format.
- Semantic scoring, embeddings or indexes, and synthesis remain future design work. Do not present them as current or benchmarked CLP-S capabilities.
- The current SQL connector supplies the typed semantic and programmatic boundary on which this future interface could build.

The mechanism is plausible because CLP retains:

- Archive-local schema structure
- Log-template and log-format-pattern dictionaries
- Typed variables
- Pattern frequencies
- Time bounds and statistics

Potential future flow:

1. Summarize archive schemas, log-format patterns, frequencies, and time/statistical context.
2. Preload that compact representation into an LLM instead of beginning with repeated raw events.
3. Narrow candidate schemas, log templates, or encoded regions using archive metadata.
4. Apply semantic selection to the reduced candidate set.
5. Map relevant patterns and structures back to source events when detailed evidence is required.

Do not publish claims that this is already deployed, GPU-free, “for pennies,” or exceptionally inexpensive until workload, denominator, latency, resources, and comparison baseline are defined.

The conclusion deliberately ends on the forward-looking note: the same metadata that avoids query work today could also answer an agent's questions without requiring it to read the underlying events. Keep that ending.

---

## 16. Editorial Guardrails

### Terminology

- Use **CLP** consistently for the newer-generation system.
- Use **CLP-S** only when a historical source/name requires it.
- Use **Presto Native** for the product and **Presto Native worker** for compute.
- Use **semi-structured** in the article's prose and headings. The shipped draft never says "schema-less", describing CLP instead as requiring neither a global schema nor a predefined list of important fields.
- Use **complete event**, not “entire row,” for reconstructed JSON.
- Use **archive/split-local schema tree**, not dataset-global schema.

### Capability boundaries

- Current wildcard functions search values across unknown fields.
- Key discovery and general wildcard projection are future work.
- Production-only function syntax must be labeled and verified.
- Early termination is Uber production behavior, not current OSS.
- Mixed KV-IR/archive queries are current OSS.
- Offboarding, rehydration, and dual ingestion are future Uber projects. Mixed KV-IR/archive overlap precedence is current Uber production behavior.
- In the Part III narrative, call the production backend **Uber's metastore** while keeping its implementation behind a generic connector boundary.

### Claim strength

- Do not imply five workers scan 25 PB for every query.
- Do not claim perfect byte isolation.
- Do not claim universal immunity to ingestion loss.
- Do not claim all bursts compress unusually well.
- Do not call the object-storage path unlimited.
- Do not claim natural-language querying, semantic search or output, similarity-aware consolidation, offboarding, rehydration, or dual ingestion is deployed or benchmarked.
- Do not claim LLM context preloading or metadata-driven summarization is deployed or benchmarked.
- Do not call the public connector a preview.
- Do not call unshredded Variant opaque JSON.

### Article scope

- Keep the Uber article production-led and concise.
- Link **[PRESTO_BLOG_URL](PRESTO_BLOG_URL)** for the detailed Variant/shredding and connector discussion.
- Remove the placeholder before publication.
- Keep only one architecture figure (Figure 1, the KV-IR to archive storage flow) and two SQL examples: the main access-forms query and the agent worked example in the callout. The draft carries no work-elimination figure. The 200,000x aggregate work reduction is stated in prose with a footnote.

---

## 17. Publication Checklist

- Replace every `PRESTO_BLOG_URL` placeholder with the published earlier PrestoDB blog-post URL.
- Metrics are signed off and published in the draft. No further approval is needed for: hundreds of weekly active users (WAU), thousands of successful queries per day, roughly two-second mean response time, 99% within 26 seconds, a few engineers maintaining the system, more than half of queries from agents, more than 200,000 times aggregate work reduction, 750–850 TB at about 97% less physical storage, and 5–10% typical worker CPU and memory.
- Confirm 30-second-to-few-minute freshness remains current.
- The users/queries time-series figure and the timestamp-only versus top-level-metadata pruning comparison were both dropped from the draft. Neither needs confirmation.
- The draft attributes query traffic to agents rather than to an MCP server, so no MCP role, denominator, or measurement window needs defining.
- Keep agent-issued SQL queries labeled as current production behavior. Label the proposed `SCHEMA()`, `PATTERN()`, and `SEMANTIC()` functions, variable naming, offboarding, selective rehydration, and dual ingestion as future directions in conditional tense.
- Confirm publishable wording for rare hot/indexed-path loss and CLP-backed peak resilience.
- Confirm publishable wording for object-storage throughput headroom.
- Keep offboarding, selective rehydration, and dual-ingestion wording explicitly scoped to future projects.
- Verify the exact public function/path syntax for the release used in the example.
- Verify the wildcard pattern used in the SQL example is supported by pushdown.
- Confirm QueryRunner responsibility wording.
- Confirm contributor names and organization attribution.
- Decide whether the double-digit-petabyte partner statement is approved. Omit it otherwise.
- Do not restore the incomplete performance table.
- Add a representative query only if every metric comes from one consistent definition.
- Validate Markdown, terminology, section budgets, and `git diff --check`.
- Do not commit unless explicitly requested.

---

## 18. Reference Index

### Uber series

- [Part I](https://www.uber.com/en-GB/blog/reducing-logging-cost-by-two-orders-of-magnitude-using-clp/)
- [Part II](https://www.uber.com/en-GB/blog/modernizing-logging-with-clp-ii/)

### CLP and research

- [CLP repository](https://github.com/y-scope/clp)
- [CLP Presto connector](https://github.com/y-scope/clp-plugin-presto-connector)
- [KV-IR design](https://docs.yscope.com/clp/main/dev-docs/design-kv-ir-streams/index.html)
- [KV-IR background](https://docs.yscope.com/clp/main/dev-docs/design-kv-ir-streams/background)
- [Using Presto with CLP](https://docs.yscope.com/clp/main/user-docs/guides-using-presto.html)
- [CLP connector documentation](https://docs.yscope.com/presto/connector/clp.html)
- [YScope optimizer walkthrough](https://blog.yscope.com/querying-clp-compressed-logs-with-prestosql-71dff29a0b82)
- [μSlope, OSDI '24](https://www.usenix.org/conference/osdi24/presentation/wang-rui)
- [Original CLP paper, OSDI '21](https://www.usenix.org/conference/osdi21/presentation/rodrigues)

### Presto infrastructure

- [Presto concepts](https://prestodb.io/docs/current/overview/concepts.html)
- [Presto Native / Presto C++](https://prestodb.io/docs/current/presto-cpp.html)
- [PR #27653](https://github.com/prestodb/presto/pull/27653)
- [PR #27652](https://github.com/prestodb/presto/pull/27652)

### Uber query ecosystem

- [DataCentral](https://www.uber.com/en-CA/blog/datacentral-ubers-observability-and-chargeback-platform/)
- [Presto Express](https://www.uber.com/blog/presto-express/)
- [Preon](https://www.uber.com/en-DE/blog/preon/)
- [Engineering SQL Support on Apache Pinot at Uber](https://www.uber.com/en-GB/blog/engineering-sql-support-on-apache-pinot/)
- [Serving Millions of Apache Pinot Queries with Neutrino](https://www.uber.com/en-US/blog/serving-millions-of-apache-pinot-queries-with-neutrino/)

---

## 19. Working Abstract

Uber and YScope expanded CLP from its original Spark deployment to machine-learning/AI, streaming, and data platforms that collectively generate approximately 25 PB of raw logs per month. A collector on every host monitors and enriches container logs and incrementally compresses them into an appendable KV-IR file. Periodic publications overwrite the active object path with a longer snapshot. Size- or time-based rotation closes that file and begins another at a new path. Kafka metadata updates make each published extent discoverable. Fresh logs remain searchable in KV-IR before asynchronous aggregation transforms them into denser immutable CLP archives, and one open-source connector query can span both formats without duplicate events. Within each archive, CLP groups recurring key-and-type schemas into encoded tables and columnizes every schema node. The Presto Native connector rewrites supported SQL expressions, resolves paths against split-local structure, and pushes filtering and projection into CLP's encoded execution. Metadata pruning and Uber's latest-N file selection avoid files and splits before object-storage reads begin. In production, CLP reduces the monthly raw volume to approximately 750–850 TB, while five lightly utilized Presto Native workers serve hundreds of weekly active users and thousands of successful queries per day, reading recent and older data directly from Uber's internal object storage without local staging or an archive cache. The same metadata that avoids that query work is the article's one forward-looking direction: compact structural context for the agents that already issue more than half of CLP queries, and for whatever telemetry beyond logs CLP comes to store.
